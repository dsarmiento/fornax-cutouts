import gc
import time
from collections import defaultdict
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlparse

import astrocut
from astropy.coordinates import SkyCoord
from astropy.io.fits.hdu.hdulist import HDUList
from celery import Task, chord
from fsspec import AbstractFileSystem, filesystem
from vo_models.uws.models import ExecutionPhase

from fornax_cutouts.app.celery_app import celery_app, get_pool_size_for_queue, logger, redis_client_factory
from fornax_cutouts.config import CONFIG
from fornax_cutouts.jobs.redis import SyncRedisCutoutJob
from fornax_cutouts.jobs.results import CutoutResults
from fornax_cutouts.models.base import TargetPosition
from fornax_cutouts.models.cutouts import ColorFilter, CutoutResponse
from fornax_cutouts.sources import cutout_registry
from fornax_cutouts.utils.santa_resolver import resolve_positions


@celery_app.task(
    bind=True,
    ignore_result=True,
    soft_time_limit=30 * 60,
    time_limit=35 * 60,
    queue="high_mem",
)
def schedule_job(
    self: Task,
    job_id: str,
):
    start_time = time.perf_counter()
    r = SyncRedisCutoutJob(redis_client=redis_client_factory(), job_id=job_id)

    job_parameters = r.get_job_parameters()
    size = job_parameters.pop("size")
    output_format = job_parameters.pop("output_format")

    source_names = cutout_registry.get_source_names()
    mission_params = {mission: params for mission, params in job_parameters.items() if mission in source_names}

    logger.debug(
        f"Job {job_id} received: missions={list(mission_params.keys())} size={size} format={output_format}",
        extra={
            "event": "job_parameters",
            "job_id": job_id,
            "missions": list(mission_params.keys()),
            "size": size,
            "output_format": output_format,
        },
    )

    r.update_job_phase(ExecutionPhase.QUEUED)
    redis_update_time = time.perf_counter()

    validated_params = cutout_registry.validate_mission_params(mission_params=mission_params, size=size)

    valid_mission_params: dict[str, dict] = {}
    for mission, is_valid in validated_params.items():
        if not is_valid:
            logger.error(
                f"Mission {mission!r} params are not valid",
                extra={"event": "mission_params_invalid", "job_id": job_id, "mission": mission},
            )
        else:
            valid_mission_params[mission] = mission_params[mission]
    validate_mission_params_time = time.perf_counter()

    total_jobs = 0
    mission_cutout_counts: defaultdict[str, int] = defaultdict(int)
    source_file_usage: defaultdict[str, int] = defaultdict(int)

    for positions in r.scan_job_positions():
        resolved_positions = resolve_positions(positions)

        target_fnames = cutout_registry.get_target_filenames(
            position=resolved_positions,
            mission_params=valid_mission_params,
            size=size,
            include_metadata=True,
        )

        descriptors = []
        for target_fname in target_fnames:
            for filename_obj in target_fname.filenames:
                descriptor = {
                    "job_id": job_id,
                    "source_file": filename_obj.filename,
                    "target": [target_fname.target.ra, target_fname.target.dec],  # Convert NamedTuple to list for JSON
                    "size": target_fname.size or size,
                    "output_format": output_format,
                    "output_dir": f"{CONFIG.storage.prefix}/cutouts/async/{job_id}/{target_fname.mission}",
                    "mission": target_fname.mission,
                    "metadata": filename_obj.metadata,
                }
                descriptors.append(descriptor)
                mission_cutout_counts[target_fname.mission] += 1
                source_file_usage[filename_obj.filename] += 1

        num_jobs = len(descriptors)

        if num_jobs == 0:
            r.set_start_time()
            r.set_end_time()
            r.update_job_phase(ExecutionPhase.COMPLETED)
            del target_fnames, descriptors, resolved_positions
            gc.collect()
            logger.info(
                f"Job {job_id} completed immediately: no matching source files found",
                extra={"event": "job_no_cutouts", "job_id": job_id, "missions": list(valid_mission_params.keys())},
            )
            return

        r.push_pending_tasks(descriptors)
        total_jobs += num_jobs
        del target_fnames, descriptors, resolved_positions

    push_pending_tasks_time = time.perf_counter()
    r.set_total_task_count(total_jobs)
    r.update_job_phase(ExecutionPhase.EXECUTING)
    r.set_start_time()
    metadata_update_time = time.perf_counter()

    batch_cutouts_task = batch_cutouts.s(job_id=job_id)
    batch_cutouts_task.set(task_id=f"batch_cutouts-{job_id}-0")
    batch_cutouts_task.delay()

    batch_cutouts_task_time = time.perf_counter()

    logger.info(
        f"Job {job_id} scheduled: {total_jobs} cutout(s) across {len(mission_cutout_counts)} mission(s)",
        extra={
            "event": "job_scheduled",
            "job_id": job_id,
            "total_cutouts": total_jobs,
            "cutouts_per_mission": mission_cutout_counts,
            "unique_source_files": len(source_file_usage),
            "source_file_usage": source_file_usage,
            "timings_s": {
                "redis_update": round(redis_update_time - start_time, 4),
                "validate_mission_params": round(validate_mission_params_time - redis_update_time, 4),
                "push_pending_tasks": round(push_pending_tasks_time - validate_mission_params_time, 4),
                "metadata_update": round(metadata_update_time - push_pending_tasks_time, 4),
                "dispatch_batch": round(batch_cutouts_task_time - metadata_update_time, 4),
                "total": round(batch_cutouts_task_time - start_time, 4),
            },
        },
    )


@celery_app.task(
    bind=True,
    ignore_result=True,
    soft_time_limit=30 * 60,
    time_limit=35 * 60,
    queue="high_mem",
)
def batch_cutouts(self: Task, job_id: str):
    """
    Chunked batcher: pops descriptors from Redis in batches and creates generate_cutout tasks
    using a Celery chord. The chord callback (write_results) handles completion and looping back.
    """
    start_time = time.perf_counter()
    r = SyncRedisCutoutJob(redis_client=redis_client_factory(), job_id=job_id)

    batch_num = r.increment_batch_num()
    batch_num_time = time.perf_counter()

    pool_size = get_pool_size_for_queue("cutouts")
    batch_size = pool_size * CONFIG.worker.batch_size_per_worker

    descriptors = r.pop_pending_tasks(batch_size)
    pop_pending_tasks_time = time.perf_counter()

    if not descriptors:
        return
    r.increment_queued_task_count(len(descriptors))
    increment_queued_task_count_time = time.perf_counter()

    # Build signatures for all cutout tasks in this batch
    cutout_sigs = []
    for increment_id, desc in enumerate(descriptors):
        # Convert target list back to TargetPosition NamedTuple
        target = TargetPosition(ra=desc["target"][0], dec=desc["target"][1])
        sig = execute_cutout.si(
            job_id=desc["job_id"],
            source_file=desc["source_file"],
            target=target,
            size=desc["size"],
            output_format=desc["output_format"],
            output_dir=desc["output_dir"],
            mission=desc["mission"],
            metadata=desc.get("metadata"),
        )
        sig.set(task_id=f"generate_cutout-{job_id}-{batch_num}-{increment_id}")
        cutout_sigs.append(sig)

    build_cutout_sigs_time = time.perf_counter()

    # Use chord to run all cutout tasks in parallel, then call write_results with their results
    write_results_sig = write_results.s(job_id=job_id)
    write_results_sig.set(task_id=f"write_results-{job_id}-{batch_num}")
    chord(cutout_sigs)(write_results_sig)

    chord_time = time.perf_counter()

    logger.info(
        f"Job {job_id} batch {batch_num}: dispatched {len(descriptors)} cutout(s)",
        extra={
            "event": "batch_dispatched",
            "job_id": job_id,
            "batch_num": batch_num,
            "pool_size": pool_size,
            "batch_size": batch_size,
            "num_cutouts": len(descriptors),
            "timings_s": {
                "batch_num": round(batch_num_time - start_time, 4),
                "pop_pending_tasks": round(pop_pending_tasks_time - batch_num_time, 4),
                "increment_queued_count": round(increment_queued_task_count_time - pop_pending_tasks_time, 4),
                "build_cutout_sigs": round(build_cutout_sigs_time - increment_queued_task_count_time, 4),
                "chord_dispatch": round(chord_time - build_cutout_sigs_time, 4),
                "total": round(chord_time - start_time, 4),
            },
        },
    )


@celery_app.task(
    bind=True,
    queue="high_mem",
)
def write_results(self: Task, results: list[CutoutResponse | dict | None], job_id: str):
    """
    Chord callback: receives results from a batch of generate_cutout tasks and writes them to AsyncCutoutResults.
    Checks if job is complete; if not, loops back to batch_cutouts for the next batch.

    Args:
        results: List of CutoutResponse objects (or dicts/None for failed tasks) from the chord
        job_id: The job ID to write results for
    """
    start_time = time.perf_counter()
    r = SyncRedisCutoutJob(redis_client=redis_client_factory(), job_id=job_id)

    # Filter out None/failed results and validate
    cutout_results = []
    for rp in results:
        if rp is None:
            continue
        elif isinstance(rp, dict):
            cutout_results.append(CutoutResponse.model_validate(rp))
        elif isinstance(rp, CutoutResponse):
            cutout_results.append(rp)
        else:
            continue
    filter_results_time = time.perf_counter()

    # Write results to Parquet storage if we have any
    batch_num = None
    if cutout_results:
        results_writer = CutoutResults(job_id)
        batch_num = r.get_batch_num()
        results_writer.add_results(results=cutout_results, batch_num=batch_num)
    write_results_time = time.perf_counter()

    # Check if job is complete and update job phase
    job_status = r.get_job_result_status()
    completed_tasks = job_status["completed_jobs"]
    failed_tasks = job_status["failed_jobs"]
    pending_tasks = job_status["pending_jobs"]
    expected_total = job_status["total_jobs"]
    total_completed = completed_tasks + failed_tasks
    job_complete = total_completed >= expected_total and pending_tasks == 0

    if job_complete:
        r.update_job_phase(ExecutionPhase.COMPLETED)
        r.set_end_time()

    elif pending_tasks > 0:
        next_batch = r.increment_batch_num()
        batch_cutouts_task = batch_cutouts.s(job_id=job_id)
        batch_cutouts_task.set(task_id=f"batch_cutouts-{job_id}-{next_batch}")
        batch_cutouts_task.delay()

    update_job_time = time.perf_counter()

    cutouts_per_mission: dict[str, int] = {}
    for cr in cutout_results:
        cutouts_per_mission[cr.mission] = cutouts_per_mission.get(cr.mission, 0) + 1

    logger.info(
        f"Job {job_id} batch results: {len(cutout_results)} succeeded, {len(results) - len(cutout_results)} failed",
        extra={
            "event": "batch_results_written",
            "job_id": job_id,
            "batch_num": batch_num,
            "successful_cutouts": len(cutout_results),
            "failed_in_batch": len(results) - len(cutout_results),
            "cutouts_per_mission": cutouts_per_mission,
            "total_completed": total_completed,
            "total_failed": failed_tasks,
            "pending_tasks": pending_tasks,
            "expected_total": expected_total,
            "job_complete": job_complete,
            "timings_s": {
                "filter_results": round(filter_results_time - start_time, 4),
                "write_results": round(write_results_time - filter_results_time, 4),
                "update_job": round(update_job_time - write_results_time, 4),
                "total": round(update_job_time - start_time, 4),
            },
        },
    )


def get_fits_filter(fits_cutout: HDUList) -> str | None:
    filter = None
    try:
        cutout_header = fits_cutout["CUTOUT"].header
        filter = cutout_header["*FILTER*"][0]
    except KeyError:
        pass

    return filter


def generate_cutout(
    source_file: str,
    target: TargetPosition,
    size: int | tuple[int, int],
    output_format: list[str],
    output_dir: str,
    mission: str = "sync_cutout",
    metadata: dict = {},
    job_id: str = "",
) -> CutoutResponse:
    """
    Execute a cutout within the specific source file

    Args:
        source_file (str): Source file
        target (TargetPosition): Target to center the cutout around
        size (int | tuple[int, int]): Size of the cutout
        output_format (list[str]): Formats of the resulting files (fits, jp(e)g)
        output_dir (str): Destination directory.
        mission (str, optional): The mission name (e.g., "ps1").
            Defaults to "sync_cutout".
        metadata (dict, optional): Mission-specific metadata dictionary.
            Defaults to {}.
    """
    start_time = time.perf_counter()

    if isinstance(size, int):
        size = (size, size)

    cutout_prefix = urlparse(source_file).path
    cutout_prefix = Path(cutout_prefix).stem

    fs: AbstractFileSystem
    output_is_s3 = output_dir.startswith("s3://")
    if output_is_s3:
        fs = filesystem("s3")
    else:
        fs = filesystem("local")

    # Only create directories for local filesystem; S3 doesn't need them
    # and the isdir/mkdir calls are expensive LIST operations
    if not output_is_s3 and not fs.isdir(output_dir):
        fs.mkdir(output_dir)

    init_time = time.perf_counter()

    fits_fname = ""
    img_fname = ""
    fits_size_bytes = 0
    jpg_size_bytes = 0

    with TemporaryDirectory(prefix="fornax-cutouts-") as temp_output_dir:
        astrocut_init_start = time.perf_counter()
        cutout = astrocut.FITSCutout(
            input_files=source_file,
            coordinates=SkyCoord(ra=target[0], dec=target[1], unit="deg", frame="icrs"),
            cutout_size=size,
            single_outfile=False,
        )
        astrocut_init_time = time.perf_counter()

        if "fits" in output_format:
            fits_fname = cutout.write_as_fits(
                output_dir=temp_output_dir,
                cutout_prefix=cutout_prefix,
            )[0]
        fits_write_time = time.perf_counter()

        if "jpg" in output_format or "jpeg" in output_format:
            img_fname = cutout.write_as_img(
                output_dir=temp_output_dir,
                cutout_prefix=cutout_prefix,
            )[0]
        jpg_write_time = time.perf_counter()

        if fits_fname:
            fits_size_bytes = Path(fits_fname).stat().st_size
            fits_dest_fname = fits_fname.replace(temp_output_dir, output_dir)
            fs.put(lpath=fits_fname, rpath=fits_dest_fname)
            fits_fname = fits_dest_fname

        if img_fname:
            jpg_size_bytes = Path(img_fname).stat().st_size
            img_dest_fname = img_fname.replace(temp_output_dir, output_dir)
            fs.put(lpath=img_fname, rpath=img_dest_fname)
            img_fname = img_dest_fname

        upload_time = time.perf_counter()

    end_time = time.perf_counter()

    logger.info(
        f"Cutout generated: job {job_id} - mission='{mission}' source='{source_file}' size={size[0]}x{size[1]}px",
        extra={
            "event": "cutout_generated",
            "job_id": job_id,
            "mission": mission,
            "source_file": source_file,
            "target": target,
            "size_px": size,
            "fits_size_bytes": fits_size_bytes,
            "jpg_size_bytes": jpg_size_bytes,
            "timings_s": {
                "init": round(init_time - start_time, 4),
                "astrocut_init": round(astrocut_init_time - astrocut_init_start, 4),
                "fits_write": round(fits_write_time - astrocut_init_time, 4) if fits_fname else None,
                "jpg_write": round(jpg_write_time - fits_write_time, 4) if img_fname else None,
                "upload": round(upload_time - jpg_write_time, 4),
                "total": round(end_time - start_time, 4),
            },
        },
    )

    filter_val = metadata.get("filter") or get_fits_filter(cutout.fits_cutouts[0])
    mission_extras = {k: v for k, v in metadata.items() if k != "filter"}
    return CutoutResponse(
        mission=mission,
        position=target,
        size_px=size,
        filter=filter_val,
        fits=fits_fname,
        preview=img_fname,
        mission_extras=mission_extras,
    )


def generate_color_preview(
    red: str,
    green: str,
    blue: str,
    target: TargetPosition,
    size: int | tuple[int, int],
    output_dir: str,
) -> CutoutResponse:
    """
    Generate a color preview of a cutout
    """
    if isinstance(size, int):
        size = (size, size)

    cutout_prefix = urlparse(red).path
    cutout_prefix = Path(cutout_prefix).stem + "_color"

    start_time = time.perf_counter()
    cutout = astrocut.FITSCutout(
        input_files=[red, green, blue],
        coordinates=SkyCoord(ra=target.ra, dec=target.dec, unit="deg", frame="icrs"),
        cutout_size=size,
        single_outfile=False,
    )
    astrocut_time = time.perf_counter()

    with TemporaryDirectory(prefix="fornax-cutouts-") as temp_output_dir:
        img_fname = cutout.write_as_img(
            output_dir=temp_output_dir,
            cutout_prefix=cutout_prefix,
            colorize=True,
        )
        write_time = time.perf_counter()

        jpg_size_bytes = Path(img_fname).stat().st_size

        fs: AbstractFileSystem
        output_is_s3 = output_dir.startswith("s3://")
        if output_is_s3:
            fs = filesystem("s3")
        else:
            fs = filesystem("local")

        # Only create directories for local filesystem; S3 doesn't need them
        # and the isdir/mkdir calls are expensive LIST operations
        if not output_is_s3 and not fs.isdir(output_dir):
            fs.mkdir(output_dir)

        fs.put(lpath=img_fname, rpath=output_dir)

        img_fname = img_fname.replace(temp_output_dir, output_dir)
        upload_time = time.perf_counter()

    logger.info(
        f"Color preview generated: size={size[0]}x{size[1]}px",
        extra={
            "event": "color_preview_generated",
            "target": target,
            "size_px": size,
            "jpg_size_bytes": jpg_size_bytes,
            "source_files": {"red": red, "green": green, "blue": blue},
            "timings_s": {
                "astrocut_init": round(astrocut_time - start_time, 4),
                "jpg_write": round(write_time - astrocut_time, 4),
                "upload": round(upload_time - write_time, 4),
                "total": round(upload_time - start_time, 4),
            },
        },
    )

    return CutoutResponse(
        mission="color_preview",
        position=target,
        size_px=size,
        filter=ColorFilter(
            red=get_fits_filter(cutout.fits_cutouts[0]),
            green=get_fits_filter(cutout.fits_cutouts[1]),
            blue=get_fits_filter(cutout.fits_cutouts[2]),
        ),
        preview=img_fname,
    )


@celery_app.task(
    bind=True,
    pydantic=True,
    queue="cutouts",
)
def execute_cutout(
    self: Task,
    job_id: str,
    source_file: str,
    target: TargetPosition | list[float],
    size: int | tuple[int, int],
    output_format: list[str],
    output_dir: str = "",
    mission: str = "",
    metadata: dict = {},
) -> CutoutResponse:
    """
    Generate a cutout within the specific source file

    Args:
        job_id (str): The job ID to generate the cutout for
        source_file (str): Source file
        target (TargetPosition): Target to center the cutout around
        size (int | tuple[int, int]): Size of the cutout
        output_format (list[str]): Formats of the resulting files (fits, jp(e)g)
        output_dir (str, optional): Destination directory.
            Defaults to "".
        mission (str, optional): The mission name (e.g., "ps1").
            Defaults to "".
        metadata (dict, optional): Mission-specific metadata dictionary.
            Defaults to None.
    """
    if isinstance(target, list):
        target = TargetPosition(ra=target[0], dec=target[1])

    r = SyncRedisCutoutJob(redis_client=redis_client_factory(), job_id=job_id)
    r.decrement_queued_task_count()
    r.increment_executing_task_count()
    try:
        resp = generate_cutout(
            job_id=job_id,
            source_file=source_file,
            target=target,
            size=size,
            output_format=output_format,
            output_dir=output_dir,
            mission=mission,
            metadata=metadata,
        )
    except Exception as e:
        r.decrement_executing_task_count()
        r.push_failed_task(
            task_kwargs={
                "job_id": job_id,
                "source_file": source_file,
                "target": target,
                "size": size,
                "output_format": output_format,
                "output_dir": output_dir,
                "mission": mission,
                "metadata": metadata,
            },
            error_message=str(e),
        )
        logger.error(
            f"Cutout failed for job {job_id}: {e!r}",
            extra={
                "event": "cutout_failed",
                "job_id": job_id,
                "mission": mission,
                "source_file": source_file,
                "target": target,
                "size": size,
                "error": e.__repr__(),
                "error_type": type(e).__name__,
            },
            exc_info=True,
        )
        return None

    r.decrement_executing_task_count()
    r.increment_completed_task_count()

    return resp
