import time
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlparse

import astrocut
from astrocut.exceptions import InvalidQueryError
from astropy.io.fits.hdu.hdulist import HDUList
from celery import Task, chord
from fsspec import AbstractFileSystem, filesystem
from vo_models.uws.models import ExecutionPhase

from fornax_cutouts.app.celery_app import celery_app, logger, redis_client_factory
from fornax_cutouts.config import CONFIG
from fornax_cutouts.jobs.redis import SyncRedisCutoutJob
from fornax_cutouts.jobs.results import CutoutResults
from fornax_cutouts.models.base import TargetPosition
from fornax_cutouts.models.cutouts import ColorFilter, CutoutResponse
from fornax_cutouts.sources import cutout_registry
from fornax_cutouts.utils.santa_resolver import resolve_positions


@celery_app.task(bind=True, ignore_result=True)
def schedule_job(
    self: Task,
    job_id: str,
    position: list[str],
    size: int,
    mission_params: dict[str, dict],
    output_format: list[str],
    mode: str = "fits_cut",
):
    r = SyncRedisCutoutJob(redis_client=redis_client_factory(), job_id=job_id)

    r.update_job_phase(ExecutionPhase.QUEUED)

    resolved_position = resolve_positions(position)

    validated_params = cutout_registry.validate_mission_params(mission_params=mission_params, size=size)

    valid_mission_params: dict[str, dict] = {}
    for mission, is_valid in validated_params.items():
        if not is_valid:
            logger.error(f"{mission!r} params are not valid, please recheck and resubmit.")
        else:
            valid_mission_params[mission] = mission_params[mission]

    target_fnames = cutout_registry.get_target_filenames(
        position=resolved_position,
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
                "ttl": CONFIG.async_ttl,
                "mission": target_fname.mission,
                "metadata": filename_obj.metadata,
                "mode": mode,
            }
            descriptors.append(descriptor)

    total_jobs = len(descriptors)

    if total_jobs == 0:
        r.set_start_time()
        r.set_end_time()
        r.update_job_phase(ExecutionPhase.COMPLETED)
        return

    r.push_pending_tasks(descriptors)
    r.set_total_task_count(total_jobs)
    r.update_job_phase(ExecutionPhase.EXECUTING)
    r.set_start_time()

    batch_cutouts_task = batch_cutouts.s(job_id=job_id)
    batch_cutouts_task.set(task_id=f"batch_cutouts-{job_id}-0")
    batch_cutouts_task.delay()


@celery_app.task(bind=True, ignore_result=True)
def batch_cutouts(self: Task, job_id: str):
    """
    Chunked batcher: pops descriptors from Redis in batches and creates generate_cutout tasks
    using a Celery chord. The chord callback (write_results) handles completion and looping back.
    """
    r = SyncRedisCutoutJob(redis_client=redis_client_factory(), job_id=job_id)

    batch_num = r.increment_batch_num()

    descriptors = r.pop_pending_tasks(CONFIG.worker.batch_size)
    if not descriptors:
        return
    r.increment_queued_task_count(len(descriptors))

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
            mode=desc.get("mode"),
        )
        sig.set(task_id=f"generate_cutout-{job_id}-{batch_num}-{increment_id}")
        cutout_sigs.append(sig)

    # Use chord to run all cutout tasks in parallel, then call write_results with their results
    write_results_sig = write_results.s(job_id=job_id)
    write_results_sig.set(task_id=f"write_results-{job_id}-{batch_num}")
    chord(cutout_sigs)(write_results_sig)


@celery_app.task(bind=True)
def write_results(self: Task, results: list[CutoutResponse | dict | None], job_id: str):
    """
    Chord callback: receives results from a batch of generate_cutout tasks and writes them to AsyncCutoutResults.
    Checks if job is complete; if not, loops back to batch_cutouts for the next batch.

    Args:
        results: List of CutoutResponse objects (or dicts/None for failed tasks) from the chord
        job_id: The job ID to write results for
    """
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

    # Write results to Parquet storage if we have any
    if cutout_results:
        results_writer = CutoutResults(job_id)
        batch_num = r.get_batch_num()
        results_writer.add_results(results=cutout_results, batch_num=batch_num)

    # Check if job is complete and update job phase
    job_status = r.get_job_result_status()
    completed_tasks = job_status["completed_jobs"]
    failed_tasks = job_status["failed_jobs"]
    pending_tasks = job_status["pending_jobs"]
    expected_total = job_status["total_jobs"]
    total_completed = completed_tasks + failed_tasks

    if total_completed >= expected_total and pending_tasks == 0:
        r.update_job_phase(ExecutionPhase.COMPLETED)
        r.set_end_time()

    elif pending_tasks > 0:
        next_batch = r.increment_batch_num()
        batch_cutouts_task = batch_cutouts.s(job_id=job_id)
        batch_cutouts_task.set(task_id=f"batch_cutouts-{job_id}-{next_batch}")
        batch_cutouts_task.delay()


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
    mode: str = "fits_cut",
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
    print(f"[generate_cutout] mode: {mode}")

    start_time = time.time()

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

    init_time = time.time()

    if mode == "in_memory":
        if "fits" in output_format:
            cutout_start_time = time.time()
            cutout = astrocut.fits_cut(
                source_file,
                f"{target[0]} {target[1]}",
                size,
                memory_only=True,
            )[0]

            cutout_time = time.time()

            with fs.open(f"{output_dir}/{cutout_prefix}.fits", "wb") as f:
                cutout.writeto(f)

            write_time = time.time()
    else:
        with TemporaryDirectory(prefix="fornax-cutouts-") as temp_output_dir:
            fits_fname = ""
            img_fname = ""

            if mode == "FITSCutout":
                cutout_start_time = time.time()
                cutout = astrocut.FITSCutout(
                    input_files=source_file,
                    coordinates=f"{target[0]} {target[1]}",
                    cutout_size=size,
                    single_outfile=False,
                )

                if "fits" in output_format:
                    fits_fname = cutout.write_as_fits(
                        output_dir=temp_output_dir,
                        cutout_prefix=cutout_prefix,
                    )[0]

                if "jpg" in output_format or "jpeg" in output_format:
                    img_fname = cutout.write_as_img(
                        output_dir=temp_output_dir,
                        cutout_prefix=cutout_prefix,
                    )[0]

                cutout_time = time.time()

            elif mode == "fits_cut":
                cutout_start_time = time.time()

                if "fits" in output_format:
                    fits_fname = astrocut.fits_cut(
                        source_file,
                        f"{target[0]} {target[1]}",
                        size,
                        output_dir=temp_output_dir,
                        cutout_prefix=cutout_prefix,
                        single_outfile=True,
                    )

                if "jpg" in output_format or "jpeg" in output_format:
                    img_fname = astrocut.img_cut(
                        source_file,
                        f"{target[0]} {target[1]}",
                        size,
                        output_dir=temp_output_dir,
                        cutout_prefix=cutout_prefix,
                        single_outfile=True,
                    )

                cutout_time = time.time()

            if fits_fname:
                fs.put(lpath=fits_fname, rpath=output_dir)
                fits_fname = fits_fname.replace(temp_output_dir, output_dir)

            if img_fname:
                fs.put(lpath=img_fname, rpath=output_dir)
                img_fname = img_fname.replace(temp_output_dir, output_dir)

            write_time = time.time()

    end_time = time.time()

    timings = {
        "init_time": init_time - start_time,
        f"{mode}_start_time": cutout_start_time - init_time,
        "cutout_time": cutout_time - cutout_start_time,
        "write_time": write_time - cutout_time,
        "total_time": end_time - start_time
    }
    logger.info(timings)

    return CutoutResponse(
        mission=mission,
        position=target,
        size_px=size,
        filter=metadata.pop("filter") or get_fits_filter(cutout.fits_cutouts[0]),
        fits=fits_fname,
        preview=img_fname,
        mission_extras=metadata,
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

    cutout = astrocut.FITSCutout(
        input_files=[red, green, blue],
        coordinates=f"{target[0]} {target[1]}",
        cutout_size=size,
        single_outfile=False,
    )

    with TemporaryDirectory(prefix="fornax-cutouts-") as temp_output_dir:
        img_fname = cutout.write_as_img(
            output_dir=temp_output_dir,
            cutout_prefix=cutout_prefix,
            colorize=True,
        )

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


@celery_app.task(bind=True, pydantic=True)
def execute_cutout(
    self: Task,
    job_id: str,
    source_file: str,
    target: TargetPosition,
    size: int | tuple[int, int],
    output_format: list[str],
    output_dir: str = "",
    mission: str = "",
    metadata: dict = {},
    mode: str = "fits_cut",
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
    start_time = time.time()
    r = SyncRedisCutoutJob(redis_client=redis_client_factory(), job_id=job_id)
    redis_factory_time = time.time()

    r.decrement_queued_task_count()
    r.increment_executing_task_count()
    status_update_time = time.time()
    try:
        resp = generate_cutout(
            source_file=source_file,
            target=target,
            size=size,
            output_format=output_format,
            output_dir=output_dir,
            mission=mission,
            metadata=metadata,
            mode=mode,
        )
        cutout_time = time.time()
    except InvalidQueryError as e:
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
                "mode": mode,
            },
            error_message=str(e),
        )
        logger.error(f"Failed to generate cutout for job {job_id}: {e}")
        return None

    r.decrement_executing_task_count()
    r.increment_completed_task_count()
    end_status_update_time = time.time()

    timings = {
        "redis_factory_time": redis_factory_time - start_time,
        "start_status_update_time": status_update_time - redis_factory_time,
        "cutout_time": cutout_time - status_update_time,
        "end_status_update_time": end_status_update_time - cutout_time,
        "total_time": end_status_update_time - start_time
    }
    logger.info(timings)
    return resp
