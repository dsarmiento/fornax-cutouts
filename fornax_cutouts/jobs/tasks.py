import gc
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlparse

import astrocut
from astropy.coordinates import SkyCoord
from astropy.io.fits.hdu.hdulist import HDUList
from celery import Task
from fsspec import AbstractFileSystem, filesystem
from vo_models.uws.models import ExecutionPhase
from vo_models.uws.types import ErrorType

from fornax_cutouts.app.celery_app import celery_app, get_pool_size_for_queue, logger, redis_client_factory
from fornax_cutouts.auth.limits import CutoutLimiter
from fornax_cutouts.config import CONFIG
from fornax_cutouts.jobs.redis import SyncRedisCutoutJob
from fornax_cutouts.jobs.results import CutoutResults
from fornax_cutouts.models.base import TargetPosition
from fornax_cutouts.models.cutouts import ColorFilter, CutoutResponse
from fornax_cutouts.sources import cutout_registry
from fornax_cutouts.utils.exceptions import CutoutLimitExceededError
from fornax_cutouts.utils.santa_resolver import resolve_positions

STRETCH = "asinh"  # "sinh"
MINMAX_PERCENT: list[float] = [0.5, 99.5]

SCHEDULE_JOB_TASK_ID_TEMPLATE = "schedule_job-{job_id}"
BATCH_CUTOUTS_TASK_ID_TEMPLATE = "batch_cutouts-{job_id}-{batch_num}"
WRITE_RESULTS_TASK_ID_TEMPLATE = "write_results-{job_id}-{batch_num}"
BATCH_WATCHDOG_TASK_ID_TEMPLATE = "batch_watchdog-{job_id}-{batch_num}"
EXECUTE_CUTOUT_TASK_ID_TEMPLATE = "execute_cutout-{job_id}-{batch_num}-{increment_id}"


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
    limiter = CutoutLimiter(redis_client_factory())
    cutout_limit_identity, cutout_limit_max, cutout_limit_window_seconds = r.get_cutout_limit_budget()
    logger.debug(f"Cutout limit identity: {cutout_limit_identity} limit: {cutout_limit_max}")

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
            logger.warning(
                f"Mission {mission!r} params are not valid",
                extra={"event": "mission_params_invalid", "job_id": job_id, "mission": mission},
            )
        else:
            valid_mission_params[mission] = mission_params[mission]
    validate_mission_params_time = time.perf_counter()

    total_jobs = 0
    mission_cutout_counts: defaultdict[str, int] = defaultdict(int)

    for positions in r.scan_job_positions():
        resolved_positions = resolve_positions(positions)

        target_fnames = cutout_registry.get_target_filenames(
            position=resolved_positions,
            mission_params=valid_mission_params,
            size=size,
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

        num_jobs = len(descriptors)

        if num_jobs == 0:
            limiter.reconcile(
                identity=cutout_limit_identity,
                job_id=job_id,
                actual=0,
                cutout_limit=cutout_limit_max,
                window_seconds=cutout_limit_window_seconds,
            )
            r.start_job()
            r.complete_job()
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
    r.increment_total_pending_tasks(total_jobs)
    try:
        limiter.reconcile(
            identity=cutout_limit_identity,
            job_id=job_id,
            actual=total_jobs,
            cutout_limit=cutout_limit_max,
            window_seconds=cutout_limit_window_seconds,
        )
    except CutoutLimitExceededError as exc:
        r.fail_job(str(exc), ErrorType.TRANSIENT)
        logger.warning(
            f"Job {job_id} failed on reconcile: cutout count {total_jobs} exceeds identity's cutout limit",
            extra={
                "event": "job_failed_cutout_limit_reconcile",
                "job_id": job_id,
                "identity": cutout_limit_identity,
                "actual": total_jobs,
                "limit": cutout_limit_max,
            },
        )
        return

    metadata_update_time = time.perf_counter()

    batch_num = r.increment_batch_num()
    batch_cutouts.apply_async(
        kwargs={"job_id": job_id, "batch_num": batch_num},
        task_id=BATCH_CUTOUTS_TASK_ID_TEMPLATE.format(job_id=job_id, batch_num=batch_num),
    )

    batch_cutouts_task_time = time.perf_counter()

    logger.info(
        f"Job {job_id} scheduled: {total_jobs} cutout(s) across {len(mission_cutout_counts)} mission(s)",
        extra={
            "event": "job_scheduled",
            "job_id": job_id,
            "total_cutouts": total_jobs,
            "cutouts_per_mission": mission_cutout_counts,
            "total_s": round(batch_cutouts_task_time - start_time, 4),
        },
    )
    logger.debug(
        f"Job {job_id} scheduled timings",
        extra={
            "event": "job_scheduled_timings",
            "job_id": job_id,
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
def batch_cutouts(self: Task, job_id: str, batch_num: int):
    """
    Chunked batcher: pops descriptors from Redis in batches, dispatches execute_cutout tasks,
    and relies on the last task in the batch to enqueue write_results after Redis-tracked completion.
    """
    start_time = time.perf_counter()
    r = SyncRedisCutoutJob(redis_client=redis_client_factory(), job_id=job_id)

    pool_size = get_pool_size_for_queue("cutouts")
    batch_size = pool_size * CONFIG.worker.batch_size_per_worker

    descriptors = r.pop_pending_tasks(batch_size)
    pop_pending_tasks_time = time.perf_counter()

    if not descriptors:
        return

    r.decrement_total_pending_tasks(len(descriptors))
    r.increment_queued_task_count(len(descriptors))
    increment_queued_task_count_time = time.perf_counter()

    r.delete_batch_keys(batch_num)
    r.set_batch_descriptors(batch_num, descriptors)
    r.set_batch_outstanding(batch_num, len(descriptors))

    eta = datetime.now(tz=timezone.utc) + timedelta(minutes=CONFIG.worker.batch_watchdog_timeout_minutes)
    batch_watchdog.apply_async(
        kwargs={
            "job_id": job_id,
            "batch_num": batch_num,
            "expected_count": len(descriptors),
        },
        eta=eta,
        task_id=BATCH_WATCHDOG_TASK_ID_TEMPLATE.format(job_id=job_id, batch_num=batch_num),
    )

    for increment_id, desc in enumerate(descriptors):
        target = TargetPosition(ra=desc["target"][0], dec=desc["target"][1])
        execute_cutout.apply_async(
            kwargs={
                "job_id": desc["job_id"],
                "source_file": desc["source_file"],
                "target": target,
                "size": desc["size"],
                "output_format": desc["output_format"],
                "output_dir": desc["output_dir"],
                "mission": desc["mission"],
                "metadata": desc.get("metadata"),
                "batch_num": batch_num,
                "increment_id": increment_id,
            },
            task_id=EXECUTE_CUTOUT_TASK_ID_TEMPLATE.format(
                job_id=job_id, batch_num=batch_num, increment_id=increment_id
            ),
            priority=2,
        )

    dispatch_time = time.perf_counter()

    logger.info(
        f"Job {job_id} batch {batch_num}: dispatched {len(descriptors)} cutout(s)",
        extra={
            "event": "batch_dispatched",
            "job_id": job_id,
            "batch_num": batch_num,
            "pool_size": pool_size,
            "batch_size": batch_size,
            "num_cutouts": len(descriptors),
            "total_s": round(dispatch_time - start_time, 4),
        },
    )
    logger.debug(
        f"Job {job_id} batch {batch_num} timings",
        extra={
            "event": "batch_dispatched_timings",
            "job_id": job_id,
            "batch_num": batch_num,
            "timings_s": {
                "pop_pending_tasks": round(pop_pending_tasks_time - start_time, 4),
                "increment_queued_count": round(increment_queued_task_count_time - pop_pending_tasks_time, 4),
                "dispatch_cutouts": round(dispatch_time - increment_queued_task_count_time, 4),
                "total": round(dispatch_time - start_time, 4),
            },
        },
    )


@celery_app.task(
    bind=True,
    ignore_result=True,
    queue="high_mem",
)
def batch_watchdog(self: Task, job_id: str, batch_num: int, expected_count: int):
    """Recover a stalled batch by requeueing stranded descriptors."""
    r = SyncRedisCutoutJob(redis_client=redis_client_factory(), job_id=job_id)
    outstanding = r.get_batch_outstanding(batch_num)
    if outstanding <= 0:
        return

    logger.warning(
        f"Job {job_id} watchdog {batch_num}: recovering stalled batch (outstanding={outstanding})",
        extra={
            "event": "batch_watchdog_recover",
            "job_id": job_id,
            "batch_num": batch_num,
            "outstanding": outstanding,
            "expected_count": expected_count,
        },
    )

    descriptors = r.get_batch_descriptors(batch_num)
    stranded: list[dict] = []
    for i in range(expected_count):
        if not r.batch_result_hexists(batch_num, i):
            stranded.append(descriptors[i])
            if r.batch_task_was_started(batch_num, i):
                r.decrement_executing_task_count()
            else:
                r.decrement_queued_task_count()

    if stranded:
        r.push_pending_tasks(stranded)
        r.increment_total_pending_tasks(len(stranded))

    r.reset_batch_outstanding(batch_num)
    write_results.run(job_id=job_id, batch_num=batch_num)


@celery_app.task(
    bind=True,
    queue="high_mem",
)
def write_results(self: Task, job_id: str, batch_num: int):
    """
    Batch result writer: collects completed cutout results and writes them to AsyncCutoutResults.
    Checks if the job is complete, and if not, schedules the next batch

    Args:
        job_id (str): The job ID to write results for
        batch_num (int): The number of the batch to write results for
    """
    start_time = time.perf_counter()
    r = SyncRedisCutoutJob(redis_client=redis_client_factory(), job_id=job_id)

    try:
        results = r.get_batch_results(batch_num)

        cutout_results = []
        for result in results:
            if result is None:
                continue
            elif isinstance(result, dict):
                cutout_results.append(CutoutResponse.model_validate(result))
            elif isinstance(result, CutoutResponse):
                cutout_results.append(result)
            else:
                continue
        filter_results_time = time.perf_counter()

        if cutout_results:
            results_writer = CutoutResults(job_id)
            results_writer.add_results(results=cutout_results, batch_num=batch_num)
        write_results_time = time.perf_counter()

        job_status = r.get_job_result_status()
        completed_tasks = job_status["completed_jobs"]
        failed_tasks = job_status["failed_jobs"]
        skipped_tasks = job_status["skipped_jobs"]
        pending_tasks = job_status["pending_jobs"]
        expected_total = job_status["total_jobs"]
        total_completed = completed_tasks + failed_tasks + skipped_tasks
        job_complete = total_completed == expected_total and pending_tasks == 0

        if job_complete:
            r.complete_job()

        elif pending_tasks > 0:
            next_batch = r.increment_batch_num()
            batch_cutouts.apply_async(
                kwargs={
                    "job_id": job_id,
                    "batch_num": next_batch,
                },
                task_id=BATCH_CUTOUTS_TASK_ID_TEMPLATE.format(job_id=job_id, batch_num=next_batch),
            )

        update_job_time = time.perf_counter()

        cutouts_per_mission: dict[str, int] = defaultdict(lambda: 0)
        for cr in cutout_results:
            cutouts_per_mission[cr.mission] += 1

        failed_in_batch = sum(1 for x in results if x is None)

        log_message = f"Job {job_id} write results {batch_num}: {len(cutout_results)} cutout(s) succeeded, {failed_in_batch} missing/null."
        if job_complete:
            log_message += " Job complete."
        else:
            log_message += f" {pending_tasks} pending task(s), queuing next batch {next_batch}."

        logger.info(
            log_message,
            extra={
                "event": "batch_results_written",
                "job_id": job_id,
                "batch_num": batch_num,
                "successful_cutouts": len(cutout_results),
                "failed_in_batch": failed_in_batch,
                "cutouts_per_mission": cutouts_per_mission,
                "total_completed": total_completed,
                "total_failed": failed_tasks,
                "pending_tasks": pending_tasks,
                "expected_total": expected_total,
                "job_complete": job_complete,
                "total_s": round(update_job_time - start_time, 4),
            },
        )

        logger.debug(
            f"Job {job_id} batch results timings",
            extra={
                "event": "batch_results_written_timings",
                "job_id": job_id,
                "batch_num": batch_num,
                "timings_s": {
                    "filter_results": round(filter_results_time - start_time, 4),
                    "write_results": round(write_results_time - filter_results_time, 4),
                    "update_job": round(update_job_time - write_results_time, 4),
                    "total": round(update_job_time - start_time, 4),
                },
            },
        )

    finally:
        r.delete_batch_keys(batch_num)


def get_fits_filter(fits_cutout: HDUList) -> str | None:
    filter = None
    try:
        cutout_header = fits_cutout["CUTOUT"].header
        filter = cutout_header["*FILTER*"][0]
    except KeyError:
        pass

    return filter


def generate_cutout(  # noqa: C901
    source_file: str,
    target: TargetPosition,
    size: tuple[int, int],
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
        size (tuple[int, int]): Size of the cutout
        output_format (list[str]): Formats of the resulting files (fits, jp(e)g)
        output_dir (str): Destination directory.
        mission (str, optional): The mission name (e.g., "ps1").
            Defaults to "sync_cutout".
        metadata (dict, optional): Mission-specific metadata dictionary.
            Defaults to {}.
    """
    start_time = time.perf_counter()

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
    try:
        if not output_is_s3 and not fs.isdir(output_dir):
            fs.mkdir(output_dir)
    except FileExistsError:
        logger.debug(f"Output directory already exists: {output_dir}")
    except Exception as e:
        logger.warning(f"Error creating output directory: {e}")
        raise e

    init_time = time.perf_counter()

    fits_fname = ""
    img_fname = ""
    science_bytes = 0
    preview_bytes = 0
    science_out_format = ""
    preview_out_format = ""

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
            science_out_format = "fits"
            fits_fname = cutout.write_as_fits(
                output_dir=temp_output_dir,
                cutout_prefix=cutout_prefix,
            )[0]
        fits_write_time = time.perf_counter()

        if "jpg" in output_format or "jpeg" in output_format:
            preview_out_format = "jpeg"
            img_fname = cutout.write_as_img(
                output_dir=temp_output_dir,
                cutout_prefix=cutout_prefix,
                stretch=STRETCH,
                minmax_percent=MINMAX_PERCENT,
            )[0]
        jpg_write_time = time.perf_counter()

        if fits_fname:
            science_bytes = Path(fits_fname).stat().st_size
            fits_dest_fname = fits_fname.replace(temp_output_dir, output_dir)
            fs.put(lpath=fits_fname, rpath=fits_dest_fname)
            fits_fname = fits_dest_fname

        if img_fname:
            preview_bytes = Path(img_fname).stat().st_size
            img_dest_fname = img_fname.replace(temp_output_dir, output_dir)
            fs.put(lpath=img_fname, rpath=img_dest_fname)
            img_fname = img_dest_fname

        upload_time = time.perf_counter()

    end_time = time.perf_counter()

    bytes = {}
    output_formats = {}
    timings_s = {
        "init": round(init_time - start_time, 4),
        "astrocut_init": round(astrocut_init_time - astrocut_init_start, 4),
        "upload": round(upload_time - jpg_write_time, 4),
        "total": round(end_time - start_time, 4),
    }

    if fits_fname:
        output_formats["science"] = science_out_format
        bytes["science"] = science_bytes
        timings_s["science_write"] = round(fits_write_time - astrocut_init_time, 4)

    if img_fname:
        output_formats["preview"] = preview_out_format
        bytes["preview"] = preview_bytes
        timings_s["preview_write"] = round(jpg_write_time - fits_write_time, 4)

    logger.info(
        f"Job {job_id} cutout generated: mission='{mission}' source='{source_file}' size={size[0]}x{size[1]}px",
        extra={
            "event": "cutout_generated",
            "job_id": job_id,
            "mission": mission,
            "source_file": source_file,
            "target": {
                "ra": target.ra,
                "dec": target.dec,
            },
            "size_px": {
                "x": size[0],
                "y": size[1],
                "area": size[0] * size[1],
            },
            "bytes": bytes,
            "total_s": timings_s["total"],
            "output_formats": output_formats,
        },
    )
    logger.debug(
        f"Job {job_id} cutout generated timings: mission='{mission}'",
        extra={
            "event": "cutout_generated_timings",
            "job_id": job_id,
            "mission": mission,
            "timings_s": timings_s,
        },
    )

    filter_val = metadata.get("filter") or get_fits_filter(cutout.fits_cutouts[0])
    mission_extras = {k: v for k, v in metadata.items() if k != "filter"}
    return CutoutResponse(
        mission=mission,
        position=target,
        size_px=size,
        filter=filter_val,
        science=fits_fname,
        preview=img_fname,
        mission_extras=mission_extras,
    )


def generate_color_preview(
    red: str,
    green: str,
    blue: str,
    target: TargetPosition,
    size: tuple[int, int],
    output_dir: str,
) -> CutoutResponse:
    """
    Generate a color preview of a cutout
    """
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
            stretch=STRETCH,
            minmax_percent=MINMAX_PERCENT,
        )
        write_time = time.perf_counter()

        preview_bytes = Path(img_fname).stat().st_size

        fs: AbstractFileSystem
        output_is_s3 = output_dir.startswith("s3://")
        if output_is_s3:
            fs = filesystem("s3")
        else:
            fs = filesystem("local")

        # Only create directories for local filesystem; S3 doesn't need them
        # and the isdir/mkdir calls are expensive LIST operations
        try:
            if not output_is_s3 and not fs.isdir(output_dir):
                fs.mkdir(output_dir)
        except FileExistsError:
            logger.debug(f"Output directory already exists: {output_dir}")
        except Exception as e:
            logger.warning(f"Error creating output directory: {e}")
            raise e

        fs.put(lpath=img_fname, rpath=output_dir)

        img_fname = img_fname.replace(temp_output_dir, output_dir)
        upload_time = time.perf_counter()

    logger.info(
        f"Color preview generated: size={size[0]}x{size[1]}px",
        extra={
            "event": "color_preview_generated",
            "target": {
                "ra": target.ra,
                "dec": target.dec,
            },
            "size_px": {
                "x": size[0],
                "y": size[1],
                "area": size[0] * size[1],
            },
            "bytes": {
                "preview": preview_bytes,
            },
            "source_files": {
                "red": red,
                "green": green,
                "blue": blue,
            },
            "total_s": round(upload_time - start_time, 4),
        },
    )
    logger.debug(
        f"Color preview generated timings: size={size[0]}x{size[1]}px",
        extra={
            "event": "color_preview_generated_timings",
            "timings_s": {
                "astrocut_init": round(astrocut_time - start_time, 4),
                "preview_write": round(write_time - astrocut_time, 4),
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
def execute_color_preview(
    self: Task,
    red: str,
    green: str,
    blue: str,
    target: TargetPosition | list[float],
    size: int | tuple[int, int],
    output_dir: str,
) -> CutoutResponse:
    if isinstance(target, list):
        target = TargetPosition(ra=target[0], dec=target[1])
    if isinstance(size, int):
        size = (size, size)
    return generate_color_preview(
        red=red,
        green=green,
        blue=blue,
        target=target,
        size=size,
        output_dir=output_dir,
    )


@celery_app.task(
    bind=True,
    pydantic=True,
    queue="cutouts",
)
def execute_cutout(  # noqa: C901
    self: Task,
    job_id: str,
    source_file: str,
    target: TargetPosition | list[float],
    size: int | tuple[int, int],
    output_format: list[str],
    output_dir: str = "",
    mission: str = "",
    metadata: dict = {},
    batch_num: int = 0,
    increment_id: int = 0,
) -> CutoutResponse | None:
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
        batch_num (int): Async batch identifier; 0 for non-batched (e.g. sync) tasks.
        increment_id (int): Index within the batch for Redis aggregation.
    """
    is_async = job_id != "sync"
    if isinstance(target, list):
        target = TargetPosition(ra=target[0], dec=target[1])
    if isinstance(size, int):
        size = (size, size)

    resp = None
    r: SyncRedisCutoutJob | None = None
    remaining = None

    if is_async:
        r = SyncRedisCutoutJob(redis_client=redis_client_factory(), job_id=job_id)
        r.start_task(batch_num, increment_id)

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

    except astrocut.exceptions.InvalidQueryError:
        if is_async:
            remaining = r.skip_task(batch_num, increment_id)

        logger.info(
            f"Job {job_id} cutout skipped (cutout has no data): mission='{mission}' source='{source_file}'",
            extra={
                "event": "cutout_skipped",
                "job_id": job_id,
                "mission": mission,
                "source_file": source_file,
                "target": {
                    "ra": target.ra,
                    "dec": target.dec,
                },
                "size_px": {
                    "x": size[0],
                    "y": size[1],
                    "area": size[0] * size[1],
                },
            },
        )

    except Exception as e:
        if not is_async:
            remaining = r.fail_task(
                batch_num=batch_num,
                increment_id=increment_id,
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

        logger.warning(
            f"Job {job_id} cutout failed: {e!r}",
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

    else:
        if is_async:
            result_payload = resp.model_dump_json() if resp is not None else None
            remaining = r.complete_task(batch_num, increment_id, result_payload)

    finally:
        if is_async and remaining is not None and remaining <= 0:
            celery_app.control.revoke(
                BATCH_WATCHDOG_TASK_ID_TEMPLATE.format(job_id=job_id, batch_num=batch_num),
                terminate=False,
            )
            write_results.apply_async(
                kwargs={"job_id": job_id, "batch_num": batch_num},
                task_id=WRITE_RESULTS_TASK_ID_TEMPLATE.format(job_id=job_id, batch_num=batch_num),
            )

    return resp
