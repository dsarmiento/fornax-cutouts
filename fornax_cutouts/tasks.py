import asyncio
import gc
from pathlib import Path
from urllib.parse import urlparse

import astrocut
from astropy.io.fits.hdu.hdulist import HDUList
from celery import Task, chain, group
from fsspec import AbstractFileSystem, filesystem
from vo_models.uws.models import ExecutionPhase

from fornax_cutouts.app.celery_app import celery_app, logger
from fornax_cutouts.config import CONFIG
from fornax_cutouts.constants import CUTOUT_STORAGE_IS_S3, CUTOUT_STORAGE_PREFIX
from fornax_cutouts.models.base import TargetPosition
from fornax_cutouts.models.cutouts import ColorFilter, CutoutResponse
from fornax_cutouts.sources import cutout_registry
from fornax_cutouts.utils.redis_uws import redis_uws_client
from fornax_cutouts.utils.santa_resolver import resolve_positions


@celery_app.task(bind=True, ignore_result=True)
def schedule_job(
    self: Task,
    job_id: str,
    position: list[str],
    size: int,
    mission_params: dict[str, dict],
    output_format: list[str],
):
    async def task():
        r = redis_uws_client()

        await r.update_job_phase(job_id, ExecutionPhase.QUEUED)

        resolved_position = resolve_positions(position)

        validated_params = cutout_registry.validate_mission_params(mission_params=mission_params, size=size)

        for mission, is_valid in validated_params.items():
            if not is_valid:
                # Specific mission isn't valid, let user know
                # TODO: Should we allow the user re-submit the mission params for re-scheduling?
                logger.error(f"{mission!r} params are not valid, please recheck and resubmit.")
                del mission_params[mission]

        target_fnames = cutout_registry.get_target_filenames(
            position=resolved_position,
            mission_params=mission_params,
            size=size,
            include_metadata=True,
        )

        jobs = []
        for target_fname in target_fnames:
            for filename_obj in target_fname.filenames:
                job = generate_cutout.si(
                    job_id=job_id,
                    source_file=filename_obj.filename,
                    target=target_fname.target,
                    size=size,
                    output_format=output_format,
                    output_dir=target_fname.mission,
                    ttl=CONFIG.async_ttl,
                    mission=target_fname.mission,
                    metadata=filename_obj.metadata,
                )
                jobs.append(job)

        await r.update_job_phase(job_id, ExecutionPhase.EXECUTING)
        await r.set_start_time(job_id)

        # Split jobs into batches
        batch_size = CONFIG.worker.batch_size
        total_batches = (len(jobs) + batch_size - 1) // batch_size  # Ceiling division

        if total_batches == 0:
            # No jobs to process, complete immediately
            await r.update_job_phase(job_id, ExecutionPhase.COMPLETED)
            await r.set_end_time(job_id)
            return

        # Create chords for each batch
        batch_chords = []
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(jobs))
            batch_jobs = jobs[start_idx:end_idx]

            # Assign custom task IDs to each job in the batch
            for batch_idx, job in enumerate(batch_jobs):
                task_id = f"generate_cutout-{job_id}-{batch_num}-{batch_idx}"
                job.set(task_id=task_id)

            # Assign custom task ID to batch_done
            batch_done_task = batch_done.s(job_id=job_id, batch_num=batch_num, total_batches=total_batches)
            batch_done_task_id = f"batch_done-{job_id}-{batch_num}"
            batch_done_task.set(task_id=batch_done_task_id)

            batch_chord = group(batch_jobs) | batch_done_task
            batch_chords.append(batch_chord)

        # Chain all batch chords sequentially
        chain(*batch_chords).delay()

    asyncio.run(task())


@celery_app.task(bind=True, pydantic=True)
def batch_done(
    self: Task,
    job_results: list[dict],
    job_id: str,
    batch_num: int,
    total_batches: int,
) -> None:
    async def task():
        cutout_results = [CutoutResponse.model_validate(result) for result in job_results]

        r = redis_uws_client()
        await r.append_job_cutout_result(job_id, cutout_results, batch_num)

        is_last_batch = batch_num == total_batches - 1
        if is_last_batch:
            await r.update_job_phase(job_id, ExecutionPhase.COMPLETED)
            await r.set_end_time(job_id)

    asyncio.run(task())

    if hasattr(self.request, "chord") and self.request.chord:
        from celery.result import AsyncResult

        # Forget each task result to free up backend storage
        for task_id in self.request.chord:
            AsyncResult(task_id, app=celery_app).forget()


def get_fits_filter(fits_cutout: HDUList) -> str | None:
    filter = None
    try:
        cutout_header = fits_cutout["CUTOUT"].header
        filter = cutout_header["*FILTER*"][0]
    except KeyError:
        pass

    return filter


@celery_app.task(bind=True, pydantic=True)
def generate_cutout(  # noqa: C901
    self: Task,
    job_id: str,
    source_file: str | list[str],
    target: TargetPosition,
    size: int | tuple[int, int],
    output_format: list[str],
    output_dir: str = "",
    colorize: bool = False,
    ttl: int = CONFIG.sync_ttl,
    mission: str = "",
    metadata: dict | None = None,
) -> CutoutResponse:
    """
    Generate a cutout within the specific source file

    Args:
        job_id (str): The job ID to generate the cutout for
        source_file (str | list[str]): Source file(s)
        target (TargetPosition): Target to center the cutout around
        size (int | tuple[int, int]): Size of the cutout
        output_format (list[str]): Formats of the resulting files (fits, jp(e)g)
        output_dir (str, optional): Destination directory.
            Defaults to "".
        colorize (bool, optional): Generate a colorized JPEG image.
            Defaults to False.
        ttl (int, optional): If destination is S3, time to live of the signed url in seconds.
            Defaults to 1 hr.
        mission (str, optional): The mission name (e.g., "ps1").
            Defaults to "".
        metadata (dict | None, optional): Mission-specific metadata dictionary.
            Defaults to None.
    """

    async def task(
        self: Task,
        job_id,
        source_file,
        target,
        size,
        output_format,
        output_dir,
        colorize,
        ttl,
        mission,
        metadata,
    ):
        if isinstance(size, int):
            size = (size, size)

        if colorize:
            assert isinstance(source_file, list) and len(source_file) == 3, (
                "Color image must have exactly 3 source images"
            )

        else:
            if isinstance(source_file, str):
                source_file = [source_file]
            assert isinstance(source_file, list) and len(source_file) == 1, "Cutout must have exactly one source"

        temp_output_dir = f"/tmp/cutouts/{self.request.id}"

        cutout = astrocut.FITSCutout(
            input_files=source_file,
            coordinates=f"{target[0]} {target[1]}",
            cutout_size=size,
            single_outfile=False,
        )

        filter: str | ColorFilter
        if metadata is not None and "filter" in metadata:
            filter = metadata.pop("filter")
        elif colorize:
            filter = ColorFilter(
                red=get_fits_filter(cutout.fits_cutouts[0]),
                green=get_fits_filter(cutout.fits_cutouts[1]),
                blue=get_fits_filter(cutout.fits_cutouts[2]),
            )
        else:
            filter = get_fits_filter(cutout.fits_cutouts[0])

        cutout_prefix = source_file[0]
        cutout_prefix = urlparse(cutout_prefix).path
        cutout_prefix = Path(cutout_prefix).stem

        lpaths = []

        fits_fname = ""
        if "fits" in output_format:
            fits_fname = cutout.write_as_fits(
                output_dir=temp_output_dir,
                cutout_prefix=cutout_prefix,
            )[0]

            lpaths.append(fits_fname)

        img_fname = ""
        if "jpg" in output_format or "jpeg" in output_format:
            img_fname = cutout.write_as_img(
                output_dir=temp_output_dir,
                cutout_prefix=cutout_prefix,
                colorize=colorize,
            )

            if not colorize:
                img_fname = img_fname[0]

            lpaths.append(img_fname)

        fs: AbstractFileSystem
        if CUTOUT_STORAGE_IS_S3:
            fs = filesystem("s3")
        else:
            fs = filesystem("local")

        rpath = f"{CUTOUT_STORAGE_PREFIX}/cutouts/{job_id}"
        if output_dir:
            rpath += f"/{output_dir}"

        if not fs.isdir(rpath):
            fs.mkdir(rpath)

        for lpath in lpaths:
            fs.put(
                lpath=lpath,
                rpath=rpath,
            )

        fits_url = fits_fname.replace(temp_output_dir, rpath)
        img_url = img_fname.replace(temp_output_dir, rpath)

        if CUTOUT_STORAGE_IS_S3:
            if fits_url:
                fits_url = fs.sign(fits_url, expiration=ttl)
            if img_url:
                img_url = fs.sign(img_url, expiration=ttl)
        # If generating local cutouts just return the relative path
        else:
            if fits_url:
                fits_url = fits_url.replace(CUTOUT_STORAGE_PREFIX, "")
            if img_url:
                img_url = img_url.replace(CUTOUT_STORAGE_PREFIX, "")

        resp = CutoutResponse(
            mission=mission,
            position=target,
            size_px=size,
            filter=filter,
            fits=fits_url,
            preview=img_url,
            mission_extras=metadata or {},
        )

        l_fs: AbstractFileSystem = filesystem("local")
        l_fs.rm(temp_output_dir, recursive=True)
        del cutout
        # Force garbage collection to free memory immediately
        gc.collect()

        return resp

    resp = asyncio.run(
        task(
            self=self,
            job_id=job_id,
            source_file=source_file,
            target=target,
            size=size,
            output_format=output_format,
            output_dir=output_dir,
            colorize=colorize,
            ttl=ttl,
            mission=mission,
            metadata=metadata,
        )
    )

    return resp
