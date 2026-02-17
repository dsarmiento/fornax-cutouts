import asyncio
import json
import logging
import time
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlparse

import astrocut
from astrocut.exceptions import InvalidQueryError
from astropy.io.fits.hdu.hdulist import HDUList
from celery import Task, chord
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

logging.getLogger("astrocut").setLevel(logging.ERROR)
logging.getLogger("astropy").setLevel(logging.ERROR)

@celery_app.task(bind=True, ignore_result=True)
def schedule_job(
    self: Task,
    job_id: str,
    position: list[str],
    size: int,
    mission_params: dict[str, dict],
    output_format: list[str],
    mode: str = "FITSCutout",
):
    async def task():
        r = redis_uws_client()

        await r.update_job_phase(job_id, ExecutionPhase.QUEUED)

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
                    "output_dir": f"{CUTOUT_STORAGE_PREFIX}/cutouts/async/{job_id}/{target_fname.mission}",
                    "ttl": CONFIG.async_ttl,
                    "mission": target_fname.mission,
                    "metadata": filename_obj.metadata,
                    "mode": mode,
                }
                descriptors.append(descriptor)

        total_jobs = len(descriptors)

        print(f"Total jobs: {total_jobs}")

        if total_jobs == 0:
            await r.update_job_phase(job_id, ExecutionPhase.COMPLETED)
            await r.set_end_time(job_id)
            return

        print(f"Pushing {total_jobs} descriptors to pending queue")
        for descriptor in descriptors:
            await r.push_pending_descriptor(job_id, descriptor)

        await r.set_expected_results(job_id, total_jobs)
        await r.reset_completed_count(job_id)
        await r.reset_batch_num(job_id)
        await r.reset_task_failures(job_id, "generate_cutout")

        await r.update_job_phase(job_id, ExecutionPhase.EXECUTING)
        await r.set_start_time(job_id)

        batch_cutouts_task = batch_cutouts.s(job_id=job_id)
        batch_cutouts_task.set(task_id=f"batch_cutouts-{job_id}-0")
        batch_cutouts_task.delay()

    asyncio.run(task())


@celery_app.task(bind=True, ignore_result=True)
def batch_cutouts(self: Task, job_id: str):
    """
    Chunked batcher: pops descriptors from Redis in batches and creates generate_cutout tasks
    using a Celery chord. The chord callback (write_results) handles completion and looping back.
    """
    async def task():
        r = redis_uws_client()

        batch_num = await r.get_and_increment_batch_num(job_id)

        descriptors = await r.pop_pending_descriptors(job_id, max_items=CONFIG.worker.batch_size)
        if not descriptors:
            return

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

    asyncio.run(task())


@celery_app.task(bind=True)
def write_results(self: Task, results: list[CutoutResponse | dict | None], job_id: str):
    """
    Chord callback: receives results from a batch of generate_cutout tasks and writes them to AsyncCutoutResults.
    Checks if job is complete; if not, loops back to batch_cutouts for the next batch.

    Args:
        results: List of CutoutResponse objects (or dicts/None for failed tasks) from the chord
        job_id: The job ID to write results for
    """
    async def task():
        r = redis_uws_client()

        # Filter out None/failed results and validate
        valid_results = [rp for rp in results if rp is not None]
        cutout_results = []
        for rp in valid_results:
            if isinstance(rp, dict):
                cutout_results.append(CutoutResponse.model_validate(rp))
            elif isinstance(rp, CutoutResponse):
                cutout_results.append(rp)
            else:
                # Skip invalid result types
                continue

        # Write results to Parquet storage if we have any
        if cutout_results:
            results_writer = r.get_job_cutout_results(job_id)
            batch_num = await r.get_and_increment_batch_num(job_id)
            results_writer.add_results(cutout_results, batch_num=batch_num)

        # Check if job is complete
        expected = await r.get_expected_results(job_id)
        completed = await r.get_completed_count(job_id)
        failed_generate = await r.get_task_failures(job_id, "generate_cutout")
        pending = await r.get_pending_count(job_id)

        effective_completed = completed + failed_generate

        if effective_completed >= expected and pending == 0:
            # All work is done
            await r.update_job_phase(job_id, ExecutionPhase.COMPLETED)
            await r.set_end_time(job_id)
        elif pending > 0:
            # More descriptors remain, batch next batch
            # Get the next batch number (current + 1) for task_id
            next_batch = await r.get_batch_num(job_id) + 1
            batch_cutouts_task = batch_cutouts.s(job_id=job_id)
            batch_cutouts_task.set(task_id=f"batch_cutouts-{job_id}-{next_batch}")
            batch_cutouts_task.delay()

    asyncio.run(task())


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
    mode: str = "FITSCutout",
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

    if isinstance(size, int):
        size = (size, size)

    cutout_prefix = urlparse(source_file).path
    cutout_prefix = Path(cutout_prefix).stem

    with TemporaryDirectory(prefix="fornax-cutouts-") as temp_output_dir:
        fits_fname = ""
        img_fname = ""

        if mode == "FITSCutout":
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

        elif mode == "fits_cut":

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

        if fits_fname:
            fs.put(lpath=fits_fname, rpath=output_dir)
            fits_fname = fits_fname.replace(temp_output_dir, output_dir)

        if img_fname:
            fs.put(lpath=img_fname, rpath=output_dir)
            img_fname = img_fname.replace(temp_output_dir, output_dir)

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
def execute_cutout(  # noqa: C901
    self: Task,
    job_id: str,
    source_file: str,
    target: TargetPosition,
    size: int | tuple[int, int],
    output_format: list[str],
    output_dir: str = "",
    mission: str = "",
    metadata: dict = {},
    mode: str = "FITSCutout",
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

    async def task(
        self: Task,
        job_id,
        source_file,
        target,
        size,
        output_format,
        output_dir,
        mission,
        metadata,
        mode,
    ):

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

        except InvalidQueryError:
            uws_client = redis_uws_client()
            await uws_client.increment_task_failures(job_id, "generate_cutout")

            await uws_client.close()
            del uws_client

            return

        uws_client = redis_uws_client()
        await uws_client.increment_completed(job_id, amount=1)

        await uws_client.close()
        del uws_client

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
            mission=mission,
            metadata=metadata,
            mode=mode,
        )
    )

    return resp
