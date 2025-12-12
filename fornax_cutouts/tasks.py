import asyncio
import json
import time
from pathlib import Path
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
                    "output_dir": target_fname.mission,
                    "ttl": CONFIG.async_ttl,
                    "mission": target_fname.mission,
                    "metadata": filename_obj.metadata,
                }
                descriptors.append(descriptor)

        total_jobs = len(descriptors)

        if total_jobs == 0:
            await r.update_job_phase(job_id, ExecutionPhase.COMPLETED)
            await r.set_end_time(job_id)
            return

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
            sig = generate_cutout.si(
                job_id=desc["job_id"],
                source_file=desc["source_file"],
                target=target,
                size=desc["size"],
                output_format=desc["output_format"],
                output_dir=desc["output_dir"],
                ttl=desc["ttl"],
                mission=desc["mission"],
                metadata=desc.get("metadata"),
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

        time_task_start = time.time()
        time_start = time_task_start
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
        time_end = time.time()
        setup_time = time_end - time_start

        try:
            time_start = time.time()
            cutout = astrocut.FITSCutout(
                input_files=source_file,
                coordinates=f"{target[0]} {target[1]}",
                cutout_size=size,
                single_outfile=False,
            )
            time_end = time.time()
            cutout_time = time_end - time_start

        except InvalidQueryError as exc:
            # Preserve behavior for sync endpoints which expect this exception
            if job_id in ("sync", "color_preview"):
                raise exc

            uws_client = redis_uws_client()
            await uws_client.increment_task_failures(job_id, "generate_cutout")

            local_fs: AbstractFileSystem = filesystem("local")
            if local_fs.isdir(temp_output_dir):
                local_fs.rm(temp_output_dir, recursive=True)

            print(f"Error: {json.dumps({
                    'error': str(exc),
                    'input_files': source_file,
                    'coordinates': target,
                    'cutout_size': size
                })}")

            await uws_client.close()

            del local_fs
            del uws_client

            return

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


        time_start = time.time()
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

        time_end = time.time()
        local_write_time = time_end - time_start


        time_start = time.time()
        fs: AbstractFileSystem
        if CUTOUT_STORAGE_IS_S3:
            fs = filesystem("s3")
        else:
            fs = filesystem("local")

        rpath = f"{CUTOUT_STORAGE_PREFIX}/cutouts/{job_id}/"
        if output_dir:
            rpath += f"{output_dir}/"

        # Only create directories for local filesystem; S3 doesn't need them
        # and the isdir/mkdir calls are expensive LIST operations
        if not CUTOUT_STORAGE_IS_S3 and not fs.isdir(rpath):
            fs.mkdir(rpath)

        for lpath in lpaths:
            fs.put(
                lpath=lpath,
                rpath=rpath,
            )

        time_end = time.time()
        remote_write_time = time_end - time_start


        time_start = time.time()
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

        uws_client = redis_uws_client()
        await uws_client.increment_completed(job_id, amount=1)
        await uws_client.close()

        del uws_client
        del cutout
        del l_fs
        del fs
        time_end = time.time()
        create_response_time = time_end - time_start
        total_time = time_end - time_task_start

        print(json.dumps({
            "source_file": source_file,
            "setup_time": round(setup_time, 2),
            "cutout_time": round(cutout_time, 2),
            "local_write_time": round(local_write_time, 2),
            "remote_write_time": round(remote_write_time, 2),
            "create_response_time": round(create_response_time, 2),
            "total_time": round(total_time, 2)
        }))

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
