import asyncio
from pathlib import Path
from urllib.parse import urlparse

import astrocut
from astropy.io.fits.hdu.hdulist import HDUList
from celery import chord
from fsspec import AbstractFileSystem, filesystem
from vo_models.uws.models import ExecutionPhase

from fornax_cutouts.app.celery_app import celery_app, logger
from fornax_cutouts.config import CONFIG
from fornax_cutouts.constants import CUTOUT_STORAGE_IS_S3, CUTOUT_STORAGE_PREFIX
from fornax_cutouts.models.base import TargetPosition
from fornax_cutouts.models.cutouts import ColorFilter, CutoutResponse, FileResponse
from fornax_cutouts.sources import cutout_registry
from fornax_cutouts.utils.santa_resolver import resolve_positions
from fornax_cutouts.utils.uws_redis import uws_redis_client


@celery_app.task()
def schedule_job(
    job_id: str,
    position: list[str],
    size: int,
    mission_params: dict[str, dict],
    output_format: list[str],
):
    async def task():
        r = uws_redis_client()

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
        )

        jobs = []
        for target_fname in target_fnames:
            for fname in target_fname.filenames:
                job = generate_cutout.s(
                    job_id=job_id,
                    source_file=fname,
                    target=target_fname.target,
                    size=size,
                    output_format=output_format,
                    output_dir=target_fname.mission,
                    ttl=CONFIG.async_ttl,
                )
                jobs.append(job)

        await r.update_job_phase(job_id, ExecutionPhase.EXECUTING)
        await r.set_start_time(job_id)

        chord(jobs)(all_done.s(job_id=job_id))

    asyncio.run(task())


@celery_app.task()
def all_done(_, job_id: str):
    async def task():
        r = uws_redis_client()

        await r.update_job_phase(job_id, ExecutionPhase.COMPLETED)
        await r.set_end_time(job_id)

    asyncio.run(task())


def get_fits_filter(fits_cutout: HDUList) -> str | None:
    filter = None
    try:
        cutout_header = fits_cutout["CUTOUT"].header
        filter = cutout_header["*FILTER*"][0]
    except KeyError:
        pass

    return filter


@celery_app.task()
def generate_cutout(  # noqa: C901
    job_id: str,
    source_file: str | list[str],
    target: TargetPosition,
    size: int | tuple[int, int],
    output_format: list[str],
    output_dir: str = "",
    colorize: bool = False,
    ttl: int = CONFIG.sync_ttl,
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
    """
    async def task(
        job_id,
        source_file,
        target,
        size,
        output_format,
        output_dir,
        colorize,
        ttl,
    ):
        if isinstance(size, int):
            size = (size, size)

        if colorize:
            assert isinstance(source_file, list) and len(source_file) == 3, "Color image must have exactly 3 source images"

        else:
            if isinstance(source_file, str):
                source_file = [source_file]
            assert isinstance(source_file, list) and len(source_file) == 1, "Cutout must have exactly one source"

        temp_output_dir = "/tmp/cutouts/"

        cutout = astrocut.FITSCutout(
            input_files=source_file,
            coordinates=f"{target[0]} {target[1]}",
            cutout_size=size,
            single_outfile=False,
        )

        filter: str | ColorFilter | None = None
        if colorize:
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

        rpath = f"{CUTOUT_STORAGE_PREFIX}/cutouts/{job_id}/"
        if output_dir:
            rpath += f"{output_dir}/"

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

        fits = None
        if fits_url:
            fits = FileResponse(
                filename=fits_fname.replace(temp_output_dir, ""),
                url=fits_url,
            )

        preview = None
        if img_url:
            preview = FileResponse(
                filename=img_fname.replace(temp_output_dir, ""),
                url=img_url,
            )

        resp = CutoutResponse(
            fits=fits,
            preview=preview,
            position=target,
            size_px=size,
            filter=filter,
        )

        if job_id != "sync":
            r = uws_redis_client()
            await r.append_job_result(job_id, resp.model_dump())

        return resp

    resp = asyncio.run(task(
        job_id=job_id,
        source_file=source_file,
        target=target,
        size=size,
        output_format=output_format,
        output_dir=output_dir,
        colorize=colorize,
        ttl=ttl,
    ))

    return resp
