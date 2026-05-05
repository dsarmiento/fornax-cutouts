import asyncio
import uuid
from typing import Annotated
from urllib.parse import urlencode

from astrocut.exceptions import InvalidQueryError
from celery.exceptions import TimeoutError as CeleryTimeoutError
from fastapi import APIRouter, HTTPException, Query, Request, status
from fastapi.responses import RedirectResponse
from fastapi_utils.cbv import cbv
from fsspec import AbstractFileSystem, filesystem

from fornax_cutouts.config import CONFIG
from fornax_cutouts.jobs.tasks import execute_color_preview, execute_cutout
from fornax_cutouts.models.base import TargetPosition
from fornax_cutouts.models.cutouts import CutoutResponse

sync_router = APIRouter(prefix="/cutouts", tags=["Sync Cutouts"])


def _invalid_query_from_exc(exc: BaseException) -> InvalidQueryError | None:
    seen: set[int] = set()
    cur: BaseException | None = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, InvalidQueryError):
            return cur
        cur = cur.__cause__
    return None


async def _await_cutout_result(async_result, *, timeout: float) -> CutoutResponse:
    try:
        out = await asyncio.to_thread(async_result.get, timeout=timeout)
    except CeleryTimeoutError as e:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Cutout generation timed out",
        ) from e
    except Exception as e:
        iq = _invalid_query_from_exc(e)
        if iq is not None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=str(iq),
            ) from e
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Cutout generation failed",
        ) from e
    if out is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Cutout generation returned no result",
        )
    return out


@cbv(sync_router)
class CutoutsSyncHandler:
    @sync_router.get(
        "/sync",
        summary="Redirect to single cutout",
        description="Redirects to /sync/single, preserving all query parameters.",
    )
    async def get_cutout(self, request: Request):
        """
        Redirect to the /single endpoint, keeping all query params
        """
        query_params = dict(request.query_params)
        new_query = urlencode(query_params)
        redirect_url = f"{request.url.path}/single?{new_query}"
        return RedirectResponse(url=redirect_url, status_code=status.HTTP_303_SEE_OTHER)

    @sync_router.get(
        "/sync/single",
        summary="Generate single FITS/JPEG cutout",
        description="Generate a FITS and optional JPEG preview cutout for a specified source at given coordinates.",
    )
    async def get_single_cutout(
        self,
        filename: Annotated[str, Query(description="Publicly available source URL/S3 URI to generate a cutout for")],
        ra: Annotated[float, Query(description="Central RA coordinate to generate cutout for")],
        dec: Annotated[float, Query(description="Central Dec coordinate to generate cutout for")],
        size: Annotated[int, Query(description="Width and height of the cutout in pixels")],
        include_preview: Annotated[bool, Query(description="Include a JPEG preview of the cutout")] = True,
        job_id: Annotated[str, Query(description="Job ID to generate the cutout for")] = "",
    ) -> CutoutResponse:
        """
        Generate a FITS and JPEG cutout for a specified source
        """
        output_formats = ["fits"]
        if include_preview:
            output_formats.append("jpeg")

        if not job_id:
            job_id = uuid.uuid4().hex[:8]

        output_dir = f"{CONFIG.storage.prefix}/cutouts/sync/{job_id}"
        task_uid = uuid.uuid4().hex[:12]
        print(f"Executing single cutout task: job_id={job_id}, task_uid={task_uid}")
        async_result = execute_cutout.apply_async(
            kwargs={
                "job_id": job_id,
                "source_file": filename,
                "target": TargetPosition(ra, dec),
                "size": size,
                "output_format": output_formats,
                "output_dir": output_dir,
                "mission": "sync",
            },
            task_id=f"sync-single-{job_id}-{task_uid}",
            priority=0,
        )
        ret = await _await_cutout_result(
            async_result,
            timeout=15,
        )

        ret = CutoutResponse.model_validate(ret)

        if CONFIG.storage.is_s3:
            fs: AbstractFileSystem = filesystem("s3")
            if ret.science:
                ret.science = fs.sign(ret.science, expiration=CONFIG.sync_ttl)
            if ret.preview:
                ret.preview = fs.sign(ret.preview, expiration=CONFIG.sync_ttl)
        else:
            if ret.science:
                ret.science = ret.science.replace(CONFIG.storage.prefix, "")
            if ret.preview:
                ret.preview = ret.preview.replace(CONFIG.storage.prefix, "")

        return ret

    @sync_router.get(
        "/sync/color",
        summary="Generate color JPEG cutout",
        description="Generate a color JPEG preview by combining red, green, and blue channel cutouts.",
    )
    async def get_color_cutout(
        self,
        red: Annotated[str, Query(description="Red channel for a color cutout preview")],
        green: Annotated[str, Query(description="Green channel for a color cutout preview")],
        blue: Annotated[str, Query(description="Blue channel for a color cutout preview")],
        ra: Annotated[float, Query(description="Central RA coordinate to generate cutout for")],
        dec: Annotated[float, Query(description="Central Dec coordinate to generate cutout for")],
        size: Annotated[int, Query(description="Width and height of the cutout in pixels")],
        job_id: Annotated[str, Query(description="Job ID to generate the cutout for")] = "",
    ) -> CutoutResponse:
        """
        Generate a color JPEG preview of a cutout
        """
        if not job_id:
            job_id = uuid.uuid4().hex[:8]

        output_dir = f"{CONFIG.storage.prefix}/cutouts/sync/{job_id}"
        task_uid = uuid.uuid4().hex[:12]
        print(f"Executing color preview task: job_id={job_id}, task_uid={task_uid}")
        async_result = execute_color_preview.apply_async(
            kwargs={
                "red": red,
                "green": green,
                "blue": blue,
                "target": TargetPosition(ra, dec),
                "size": size,
                "output_dir": output_dir,
            },
            task_id=f"sync-color-{job_id}-{task_uid}",
            priority=0,
        )
        ret = await _await_cutout_result(
            async_result,
            timeout=15,
        )
        ret = CutoutResponse.model_validate(ret)

        if CONFIG.storage.is_s3:
            fs: AbstractFileSystem = filesystem("s3")
            ret.preview = fs.sign(ret.preview, expiration=CONFIG.sync_ttl)
        else:
            ret.preview = ret.preview.replace(CONFIG.storage.prefix, "")

        return ret
