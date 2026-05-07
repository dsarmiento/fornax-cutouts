import asyncio
import time
import uuid
from typing import Annotated
from urllib.parse import urlencode

from fastapi import APIRouter, Query, Request, status
from fastapi.responses import RedirectResponse
from fastapi_utils.cbv import cbv
from fsspec import AbstractFileSystem, filesystem

from fornax_cutouts.config import CONFIG
from fornax_cutouts.jobs.tasks import execute_color_preview, execute_cutout
from fornax_cutouts.models.base import TargetPosition
from fornax_cutouts.models.cutouts import CutoutResponse

sync_router = APIRouter(prefix="/cutouts", tags=["Sync Cutouts"])


async def _wait_for_result(async_result, timeout: float = 15.0, poll_interval: float = 0.2):
    """
    Poll for a Celery task result by repeatedly calling ready() instead of using the
    pubsub-based AsyncResult.get(). The pubsub get() holds a single socket open and is
    not safe to call concurrently from asyncio.to_thread: concurrent threads share the
    same pubsub connection and interleave RESP2 reads, producing InvalidResponse errors.
    ready() uses the normal connection pool (one pooled GET per call) and is safe.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await asyncio.to_thread(async_result.ready):
            return await asyncio.to_thread(async_result.get, propagate=True)
        await asyncio.sleep(poll_interval)
    raise TimeoutError(f"Sync cutout task did not complete within {timeout}s")


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
        ret = await _wait_for_result(async_result, timeout=CONFIG.redis.timeout)
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
        ret = await _wait_for_result(async_result, timeout=CONFIG.redis.timeout)
        ret = CutoutResponse.model_validate(ret)

        if CONFIG.storage.is_s3:
            fs: AbstractFileSystem = filesystem("s3")
            ret.preview = fs.sign(ret.preview, expiration=CONFIG.sync_ttl)
        else:
            ret.preview = ret.preview.replace(CONFIG.storage.prefix, "")

        return ret
