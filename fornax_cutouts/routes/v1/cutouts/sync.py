import asyncio
import uuid
from typing import Annotated
from urllib.parse import urlencode

from astrocut.exceptions import InvalidQueryError
from fastapi import APIRouter, HTTPException, Query, Request, status
from fastapi.responses import RedirectResponse
from fastapi_utils.cbv import cbv
from fsspec import AbstractFileSystem, filesystem

from fornax_cutouts.config import CONFIG
from fornax_cutouts.jobs.tasks import generate_color_preview, generate_cutout
from fornax_cutouts.models.base import TargetPosition
from fornax_cutouts.models.cutouts import CutoutResponse

sync_router = APIRouter(prefix="/cutouts")


@cbv(sync_router)
class CutoutsSyncHandler:
    @sync_router.get("/sync")
    async def get_cutout(self, request: Request):
        """
        Redirect to the /single endpoint, keeping all query params
        """
        query_params = dict(request.query_params)
        new_query = urlencode(query_params)
        redirect_url = f"{request.url.path}/single?{new_query}"
        return RedirectResponse(url=redirect_url, status_code=status.HTTP_303_SEE_OTHER)

    @sync_router.get("/sync/single")
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

        try:
            ret = await asyncio.to_thread(
                generate_cutout,
                source_file=filename,
                target=TargetPosition(ra, dec),
                size=size,
                output_format=output_formats,
                output_dir=output_dir,
                mission="sync",
            )
        except InvalidQueryError as e:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=e,
            )

        if CONFIG.storage.is_s3:
            fs: AbstractFileSystem = filesystem("s3")
            if ret.fits:
                ret.fits = fs.sign(ret.fits, expiration=CONFIG.sync_ttl)
            if ret.preview:
                ret.preview = fs.sign(ret.preview, expiration=CONFIG.sync_ttl)
        else:
            if ret.fits:
                ret.fits = ret.fits.replace(CONFIG.storage.prefix, "")
            if ret.preview:
                ret.preview = ret.preview.replace(CONFIG.storage.prefix, "")

        return ret

    @sync_router.get("/sync/color")
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

        try:
            ret = await asyncio.to_thread(
                generate_color_preview,
                red=red,
                green=green,
                blue=blue,
                target=TargetPosition(ra, dec),
                size=size,
                output_dir=output_dir,
            )
        except InvalidQueryError as e:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=e,
            )

        if CONFIG.storage.is_s3:
            fs: AbstractFileSystem = filesystem("s3")
            ret.preview = fs.sign(ret.preview, expiration=CONFIG.sync_ttl)
        else:
            ret.preview = ret.preview.replace(CONFIG.storage.prefix, "")

        return ret
