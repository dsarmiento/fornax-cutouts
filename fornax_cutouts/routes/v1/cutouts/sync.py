from typing import Annotated
from urllib.parse import urlencode

from astrocut.exceptions import InvalidQueryError
from fastapi import APIRouter, HTTPException, Query, Request, status
from fastapi.responses import RedirectResponse
from fastapi_utils.cbv import cbv

from fornax_cutouts.config import CONFIG
from fornax_cutouts.models.base import TargetPosition
from fornax_cutouts.models.cutouts import CutoutResponse
from fornax_cutouts.tasks import generate_cutout

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
    ) -> CutoutResponse:
        """
        Generate a FITS and JPEG cutout for a specified source
        """
        output_formats = ["fits"]
        if include_preview:
            output_formats.append("jpeg")

        try:
            return generate_cutout(
                job_id="sync",
                output_format=output_formats,
                source_file=filename,
                target=TargetPosition(ra, dec),
                size=size,
                ttl=CONFIG.sync_ttl,
            )
        except InvalidQueryError as e:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=e,
            )

    @sync_router.get("/sync/color")
    async def get_color_cutout(
        self,
        r: Annotated[str, Query(description="Red channel for a color cutout preview")],
        g: Annotated[str, Query(description="Green channel for a color cutout preview")],
        b: Annotated[str, Query(description="Blue channel for a color cutout preview")],
        ra: Annotated[float, Query(description="Central RA coordinate to generate cutout for")],
        dec: Annotated[float, Query(description="Central Dec coordinate to generate cutout for")],
        size: Annotated[int, Query(description="Width and height of the cutout in pixels")],
    ) -> CutoutResponse:
        """
        Generate a color JPEG preview of a cutout
        """
        try:
            return generate_cutout(
                job_id="color_preview",
                output_format=["jpeg"],
                source_file=[r, g, b],
                target=TargetPosition(ra, dec),
                size=size,
                colorize=True,
                ttl=CONFIG.sync_ttl,
            )
        except InvalidQueryError as e:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=e,
            )
