from fastapi import APIRouter

from fornax_cutouts.routes.v1.cutouts.async_uws import uws_router
from fornax_cutouts.routes.v1.cutouts.sync import sync_router
from fornax_cutouts.routes.v1.metadata import metadata_router

api_v1 = APIRouter()
api_v1.include_router(metadata_router)
api_v1.include_router(sync_router)
api_v1.include_router(uws_router)
