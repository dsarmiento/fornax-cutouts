from fastapi import FastAPI

from fornax_cutouts.routes.v0.metadata import metadata_router
from fornax_cutouts.routes.v0.sync import sync_router
from fornax_cutouts.routes.v0.uws_async import uws_router

app_v0 = FastAPI()
app_v0.include_router(metadata_router)
app_v0.include_router(sync_router)
app_v0.include_router(uws_router)
