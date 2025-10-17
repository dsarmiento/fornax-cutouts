import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Annotated

from fastapi import Depends, FastAPI
from redis.exceptions import ConnectionError as RedisConnectionError

from mast.cutouts.services.rest_api.routes.metadata import metadata_router
from mast.cutouts.services.rest_api.routes.sync import sync_router
from mast.cutouts.services.rest_api.routes.uws import uws_router
from mast.cutouts.uws_redis import UWSRedis, uws_redis_client


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        r = uws_redis_client()
        await r._setup_index()
        await r.close()
    except Exception as e:
        print("redis isn't up")
        print(f"{e}")

    yield


app_v0 = FastAPI()
app_v0.include_router(metadata_router)
app_v0.include_router(uws_router)
app_v0.include_router(sync_router)

main_app = FastAPI(lifespan=lifespan)
main_app.mount(path="/api/v0", app=app_v0)


@main_app.get("/api/health")
async def health_check(
    r: Annotated[UWSRedis, Depends(uws_redis_client)],
):
    status = "ok"
    details = ""
    try:
        await asyncio.wait_for(r.ping(), timeout=0.5)
    except asyncio.TimeoutError:
        status = "degraded"
        details = "database timeout"
    except RedisConnectionError:
        status = "degraded"
        details = "database connection error"

    return {
        "status": status,
        "details": details,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
