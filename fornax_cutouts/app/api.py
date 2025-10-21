import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Annotated

from fastapi import Depends, FastAPI
from redis.exceptions import ConnectionError as RedisConnectionError

from fornax_cutouts.routes.v1 import api_v1
from fornax_cutouts.sources import cutout_registry
from fornax_cutouts.utils.uws_redis import UWSRedis, uws_redis_client

logger = logging.getLogger("uvicorn")

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        r = uws_redis_client()
        await r._setup_index()
        await r.close()
    except Exception as e:
        print("redis isn't up")
        print(f"{e}")

    logger.info(f"Registered sources: {cutout_registry.get_source_names()}")

    yield


main_app = FastAPI(lifespan=lifespan)
main_app.include_router(api_v1, prefix="/api/v0")   # Beta routes, eventually will be promoted to v1


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
