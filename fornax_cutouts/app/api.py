import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import Depends, FastAPI
from redis.asyncio import Redis, RedisCluster
from redis.exceptions import ConnectionError as RedisConnectionError

from fornax_cutouts.jobs.redis import async_redis_client_factory, setup_index, sync_redis_client_factory
from fornax_cutouts.routes.v1 import api_v1
from fornax_cutouts.sources import cutout_registry

logger = logging.getLogger("uvicorn")

@asynccontextmanager
async def lifespan(app: FastAPI):
    cutout_registry.discover_sources()

    try:
        redis_client = sync_redis_client_factory()
        setup_index(redis_client)
        redis_client.close()
    except Exception as e:
        logger.error("Redis DB is not up")
        logger.error(f"{e}")

    yield


main_app = FastAPI(lifespan=lifespan)
main_app.include_router(api_v1, prefix="/api/v0")   # Beta routes, eventually will be promoted to v1


@main_app.get("/api/health")
async def health_check(
    redis_client: Redis | RedisCluster = Depends(async_redis_client_factory)
):
    status = "ok"
    details = ""
    try:
        await asyncio.wait_for(redis_client.ping(), timeout=0.5)
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


@main_app.get("/api/dev/flushdb")
async def flush_db(
    redis_client: Redis | RedisCluster = Depends(async_redis_client_factory)
):
    await redis_client.flushdb()
    return {"message": "Database flushed"}
