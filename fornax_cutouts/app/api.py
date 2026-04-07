import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Annotated

from fastapi import Depends, FastAPI
from redis.asyncio import Redis, RedisCluster
from redis.exceptions import ConnectionError as RedisConnectionError

from fornax_cutouts.config import CONFIG
from fornax_cutouts.jobs.redis import async_redis_client_factory, setup_index, sync_redis_client_factory
from fornax_cutouts.routes.v1 import api_v1
from fornax_cutouts.sources import cutout_registry
from fornax_cutouts.utils.logging import setup_structured_logging
from fornax_cutouts.utils.middleware import RequestLoggingMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_structured_logging(service="api")
    logger = logging.getLogger(CONFIG.log.name)
    logger.info("Application startup initiated", extra={"event": "startup"})
    cutout_registry.discover_sources()

    try:
        redis_client = sync_redis_client_factory()
        setup_index(redis_client)
        redis_client.close()
        logger.info("Redis connection established and index setup complete", extra={"event": "redis_ready"})
    except Exception as e:
        logger.error(
            "Failed to connect to Redis",
            extra={"event": "redis_connection_failed", "error": str(e), "error_type": type(e).__name__},
            exc_info=True,
        )

    yield

    logger.info("Application shutdown initiated", extra={"event": "shutdown"})


main_app = FastAPI(
    title=f"{CONFIG.service_name} API",
    description="Pluggable backend for async FITS image cutouts. Implements IVOA UWS 1.1 for job management.",
    version="0.1.0",
    lifespan=lifespan,
)

# Add structured logging middleware
main_app.add_middleware(RequestLoggingMiddleware)
main_app.include_router(api_v1, prefix="/api/v0")  # Beta routes, eventually will be promoted to v1


@main_app.get(
    "/api/health",
    tags=["Health"],
    summary="Health check",
    description="Returns service status. Checks database connectivity; returns 'degraded' if the database is unreachable.",
)
async def health_check(redis_client: Annotated[Redis | RedisCluster, Depends(async_redis_client_factory)]):
    health_response = {
        "status": "ok",
        "details": "",
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }

    try:
        await asyncio.wait_for(redis_client.ping(), timeout=0.5)
    except asyncio.TimeoutError:
        health_response["status"] = "degraded"
        health_response["details"] = "database timeout"
    except RedisConnectionError:
        health_response["status"] = "degraded"
        health_response["details"] = "database connection error"

    if CONFIG.deployment_environment != "prod":
        health_response["environment"] = CONFIG.deployment_environment

    return health_response
