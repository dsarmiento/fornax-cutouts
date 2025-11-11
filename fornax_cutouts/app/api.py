import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Annotated

from fastapi import Depends, FastAPI
from redis.exceptions import ConnectionError as RedisConnectionError

from fornax_cutouts.config import CONFIG
from fornax_cutouts.routes.v1 import api_v1
from fornax_cutouts.utils.logging import RequestLoggingMiddleware, setup_structured_logging
from fornax_cutouts.utils.redis_uws import RedisUWS, redis_uws_client

# Initialize structured logging
setup_structured_logging(log_level=CONFIG.log.level, format=CONFIG.log.format)

logger = logging.getLogger("uvicorn")

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application startup initiated", extra={"event": "startup"})
    try:
        r = redis_uws_client()
        await r._setup_index()
        await r.close()
        logger.info("Redis connection established and index setup complete", extra={"event": "redis_ready"})
    except Exception as e:
        logger.error(
            "Failed to connect to Redis",
            extra={"event": "redis_connection_failed", "error": str(e), "error_type": type(e).__name__},
            exc_info=True
        )

    yield

    logger.info("Application shutdown initiated", extra={"event": "shutdown"})


main_app = FastAPI(lifespan=lifespan)

# Add structured logging middleware
main_app.add_middleware(RequestLoggingMiddleware)

main_app.include_router(api_v1, prefix="/api/v0")   # Beta routes, eventually will be promoted to v1


@main_app.get("/api/health")
async def health_check(
    r: Annotated[RedisUWS, Depends(redis_uws_client)],
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
