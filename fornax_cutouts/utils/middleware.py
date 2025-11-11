import logging
import time
import uuid

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from fornax_cutouts.constants import LOGGER_NAME


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware to log HTTP requests and responses with structured JSON logging.
    Adds correlation IDs for distributed tracing.
    """

    async def dispatch(self, request: Request, call_next):
        """Process request and log details."""
        start_time = time.time()
        correlation_id = request.headers.get("X-Correlation-ID") or str(uuid.uuid4())
        request.state.correlation_id = correlation_id

        logger = logging.getLogger(LOGGER_NAME)
        request_data = {
            "event": "request_started",
            "method": request.method,
            "path": request.url.path,
            "client_ip": request.client.host if request.client else None,
            "user_agent": request.headers.get("user-agent"),
            "correlation_id": correlation_id,
        }
        logger.info("HTTP request started", extra=request_data)

        try:
            response: Response = await call_next(request)
            response_time = time.time() - start_time

            response.headers["X-Correlation-ID"] = correlation_id
            response_data = {
                "event": "request_completed",
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "response_time_ms": round(response_time * 1000, 2),
                "client_ip": request.client.host if request.client else None,
                "correlation_id": correlation_id,
            }
            logger.info("HTTP request completed", extra=response_data)

            return response

        except Exception as exc:
            response_time = time.time() - start_time
            error_data = {
                "event": "request_failed",
                "method": request.method,
                "path": request.url.path,
                "response_time_ms": round(response_time * 1000, 2),
                "client_ip": request.client.host if request.client else None,
                "correlation_id": correlation_id,
                "error": str(exc),
                "error_type": type(exc).__name__,
            }
            logger.error("HTTP request failed", extra=error_data, exc_info=True)
            raise
