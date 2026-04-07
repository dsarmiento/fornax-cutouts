import logging
import time
import uuid

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from fornax_cutouts.config import CONFIG


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

        client_ip = request.client.host if request.client else None
        user_agent = request.headers.get("user-agent")

        logger = logging.getLogger(CONFIG.log.name)
        request_data = {
            "event": "request_started",
            "method": request.method,
            "path": request.url.path,
            "client_ip": client_ip,
            "user_agent": user_agent,
            "correlation_id": correlation_id,
        }
        log_line = f"{request.method} {request.url.path} ({client_ip})"
        logger.debug(log_line, extra=request_data)

        try:
            response: Response = await call_next(request)
            response_time_ms = round((time.time() - start_time) * 1000, 2)

            response.headers["X-Correlation-ID"] = correlation_id
            response_data = {
                "event": "request_completed",
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "response_time_ms": response_time_ms,
                "client_ip": client_ip,
                "user_agent": user_agent,
                "correlation_id": correlation_id,
            }
            log_line = f"{response.status_code} {request.method} {request.url.path} ({client_ip}) {response_time_ms}ms"
            logger.info(log_line, extra=response_data)

            return response

        except Exception as exc:
            response_time_ms = round((time.time() - start_time) * 1000, 2)
            error_data = {
                "event": "request_failed",
                "method": request.method,
                "path": request.url.path,
                "response_time_ms": response_time_ms,
                "client_ip": client_ip,
                "correlation_id": correlation_id,
                "error": str(exc),
                "error_type": type(exc).__name__,
            }
            log_line = f"{request.method} {request.url.path} ({client_ip}) {response_time_ms}ms"
            logger.error(log_line, extra=error_data, exc_info=True)
            raise
