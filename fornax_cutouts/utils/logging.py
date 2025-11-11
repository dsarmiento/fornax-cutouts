import json
import logging
import sys
import time
import uuid
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

# Context variable for correlation ID
correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="")


class StructuredJSONFormatter(logging.Formatter):
    """
    JSON formatter for structured logging compatible with Datadog.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add correlation ID if available
        correlation_id = correlation_id_var.get()
        if correlation_id:
            log_data["dd.trace_id"] = correlation_id
            log_data["correlation_id"] = correlation_id

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Add extra fields from record
        if hasattr(record, "extra"):
            log_data.update(record.extra)

        # Add any custom attributes
        for key, value in record.__dict__.items():
            if key not in [
                "name",
                "msg",
                "args",
                "created",
                "filename",
                "funcName",
                "levelname",
                "levelno",
                "lineno",
                "module",
                "msecs",
                "message",
                "pathname",
                "process",
                "processName",
                "relativeCreated",
                "thread",
                "threadName",
                "exc_info",
                "exc_text",
                "stack_info",
                "extra",
            ]:
                log_data[key] = value

        return json.dumps(log_data)


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware to log HTTP requests and responses with structured JSON logging.
    Adds correlation IDs for distributed tracing.
    """

    async def dispatch(self, request: Request, call_next):
        """Process request and log details."""
        # Generate or extract correlation ID
        correlation_id = request.headers.get("X-Correlation-ID") or str(uuid.uuid4())
        correlation_id_var.set(correlation_id)

        # Capture request start time
        start_time = time.time()

        # Log incoming request
        logger = logging.getLogger("fornax_cutouts.request")
        request_data = {
            "event": "request_started",
            "method": request.method,
            "path": request.url.path,
            "query_params": str(request.query_params),
            "client_ip": request.client.host if request.client else None,
            "user_agent": request.headers.get("user-agent"),
            "correlation_id": correlation_id,
        }
        logger.info("HTTP request started", extra=request_data)

        # Process request
        try:
            response: Response = await call_next(request)

            # Calculate response time
            response_time = time.time() - start_time

            # Add correlation ID to response headers
            response.headers["X-Correlation-ID"] = correlation_id

            # Log response
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
            # Log exception
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


def setup_structured_logging(log_level: str = "INFO", format: str = "text") -> None:
    """
    Configure structured logging for the application.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format: Log format - "json" for JSON structured logging, "text" for plain text
    """
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level.upper())

    # Remove existing handlers
    root_logger.handlers.clear()

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level.upper())

    # Set formatter based on format
    if format == "json":
        formatter = StructuredJSONFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Configure uvicorn loggers
    for logger_name in ["uvicorn", "uvicorn.error"]:
        logger = logging.getLogger(logger_name)
        logger.setLevel(log_level.upper())
        logger.handlers.clear()
        logger.addHandler(console_handler)
        logger.propagate = False

    # Configure uvicorn.access separately to avoid conflicts with its custom formatter
    access_logger = logging.getLogger("uvicorn.access")
    if format == "json":
        # Disable uvicorn.access logging when using JSON format
        # The middleware handles request/response logging with structured data
        access_logger.setLevel(logging.CRITICAL)
        access_logger.handlers.clear()
        access_logger.propagate = False
    else:
        # Keep default access logging for text format
        access_logger.setLevel(log_level.upper())
        access_logger.propagate = False
