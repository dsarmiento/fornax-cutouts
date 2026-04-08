import json
import time
import uuid
from typing import Any
from urllib.parse import parse_qs

from starlette.datastructures import UploadFile
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from fornax_cutouts.utils.logging import get_logger


def client_ip_from_request(request: Request) -> str | None:
    """Best-effort original client IP (e.g. behind ALB); falls back to the TCP peer."""
    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        first = forwarded_for.split(",")[0].strip()
        if first:
            return first
    real_ip = request.headers.get("x-real-ip")
    if real_ip:
        stripped = real_ip.strip()
        if stripped:
            return stripped
    return request.client.host if request.client else None


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return str(value)


def _serialize_form_item(value: Any) -> Any:
    if isinstance(value, UploadFile):
        return {
            "_upload": True,
            "filename": value.filename,
            "content_type": value.content_type,
        }
    return _json_safe(value)


async def request_log_context(request: Request) -> dict[str, Any]:
    """Serializable request slice for logs (reads body; Starlette caches it for the app)."""
    body = await request.body()
    body_params: dict[str, Any] = {}
    if body:
        content_type = request.headers.get("content-type", "")
        media_type = content_type.split(";")[0].strip().lower()
        if media_type == "application/json":
            try:
                parsed = json.loads(body)
                if isinstance(parsed, dict):
                    body_params = {str(k): _json_safe(v) for k, v in parsed.items()}
                else:
                    body_params = {"_json": _json_safe(parsed)}
            except json.JSONDecodeError:
                body_params = {"_invalid_json": True}
        elif media_type == "application/x-www-form-urlencoded":
            pairs = parse_qs(body.decode(errors="replace"), keep_blank_values=True)
            body_params = {
                k: (_serialize_form_item(vals[0]) if len(vals) == 1 else [_serialize_form_item(v) for v in vals])
                for k, vals in pairs.items()
            }
        elif media_type.startswith("multipart/form-data"):
            try:
                form = await request.form()
                body_params = {}
                for key in form.keys():
                    values = form.getlist(key)
                    serialized = [_serialize_form_item(v) for v in values]
                    body_params[key] = serialized[0] if len(serialized) == 1 else serialized
            except Exception:
                body_params = {"_form_parse_error": True}

    return {
        "client_ip": client_ip_from_request(request),
        "user_agent": request.headers.get("user-agent"),
        "method": request.method,
        "path": request.url.path,
        "query": request.url.query,
        "body_params": body_params,
    }


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

        request_block = await request_log_context(request)

        logger = get_logger()
        request_data = {
            "event": "request_started",
            "request": request_block,
            "correlation_id": correlation_id,
        }
        log_line = f"{request.method} {request.url.path} ({request_block['client_ip']})"
        logger.debug(log_line, extra=request_data)

        try:
            response: Response = await call_next(request)
            response_time_ms = round((time.time() - start_time) * 1000, 2)
            response_block = {
                "status_code": response.status_code,
                "response_time_ms": response_time_ms,
            }

            response.headers["X-Correlation-ID"] = correlation_id
            response_data = {
                "event": "request_completed",
                "request": request_block,
                "response": response_block,
                "correlation_id": correlation_id,
            }
            log_line = f"{response.status_code} {request.method} {request.url.path} ({request_block['client_ip']}) {response_time_ms}ms"
            logger.info(log_line, extra=response_data)

            return response

        except Exception as exc:
            response_time_ms = round((time.time() - start_time) * 1000, 2)
            error_data = {
                "event": "request_failed",
                "request": request_block,
                "response_time_ms": response_time_ms,
                "correlation_id": correlation_id,
                "error": str(exc),
                "error_type": type(exc).__name__,
            }
            log_line = f"{request.method} {request.url.path} ({request_block['client_ip']}) {response_time_ms}ms"
            logger.error(log_line, extra=error_data, exc_info=True)
            raise
