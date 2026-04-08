import json
import logging
import sys
import warnings
from datetime import datetime, timezone
from typing import Any

from fornax_cutouts.config import CONFIG

_STANDARD_LOG_RECORD_FIELDS = frozenset(
    {
        "timestamp",
        "level",
        "logger",
        "message",
        "color_message",
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "taskName",
        "extra",
    }
)


def get_logger() -> logging.Logger:
    """Get the unified logger for fornax cutouts."""
    return logging.getLogger(CONFIG.log.name)


class CeleryTaskContextFilter(logging.Filter):
    """Injects the active Celery task ID and name into every log record.

    Attach to worker log handlers so that task_id and task_name appear in all
    log output emitted during task execution, even from non-task loggers.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            from celery import current_task

            request = current_task.request
            record.task_id = request.id
            record.task_name = current_task.name
        except Exception:
            pass
        return True


class StructuredJSONFormatter(logging.Formatter):
    def __init__(self, app: str) -> None:
        super().__init__()
        self.app = app

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "app": self.app,
            "message": record.getMessage(),
        }

        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        if hasattr(record, "extra"):
            log_data.update(record.extra)

        for key, value in record.__dict__.items():
            if key not in _STANDARD_LOG_RECORD_FIELDS:
                log_data[key] = value

        return json.dumps(log_data)


def _make_handler(log_level: str, log_format: str, app: str) -> logging.StreamHandler:
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)
    if log_format == "JSON":
        handler.setFormatter(StructuredJSONFormatter(app=app))
    return handler


def _configure_logger(
    logger: logging.Logger,
    log_level: str,
    log_format: str,
    *,
    filter: logging.Filter | None = None,
    app: str,
) -> None:
    logger.setLevel(log_level)
    logger.propagate = False

    if not logger.handlers:
        logger.addHandler(_make_handler(log_level, log_format, app=app))

    for handler in logger.handlers:
        handler.setLevel(log_level)
        if log_format == "JSON":
            handler.setFormatter(StructuredJSONFormatter(app=app))
        if filter is not None and filter not in handler.filters:
            handler.addFilter(filter)


def setup_api_logging() -> None:
    """Configure logging for the FastAPI API service.

    Call from the FastAPI lifespan context manager before serving requests.
    """
    disable_astro_warnings()

    log_level = CONFIG.log.level.upper()
    log_format = CONFIG.log.format.upper()

    _configure_logger(get_logger(), log_level, log_format, app="api")

    uvicorn_logger = logging.getLogger("uvicorn")
    if uvicorn_logger.handlers:
        if log_format == "JSON":
            uvicorn_logger.handlers[0].setFormatter(StructuredJSONFormatter(app="api"))
        elif log_format == "TEXT":
            formatter = uvicorn_logger.handlers[0].formatter
            get_logger().handlers[0].setFormatter(formatter)


def setup_worker_logging() -> None:
    """Configure logging for the Celery worker.

    Called via the setup_logging signal, which fires in the main worker
    process before the prefork pool is created. Connecting to that signal
    causes Celery to skip its own logging setup entirely. All forked worker
    processes inherit this configuration.

    Attaches CeleryTaskContextFilter so that task_id and task_name are
    included in every log record emitted during task execution.
    """
    log_level = CONFIG.log.level.upper()
    log_format = CONFIG.log.format.upper()

    disable_astro_warnings()

    task_filter = CeleryTaskContextFilter()
    for logger_name in ("celery", "celery.task", "celery.worker", CONFIG.log.name):
        _configure_logger(logging.getLogger(logger_name), log_level, log_format, filter=task_filter, app="worker")


def disable_astro_warnings() -> None:
    logging.getLogger("astrocut").setLevel(logging.ERROR)
    logging.getLogger("astropy").setLevel(logging.ERROR)
    warnings.filterwarnings("ignore", module="astropy.*")
    warnings.filterwarnings("ignore", module="astrocut.*")
