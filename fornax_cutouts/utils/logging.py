import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Literal

from fornax_cutouts.config import CONFIG
from fornax_cutouts.constants import LOGGER_NAME


class StructuredJSONFormatter(logging.Formatter):

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
        }

        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        if hasattr(record, "extra"):
            log_data.update(record.extra)

        # Add extra fields from record
        for key, value in record.__dict__.items():
            if key not in [
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
            ]:
                log_data[key] = value

        return json.dumps(log_data)


def setup_structured_logging(service: Literal["api", "worker"]):
    log_level = CONFIG.log.level.upper()
    log_format = CONFIG.log.format.upper()

    logger = logging.getLogger(LOGGER_NAME)
    logger.handlers.clear()
    logger.setLevel(log_level)
    logger.propagate = False

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)

    if log_format == "JSON":
        console_handler.setFormatter(StructuredJSONFormatter())
        if service == "api":
            logging.getLogger("uvicorn").handlers[0].setFormatter(StructuredJSONFormatter())

    elif log_format == "TEXT" and service == "api":
        formatter = logging.getLogger("uvicorn").handlers[0].formatter
        console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
