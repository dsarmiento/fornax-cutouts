import logging
import ssl
import sys

from celery import Celery
from celery.utils.log import get_task_logger

from fornax_cutouts.config import CONFIG
from fornax_cutouts.utils.logging import StructuredJSONFormatter

logger = get_task_logger("cutouts")

celery_app = Celery(
    "fornax-cutouts",
    broker=CONFIG.redis.uri,
    backend=CONFIG.redis.uri,
    include=["fornax_cutouts.tasks"]
)

conf_update = {
    # Redis Options
    "broker_transport_options": {
        "global_keyprefix": f"{CONFIG.worker.redis_prefix}:celery:broker:",
    },
    "result_backend_transport_options": {
        "global_keyprefix": f"{CONFIG.worker.redis_prefix}:celery:results:",
    },
    "result_expires": 1 * 60 * 60,  # 1 Hour,
}

if CONFIG.redis.use_ssl:
    conf_update["broker_use_ssl"] = {"ssl_cert_reqs": ssl.CERT_NONE}
    conf_update["redis_backend_use_ssl"] = {"ssl_cert_reqs": ssl.CERT_NONE}

celery_app.conf.update(**conf_update)

# Configure structured logging for Celery loggers
if CONFIG.log.format == "json":
    # Disable Celery's default logging setup to prevent interference
    celery_app.conf.update(
        worker_hijack_root_logger=False,
        worker_log_format="",
        worker_task_log_format="",
    )
    # Configure Celery loggers to use JSON formatter
    log_level = CONFIG.log.level.upper()

    # Create console handler with JSON formatter
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(StructuredJSONFormatter())

    # Configure Celery loggers
    for logger_name in ["celery", "celery.task", "celery.worker"]:
        celery_logger = logging.getLogger(logger_name)
        celery_logger.setLevel(log_level)
        celery_logger.handlers.clear()
        celery_logger.addHandler(console_handler)
        celery_logger.propagate = False

    # Configure the cutouts task logger
    cutouts_logger = logging.getLogger("cutouts")
    cutouts_logger.setLevel(log_level)
    cutouts_logger.handlers.clear()
    cutouts_logger.addHandler(console_handler)
    cutouts_logger.propagate = False
