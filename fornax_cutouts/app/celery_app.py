import ssl

from celery import Celery
from celery.utils.log import get_task_logger

from fornax_cutouts.config import CONFIG

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
    # Worker memory management
    "worker_prefetch_multiplier": 1,  # Only prefetch 1 task at a time to prevent memory buildup
    "task_acks_late": True,  # Acknowledge tasks after completion, not before
    "worker_max_tasks_per_child": 50,  # Restart worker after N tasks to prevent memory leaks
}

if CONFIG.redis.use_ssl:
    conf_update["broker_use_ssl"] = {"ssl_cert_reqs": ssl.CERT_NONE}
    conf_update["redis_backend_use_ssl"] = {"ssl_cert_reqs": ssl.CERT_NONE}

celery_app.conf.update(**conf_update)
