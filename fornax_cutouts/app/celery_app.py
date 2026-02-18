import logging
import ssl

from celery import Celery
from celery.signals import worker_process_init, worker_process_shutdown
from celery.utils.log import get_task_logger
from redis import Redis, RedisCluster

from fornax_cutouts.config import CONFIG
from fornax_cutouts.jobs.redis import sync_redis_client_factory
from fornax_cutouts.sources import cutout_registry

logger = get_task_logger("cutouts")

redis_client: Redis | RedisCluster | None= None

def redis_client_factory() -> Redis | RedisCluster:
    global redis_client

    if redis_client is None:
        redis_client = sync_redis_client_factory()

    return redis_client

celery_app = Celery(
    "fornax-cutouts",
    broker=CONFIG.redis.uri,
    backend=CONFIG.redis.uri,
    include=["fornax_cutouts.jobs.tasks"]
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


@worker_process_init.connect
def setup_worker_process(**kwargs):
    logging.getLogger("astrocut").setLevel(logging.ERROR)
    logging.getLogger("astropy").setLevel(logging.ERROR)

    cutout_registry.discover_sources()

    redis_client_factory()
    logger.info("Redis client setup complete")


@worker_process_shutdown.connect
def teardown_worker_process(**kwargs):
    redis_client_factory().close()
    logger.info("Redis client teardown complete")
