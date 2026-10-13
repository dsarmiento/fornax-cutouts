import signal
import ssl

from celery import Celery
from celery.signals import setup_logging, worker_process_init, worker_process_shutdown
from celery.worker import state as celery_worker_state
from redis import Redis, RedisCluster

from fornax_cutouts.app.discovery import discover_sources
from fornax_cutouts.config import CONFIG
from fornax_cutouts.jobs.redis import sync_redis_client_factory
from fornax_cutouts.utils.logging import get_logger, setup_worker_logging

logger = get_logger()

redis_client: Redis | RedisCluster | None = None


def redis_client_factory() -> Redis | RedisCluster:
    global redis_client

    if redis_client is None:
        redis_client = sync_redis_client_factory()

    return redis_client


celery_app = Celery(
    "fornax-cutouts",
    broker=CONFIG.redis.uri,
    backend=CONFIG.redis.uri,
    include=["fornax_cutouts.jobs.tasks"],
)

conf_update = {
    # Redis Options
    "broker_transport_options": {
        "global_keyprefix": f"{CONFIG.worker.redis_prefix}:celery:broker:",
        "queue_order_strategy": "priority",
        "priority_steps": list(range(3)),
        "sep": ":",
    },
    "task_default_priority": 1,
    "result_backend_transport_options": {
        "global_keyprefix": f"{CONFIG.worker.redis_prefix}:celery:results:",
    },
    "result_expires": 1 * 60 * 60,  # 1 Hour,
    # Worker memory management
    "task_acks_late": True,
    "task_reject_on_worker_lost": True,
    "worker_prefetch_multiplier": CONFIG.worker.prefetch_multiplier,
    "worker_max_tasks_per_child": CONFIG.worker.max_tasks_per_child,
}

if CONFIG.redis.use_ssl:
    conf_update["broker_use_ssl"] = {"ssl_cert_reqs": ssl.CERT_NONE}
    conf_update["redis_backend_use_ssl"] = {"ssl_cert_reqs": ssl.CERT_NONE}

celery_app.conf.update(**conf_update)


@setup_logging.connect
def configure_logging(**kwargs):
    """Configure logging before Celery starts its worker pool.

    Connecting to this signal causes Celery to skip its own logging setup
    entirely. Runs in the main process before workers are forked, so all
    worker processes inherit the configured loggers.
    """
    setup_worker_logging()


def _register_worker_sigterm_handler():
    """Reject reserved tasks before ECS SIGKILL."""

    def handle_sigterm(signum, frame):
        num_reserved_tasks = len(celery_worker_state.reserved_requests)
        if num_reserved_tasks > 0:
            try:
                logger.warning(
                    f"SIGTERM received: requeueing ({num_reserved_tasks}) reserved tasks and exiting worker child",
                    extra={"event": "worker_sigterm", "num_reserved_tasks": num_reserved_tasks},
                )
                for req in list(celery_worker_state.reserved_requests):
                    try:
                        req.reject(requeue=True)
                    except Exception as e:
                        logger.error(f"Failed to reject reserved task during SIGTERM: {e}")
            except Exception as e:
                logger.error(f"SIGTERM handler error: {e}")
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)


@worker_process_init.connect
def setup_worker_process(**kwargs):
    discover_sources()

    redis_client_factory()
    logger.debug("Redis client setup complete")

    _register_worker_sigterm_handler()


@worker_process_shutdown.connect
def teardown_worker_process(**kwargs):
    redis_client_factory().close()
    logger.debug("Redis client teardown complete")


def get_pool_size_for_queue(queue_name: str) -> int:
    inspector = celery_app.control.inspect()
    active_queues = inspector.active_queues()
    stats = inspector.stats()

    if not active_queues or not stats:
        return 0

    total = 0
    for node_name, queues in active_queues.items():
        queue_names = [q["name"] for q in queues]
        if queue_name in queue_names and node_name in stats:
            pool_size = stats[node_name].get("pool", {}).get("max-concurrency", 0)
            total += pool_size

    return total
