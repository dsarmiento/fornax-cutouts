import logging
import os
import ssl
import sys

from celery import Celery
from celery.signals import worker_process_init, worker_process_shutdown
from celery.utils.log import get_task_logger
from redis import Redis, RedisCluster

from fornax_cutouts.config import CONFIG
from fornax_cutouts.jobs.redis import sync_redis_client_factory
from fornax_cutouts.sources import cutout_registry
from fornax_cutouts.utils.logging import StructuredJSONFormatter

logger = get_task_logger("cutouts")

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
    },
    "result_backend_transport_options": {
        "global_keyprefix": f"{CONFIG.worker.redis_prefix}:celery:results:",
    },
    "result_expires": 1 * 60 * 60,  # 1 Hour,
    # Worker memory management
    "task_acks_late": True,
    "worker_prefetch_multiplier": CONFIG.worker.prefetch_multiplier,
    "worker_max_tasks_per_child": CONFIG.worker.max_tasks_per_child,
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


@worker_process_init.connect
def setup_worker_process(**kwargs):
    logging.getLogger("astrocut").setLevel(logging.ERROR)
    logging.getLogger("astropy").setLevel(logging.ERROR)

    cutout_registry.discover_sources()

    redis_client_factory()
    logger.info("Redis client setup complete")

    _monkey_patch_astrocut()
    logger.info("Astrocut monkey patch complete")


@worker_process_shutdown.connect
def teardown_worker_process(**kwargs):
    redis_client_factory().close()
    logger.info("Redis client teardown complete")


def _monkey_patch_astrocut():
    """
    If block_size_mib is set, monkeypatch astrocut to pass block_size in fsspec_kwargs.
    """
    block_size_mib = os.environ.get("S3FS_BLOCK_SIZE")
    block_size_mib = float(block_size_mib) if block_size_mib is not None else 1.0
    if block_size_mib <= 0:
        return
    block_size_bytes = int(block_size_mib * 1024 * 1024)
    try:
        import astrocut.fits_cutout as fits_cutout
        import numpy as np
        from astropy.io import fits

        _orig_load = fits_cutout.FITSCutout._load_file_data

        def _patched_load(self, input_file):
            fsspec_kwargs = (
                {
                    "anon": True,
                    "default_block_size": block_size_bytes,
                }
                if "s3://" in str(input_file)
                else None
            )
            hdulist = fits.open(input_file, mode="denywrite", memmap=True, fsspec_kwargs=fsspec_kwargs)
            infile_exts = np.where([hdu.is_image and hdu.size > 0 for hdu in hdulist])[0]
            cutout_inds = self._parse_extensions(input_file, infile_exts)
            return (hdulist, cutout_inds)

        fits_cutout.FITSCutout._load_file_data = _patched_load  # type: ignore[method-assign]
    except Exception as e:
        logger.warning("Could not apply s3fs block_size override: %s", e)


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
