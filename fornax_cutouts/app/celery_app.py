import ssl

from celery import Celery
from celery.utils.log import get_task_logger
from kombu.serialization import register

from fornax_cutouts.config import CONFIG
from fornax_cutouts.utils.pydantic_serializer import pydantic_dumps, pydantic_loads

logger = get_task_logger("cutouts")

register(
    "pydantic",
    pydantic_dumps,
    pydantic_loads,
    content_type="application/x-pydantic",
    content_encoding="utf-8",
)

celery_app = Celery(
    "fornax-cutouts",
    broker=CONFIG.redis.uri,
    backend=CONFIG.redis.uri,
)

conf_update = {
    # Redis Options
    "broker_transport_options": {
        "global_keyprefix": "celery:broker:",
    },
    "result_backend_transport_options": {
        "global_keyprefix": "celery:results:",
    },
    "result_expires": 1 * 60 * 60,  # 1 Hour,

    # Pydantic serialization
    "task_serializer": "pydantic",
    "result_serializer": "pydantic",
    "event_serializer": "pydantic",
    "accept_content": ["application/json", "application/x-pydantic"],
    "result_accept_content": ["application/json", "application/x-pydantic"],
}

if CONFIG.redis.use_ssl:
    conf_update["broker_use_ssl"] = {"ssl_cert_reqs": ssl.CERT_NONE}
    conf_update["redis_backend_use_ssl"] = {"ssl_cert_reqs": ssl.CERT_NONE}

celery_app.conf.update(**conf_update)
