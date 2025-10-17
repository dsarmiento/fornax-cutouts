import ssl

from celery import Celery

from fornax_cutouts.constants import REDIS_URI, REDIS_USE_SSL

celery_app = Celery("fornax-cutouts", broker=REDIS_URI, backend=REDIS_URI)

conf_update = {
    "broker_transport_options": {
        "global_keyprefix": "celery:broker:",
    },
    "result_backend_transport_options": {
        "global_keyprefix": "celery:results:",
    },
    "result_expires": 1 * 60 * 60,  # 1 Hour
}

if REDIS_USE_SSL:
    conf_update["broker_use_ssl"] = {"ssl_cert_reqs": ssl.CERT_NONE}
    conf_update["redis_backend_use_ssl"] = {"ssl_cert_reqs": ssl.CERT_NONE}

celery_app.conf.update(**conf_update)
