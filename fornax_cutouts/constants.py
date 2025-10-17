import os
from typing import Final

################
# Units
################

ARCMIN_TO_DEG: Final[float] = 1 / 60
ARCSEC_TO_DEG: Final[float] = ARCMIN_TO_DEG / 60


################
# Setup
################

ENVIRONMENT_NAME: Final[str] = os.getenv("ENVIRONMENT_NAME", "dev")
LOG_LEVEL: Final[str] = os.getenv("LOG_LEVEL", "info")


################
# Deployment
################

DEPLOYMENT_TYPE: Final[str] = os.getenv("DEPLOYMENT_TYPE", "local")
AWS_S3_REGION: Final[str] = os.getenv("AWS_S3_REGION", "us-east-1")


################
# Storage
################

SYNC_TTL: Final[int] = 1 * 60 * 60  # 1 Hour
ASYNC_TTL: Final[int] = 2 * 7 * 24 * 60 * 60  # 2 Weeks
CUTOUT_STORAGE_PREFIX: Final[str] = os.getenv("CUTOUT_STORAGE_PREFIX", "/tmp")
CUTOUT_STORAGE_IS_S3: Final[bool] = CUTOUT_STORAGE_PREFIX.startswith("s3://")


################
# Redis
################

REDIS_HOST: Final[str] = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT: Final[int] = int(os.getenv("REDIS_PORT", 6379))
REDIS_IS_CLUSTER: Final[bool] = os.getenv("REDIS_IS_CLUSTER", "FALSE").upper() == "TRUE"
REDIS_USE_SSL: Final[bool] = os.getenv("REDIS_USE_SSL", "FALSE").upper() == "TRUE"
WAIT_FOR_REDIS: Final[float] = float(os.getenv("WAIT_FOR_REDIS", 15))
REDIS_URI: Final[str] = f"redis{'s' if REDIS_USE_SSL else ''}://{REDIS_HOST}:{REDIS_PORT}/0"
