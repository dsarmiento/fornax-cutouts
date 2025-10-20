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


################
# Deployment
################

DEPLOYMENT_TYPE: Final[str] = os.getenv("DEPLOYMENT_TYPE", "local")
AWS_S3_REGION: Final[str] = os.getenv("AWS_S3_REGION", "us-east-1")


################
# Storage
################

CUTOUT_STORAGE_PREFIX: Final[str] = os.getenv("CUTOUT_STORAGE_PREFIX", "/tmp")
CUTOUT_STORAGE_IS_S3: Final[bool] = CUTOUT_STORAGE_PREFIX.startswith("s3://")
