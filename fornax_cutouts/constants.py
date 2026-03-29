import os
from typing import Final

################
# Units
################

ARCMIN_TO_DEG: Final[float] = 1 / 60
ARCSEC_TO_DEG: Final[float] = ARCMIN_TO_DEG / 60


################
# Deployment
################

AWS_S3_REGION: Final[str] = os.getenv("AWS_S3_REGION", "us-east-1")
