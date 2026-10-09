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

################
# S3FS Configuration
################

S3FS_BLOCK_SIZE_MIB: Final[float] = float(os.environ.get("S3FS_BLOCK_SIZE", "1.0"))
S3FS_BLOCK_SIZE_BYTES: Final[int | None] = int(S3FS_BLOCK_SIZE_MIB * 1024 * 1024) if S3FS_BLOCK_SIZE_MIB > 0 else None
