import os
from typing import Final

################
# Deployment
################

AWS_S3_REGION: Final[str] = os.getenv("AWS_S3_REGION", "us-east-1")
