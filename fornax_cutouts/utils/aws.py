import os

import boto3


def get_aws_credentials() -> tuple[str, str, str]:
    # Get credentials from the ECS IAM role
    session = boto3.Session()
    credentials = session.get_credentials().get_frozen_credentials()

    os.environ["AWS_ACCESS_KEY_ID"] = credentials.access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = credentials.secret_key
    if credentials.token:
        os.environ["AWS_SESSION_TOKEN"] = credentials.token

    return credentials.access_key, credentials.secret_key, credentials.token
