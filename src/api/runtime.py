import os
from typing import Any

import boto3

from src.api.main import create_app
from src.infrastructure.database.dynamodb_repository import DynamoDBExecutionRepository


def create_runtime_app():
    return create_app(create_repository())


def create_repository() -> DynamoDBExecutionRepository:
    return DynamoDBExecutionRepository(
        create_dynamodb_client(), os.getenv("DYNAMODB_TABLE", "execution-service")
    )


def create_dynamodb_client() -> Any:
    endpoint_url = os.getenv("DYNAMODB_ENDPOINT_URL") or None
    options: dict[str, Any] = {
        "region_name": os.getenv("AWS_REGION", "us-east-1"),
        "endpoint_url": endpoint_url,
    }
    if endpoint_url:
        options.update(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "local"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "local"),
        )
    client = boto3.client("dynamodb", **options)
    return client
