import os
import time

from src.api.runtime import create_dynamodb_client
from src.infrastructure.database.dynamodb_repository import (
    DynamoDBExecutionRepository,
)
from src.infrastructure.messaging.consumer import SQSConsumer
from src.infrastructure.messaging.handler import process_event
from src.infrastructure.messaging.outbox_relay import (
    DynamoDBOutboxStore,
    OutboxRelay,
)
from src.infrastructure.messaging.sqs_client import (
    SQS_LONG_POLL_WAIT_SECONDS,
    SQSClient,
)
from src.infrastructure.observability.logging import log_event

OUTBOUND_EVENT_TYPES = (
    "DiagnosisStartedV1",
    "DiagnosisCompletedV1",
    "ExecutionQueuedV1",
    "ExecutionStartedV1",
    "ExecutionCompletedV1",
    "ExecutionFailedV1",
    "ExecutionCancelledV1",
)


def main() -> None:
    environment = os.getenv("APP_ENV", "local")
    table_name = os.getenv("DYNAMODB_TABLE", "execution-service")
    dynamodb = create_dynamodb_client()
    repository = DynamoDBExecutionRepository(dynamodb, table_name)
    sqs = SQSClient.from_env()
    queue_urls = {
        service: sqs.queue_url(f"{environment}-{service}-events")
        for service in ("os-service", "execution-service")
    }
    routes = {
        event_type: queue_urls["os-service"] for event_type in OUTBOUND_EVENT_TYPES
    }
    relay = OutboxRelay(
        DynamoDBOutboxStore(dynamodb, table_name),
        sqs,
        routes,
    )
    consumer = SQSConsumer(
        sqs,
        queue_urls["execution-service"],
        wait_time_seconds=SQS_LONG_POLL_WAIT_SECONDS,
    )
    log_event("worker.started", service="execution-service")

    while True:
        relay.run_once()
        result = consumer.poll_once(lambda message: process_event(repository, message))
        if result == "empty":
            time.sleep(0.5)


if __name__ == "__main__":
    main()
