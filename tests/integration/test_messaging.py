from uuid import UUID, uuid4

import boto3
import pytest
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import EndpointConnectionError
from moto import mock_aws

from src.domain.execution import Execution
from src.infrastructure.database.bootstrap import create_execution_table
from src.infrastructure.database.dynamodb_repository import (
    DynamoDBExecutionRepository,
)
from src.infrastructure.messaging.consumer import SQSConsumer
from src.infrastructure.messaging.outbox_relay import (
    DynamoDBOutboxStore,
    OutboxRelay,
)
from src.infrastructure.messaging.sqs_client import SQSClient

TABLE_NAME = "execution-messaging-test"


@pytest.fixture
def infrastructure():
    with mock_aws(
        config={"core": {"service_whitelist": ["dynamodb", "sqs"]}}
    ):
        dynamodb = boto3.client(
            "dynamodb",
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )
        sqs = boto3.client(
            "sqs",
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )
        create_execution_table(dynamodb, TABLE_NAME)
        queue_url = sqs.create_queue(QueueName="execution-events")["QueueUrl"]
        yield dynamodb, sqs, queue_url


def execution() -> Execution:
    aggregate = Execution(
        id=UUID("50000000-0000-4000-8000-000000000005"),
        order_id=uuid4(),
        saga_id=uuid4(),
        correlation_id=uuid4(),
    )
    aggregate.start_diagnosis()
    return aggregate


def test_sqs_client_reads_elasticmq_endpoint_from_environment(monkeypatch):
    monkeypatch.setenv("SQS_ENDPOINT_URL", "http://elasticmq:9324")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "local")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "local")

    client = SQSClient.from_env()

    assert client.endpoint_url == "http://elasticmq:9324"


def outbox_item(dynamodb) -> dict:
    deserialize = TypeDeserializer().deserialize
    raw = next(
        item
        for item in dynamodb.scan(TableName=TABLE_NAME)["Items"]
        if item["entity_type"]["S"] == "OUTBOX"
    )
    return {key: deserialize(value) for key, value in raw.items()}


def test_dynamodb_relay_leases_publishes_and_marks_outbox(infrastructure):
    dynamodb, raw_sqs, queue_url = infrastructure
    repository = DynamoDBExecutionRepository(dynamodb, TABLE_NAME)
    repository.save_with_event(
        execution(),
        incoming_event_id=uuid4(),
        consumer="execution-test",
        outgoing_event_type="DiagnosisStartedV1",
        causation_id=uuid4(),
    )
    store = DynamoDBOutboxStore(dynamodb, TABLE_NAME)
    relay = OutboxRelay(store, SQSClient(raw_sqs), queue_url)

    assert len(store.claim_batch(limit=1, lease_seconds=30)) == 1
    assert store.claim_batch(limit=1, lease_seconds=30) == []
    leased = outbox_item(dynamodb)
    dynamodb.update_item(
        TableName=TABLE_NAME,
        Key={"PK": {"S": leased["PK"]}, "SK": {"S": leased["SK"]}},
        UpdateExpression="REMOVE lease_until",
    )
    assert relay.run_once() == 1

    assert outbox_item(dynamodb)["published"] is True
    received = SQSClient(raw_sqs).receive(queue_url)
    assert received[0].body["event_type"] == "DiagnosisStartedV1"


def test_dynamodb_relay_failure_records_retry(infrastructure):
    dynamodb, _, _ = infrastructure
    repository = DynamoDBExecutionRepository(dynamodb, TABLE_NAME)
    repository.save_with_event(
        execution(),
        incoming_event_id=uuid4(),
        consumer="execution-test",
        outgoing_event_type="DiagnosisStartedV1",
        causation_id=uuid4(),
    )

    class UnavailableSQS:
        def send(self, queue_url, payload):
            raise EndpointConnectionError(endpoint_url="http://unavailable")

    relay = OutboxRelay(
        DynamoDBOutboxStore(dynamodb, TABLE_NAME),
        UnavailableSQS(),
        "http://queue",
        base_retry_seconds=2,
    )

    assert relay.run_once() == 0
    item = outbox_item(dynamodb)
    assert item["attempts"] == 1
    assert "next_attempt_at" in item
    assert "lease_until" not in item


def test_consumer_keeps_handler_failure_for_retry(infrastructure):
    _, raw_sqs, queue_url = infrastructure
    sqs = SQSClient(raw_sqs)
    item = {
        "event_id": str(uuid4()),
        "event_type": "ExecutionRequestedV1",
        "event_version": 1,
        "occurred_at": "2026-11-11T12:00:00+00:00",
        "correlation_id": str(uuid4()),
        "causation_id": str(uuid4()),
        "saga_id": str(uuid4()),
        "order_id": str(uuid4()),
        "payload": {},
    }
    sqs.send(queue_url, item)
    consumer = SQSConsumer(sqs, queue_url)

    assert consumer.poll_once(lambda message: (_ for _ in ()).throw(
        RuntimeError("out of order")
    )) == "failed"
