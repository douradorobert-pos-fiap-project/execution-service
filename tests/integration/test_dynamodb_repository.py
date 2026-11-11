from uuid import UUID, uuid4

import boto3
import pytest
from boto3.dynamodb.types import TypeDeserializer
from moto import mock_aws

from src.domain.execution import Execution
from src.infrastructure.database.bootstrap import create_execution_table
from src.infrastructure.database.dynamodb_repository import (
    ConcurrentExecutionUpdate,
    DuplicateEvent,
    DynamoDBExecutionRepository,
)

TABLE_NAME = "execution-service-test"
ORDER_ID = UUID("10000000-0000-4000-8000-000000000001")
SAGA_ID = UUID("20000000-0000-4000-8000-000000000002")
CORRELATION_ID = UUID("30000000-0000-4000-8000-000000000003")
SERVICE_ID = UUID("40000000-0000-4000-8000-000000000004")


@pytest.fixture
def dynamodb():
    with mock_aws(config={"core": {"service_whitelist": ["dynamodb"]}}):
        yield boto3.client(
            "dynamodb",
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )


@pytest.fixture
def repository(dynamodb) -> DynamoDBExecutionRepository:
    create_execution_table(dynamodb, TABLE_NAME)
    return DynamoDBExecutionRepository(dynamodb, TABLE_NAME)


def execution() -> Execution:
    return Execution(
        id=UUID("50000000-0000-4000-8000-000000000005"),
        order_id=ORDER_ID,
        saga_id=SAGA_ID,
        correlation_id=CORRELATION_ID,
    )


def persist(
    repository: DynamoDBExecutionRepository,
    aggregate: Execution,
    incoming_event_id: UUID,
    outgoing_event_type: str,
) -> None:
    repository.save_with_event(
        aggregate,
        incoming_event_id=incoming_event_id,
        consumer="execution-commands",
        outgoing_event_type=outgoing_event_type,
        causation_id=incoming_event_id,
    )


def decoded_items(dynamodb) -> list[dict]:
    deserialize = TypeDeserializer().deserialize
    return [
        {key: deserialize(value) for key, value in item.items()}
        for item in dynamodb.scan(TableName=TABLE_NAME)["Items"]
    ]


def test_save_atomically_persists_execution_inbox_and_outbox(
    repository, dynamodb
):
    aggregate = execution()
    incoming_event_id = uuid4()
    aggregate.start_diagnosis()

    persist(repository, aggregate, incoming_event_id, "DiagnosisStartedV1")

    saved = repository.get(aggregate.id)
    assert saved.status.value == "DIAGNOSING"
    assert saved.version == 1
    items = decoded_items(dynamodb)
    assert {(item["entity_type"], item["SK"]) for item in items} == {
        ("EXECUTION", "STATE"),
        ("INBOX", "CONSUMER#execution-commands"),
        ("OUTBOX", "EVENT"),
    }
    outbox = next(item for item in items if item["entity_type"] == "OUTBOX")
    assert outbox["event_type"] == "DiagnosisStartedV1"
    assert outbox["envelope"]["order_id"] == str(ORDER_ID)
    assert outbox["published"] is False


def test_duplicate_incoming_event_changes_nothing(repository, dynamodb):
    aggregate = execution()
    incoming_event_id = uuid4()
    aggregate.start_diagnosis()
    persist(repository, aggregate, incoming_event_id, "DiagnosisStartedV1")
    current = repository.get(aggregate.id)
    current.complete_diagnosis("Trocar pastilhas", [SERVICE_ID])

    with pytest.raises(DuplicateEvent):
        persist(repository, current, incoming_event_id, "DiagnosisCompletedV1")

    assert repository.get(aggregate.id).status.value == "DIAGNOSING"
    assert len(decoded_items(dynamodb)) == 3


def test_stale_version_rolls_back_inbox_and_outbox(repository, dynamodb):
    aggregate = execution()
    first_event = uuid4()
    aggregate.start_diagnosis()
    persist(repository, aggregate, first_event, "DiagnosisStartedV1")
    first_reader = repository.get(aggregate.id)
    stale_reader = repository.get(aggregate.id)
    first_reader.complete_diagnosis("Trocar pastilhas", [SERVICE_ID])
    persist(repository, first_reader, uuid4(), "DiagnosisCompletedV1")
    stale_reader.complete_diagnosis("Trocar disco", [SERVICE_ID])

    with pytest.raises(ConcurrentExecutionUpdate):
        persist(repository, stale_reader, uuid4(), "DiagnosisCompletedV1")

    assert repository.get(aggregate.id).diagnosis == "Trocar pastilhas"
    assert len(decoded_items(dynamodb)) == 5


def test_queue_index_returns_oldest_execution_first(repository):
    newer = execution()
    newer.id = uuid4()
    older = execution()
    older.id = uuid4()
    for aggregate in (newer, older):
        aggregate.start_diagnosis()
        aggregate.complete_diagnosis("Trocar pastilhas", [SERVICE_ID])
        aggregate.request_execution()
    newer.queued_at = "2026-11-11T12:00:00+00:00"
    older.queued_at = "2026-11-11T10:00:00+00:00"
    persist(repository, newer, uuid4(), "ExecutionQueuedV1")
    persist(repository, older, uuid4(), "ExecutionQueuedV1")

    queued = repository.list_queue()

    assert [item.id for item in queued] == [older.id, newer.id]
