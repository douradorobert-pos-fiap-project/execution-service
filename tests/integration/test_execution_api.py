from uuid import UUID, uuid4

import boto3
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws

from src.api.main import create_app
from src.infrastructure.database.bootstrap import create_execution_table
from src.infrastructure.database.dynamodb_repository import (
    DynamoDBExecutionRepository,
)

TABLE_NAME = "execution-api-test"
ORDER_ID = UUID("10000000-0000-4000-8000-000000000001")
SAGA_ID = UUID("20000000-0000-4000-8000-000000000002")
CORRELATION_ID = UUID("30000000-0000-4000-8000-000000000003")
SERVICE_ID = UUID("40000000-0000-4000-8000-000000000004")


@pytest.fixture
def api():
    with mock_aws(config={"core": {"service_whitelist": ["dynamodb"]}}):
        dynamodb = boto3.client(
            "dynamodb",
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )
        create_execution_table(dynamodb, TABLE_NAME)
        repository = DynamoDBExecutionRepository(dynamodb, TABLE_NAME)
        with TestClient(create_app(repository)) as client:
            yield client, dynamodb


def command_headers(event_id: UUID | None = None) -> dict[str, str]:
    return {
        "X-Event-ID": str(event_id or uuid4()),
        "X-Correlation-ID": str(CORRELATION_ID),
    }


def start_diagnosis(client: TestClient) -> dict:
    response = client.post(
        "/api/v1/executions/diagnosis",
        headers=command_headers(),
        json={"order_id": str(ORDER_ID), "saga_id": str(SAGA_ID)},
    )
    assert response.status_code == 201
    return response.json()


def complete_diagnosis(client: TestClient, execution_id: str) -> dict:
    response = client.post(
        f"/api/v1/executions/{execution_id}/diagnosis/complete",
        headers=command_headers(),
        json={
            "diagnosis": "Trocar pastilhas",
            "recommended_service_ids": [str(SERVICE_ID)],
        },
    )
    assert response.status_code == 200
    return response.json()


def queue_execution(client: TestClient, execution_id: str) -> dict:
    response = client.post(
        f"/api/v1/executions/{execution_id}/queue",
        headers=command_headers(),
    )
    assert response.status_code == 200
    return response.json()


def test_operational_api_runs_diagnosis_queue_and_repair(api):
    client, dynamodb = api
    execution = start_diagnosis(client)
    assert execution["status"] == "DIAGNOSING"
    execution = complete_diagnosis(client, execution["id"])
    assert execution["status"] == "DIAGNOSED"
    execution = queue_execution(client, execution["id"])
    assert execution["status"] == "QUEUED"

    queue = client.get("/api/v1/executions/queue")
    assert queue.status_code == 200
    assert [item["id"] for item in queue.json()] == [execution["id"]]

    started = client.post(
        f"/api/v1/executions/{execution['id']}/repair/start",
        headers=command_headers(),
    )
    assert started.status_code == 200
    assert started.json()["status"] == "IN_PROGRESS"
    completed = client.post(
        f"/api/v1/executions/{execution['id']}/repair/complete",
        headers=command_headers(),
        json={"notes": "Pastilhas substituídas e testadas"},
    )
    assert completed.status_code == 200
    assert completed.json()["status"] == "COMPLETED"

    detail = client.get(f"/api/v1/executions/{execution['id']}")
    assert detail.status_code == 200
    assert detail.json()["repair_notes"] == "Pastilhas substituídas e testadas"
    outbox_types = {
        item["event_type"]["S"]
        for item in dynamodb.scan(TableName=TABLE_NAME)["Items"]
        if item["entity_type"]["S"] == "OUTBOX"
    }
    assert outbox_types == {
        "DiagnosisStartedV1",
        "DiagnosisCompletedV1",
        "ExecutionQueuedV1",
        "ExecutionStartedV1",
        "ExecutionCompletedV1",
    }
    diagnosis_event = next(
        item
        for item in dynamodb.scan(TableName=TABLE_NAME)["Items"]
        if item.get("event_type", {}).get("S") == "DiagnosisCompletedV1"
    )
    assert diagnosis_event["envelope"]["M"]["payload"]["M"]["recommended_service_ids"][
        "L"
    ][0]["S"] == str(SERVICE_ID)


def test_api_rejects_compensation_after_repair_starts(api):
    client, _ = api
    execution = start_diagnosis(client)
    complete_diagnosis(client, execution["id"])
    queue_execution(client, execution["id"])
    started = client.post(
        f"/api/v1/executions/{execution['id']}/repair/start",
        headers=command_headers(),
    )

    cancelled = client.post(
        f"/api/v1/executions/{execution['id']}/cancel",
        headers=command_headers(),
        json={"reason": "Saga compensated"},
    )

    assert started.status_code == 200
    assert cancelled.status_code == 409
    assert "cannot be compensated" in cancelled.json()["detail"]
    detail = client.get(f"/api/v1/executions/{execution['id']}")
    assert detail.json()["status"] == "IN_PROGRESS"


def test_api_records_repair_failure_and_event(api):
    client, dynamodb = api
    execution = start_diagnosis(client)
    complete_diagnosis(client, execution["id"])
    queue_execution(client, execution["id"])
    client.post(
        f"/api/v1/executions/{execution['id']}/repair/start",
        headers=command_headers(),
    )

    failed = client.post(
        f"/api/v1/executions/{execution['id']}/repair/fail",
        headers=command_headers(),
        json={"reason": "Peça incompatível"},
    )

    assert failed.status_code == 200
    assert failed.json()["status"] == "FAILED"
    assert failed.json()["failure_reason"] == "Peça incompatível"
    event_types = {
        item["event_type"]["S"]
        for item in dynamodb.scan(TableName=TABLE_NAME)["Items"]
        if item["entity_type"]["S"] == "OUTBOX"
    }
    assert "ExecutionFailedV1" in event_types
