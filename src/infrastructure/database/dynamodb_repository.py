from datetime import UTC, datetime
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError

from src.domain.execution import Execution, ExecutionStatus


class DuplicateEvent(RuntimeError):
    pass


class ConcurrentExecutionUpdate(RuntimeError):
    pass


class DynamoDBExecutionRepository:
    def __init__(self, client: Any, table_name: str) -> None:
        self._client = client
        self._table_name = table_name
        self._serialize = TypeSerializer().serialize
        self._deserialize = TypeDeserializer().deserialize

    def get(self, execution_id: UUID) -> Execution | None:
        response = self._client.get_item(
            TableName=self._table_name,
            Key=self._item({"PK": f"EXECUTION#{execution_id}", "SK": "STATE"}),
            ConsistentRead=True,
        )
        item = response.get("Item")
        return self._to_execution(item) if item else None

    def save_with_event(
        self,
        execution: Execution,
        *,
        incoming_event_id: UUID,
        consumer: str,
        outgoing_event_type: str,
        causation_id: UUID,
    ) -> None:
        inbox_key = {
            "PK": f"INBOX#{incoming_event_id}",
            "SK": f"CONSUMER#{consumer}",
        }
        if self._client.get_item(
            TableName=self._table_name,
            Key=self._item(inbox_key),
            ConsistentRead=True,
        ).get("Item"):
            raise DuplicateEvent(str(incoming_event_id))

        expected_version = execution.version
        next_version = expected_version + 1
        event_id = uuid5(
            NAMESPACE_URL,
            f"{execution.id}:{outgoing_event_type}:{incoming_event_id}",
        )
        now = datetime.now(UTC).isoformat()
        state_item = self._execution_item(execution, next_version)
        inbox_item = {
            **inbox_key,
            "entity_type": "INBOX",
            "event_id": str(incoming_event_id),
            "consumer": consumer,
            "processed_at": now,
        }
        outbox_item = {
            "PK": f"OUTBOX#{event_id}",
            "SK": "EVENT",
            "entity_type": "OUTBOX",
            "event_id": str(event_id),
            "event_type": outgoing_event_type,
            "aggregate_id": str(execution.id),
            "published": False,
            "attempts": 0,
            "envelope": {
                "event_id": str(event_id),
                "event_type": outgoing_event_type,
                "event_version": 1,
                "occurred_at": now,
                "correlation_id": str(execution.correlation_id),
                "causation_id": str(causation_id),
                "saga_id": str(execution.saga_id),
                "order_id": str(execution.order_id),
                "payload": {
                    "execution_id": str(execution.id),
                    "status": execution.status.value,
                },
            },
        }
        state_put: dict[str, Any] = {
            "TableName": self._table_name,
            "Item": self._item(state_item),
        }
        if expected_version == 0:
            state_put["ConditionExpression"] = "attribute_not_exists(PK)"
        else:
            state_put["ConditionExpression"] = "version = :expected_version"
            state_put["ExpressionAttributeValues"] = {
                ":expected_version": self._serialize(expected_version)
            }

        try:
            self._client.transact_write_items(
                TransactItems=[
                    {"Put": state_put},
                    {
                        "Put": {
                            "TableName": self._table_name,
                            "Item": self._item(inbox_item),
                            "ConditionExpression": "attribute_not_exists(PK)",
                        }
                    },
                    {
                        "Put": {
                            "TableName": self._table_name,
                            "Item": self._item(outbox_item),
                            "ConditionExpression": "attribute_not_exists(PK)",
                        }
                    },
                ]
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "TransactionCanceledException":
                raise ConcurrentExecutionUpdate(str(execution.id)) from exc
            raise
        execution.version = next_version

    def list_queue(self) -> list[Execution]:
        response = self._client.query(
            TableName=self._table_name,
            IndexName="QueueIndex",
            KeyConditionExpression="GSI1PK = :queue",
            ExpressionAttributeValues={
                ":queue": self._serialize("STATUS#QUEUED")
            },
            ScanIndexForward=True,
        )
        return [self._to_execution(item) for item in response["Items"]]

    def _execution_item(
        self, execution: Execution, version: int
    ) -> dict[str, Any]:
        item: dict[str, Any] = {
            "PK": f"EXECUTION#{execution.id}",
            "SK": "STATE",
            "entity_type": "EXECUTION",
            "id": str(execution.id),
            "order_id": str(execution.order_id),
            "saga_id": str(execution.saga_id),
            "correlation_id": str(execution.correlation_id),
            "status": execution.status.value,
            "recommended_service_ids": [
                str(service_id) for service_id in execution.recommended_service_ids
            ],
            "version": version,
        }
        optional = {
            "diagnosis": execution.diagnosis,
            "repair_notes": execution.repair_notes,
            "failure_reason": execution.failure_reason,
            "queued_at": execution.queued_at,
        }
        item.update({key: value for key, value in optional.items() if value is not None})
        if execution.status is ExecutionStatus.QUEUED:
            item["GSI1PK"] = "STATUS#QUEUED"
            item["GSI1SK"] = execution.queued_at
        return item

    def _to_execution(self, raw_item: dict[str, Any]) -> Execution:
        item = {
            key: self._deserialize(value) for key, value in raw_item.items()
        }
        return Execution(
            id=UUID(item["id"]),
            order_id=UUID(item["order_id"]),
            saga_id=UUID(item["saga_id"]),
            correlation_id=UUID(item["correlation_id"]),
            status=ExecutionStatus(item["status"]),
            diagnosis=item.get("diagnosis"),
            recommended_service_ids=[
                UUID(service_id)
                for service_id in item.get("recommended_service_ids", [])
            ],
            repair_notes=item.get("repair_notes"),
            failure_reason=item.get("failure_reason"),
            queued_at=item.get("queued_at"),
            version=int(item["version"]),
        )

    def _item(self, values: dict[str, Any]) -> dict[str, Any]:
        return {key: self._serialize(value) for key, value in values.items()}
