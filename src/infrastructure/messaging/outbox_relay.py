from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError


@dataclass(frozen=True)
class OutboxRecord:
    id: str
    pk: str
    sk: str
    payload: dict[str, Any]
    attempts: int


class DynamoDBOutboxStore:
    def __init__(self, client: Any, table_name: str) -> None:
        self._client = client
        self._table_name = table_name
        self._serialize = TypeSerializer().serialize
        self._deserialize = TypeDeserializer().deserialize

    def claim_batch(self, limit: int, lease_seconds: int) -> list[OutboxRecord]:
        now = datetime.now(UTC)
        raw_items = self._client.scan(
            TableName=self._table_name,
            FilterExpression="entity_type = :outbox AND published = :false",
            ExpressionAttributeValues={
                ":outbox": self._serialize("OUTBOX"),
                ":false": self._serialize(False),
            },
        ).get("Items", [])
        records: list[OutboxRecord] = []
        for raw in raw_items:
            if len(records) >= limit:
                break
            item = {key: self._deserialize(value) for key, value in raw.items()}
            next_attempt = item.get("next_attempt_at")
            if next_attempt and next_attempt > now.isoformat():
                continue
            lease_until = (now + timedelta(seconds=lease_seconds)).isoformat()
            try:
                self._client.update_item(
                    TableName=self._table_name,
                    Key={"PK": raw["PK"], "SK": raw["SK"]},
                    UpdateExpression="SET lease_until = :lease",
                    ConditionExpression=(
                        "(attribute_not_exists(lease_until) OR lease_until < :now) "
                        "AND published = :false"
                    ),
                    ExpressionAttributeValues={
                        ":lease": self._serialize(lease_until),
                        ":now": self._serialize(now.isoformat()),
                        ":false": self._serialize(False),
                    },
                )
            except ClientError as exc:
                if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    continue
                raise
            records.append(
                OutboxRecord(
                    id=item["event_id"],
                    pk=item["PK"],
                    sk=item["SK"],
                    payload=self._json_compatible(item["envelope"]),
                    attempts=int(item["attempts"]),
                )
            )
        return records

    def mark_published(self, record: OutboxRecord) -> None:
        self._client.update_item(
            TableName=self._table_name,
            Key=self._key(record),
            UpdateExpression="SET published = :true, published_at = :now REMOVE lease_until",
            ExpressionAttributeValues={
                ":true": self._serialize(True),
                ":now": self._serialize(datetime.now(UTC).isoformat()),
            },
        )

    def mark_failed(self, record: OutboxRecord, delay_seconds: int) -> None:
        self._client.update_item(
            TableName=self._table_name,
            Key=self._key(record),
            UpdateExpression=(
                "SET attempts = if_not_exists(attempts, :zero) + :one, "
                "next_attempt_at = :next REMOVE lease_until"
            ),
            ExpressionAttributeValues={
                ":zero": self._serialize(0),
                ":one": self._serialize(1),
                ":next": self._serialize(
                    (datetime.now(UTC) + timedelta(seconds=delay_seconds)).isoformat()
                ),
            },
        )

    @staticmethod
    def _key(record: OutboxRecord) -> dict[str, dict[str, str]]:
        return {"PK": {"S": record.pk}, "SK": {"S": record.sk}}

    @classmethod
    def _json_compatible(cls, value: Any) -> Any:
        if isinstance(value, Decimal):
            return int(value) if value == value.to_integral() else str(value)
        if isinstance(value, dict):
            return {key: cls._json_compatible(item) for key, item in value.items()}
        if isinstance(value, list):
            return [cls._json_compatible(item) for item in value]
        return value


class OutboxRelay:
    def __init__(
        self,
        store: DynamoDBOutboxStore,
        sqs: Any,
        queue_url: str,
        *,
        base_retry_seconds: int = 1,
    ) -> None:
        self._store = store
        self._sqs = sqs
        self._queue_url = queue_url
        self._base_retry_seconds = base_retry_seconds

    def run_once(self) -> int:
        published = 0
        for record in self._store.claim_batch(limit=10, lease_seconds=30):
            try:
                self._sqs.send(self._queue_url, record.payload)
            except Exception:  # noqa: BLE001 - every publisher failure is retryable
                delay = min(self._base_retry_seconds * (2**record.attempts), 300)
                self._store.mark_failed(record, delay)
                continue
            self._store.mark_published(record)
            published += 1
        return published
