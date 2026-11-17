from typing import Any

from src.application.diagnose import begin_diagnosis
from src.application.execute_repair import queue_repair
from src.domain.execution import ExecutionStatus
from src.infrastructure.database.dynamodb_repository import (
    DynamoDBExecutionRepository,
)
from src.infrastructure.messaging.envelope import EventEnvelopeV1

CONSUMER_NAME = "execution-workflows"
EVENT_HANDLERS = {
    "DiagnosisRequestedV1": "diagnosis",
    "ExecutionRequestedV1": "execution",
}


def process_event(
    repository: DynamoDBExecutionRepository, message: dict[str, Any]
) -> bool:
    event = EventEnvelopeV1.model_validate(message)
    if repository.was_processed(event.event_id, CONSUMER_NAME):
        return False

    handler = EVENT_HANDLERS.get(event.event_type)
    if handler == "diagnosis":
        _start_diagnosis(repository, event)
        return True
    if handler == "execution":
        _queue_execution(repository, event)
        return True
    raise ValueError(f"execution cannot handle {event.event_type}")


def _start_diagnosis(
    repository: DynamoDBExecutionRepository, event: EventEnvelopeV1
) -> None:
    execution = repository.get_by_order(event.order_id)
    if execution is None:
        execution = begin_diagnosis(event.order_id, event.saga_id, event.correlation_id)
    elif execution.saga_id != event.saga_id:
        raise ValueError("diagnosis request belongs to a different saga")
    elif execution.status is ExecutionStatus.PENDING_DIAGNOSIS:
        execution.start_diagnosis()
    repository.save_with_event(
        execution,
        incoming_event_id=event.event_id,
        consumer=CONSUMER_NAME,
        outgoing_event_type="DiagnosisStartedV1",
        causation_id=event.event_id,
    )


def _queue_execution(
    repository: DynamoDBExecutionRepository, event: EventEnvelopeV1
) -> None:
    execution = repository.get_by_order(event.order_id)
    if execution is None or execution.saga_id != event.saga_id:
        raise LookupError("diagnosed execution not found for saga")
    if execution.status is ExecutionStatus.DIAGNOSED:
        queue_repair(execution)
    elif execution.status is not ExecutionStatus.QUEUED:
        raise ValueError(f"execution cannot be queued from {execution.status}")
    repository.save_with_event(
        execution,
        incoming_event_id=event.event_id,
        consumer=CONSUMER_NAME,
        outgoing_event_type="ExecutionQueuedV1",
        causation_id=event.event_id,
    )
