from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request, status
from pydantic import BaseModel

from src.application.diagnose import begin_diagnosis, finish_diagnosis
from src.application.execute_repair import (
    begin_repair,
    finish_repair,
    queue_repair,
    record_repair_failure,
)
from src.domain.execution import (
    Execution,
    IrreversibleRepairError,
)
from src.infrastructure.database.dynamodb_repository import (
    ConcurrentExecutionUpdate,
    DuplicateEvent,
    DynamoDBExecutionRepository,
)

router = APIRouter(prefix="/api/v1/executions", tags=["executions"])
EventIdHeader = Annotated[UUID, Header(alias="X-Event-ID")]


class DiagnosisStart(BaseModel):
    order_id: UUID
    saga_id: UUID


class DiagnosisComplete(BaseModel):
    diagnosis: str
    recommended_service_ids: list[UUID]


class RepairComplete(BaseModel):
    notes: str


class RepairFailure(BaseModel):
    reason: str


class Cancellation(BaseModel):
    reason: str


def repository(request: Request) -> DynamoDBExecutionRepository:
    return request.app.state.repository


def response(execution: Execution) -> dict[str, Any]:
    return {
        "id": str(execution.id),
        "order_id": str(execution.order_id),
        "saga_id": str(execution.saga_id),
        "correlation_id": str(execution.correlation_id),
        "status": execution.status.value,
        "diagnosis": execution.diagnosis,
        "recommended_service_ids": [
            str(item) for item in execution.recommended_service_ids
        ],
        "repair_notes": execution.repair_notes,
        "failure_reason": execution.failure_reason,
        "queued_at": execution.queued_at,
        "version": execution.version,
    }


def load(request: Request, execution_id: UUID) -> Execution:
    execution = repository(request).get(execution_id)
    if execution is None:
        raise HTTPException(status_code=404, detail="execution not found")
    return execution


def persist(
    request: Request,
    execution: Execution,
    event_id: UUID,
    event_type: str,
) -> None:
    try:
        repository(request).save_with_event(
            execution,
            incoming_event_id=event_id,
            consumer="execution-api",
            outgoing_event_type=event_type,
            causation_id=event_id,
        )
    except DuplicateEvent as exc:
        raise HTTPException(status_code=409, detail="duplicate event") from exc
    except ConcurrentExecutionUpdate as exc:
        raise HTTPException(status_code=409, detail="concurrent update") from exc


@router.post("/diagnosis", status_code=status.HTTP_201_CREATED)
def start_diagnosis(
    data: DiagnosisStart,
    request: Request,
    event_id: EventIdHeader,
    correlation_id: Annotated[UUID, Header(alias="X-Correlation-ID")],
) -> dict[str, Any]:
    execution = begin_diagnosis(data.order_id, data.saga_id, correlation_id)
    persist(request, execution, event_id, "DiagnosisStartedV1")
    return response(execution)


@router.post("/{execution_id}/diagnosis/complete")
def complete_diagnosis(
    execution_id: UUID,
    data: DiagnosisComplete,
    request: Request,
    event_id: EventIdHeader,
) -> dict[str, Any]:
    execution = load(request, execution_id)
    finish_diagnosis(
        execution, data.diagnosis, data.recommended_service_ids
    )
    persist(request, execution, event_id, "DiagnosisCompletedV1")
    return response(execution)


@router.post("/{execution_id}/queue")
def enqueue(
    execution_id: UUID, request: Request, event_id: EventIdHeader
) -> dict[str, Any]:
    execution = load(request, execution_id)
    queue_repair(execution)
    persist(request, execution, event_id, "ExecutionQueuedV1")
    return response(execution)


@router.get("/queue")
def queue(request: Request) -> list[dict[str, Any]]:
    return [response(item) for item in repository(request).list_queue()]


@router.post("/{execution_id}/repair/start")
def start_repair(
    execution_id: UUID, request: Request, event_id: EventIdHeader
) -> dict[str, Any]:
    execution = load(request, execution_id)
    begin_repair(execution)
    persist(request, execution, event_id, "ExecutionStartedV1")
    return response(execution)


@router.post("/{execution_id}/repair/complete")
def complete_repair(
    execution_id: UUID,
    data: RepairComplete,
    request: Request,
    event_id: EventIdHeader,
) -> dict[str, Any]:
    execution = load(request, execution_id)
    finish_repair(execution, data.notes)
    persist(request, execution, event_id, "ExecutionCompletedV1")
    return response(execution)


@router.post("/{execution_id}/repair/fail")
def fail_repair(
    execution_id: UUID,
    data: RepairFailure,
    request: Request,
    event_id: EventIdHeader,
) -> dict[str, Any]:
    execution = load(request, execution_id)
    record_repair_failure(execution, data.reason)
    persist(request, execution, event_id, "ExecutionFailedV1")
    return response(execution)


@router.post("/{execution_id}/cancel")
def cancel(
    execution_id: UUID,
    data: Cancellation,
    request: Request,
    event_id: EventIdHeader,
) -> dict[str, Any]:
    execution = load(request, execution_id)
    try:
        execution.cancel(data.reason)
    except IrreversibleRepairError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    persist(request, execution, event_id, "ExecutionCancelledV1")
    return response(execution)


@router.get("/{execution_id}")
def detail(execution_id: UUID, request: Request) -> dict[str, Any]:
    return response(load(request, execution_id))
