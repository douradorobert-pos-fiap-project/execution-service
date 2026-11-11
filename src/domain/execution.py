from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID, uuid4


class ExecutionStatus(StrEnum):
    PENDING_DIAGNOSIS = "PENDING_DIAGNOSIS"
    DIAGNOSING = "DIAGNOSING"
    DIAGNOSED = "DIAGNOSED"
    QUEUED = "QUEUED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class InvalidExecutionTransition(ValueError):
    pass


class IrreversibleRepairError(ValueError):
    pass


@dataclass
class Execution:
    order_id: UUID
    saga_id: UUID
    correlation_id: UUID
    id: UUID = field(default_factory=uuid4)
    status: ExecutionStatus = ExecutionStatus.PENDING_DIAGNOSIS
    diagnosis: str | None = None
    recommended_service_ids: list[UUID] = field(default_factory=list)
    repair_notes: str | None = None
    failure_reason: str | None = None
    queued_at: str | None = None
    version: int = 0

    def start_diagnosis(self) -> None:
        self._require(ExecutionStatus.PENDING_DIAGNOSIS)
        self.status = ExecutionStatus.DIAGNOSING

    def complete_diagnosis(
        self, diagnosis: str, recommended_service_ids: list[UUID]
    ) -> None:
        self._require(ExecutionStatus.DIAGNOSING)
        if not diagnosis.strip():
            raise ValueError("diagnosis must not be blank")
        self.diagnosis = diagnosis.strip()
        self.recommended_service_ids = list(recommended_service_ids)
        self.status = ExecutionStatus.DIAGNOSED

    def request_execution(self) -> None:
        self._require(ExecutionStatus.DIAGNOSED)
        self.queued_at = datetime.now(UTC).isoformat()
        self.status = ExecutionStatus.QUEUED

    def start_repair(self) -> None:
        self._require(ExecutionStatus.QUEUED)
        self.status = ExecutionStatus.IN_PROGRESS

    def complete_repair(self, notes: str) -> None:
        self._require(ExecutionStatus.IN_PROGRESS)
        self.repair_notes = notes.strip()
        self.status = ExecutionStatus.COMPLETED

    def fail_repair(self, reason: str) -> None:
        self._require(ExecutionStatus.IN_PROGRESS)
        self.failure_reason = reason.strip()
        self.status = ExecutionStatus.FAILED

    def cancel(self, reason: str) -> None:
        if self.status in {
            ExecutionStatus.IN_PROGRESS,
            ExecutionStatus.COMPLETED,
            ExecutionStatus.FAILED,
        }:
            raise IrreversibleRepairError(
                "physical repair cannot be compensated after it starts"
            )
        if self.status is ExecutionStatus.CANCELLED:
            return
        self.failure_reason = reason.strip()
        self.status = ExecutionStatus.CANCELLED

    def _require(self, expected: ExecutionStatus) -> None:
        if self.status is not expected:
            raise InvalidExecutionTransition(
                f"invalid execution transition from {self.status}"
            )
