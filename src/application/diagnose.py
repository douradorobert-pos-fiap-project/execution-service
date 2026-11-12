from uuid import UUID

from src.domain.execution import Execution


def begin_diagnosis(order_id: UUID, saga_id: UUID, correlation_id: UUID) -> Execution:
    execution = Execution(
        order_id=order_id,
        saga_id=saga_id,
        correlation_id=correlation_id,
    )
    execution.start_diagnosis()
    return execution


def finish_diagnosis(
    execution: Execution,
    diagnosis: str,
    recommended_service_ids: list[UUID],
) -> None:
    execution.complete_diagnosis(diagnosis, recommended_service_ids)
