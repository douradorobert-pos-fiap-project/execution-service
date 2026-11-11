from uuid import UUID

import pytest

from src.domain.execution import (
    Execution,
    ExecutionStatus,
    InvalidExecutionTransition,
    IrreversibleRepairError,
)

ORDER_ID = UUID("10000000-0000-4000-8000-000000000001")
SAGA_ID = UUID("20000000-0000-4000-8000-000000000002")
CORRELATION_ID = UUID("30000000-0000-4000-8000-000000000003")
SERVICE_ID = UUID("40000000-0000-4000-8000-000000000004")


def execution() -> Execution:
    return Execution(
        order_id=ORDER_ID,
        saga_id=SAGA_ID,
        correlation_id=CORRELATION_ID,
    )


def diagnosed_execution() -> Execution:
    aggregate = execution()
    aggregate.start_diagnosis()
    aggregate.complete_diagnosis("Trocar pastilhas", [SERVICE_ID])
    return aggregate


def queued_execution() -> Execution:
    aggregate = diagnosed_execution()
    aggregate.request_execution()
    return aggregate


def test_completes_diagnosis_with_operational_recommendations():
    aggregate = execution()

    aggregate.start_diagnosis()
    aggregate.complete_diagnosis("Trocar pastilhas", [SERVICE_ID])

    assert aggregate.status is ExecutionStatus.DIAGNOSED
    assert aggregate.diagnosis == "Trocar pastilhas"
    assert aggregate.recommended_service_ids == [SERVICE_ID]


def test_runs_repair_through_queue_to_completion():
    aggregate = queued_execution()

    aggregate.start_repair()
    aggregate.complete_repair("Pastilhas substituídas e testadas")

    assert aggregate.status is ExecutionStatus.COMPLETED
    assert aggregate.repair_notes == "Pastilhas substituídas e testadas"


def test_failure_after_repair_start_is_auditable():
    aggregate = queued_execution()
    aggregate.start_repair()

    aggregate.fail_repair("Peça incompatível")

    assert aggregate.status is ExecutionStatus.FAILED
    assert aggregate.failure_reason == "Peça incompatível"


@pytest.mark.parametrize(
    "prepare",
    [
        lambda aggregate: None,
        lambda aggregate: aggregate.start_diagnosis(),
        lambda aggregate: (
            aggregate.start_diagnosis(),
            aggregate.complete_diagnosis("Trocar pastilhas", [SERVICE_ID]),
        ),
        lambda aggregate: (
            aggregate.start_diagnosis(),
            aggregate.complete_diagnosis("Trocar pastilhas", [SERVICE_ID]),
            aggregate.request_execution(),
        ),
    ],
)
def test_compensation_cancels_before_physical_repair_starts(prepare):
    aggregate = execution()
    prepare(aggregate)

    aggregate.cancel("Saga compensated")

    assert aggregate.status is ExecutionStatus.CANCELLED
    assert aggregate.failure_reason == "Saga compensated"


@pytest.mark.parametrize("terminal_action", [None, "complete", "fail"])
def test_repair_cannot_be_compensated_after_it_starts(terminal_action):
    aggregate = queued_execution()
    aggregate.start_repair()
    if terminal_action == "complete":
        aggregate.complete_repair("Concluído")
    elif terminal_action == "fail":
        aggregate.fail_repair("Falha física")

    with pytest.raises(IrreversibleRepairError):
        aggregate.cancel("Saga compensated")

    assert aggregate.status in {
        ExecutionStatus.IN_PROGRESS,
        ExecutionStatus.COMPLETED,
        ExecutionStatus.FAILED,
    }


def test_repair_cannot_start_before_execution_is_requested():
    aggregate = diagnosed_execution()

    with pytest.raises(InvalidExecutionTransition):
        aggregate.start_repair()


@pytest.mark.parametrize("diagnosis", ["", "   "])
def test_blank_diagnosis_is_rejected(diagnosis):
    aggregate = execution()
    aggregate.start_diagnosis()

    with pytest.raises(ValueError, match="diagnosis must not be blank"):
        aggregate.complete_diagnosis(diagnosis, [SERVICE_ID])
