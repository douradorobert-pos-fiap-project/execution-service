from src.domain.execution import Execution


def queue_repair(execution: Execution) -> None:
    execution.request_execution()


def begin_repair(execution: Execution) -> None:
    execution.start_repair()


def finish_repair(execution: Execution, notes: str) -> None:
    execution.complete_repair(notes)


def record_repair_failure(execution: Execution, reason: str) -> None:
    execution.fail_repair(reason)
