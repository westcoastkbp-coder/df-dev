from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from app.execution.lead_estimate_contract import (
    WORKFLOW_TYPE,
    build_action_payload,
    build_decision_payload,
    build_decision_summary,
    is_workflow_task,
    task_execution_state,
    validate_decision_contract,
    validate_input_payload,
    workflow_payload_from_task,
)
from app.execution.lead_estimate_read import resolve_estimate_decision
from app.execution.lead_estimate_write import (
    bind_decision_action as _bind_decision_action,
    bind_decision_for_task as _bind_decision_for_task,
)
from app.execution.paths import TASKS_FILE


def bind_decision_action(
    *,
    task_data: dict[str, object],
    decision: Mapping[str, object],
    store_path: Path | None = None,
) -> dict[str, object]:
    return _bind_decision_action(
        task_data=task_data,
        decision=decision,
        store_path=store_path or TASKS_FILE,
    )


def bind_decision_for_task(
    *,
    task_id: object,
    decision: Mapping[str, object],
    store_path: Path | None = None,
) -> dict[str, object]:
    return _bind_decision_for_task(
        task_id=task_id,
        decision=decision,
        store_path=store_path or TASKS_FILE,
    )
