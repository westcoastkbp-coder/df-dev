from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from app.execution.lead_estimate_contract import (
    BINDABLE_ACTIONS,
    WORKFLOW_TYPE,
    build_action_payload,
    build_decision_payload,
    decision_reason_code,
    normalize_text,
)
from app.execution.paths import TASKS_FILE


def build_archive_binding(
    *,
    next_action: str,
    parent_task_id: str,
    source_lead_id: str,
    reason_code: str,
) -> dict[str, object]:
    return build_action_payload(
        {
            "binding_action": next_action,
            "binding_status": "archived",
            "child_task_created": False,
            "child_task_id": "",
            "archive_status": "archived",
            "parent_task_id": parent_task_id,
            "source_lead_id": source_lead_id,
            "action_source": WORKFLOW_TYPE,
            "reason_code": reason_code,
        }
    )


def build_child_task_input(
    *,
    child_intent: str,
    parent_task_id: str,
    source_lead_id: str,
    reason_code: str,
) -> dict[str, object]:
    return {
        "status": "created",
        "intent": child_intent,
        "payload": {
            "task_id": "",
            "parent_task_id": parent_task_id,
            "workflow_type": WORKFLOW_TYPE,
            "status": "created",
            "source_lead_id": source_lead_id,
            "action_source": WORKFLOW_TYPE,
            "reason_code": reason_code,
        },
    }


def build_child_binding(
    *,
    next_action: str,
    persisted_child: Mapping[str, object],
    parent_task_id: str,
    source_lead_id: str,
    reason_code: str,
) -> dict[str, object]:
    return build_action_payload(
        {
            "binding_action": next_action,
            "binding_status": "child_task_created",
            "child_task_created": True,
            "child_task_id": normalize_text(persisted_child.get("task_id")),
            "child_task_intent": normalize_text(persisted_child.get("intent")),
            "parent_task_id": parent_task_id,
            "source_lead_id": source_lead_id,
            "action_source": WORKFLOW_TYPE,
            "reason_code": reason_code,
        }
    )


def bind_decision_action(
    *,
    task_data: dict[str, object],
    decision: Mapping[str, object],
    store_path: Path | None = None,
) -> dict[str, object]:
    from app.orchestrator.task_factory import create_task, save_task

    normalized_decision = build_decision_payload(dict(decision))
    next_action = normalize_text(normalized_decision.get("next_step"))
    if next_action not in BINDABLE_ACTIONS:
        raise ValueError("invalid next_step")

    source_task_id = normalize_text(task_data.get("task_id"))
    payload = dict(task_data.get("payload", {}) or {})
    source_lead_id = normalize_text(payload.get("lead_id") or task_data.get("source_lead_id"))
    reason_code = decision_reason_code(next_action)

    if next_action == "archive_lead":
        return build_archive_binding(
            next_action=next_action,
            parent_task_id=source_task_id,
            source_lead_id=source_lead_id,
            reason_code=reason_code,
        )

    child_intent = {
        "create_estimate_task": "estimate_task",
        "request_missing_scope": "missing_scope_followup",
        "manual_review": "manual_review_task",
    }[next_action]
    child_task = create_task(
        build_child_task_input(
            child_intent=child_intent,
            parent_task_id=source_task_id,
            source_lead_id=source_lead_id,
            reason_code=reason_code,
        ),
        store_path=store_path or TASKS_FILE,
    )
    child_payload = dict(child_task.get("payload", {}) or {})
    child_payload.update(
        {
            "task_id": normalize_text(child_task.get("task_id")),
            "parent_task_id": source_task_id,
            "workflow_type": WORKFLOW_TYPE,
            "status": normalize_text(child_task.get("status")) or "created",
            "source_lead_id": source_lead_id,
            "action_source": WORKFLOW_TYPE,
            "reason_code": reason_code,
        }
    )
    child_task["intent"] = child_intent
    child_task["payload"] = child_payload
    persisted_child = save_task(child_task, store_path=store_path or TASKS_FILE)
    return build_child_binding(
        next_action=next_action,
        persisted_child=persisted_child,
        parent_task_id=source_task_id,
        source_lead_id=source_lead_id,
        reason_code=reason_code,
    )


def bind_decision_for_task(
    *,
    task_id: object,
    decision: Mapping[str, object],
    store_path: Path | None = None,
) -> dict[str, object]:
    from app.orchestrator.task_factory import get_task

    normalized_task_id = normalize_text(task_id)
    task_data = get_task(normalized_task_id, store_path=store_path or TASKS_FILE) or {
        "task_id": normalized_task_id,
    }
    return bind_decision_action(
        task_data=task_data,
        decision=build_decision_payload(decision),
        store_path=store_path,
    )
