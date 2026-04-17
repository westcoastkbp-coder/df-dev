from __future__ import annotations

from collections.abc import Mapping

from app.context.shared_context_store import append_event, get_context, set_context
from app.execution.lead_estimate_contract import (
    WORKFLOW_TYPE,
    build_decision_context_payload,
    build_input_payload,
    build_decision_payload,
    normalize_mapping,
    normalize_text,
    validate_decision_contract,
    validate_input_payload,
)


def _clone_json_like(value: object) -> object:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {
            normalize_text(key): _clone_json_like(item)
            for key, item in dict(value).items()
            if normalize_text(key)
        }
    if isinstance(value, (list, tuple)):
        return [_clone_json_like(item) for item in value]
    return normalize_text(value)


def _compact_context_snapshot(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        return {}
    nested_value = value.get("value", {})
    compact_nested = {}
    if isinstance(nested_value, Mapping):
        compact_nested = {
            normalize_text(key): _clone_json_like(item)
            for key, item in dict(nested_value).items()
            if normalize_text(key)
            and normalize_text(key) not in {"decision_context", "previous_context", "history"}
        }
    return {
        "task_id": normalize_text(value.get("task_id")),
        "interaction_id": normalize_text(value.get("interaction_id")),
        "updated_at": normalize_text(value.get("updated_at") or value.get("timestamp")),
        "value": compact_nested,
    }


def _has_contact_info(value: object) -> bool:
    if isinstance(value, Mapping):
        return any(normalize_text(item) for item in dict(value).values())
    if isinstance(value, (list, tuple, set)):
        return any(normalize_text(item) for item in value)
    return bool(normalize_text(value))


def _has_non_qualified_flag(value: object) -> bool:
    if isinstance(value, Mapping):
        normalized = {str(key).strip().lower(): bool(item) for key, item in dict(value).items()}
        return bool(
            normalized.get("non_qualified")
            or normalized.get("invalid")
            or normalized.get("unsupported")
        )
    return False


def _select_decision(context: Mapping[str, object]) -> dict[str, str]:
    normalized_context = build_decision_context_payload(context)
    if (
        normalized_context["project_type"]
        and normalized_context["scope_summary"]
        and normalized_context["contact_present"]
    ):
        return {
            "decision": "create_estimate",
            "confidence": "high",
            "next_step": "create_estimate_task",
        }
    if normalized_context["contact_present"]:
        return {
            "decision": "request_followup",
            "confidence": "high",
            "next_step": "request_missing_scope",
        }
    return {
        "decision": "request_followup",
        "confidence": "medium",
        "next_step": "manual_review",
    }


def resolve_estimate_decision(*, task_id: object, payload: object) -> dict[str, object]:
    normalized_task_id = normalize_text(task_id)
    raw_payload = normalize_mapping(payload)
    interaction_id = normalize_text(
        raw_payload.get("interaction_id") or raw_payload.get("session_id")
    )
    global_context = get_context("global_context")
    current_task_context = get_context(f"active_task:{normalized_task_id}")
    normalized_input = build_input_payload(normalize_mapping(payload))
    valid, reason, _ = validate_input_payload(normalized_input)
    if not valid:
        raise ValueError(reason)

    normalized_payload = normalized_input
    lead_id = normalize_text(normalized_payload.get("lead_id"))
    lead_data = normalize_mapping(normalized_payload.get("lead_data"))

    project_type = normalize_text(lead_data.get("project_type"))
    scope_summary = normalize_text(lead_data.get("scope_summary"))
    contact_info_present = _has_contact_info(lead_data.get("contact_info"))
    lead_exists = bool(lead_data.get("lead_exists", True))
    lead_invalid = bool(lead_data.get("lead_invalid", False))
    unsupported_request = bool(lead_data.get("unsupported_request", False))
    non_qualified = _has_non_qualified_flag(lead_data.get("qualification_flags"))

    if lead_invalid or unsupported_request or non_qualified:
        decision = {
            "decision": "reject_lead",
            "confidence": "high",
            "next_step": "archive_lead",
        }
    else:
        decision = _select_decision(
            {
                "project_type": project_type if lead_exists else "",
                "scope_summary": scope_summary if lead_exists else "",
                "contact_present": contact_info_present,
            }
        )

    resolved = build_decision_payload(decision)

    valid_contract, contract_reason, _ = validate_decision_contract(resolved)
    if not valid_contract:
        raise ValueError(contract_reason)
    set_context(
        f"active_task:{normalized_task_id}",
        {
            "task_id": normalized_task_id,
            "interaction_id": interaction_id,
            "workflow_type": WORKFLOW_TYPE,
            "decision": dict(resolved),
            "decision_context": {
                "global_context": dict(global_context) if isinstance(global_context, Mapping) else {},
                "active_task_context": _compact_context_snapshot(current_task_context),
                "input_payload": dict(normalized_payload),
            },
        },
        task_id=normalized_task_id,
        interaction_id=interaction_id,
    )
    append_event(
        "decision_resolved",
        {
            "workflow_type": WORKFLOW_TYPE,
            "decision": dict(resolved),
            "input_payload": dict(normalized_payload),
        },
        task_id=normalized_task_id,
        interaction_id=interaction_id,
    )
    return resolved
