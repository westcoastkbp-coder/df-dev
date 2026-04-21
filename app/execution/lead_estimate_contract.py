from __future__ import annotations

import json
from collections.abc import Mapping


WORKFLOW_TYPE = "lead_estimate_decision"

ALLOWED_DECISIONS = {
    "create_estimate",
    "request_followup",
    "reject_lead",
}
ALLOWED_CONFIDENCE = {
    "high",
    "medium",
    "low",
}
NEXT_STEPS = {
    "create_estimate_task",
    "request_missing_scope",
    "archive_lead",
    "manual_review",
}
REASON_CODES = {
    "project_defined",
    "insufficient_scope",
    "non_qualified_lead",
    "estimate_not_needed",
    "unsupported_request",
}

BINDABLE_ACTIONS = {
    "create_estimate_task",
    "request_missing_scope",
    "archive_lead",
    "manual_review",
}

INPUT_FIELDS = {"workflow_type", "lead_id", "lead_data"}
DECISION_CONTEXT_FIELDS = {"project_type", "scope_summary", "contact_present"}
LEAD_DATA_FIELDS = {
    "project_type",
    "scope_summary",
    "contact_info",
    "lead_exists",
    "lead_invalid",
    "unsupported_request",
    "qualification_flags",
}
DECISION_FIELDS = {
    "decision",
    "confidence",
    "next_step",
}
ACTION_FIELDS = {
    "binding_action",
    "binding_status",
    "child_task_created",
    "child_task_id",
    "child_task_intent",
    "archive_status",
    "parent_task_id",
    "source_lead_id",
    "action_source",
    "reason_code",
}
ACTION_REQUIRED_FIELDS = {
    "binding_action",
    "binding_status",
    "child_task_created",
    "child_task_id",
    "parent_task_id",
    "source_lead_id",
    "action_source",
    "reason_code",
}
EXECUTION_RESULT_REQUIRED_FIELDS = {"decision"}
EXECUTION_RESULT_FIELDS = {
    "result",
    "task_type",
    "result_type",
    "result_summary",
    "summary",
    "decision",
    "binding",
}
EXECUTION_RESULT_OPTIONAL_FIELDS = (
    EXECUTION_RESULT_FIELDS - EXECUTION_RESULT_REQUIRED_FIELDS
)


def _is_container(value: object) -> bool:
    return isinstance(value, (Mapping, list, tuple, set))


def _is_empty_container(value: object) -> bool:
    return isinstance(value, (Mapping, list, tuple, set)) and not bool(value)


def _reject_nested_value(
    value: object,
    *,
    field_name: str,
    contract_name: str,
    trace: dict[str, object],
) -> tuple[bool, str, dict[str, object]] | None:
    if _is_container(value):
        return False, f"{contract_name} field {field_name} must not be nested", trace
    return None


def normalize_mapping(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def normalize_text(value: object) -> str:
    return str(value or "").strip()


def workflow_payload_from_task(task_data: Mapping[str, object]) -> dict[str, object]:
    return normalize_mapping(task_data.get("payload"))


def is_workflow_task(task_data: Mapping[str, object]) -> bool:
    payload = workflow_payload_from_task(task_data)
    return (
        normalize_text(task_data.get("intent")) == WORKFLOW_TYPE
        or normalize_text(payload.get("workflow_type")) == WORKFLOW_TYPE
    )


def task_execution_state(task_id: object, *, status: str) -> dict[str, object]:
    return {
        "task_id": normalize_text(task_id),
        "status": normalize_text(status),
    }


def build_input_payload(payload: Mapping[str, object]) -> dict[str, object]:
    normalized_payload = normalize_mapping(payload)
    normalized_lead_data = normalize_mapping(normalized_payload.get("lead_data"))
    lead_data: dict[str, object] = {}
    for field in sorted(LEAD_DATA_FIELDS):
        if field not in normalized_lead_data:
            continue
        value = normalized_lead_data.get(field)
        if field == "contact_info":
            if isinstance(value, Mapping):
                contact_info = {
                    str(key).strip(): normalize_text(item)
                    for key, item in dict(value).items()
                    if normalize_text(key) and normalize_text(item)
                }
                if contact_info:
                    lead_data[field] = contact_info
            elif isinstance(value, (list, tuple, set)):
                contact_items = [
                    normalize_text(item) for item in value if normalize_text(item)
                ]
                if contact_items:
                    lead_data[field] = contact_items
            else:
                contact_value = normalize_text(value)
                if contact_value:
                    lead_data[field] = contact_value
            continue
        if field == "qualification_flags":
            qualification_flags = {
                str(key).strip(): item
                for key, item in normalize_mapping(value).items()
                if str(key).strip() and bool(item)
            }
            if qualification_flags:
                lead_data[field] = qualification_flags
            continue
        if field == "lead_exists":
            if value is False:
                lead_data[field] = False
            continue
        if field in {"lead_invalid", "unsupported_request"}:
            if bool(value):
                lead_data[field] = True
            continue
        normalized_value = normalize_text(value)
        if normalized_value:
            lead_data[field] = normalized_value
    return {
        "workflow_type": normalize_text(normalized_payload.get("workflow_type")),
        "lead_id": normalize_text(normalized_payload.get("lead_id")),
        "lead_data": lead_data,
    }


def build_decision_context_payload(payload: Mapping[str, object]) -> dict[str, object]:
    normalized_payload = normalize_mapping(payload)
    return {
        "project_type": normalize_text(normalized_payload.get("project_type")),
        "scope_summary": normalize_text(normalized_payload.get("scope_summary")),
        "contact_present": bool(normalized_payload.get("contact_present", False)),
    }


def validate_input_payload(payload: object) -> tuple[bool, str, dict[str, object]]:
    normalized_payload = normalize_mapping(payload)
    lead_data = normalize_mapping(normalized_payload.get("lead_data"))

    trace = {
        "workflow_type": normalize_text(normalized_payload.get("workflow_type")),
        "payload_fields": sorted(normalized_payload.keys()),
        "lead_data_fields": sorted(lead_data.keys()),
        "lead_id_present": bool(normalize_text(normalized_payload.get("lead_id"))),
    }

    if not normalized_payload:
        return False, "workflow payload must be a dict", trace

    missing_fields = [
        field for field in sorted(INPUT_FIELDS) if field not in normalized_payload
    ]
    if missing_fields:
        return (
            False,
            f"workflow payload missing required fields: {', '.join(missing_fields)}",
            trace,
        )

    if normalize_text(normalized_payload.get("workflow_type")) != WORKFLOW_TYPE:
        return False, f"workflow_type must equal {WORKFLOW_TYPE}", trace

    extra_fields = sorted(set(normalized_payload.keys()) - INPUT_FIELDS)
    if extra_fields:
        return (
            False,
            f"workflow payload contains unsupported fields: {', '.join(extra_fields)}",
            trace,
        )

    if not normalize_text(normalized_payload.get("lead_id")):
        return False, "lead_id must not be empty", trace

    if not lead_data:
        return False, "lead_data must be a dict", trace

    nested_error = _reject_nested_value(
        normalized_payload.get("workflow_type"),
        field_name="workflow_type",
        contract_name="workflow payload",
        trace=trace,
    )
    if nested_error is not None:
        return nested_error
    nested_error = _reject_nested_value(
        normalized_payload.get("lead_id"),
        field_name="lead_id",
        contract_name="workflow payload",
        trace=trace,
    )
    if nested_error is not None:
        return nested_error

    extra_lead_fields = sorted(set(lead_data.keys()) - LEAD_DATA_FIELDS)
    if extra_lead_fields:
        return (
            False,
            f"lead_data contains unsupported fields: {', '.join(extra_lead_fields)}",
            trace,
        )

    for field, value in lead_data.items():
        if _is_empty_container(value):
            continue
        if field in {
            "project_type",
            "scope_summary",
            "lead_exists",
            "lead_invalid",
            "unsupported_request",
        }:
            nested_error = _reject_nested_value(
                value,
                field_name=f"lead_data.{field}",
                contract_name="workflow payload",
                trace=trace,
            )
            if nested_error is not None:
                return nested_error

    return True, "workflow payload valid", trace


def validate_decision_contract(payload: object) -> tuple[bool, str, dict[str, object]]:
    normalized_payload = normalize_mapping(payload)
    trace = {
        "payload_fields": sorted(normalized_payload.keys()),
    }

    if not normalized_payload:
        return False, "decision payload must be a dict", trace

    missing_fields = [
        field for field in sorted(DECISION_FIELDS) if field not in normalized_payload
    ]
    if missing_fields:
        return (
            False,
            f"decision payload missing required fields: {', '.join(missing_fields)}",
            trace,
        )

    extra_fields = sorted(set(normalized_payload.keys()) - DECISION_FIELDS)
    if extra_fields:
        return (
            False,
            f"decision payload contains unsupported fields: {', '.join(extra_fields)}",
            trace,
        )

    for field in DECISION_FIELDS:
        nested_error = _reject_nested_value(
            normalized_payload.get(field),
            field_name=field,
            contract_name="decision payload",
            trace=trace,
        )
        if nested_error is not None:
            return nested_error

    if normalize_text(normalized_payload.get("decision")) not in ALLOWED_DECISIONS:
        return False, "invalid decision", trace

    if normalize_text(normalized_payload.get("confidence")) not in ALLOWED_CONFIDENCE:
        return False, "invalid confidence", trace

    if normalize_text(normalized_payload.get("next_step")) not in NEXT_STEPS:
        return False, "invalid next_step", trace

    return True, "decision payload valid", trace


def validate_decision_shape(payload: object) -> tuple[bool, str, dict[str, object]]:
    normalized_payload = normalize_mapping(payload)
    trace = {
        "payload_fields": sorted(normalized_payload.keys()),
    }
    if not normalized_payload:
        return False, "decision payload must be a dict", trace
    missing_fields = [
        field for field in sorted(DECISION_FIELDS) if field not in normalized_payload
    ]
    if missing_fields:
        return (
            False,
            f"decision payload missing required fields: {', '.join(missing_fields)}",
            trace,
        )
    extra_fields = sorted(set(normalized_payload.keys()) - DECISION_FIELDS)
    if extra_fields:
        return (
            False,
            f"decision payload contains unsupported fields: {', '.join(extra_fields)}",
            trace,
        )
    return True, "decision payload shape valid", trace


def validate_action_contract(payload: object) -> tuple[bool, str, dict[str, object]]:
    normalized_payload = normalize_mapping(payload)
    trace = {
        "payload_fields": sorted(normalized_payload.keys()),
        "binding_action": normalize_text(normalized_payload.get("binding_action")),
    }

    if not normalized_payload:
        return False, "action payload must be a dict", trace

    missing_fields = [
        field
        for field in sorted(ACTION_REQUIRED_FIELDS)
        if field not in normalized_payload
    ]
    if missing_fields:
        return (
            False,
            f"action payload missing required fields: {', '.join(missing_fields)}",
            trace,
        )

    extra_fields = sorted(set(normalized_payload.keys()) - ACTION_FIELDS)
    if extra_fields:
        return (
            False,
            f"action payload contains unsupported fields: {', '.join(extra_fields)}",
            trace,
        )

    if normalize_text(normalized_payload.get("binding_action")) == "":
        return False, "binding_action must not be empty", trace

    if not isinstance(normalized_payload.get("child_task_created"), bool):
        return False, "child_task_created must be a boolean", trace

    for field in ACTION_FIELDS - {"child_task_created"}:
        if field not in normalized_payload:
            continue
        nested_error = _reject_nested_value(
            normalized_payload.get(field),
            field_name=field,
            contract_name="action payload",
            trace=trace,
        )
        if nested_error is not None:
            return nested_error

    return True, "action payload valid", trace


def validate_execution_result_contract(
    payload: object,
) -> tuple[bool, str, dict[str, object]]:
    normalized_payload = normalize_mapping(payload)
    trace = {
        "payload_fields": sorted(normalized_payload.keys()),
        "has_decision": "decision" in normalized_payload,
        "has_binding": "binding" in normalized_payload,
    }

    if not normalized_payload:
        return False, "execution result must be a dict", trace

    missing_fields = sorted(EXECUTION_RESULT_REQUIRED_FIELDS - set(normalized_payload))
    if missing_fields:
        return (
            False,
            f"execution result missing required fields: {', '.join(missing_fields)}",
            trace,
        )

    extra_fields = sorted(set(normalized_payload.keys()) - EXECUTION_RESULT_FIELDS)
    if extra_fields:
        return (
            False,
            f"execution result contains unsupported fields: {', '.join(extra_fields)}",
            trace,
        )

    for field in EXECUTION_RESULT_OPTIONAL_FIELDS:
        if field not in normalized_payload:
            continue
        if field in {"decision", "binding"}:
            if _is_empty_container(normalized_payload.get(field)):
                return False, f"execution result field {field} must not be empty", trace
            continue
        nested_error = _reject_nested_value(
            normalized_payload.get(field),
            field_name=field,
            contract_name="execution result",
            trace=trace,
        )
        if nested_error is not None:
            return nested_error

    valid_decision, reason, _ = validate_decision_contract(
        normalized_payload.get("decision")
    )
    if not valid_decision:
        return False, reason, trace

    if "binding" in normalized_payload:
        valid_binding, binding_reason, _ = validate_action_contract(
            normalized_payload.get("binding")
        )
        if not valid_binding:
            return False, binding_reason, trace

    return True, "execution result valid", trace


def build_decision_payload(payload: Mapping[str, object]) -> dict[str, object]:
    normalized_payload = normalize_mapping(payload)
    valid_shape, reason, _ = validate_decision_shape(normalized_payload)
    if not valid_shape:
        raise ValueError(reason)
    decision_payload: dict[str, object] = {}
    for field in DECISION_FIELDS:
        if field not in normalized_payload:
            continue
        decision_payload[field] = normalize_text(normalized_payload.get(field))
    return decision_payload


def decision_reason_code(next_step: object) -> str:
    normalized_next_step = normalize_text(next_step)
    return {
        "create_estimate_task": "project_defined",
        "request_missing_scope": "insufficient_scope",
        "archive_lead": "non_qualified_lead",
        "manual_review": "unsupported_request",
    }.get(normalized_next_step, "")


def build_action_payload(payload: Mapping[str, object]) -> dict[str, object]:
    normalized_payload = normalize_mapping(payload)
    valid_shape, reason, _ = validate_action_contract(normalized_payload)
    if not valid_shape:
        raise ValueError(reason)
    action_payload: dict[str, object] = {}
    for field in ACTION_FIELDS:
        if field not in normalized_payload:
            continue
        if field == "child_task_created":
            action_payload[field] = bool(normalized_payload.get(field, False))
        elif field in {"child_task_intent", "archive_status"}:
            normalized_value = normalize_text(normalized_payload.get(field))
            if normalized_value:
                action_payload[field] = normalized_value
        else:
            action_payload[field] = normalize_text(normalized_payload.get(field))
    return action_payload


def build_execution_result_payload(payload: Mapping[str, object]) -> dict[str, object]:
    normalized_payload = normalize_mapping(payload)
    valid_contract, reason, _ = validate_execution_result_contract(normalized_payload)
    if not valid_contract:
        raise ValueError(reason)
    execution_result: dict[str, object] = {
        "decision": build_decision_payload(
            normalize_mapping(normalized_payload.get("decision"))
        ),
    }
    if "binding" in normalized_payload:
        execution_result["binding"] = build_action_payload(
            normalize_mapping(normalized_payload.get("binding"))
        )
    for field in ("result", "task_type", "result_type", "result_summary", "summary"):
        if field not in normalized_payload:
            continue
        normalized_value = normalize_text(normalized_payload.get(field))
        if normalized_value:
            execution_result[field] = normalized_value
    return execution_result


def payload_size_bytes(payload: object) -> int:
    return len(
        json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    )


def build_decision_summary(decision: Mapping[str, object]) -> str:
    normalized = dict(decision)
    return (
        f"decision={normalize_text(normalized.get('decision'))}; "
        f"confidence={normalize_text(normalized.get('confidence'))}; "
        f"next_step={normalize_text(normalized.get('next_step'))}"
    )
