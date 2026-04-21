from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


TASK_CONTRACT_VERSION = 1
OFFICE_TASK_TYPES = {
    "lead",
    "estimate",
    "follow_up",
    "permit",
    "project",
    "procurement",
    "payment",
}
OFFICE_LINEAGE_TRANSITIONS = {
    "lead": {"estimate"},
    "estimate": {"follow_up"},
    "follow_up": {"permit"},
    "permit": {"project"},
    "project": {"procurement", "payment"},
    "procurement": set(),
    "payment": set(),
}
INTENT_TASK_TYPE_ALIASES = {
    "new_lead": "lead",
    "lead_estimate_decision": "lead",
    "estimate_task": "estimate",
}
TASK_STATUSES = {
    "AWAITING_APPROVAL",
    "CREATED",
    "VALIDATED",
    "EXECUTING",
    "COMPLETED",
    "FAILED",
    "DEFERRED",
}
TASK_APPROVAL_STATUSES = {
    "pending",
    "approved",
    "rejected",
}
TASK_REQUIRED_FIELDS = {
    "task_contract_version",
    "task_id",
    "created_at",
    "intent",
    "payload",
    "status",
    "notes",
    "history",
}
TASK_ALLOWED_FIELDS = TASK_REQUIRED_FIELDS | {
    "interaction_id",
    "job_id",
    "trace_id",
    "last_updated_at",
    "execution_mode",
    "approval_status",
    "execution_location",
    "offload_latency",
    "routing_reason",
    "telemetry_snapshot",
    "safety_override",
    "network_snapshot",
    "network_policy",
    "execution_compute_mode",
    "runtime_profile",
    "runtime_decision",
    "runtime_validation",
    "runtime_verdict",
    "runtime_authority_chain",
    "started_at",
    "completed_at",
    "failed_at",
    "result",
    "error",
    "idempotency_key",
    "execution_key",
    "task_type",
    "parent_task_id",
    "parent_task_type",
    "source_lead_id",
    "followup_task_id",
    "original_task_id",
    "reentry_source",
    "approved_at",
    "approved_by",
    "rejected_at",
    "rejected_by",
}
TASK_EVENT_REQUIRED_FIELDS = {"timestamp", "event", "from_status", "to_status", "details"}
TASK_EVENT_ALLOWED_FIELDS = set(TASK_EVENT_REQUIRED_FIELDS)


@dataclass(slots=True)
class TaskEvent:
    timestamp: str
    event: str
    from_status: str = ""
    to_status: str = ""
    details: Any = None

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _validate_mapping_field(
    payload: dict[str, object],
    field_name: str,
) -> dict[str, object] | None:
    value = payload.get(field_name)
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a dict when provided")
    return dict(value)


def _validate_history(history: object) -> list[dict[str, object]]:
    if not isinstance(history, list):
        raise ValueError("history must be a list")
    validated_history: list[dict[str, object]] = []
    for index, event in enumerate(history):
        if not isinstance(event, dict):
            raise ValueError(f"history[{index}] must be a dict")
        missing_fields = sorted(TASK_EVENT_REQUIRED_FIELDS - set(event))
        if missing_fields:
            raise ValueError(
                f"history[{index}] missing required fields: {', '.join(missing_fields)}"
            )
        extra_fields = sorted(set(event) - TASK_EVENT_ALLOWED_FIELDS)
        if extra_fields:
            raise ValueError(
                f"history[{index}] contains unsupported fields: {', '.join(extra_fields)}"
            )
        normalized_event = {
            "timestamp": _normalize_text(event.get("timestamp")),
            "event": _normalize_text(event.get("event")),
            "from_status": _normalize_text(event.get("from_status")),
            "to_status": _normalize_text(event.get("to_status")),
            "details": event.get("details"),
        }
        if not normalized_event["timestamp"]:
            raise ValueError(f"history[{index}].timestamp must not be empty")
        if not normalized_event["event"]:
            raise ValueError(f"history[{index}].event must not be empty")
        details = normalized_event["details"]
        if not isinstance(details, dict):
            raise ValueError(f"history[{index}].details must be a dict")
        normalized_event["details"] = dict(details)
        validated_history.append(normalized_event)
    return validated_history


def canonical_task_type(task_data: object) -> str:
    if not isinstance(task_data, dict):
        return ""
    explicit_task_type = _normalize_text(task_data.get("task_type")).lower()
    if explicit_task_type:
        return explicit_task_type
    payload = task_data.get("payload")
    if isinstance(payload, dict):
        payload_task_type = _normalize_text(payload.get("task_type")).lower()
        if payload_task_type:
            return payload_task_type
    return INTENT_TASK_TYPE_ALIASES.get(_normalize_text(task_data.get("intent")).lower(), "")


def is_office_task(task_data: object) -> bool:
    return canonical_task_type(task_data) in OFFICE_TASK_TYPES


def _lineage_parent_task_id(task_data: dict[str, object]) -> str:
    payload = task_data.get("payload")
    payload_parent_task_id = ""
    if isinstance(payload, dict):
        payload_parent_task_id = _normalize_text(payload.get("parent_task_id"))
    return _normalize_text(task_data.get("parent_task_id")) or payload_parent_task_id


def validate_task_lineage(
    task_data: dict[str, object],
    *,
    parent_task: dict[str, object] | None = None,
    existing_tasks: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    validated_task = dict(task_data)
    task_type = canonical_task_type(validated_task)
    if not task_type:
        return validated_task
    if task_type not in OFFICE_TASK_TYPES:
        raise ValueError(f"task_type must be one of: {', '.join(sorted(OFFICE_TASK_TYPES))}")

    validated_task["task_type"] = task_type
    parent_task_id = _lineage_parent_task_id(validated_task)

    if task_type == "lead":
        if parent_task_id:
            raise ValueError("lead tasks must not define parent_task_id")
        if _normalize_text(validated_task.get("parent_task_type")):
            raise ValueError("lead tasks must not define parent_task_type")
        return validated_task

    if not parent_task_id:
        raise ValueError(f"office task `{task_type}` requires parent_task_id")
    if parent_task is None:
        raise ValueError(f"parent task not found: {parent_task_id}")

    resolved_parent_type = canonical_task_type(parent_task)
    if resolved_parent_type not in OFFICE_TASK_TYPES:
        raise ValueError("parent task type is not valid for office lineage")
    if task_type not in OFFICE_LINEAGE_TRANSITIONS.get(resolved_parent_type, set()):
        raise ValueError(
            f"invalid office lineage transition: {resolved_parent_type} -> {task_type}"
        )

    declared_parent_type = _normalize_text(validated_task.get("parent_task_type")).lower()
    if declared_parent_type and declared_parent_type != resolved_parent_type:
        raise ValueError(
            f"parent_task_type must equal resolved parent type: {resolved_parent_type}"
        )

    validated_task["parent_task_id"] = parent_task_id
    validated_task["parent_task_type"] = resolved_parent_type

    for existing_task in existing_tasks or []:
        if _normalize_text(existing_task.get("task_id")) == _normalize_text(validated_task.get("task_id")):
            continue
        if _lineage_parent_task_id(existing_task) != parent_task_id:
            continue
        if canonical_task_type(existing_task) != task_type:
            continue
        raise ValueError(
            f"duplicate office child task type not allowed: {resolved_parent_type} -> {task_type}"
        )

    return validated_task


def validate_task_contract(task_data: object) -> dict[str, object]:
    if not isinstance(task_data, dict):
        raise ValueError("task must be a dict")

    missing_fields = sorted(TASK_REQUIRED_FIELDS - set(task_data))
    if missing_fields:
        raise ValueError(f"task missing required fields: {', '.join(missing_fields)}")

    extra_fields = sorted(set(task_data) - TASK_ALLOWED_FIELDS)
    if extra_fields:
        raise ValueError(f"task contains unsupported fields: {', '.join(extra_fields)}")

    contract_version = task_data.get("task_contract_version")
    if contract_version != TASK_CONTRACT_VERSION:
        raise ValueError(
            f"task_contract_version must equal {TASK_CONTRACT_VERSION}"
        )

    task_id = _normalize_text(task_data.get("task_id"))
    if not task_id:
        raise ValueError("task_id must not be empty")

    created_at = _normalize_text(task_data.get("created_at"))
    if not created_at:
        raise ValueError("created_at must not be empty")
    last_updated_at = _normalize_text(task_data.get("last_updated_at")) or created_at

    intent = _normalize_text(task_data.get("intent"))
    if not intent:
        raise ValueError("intent must not be empty")

    payload = task_data.get("payload")
    if not isinstance(payload, dict):
        raise ValueError("payload must be a dict")

    from app.orchestrator.task_lifecycle import normalize_task_status

    status = normalize_task_status(task_data.get("status"))
    if status not in TASK_STATUSES:
        raise ValueError(f"status must be one of: {', '.join(sorted(TASK_STATUSES))}")

    approval_status = _normalize_text(task_data.get("approval_status")).lower()
    if approval_status:
        if approval_status not in TASK_APPROVAL_STATUSES:
            raise ValueError(
                "approval_status must be one of: "
                + ", ".join(sorted(TASK_APPROVAL_STATUSES))
            )
    elif status == "AWAITING_APPROVAL":
        approval_status = "pending"
    else:
        approval_status = "approved"

    notes = task_data.get("notes")
    if not isinstance(notes, list):
        raise ValueError("notes must be a list")
    normalized_notes = []
    for index, note in enumerate(notes):
        normalized_note = _normalize_text(note)
        if not normalized_note:
            raise ValueError(f"notes[{index}] must not be empty")
        normalized_notes.append(normalized_note)

    validated_task: dict[str, object] = {
        "task_contract_version": TASK_CONTRACT_VERSION,
        "task_id": task_id,
        "created_at": created_at,
        "last_updated_at": last_updated_at,
        "intent": intent,
        "payload": dict(payload),
        "status": status,
        "approval_status": approval_status,
        "notes": normalized_notes,
        "history": _validate_history(task_data.get("history")),
    }

    for field_name in (
        "interaction_id",
        "job_id",
        "trace_id",
        "task_type",
        "parent_task_id",
        "parent_task_type",
        "execution_mode",
        "approved_at",
        "approved_by",
        "rejected_at",
        "rejected_by",
        "execution_location",
        "started_at",
        "completed_at",
        "failed_at",
        "error",
        "idempotency_key",
        "execution_key",
        "source_lead_id",
        "followup_task_id",
        "original_task_id",
        "reentry_source",
    ):
        if field_name in task_data:
            validated_task[field_name] = _normalize_text(task_data.get(field_name))

    if "offload_latency" in task_data:
        offload_latency = task_data.get("offload_latency")
        if offload_latency is not None and not isinstance(offload_latency, (int, float)):
            raise ValueError("offload_latency must be numeric when provided")
        validated_task["offload_latency"] = offload_latency

    for field_name in (
        "routing_reason",
        "telemetry_snapshot",
        "safety_override",
        "network_snapshot",
        "network_policy",
        "runtime_decision",
        "runtime_validation",
        "runtime_verdict",
        "result",
    ):
        if field_name in task_data:
            validated_task[field_name] = _validate_mapping_field(task_data, field_name)

    if "execution_compute_mode" in task_data:
        validated_task["execution_compute_mode"] = _normalize_text(
            task_data.get("execution_compute_mode")
        )

    if "runtime_profile" in task_data:
        validated_task["runtime_profile"] = _normalize_text(task_data.get("runtime_profile"))

    if "runtime_authority_chain" in task_data:
        authority_chain = task_data.get("runtime_authority_chain")
        if isinstance(authority_chain, dict):
            validated_task["runtime_authority_chain"] = dict(authority_chain)
        elif isinstance(authority_chain, list):
            validated_task["runtime_authority_chain"] = list(authority_chain)
        else:
            raise ValueError("runtime_authority_chain must be a dict or list when provided")

    return validated_task


@dataclass(slots=True)
class Task:
    task_contract_version: int
    task_id: str
    created_at: str
    last_updated_at: str
    intent: str
    payload: dict[str, object]
    status: str
    notes: list[str] = field(default_factory=list)
    history: list[dict[str, object]] = field(default_factory=list)
    interaction_id: str = ""
    job_id: str = ""
    trace_id: str = ""
    task_type: str = ""
    parent_task_id: str = ""
    parent_task_type: str = ""
    execution_mode: str = ""
    approval_status: str = "approved"
    approved_at: str = ""
    approved_by: str = ""
    rejected_at: str = ""
    rejected_by: str = ""
    execution_location: str = ""
    offload_latency: float | None = None
    routing_reason: dict[str, object] | None = None
    telemetry_snapshot: dict[str, object] | None = None
    safety_override: dict[str, object] | None = None
    network_snapshot: dict[str, object] | None = None
    network_policy: dict[str, object] | None = None
    started_at: str = ""
    completed_at: str = ""
    failed_at: str = ""
    result: dict[str, object] | None = None
    error: str = ""
    execution_key: str = ""

    def as_dict(self) -> dict[str, object]:
        return validate_task_contract(asdict(self))
