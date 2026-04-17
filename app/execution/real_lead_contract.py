from __future__ import annotations

from collections.abc import Mapping
from typing import TypedDict

from app.execution.lead_estimate_contract import WORKFLOW_TYPE


MISSING_INPUT_FOLLOWUP_WORKFLOW_TYPE = "missing_input_followup"
ALLOWED_PROJECT_TYPES = {"kitchen", "bathroom", "adu", "general", "other"}
REQUIRED_INPUT_FIELDS = ("lead_id", "contact_info", "project_type", "scope_summary")
DEFAULT_REAL_LEAD_INPUT = {
    "lead_id": "lead-real-001",
    "contact_info": {"phone": "555-0100"},
    "project_type": "adu",
    "scope_summary": "Detached ADU with pricing request",
}
LEAD_INPUT_FIELDS = {
    "lead_id",
    "contact_info",
    "project_type",
    "scope_summary",
    "urgency_level",
    "location",
    "notes",
    "qualification_flags",
    "lead_invalid",
    "unsupported_request",
    "lead_exists",
}
FOLLOWUP_FIELDS = {
    "workflow_type",
    "parent_lead_id",
    "missing_fields",
    "required_action",
    "status",
    "updated_lead_input",
}
FOLLOWUP_REQUIRED_FIELDS = {
    "workflow_type",
    "parent_lead_id",
    "missing_fields",
    "required_action",
    "status",
}
FOLLOWUP_CONTEXT_FIELDS = {"task_id", "status", "intent", "payload"}
REENTRY_METADATA_FIELDS = {
    "source_lead_id",
    "followup_task_id",
    "original_task_id",
    "reentry_source",
}


class LeadInputPayload(TypedDict):
    lead_id: str | None
    contact_info: dict[str, str | None]
    project_type: str | None
    scope_summary: str | None
    urgency_level: str | None
    location: str | None
    notes: str | None
    qualification_flags: dict[str, object]
    lead_invalid: bool
    unsupported_request: bool
    lead_exists: bool


class FollowupPayload(TypedDict, total=False):
    workflow_type: str
    parent_lead_id: str
    missing_fields: list[str]
    required_action: str
    status: str
    updated_lead_input: LeadInputPayload


class FollowupContextPayload(TypedDict):
    task_id: str
    status: str
    intent: str
    payload: FollowupPayload


class ReentryTaskMetadataPayload(TypedDict):
    source_lead_id: str
    followup_task_id: str
    original_task_id: str
    reentry_source: str


def normalize_mapping(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def normalize_text(value: object) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _reject_unexpected_fields(
    payload: Mapping[str, object],
    *,
    allowed_fields: set[str],
    contract_name: str,
) -> None:
    unexpected_fields = sorted(set(payload.keys()) - allowed_fields)
    if unexpected_fields:
        raise ValueError(
            f"{contract_name} contains unsupported fields: {', '.join(unexpected_fields)}"
        )


def _require_fields(
    payload: Mapping[str, object],
    *,
    required_fields: set[str],
    contract_name: str,
) -> None:
    missing_fields = sorted(field for field in required_fields if field not in payload)
    if missing_fields:
        raise ValueError(
            f"{contract_name} missing required fields: {', '.join(missing_fields)}"
        )


def validate_lead_input_payload(payload: object) -> LeadInputPayload:
    normalized_payload = normalize_mapping(payload)
    if not normalized_payload:
        raise ValueError("lead_input_payload must be a dict")
    _require_fields(
        normalized_payload,
        required_fields=LEAD_INPUT_FIELDS,
        contract_name="lead_input_payload",
    )
    _reject_unexpected_fields(
        normalized_payload,
        allowed_fields=LEAD_INPUT_FIELDS,
        contract_name="lead_input_payload",
    )
    contact_info = normalize_mapping(normalized_payload.get("contact_info"))
    contact_fields = set(contact_info.keys())
    if contact_fields - {"phone", "email"}:
        raise ValueError(
            "lead_input_payload.contact_info contains unsupported fields: "
            + ", ".join(sorted(contact_fields - {"phone", "email"}))
        )
    qualification_flags = normalize_mapping(normalized_payload.get("qualification_flags"))
    return LeadInputPayload(
        {
            "lead_id": normalize_text(normalized_payload.get("lead_id")),
            "contact_info": {
                "phone": normalize_text(contact_info.get("phone")),
                "email": normalize_text(contact_info.get("email")),
            },
            "project_type": normalize_text(normalized_payload.get("project_type")),
            "scope_summary": normalize_text(normalized_payload.get("scope_summary")),
            "urgency_level": normalize_text(normalized_payload.get("urgency_level")),
            "location": normalize_text(normalized_payload.get("location")),
            "notes": normalize_text(normalized_payload.get("notes")),
            "qualification_flags": dict(qualification_flags),
            "lead_invalid": bool(normalized_payload.get("lead_invalid", False)),
            "unsupported_request": bool(normalized_payload.get("unsupported_request", False)),
            "lead_exists": bool(normalized_payload.get("lead_exists", True)),
        }
    )


def validate_followup_payload(payload: object) -> FollowupPayload:
    normalized_payload = normalize_mapping(payload)
    if not normalized_payload:
        raise ValueError("followup_payload must be a dict")
    _require_fields(
        normalized_payload,
        required_fields=FOLLOWUP_REQUIRED_FIELDS,
        contract_name="followup_payload",
    )
    _reject_unexpected_fields(
        normalized_payload,
        allowed_fields=FOLLOWUP_FIELDS,
        contract_name="followup_payload",
    )
    validated_payload: FollowupPayload = {
        "workflow_type": str(normalized_payload.get("workflow_type") or "").strip(),
        "parent_lead_id": str(normalized_payload.get("parent_lead_id") or "").strip(),
        "missing_fields": [
            str(field).strip()
            for field in list(normalized_payload.get("missing_fields") or [])
            if str(field).strip()
        ],
        "required_action": str(normalized_payload.get("required_action") or "").strip(),
        "status": str(normalized_payload.get("status") or "").strip(),
    }
    if "updated_lead_input" in normalized_payload:
        validated_payload["updated_lead_input"] = validate_lead_input_payload(
            normalized_payload.get("updated_lead_input")
        )
    return validated_payload


def validate_followup_context_payload(payload: object) -> FollowupContextPayload:
    normalized_payload = normalize_mapping(payload)
    if not normalized_payload:
        raise ValueError("followup_context_payload must be a dict")
    _require_fields(
        normalized_payload,
        required_fields=FOLLOWUP_CONTEXT_FIELDS,
        contract_name="followup_context_payload",
    )
    _reject_unexpected_fields(
        normalized_payload,
        allowed_fields=FOLLOWUP_CONTEXT_FIELDS,
        contract_name="followup_context_payload",
    )
    return {
        "task_id": str(normalized_payload.get("task_id") or "").strip(),
        "status": str(normalized_payload.get("status") or "").strip().lower(),
        "intent": str(normalized_payload.get("intent") or "").strip(),
        "payload": validate_followup_payload(normalized_payload.get("payload")),
    }


def validate_reentry_task_metadata(payload: object) -> ReentryTaskMetadataPayload:
    normalized_payload = normalize_mapping(payload)
    if not normalized_payload:
        raise ValueError("reentry_task_metadata must be a dict")
    _require_fields(
        normalized_payload,
        required_fields=REENTRY_METADATA_FIELDS,
        contract_name="reentry_task_metadata",
    )
    _reject_unexpected_fields(
        normalized_payload,
        allowed_fields=REENTRY_METADATA_FIELDS,
        contract_name="reentry_task_metadata",
    )
    return {
        "source_lead_id": str(normalized_payload.get("source_lead_id") or "").strip(),
        "followup_task_id": str(normalized_payload.get("followup_task_id") or "").strip(),
        "original_task_id": str(normalized_payload.get("original_task_id") or "").strip(),
        "reentry_source": str(normalized_payload.get("reentry_source") or "").strip(),
    }


def normalize_real_lead_input(payload: object) -> dict[str, object]:
    normalized = normalize_mapping(payload)
    qualification_flags = normalize_mapping(normalized.get("qualification_flags"))
    contact_info = {
        str(key).strip(): value
        for key, value in normalize_mapping(normalized.get("contact_info")).items()
    }
    normalized_contact_info = {
        "phone": normalize_text(contact_info.get("phone")),
        "email": normalize_text(contact_info.get("email")),
    }
    return {
        "lead_id": normalize_text(normalized.get("lead_id")),
        "contact_info": normalized_contact_info,
        "project_type": (
            str(normalized.get("project_type", "")).strip().lower() or None
        ),
        "scope_summary": normalize_text(normalized.get("scope_summary")),
        "urgency_level": normalize_text(normalized.get("urgency_level")),
        "location": normalize_text(normalized.get("location")),
        "notes": normalize_text(normalized.get("notes")),
        "qualification_flags": qualification_flags,
        "lead_invalid": bool(qualification_flags.get("invalid", False)),
        "unsupported_request": bool(qualification_flags.get("unsupported", False)),
        "lead_exists": bool(normalized.get("lead_exists", True)),
    }


def build_lead_input_payload(payload: object) -> LeadInputPayload:
    return validate_lead_input_payload(normalize_real_lead_input(payload))


def build_real_lead_workflow_payload(lead_input: Mapping[str, object]) -> dict[str, object]:
    return {
        "workflow_type": WORKFLOW_TYPE,
        "lead_id": str(lead_input.get("lead_id", "")).strip(),
        "lead_data": {
            "project_type": str(lead_input.get("project_type", "")).strip(),
            "scope_summary": str(lead_input.get("scope_summary") or "").strip(),
            "contact_info": dict(lead_input.get("contact_info", {}) or {}),
            "qualification_flags": dict(lead_input.get("qualification_flags", {}) or {}),
            "lead_invalid": bool(lead_input.get("lead_invalid", False)),
            "unsupported_request": bool(lead_input.get("unsupported_request", False)),
            "lead_exists": bool(lead_input.get("lead_exists", True)),
        },
    }


def build_followup_payload(
    *,
    workflow_type: object = MISSING_INPUT_FOLLOWUP_WORKFLOW_TYPE,
    parent_lead_id: object,
    missing_fields: object,
    required_action: object = "request_input_completion",
    status: object = "pending",
    updated_lead_input: object | None = None,
) -> FollowupPayload:
    payload: FollowupPayload = {
        "workflow_type": str(workflow_type or "").strip(),
        "parent_lead_id": str(parent_lead_id or "").strip(),
        "missing_fields": [
            str(field).strip() for field in list(missing_fields or []) if str(field).strip()
        ],
        "required_action": str(required_action or "").strip() or "request_input_completion",
        "status": str(status or "").strip() or "pending",
    }
    normalized_updated_input = normalize_mapping(updated_lead_input)
    if normalized_updated_input:
        payload["updated_lead_input"] = build_lead_input_payload(normalized_updated_input)
    return validate_followup_payload(payload)


def followup_payload_from_task(task_data: Mapping[str, object]) -> FollowupPayload:
    payload = normalize_mapping(task_data.get("payload"))
    updated_lead_input = payload.get("updated_lead_input")
    if not normalize_mapping(updated_lead_input):
        updated_lead_input = (
            payload.get("updated_lead_data")
            or payload.get("lead_input")
            or payload.get("lead_data")
            or None
        )
    return build_followup_payload(
        workflow_type=payload.get("workflow_type", ""),
        parent_lead_id=payload.get("parent_lead_id") or payload.get("lead_id"),
        missing_fields=payload.get("missing_fields", []),
        required_action=payload.get("required_action", "request_input_completion"),
        status=payload.get("status", task_data.get("status", "pending")),
        updated_lead_input=updated_lead_input,
    )


def build_followup_context_payload(task_data: Mapping[str, object]) -> FollowupContextPayload:
    return validate_followup_context_payload(
        {
        "task_id": str(task_data.get("task_id", "")).strip(),
        "status": str(task_data.get("status", "")).strip().lower(),
        "intent": str(task_data.get("intent", "")).strip(),
        "payload": followup_payload_from_task(task_data),
        }
    )


def build_reentry_task_metadata(
    *,
    source_lead_id: object,
    followup_task_id: object,
    original_task_id: object,
    reentry_source: object,
) -> ReentryTaskMetadataPayload:
    return validate_reentry_task_metadata(
        {
        "source_lead_id": str(source_lead_id or "").strip(),
        "followup_task_id": str(followup_task_id or "").strip(),
        "original_task_id": str(original_task_id or "").strip(),
        "reentry_source": str(reentry_source or "").strip(),
        }
    )


def missing_real_lead_input_fields(lead_input: Mapping[str, object]) -> list[str]:
    missing: list[str] = []
    for field in REQUIRED_INPUT_FIELDS:
        value = lead_input.get(field)
        if field == "contact_info":
            contact = normalize_mapping(value)
            if not contact or not (contact.get("phone") or contact.get("email")):
                missing.append(field)
        elif value is None:
            missing.append(field)
    return missing


def validate_real_lead_input(lead_input: Mapping[str, object]) -> tuple[bool, str]:
    missing_fields = missing_real_lead_input_fields(lead_input)
    if missing_fields:
        return False, f"missing required fields: {', '.join(missing_fields)}"
    if str(lead_input.get("project_type") or "").strip() not in ALLOWED_PROJECT_TYPES:
        return False, "invalid project_type"
    return True, "input contract valid"


def tracked_missing_followup_fields(lead_input: Mapping[str, object]) -> list[str]:
    return [
        field
        for field in ("contact_info", "project_type", "scope_summary")
        if field in missing_real_lead_input_fields(lead_input)
    ]


def build_real_lead_run_id(lead_id: str) -> str:
    return f"DF-REAL-LEAD-RUN-{lead_id.strip().upper().replace('-', '_')}"


def extract_followup_lead_input(task_data: Mapping[str, object]) -> dict[str, object]:
    payload = followup_payload_from_task(task_data)
    updated_payload = normalize_mapping(payload.get("updated_lead_input"))
    if updated_payload:
        candidate = dict(updated_payload)
    else:
        candidate = {
            "lead_id": payload.get("parent_lead_id"),
        }
    if not candidate.get("lead_id"):
        candidate["lead_id"] = payload.get("parent_lead_id")
    return build_lead_input_payload(candidate)
