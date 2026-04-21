from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping


OWNER_WORKFLOW_STATUSES = frozenset(
    {"pending", "running", "completed", "blocked", "failed", "rejected", "partial_failure"}
)
OWNER_WORKFLOW_STEP_STATUSES = frozenset(
    {"pending", "running", "awaiting_approval", "completed", "blocked", "failed", "rejected"}
)
OWNER_WORKFLOW_STEP_OUTCOMES = frozenset(
    {"success", "retryable_failure", "non_retryable_failure", "approval_rejected"}
)
OWNER_WORKFLOW_RETRY_STATUSES = frozenset(
    {"not_needed", "retrying", "succeeded_after_retry", "exhausted", "not_retryable"}
)
OWNER_WORKFLOW_TYPES = frozenset(
    {
        "browser_then_email",
        "browser_open_extract",
        "browser_open_extract_fill_submit",
        "email_draft_then_send",
        "openai_then_print_document",
        "openai_then_browser_open_fill_submit",
        "print_document",
        "openai_then_email",
    }
)
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9._:@-]+$")
_STEP_CONFIG_FIELDS = frozenset(
    {
        "title",
        "request_text",
        "target_type",
        "target_ref",
        "action_parameters",
        "max_retries",
        "timeout_seconds",
    }
)
DEFAULT_STEP_MAX_RETRIES = 2
MAX_STEP_MAX_RETRIES = 3
DEFAULT_STEP_TIMEOUT_SECONDS = 30
MAX_STEP_TIMEOUT_SECONDS = 300
_WORKFLOW_STEP_PATTERNS: dict[str, tuple[tuple[str, str, str], ...]] = {
    "browser_then_email": (
        ("browser_action", "BROWSER_ACTION", "browser"),
        ("email_action", "EMAIL_ACTION", "email"),
    ),
    "browser_open_extract": (
        ("open_page_action", "BROWSER_ACTION", "browser"),
        ("extract_action", "BROWSER_ACTION", "browser"),
    ),
    "browser_open_extract_fill_submit": (
        ("open_page_action", "BROWSER_ACTION", "browser"),
        ("extract_action", "BROWSER_ACTION", "browser"),
        ("fill_action", "BROWSER_ACTION", "browser"),
        ("submit_action", "BROWSER_ACTION", "browser"),
    ),
    "email_draft_then_send": (
        ("draft_action", "EMAIL_ACTION", "email"),
        ("send_action", "EMAIL_ACTION", "email"),
    ),
    "openai_then_browser_open_fill_submit": (
        ("openai_request", "OPENAI_REQUEST", "openai"),
        ("open_page_action", "BROWSER_ACTION", "browser"),
        ("fill_action", "BROWSER_ACTION", "browser"),
        ("submit_action", "BROWSER_ACTION", "browser"),
    ),
    "openai_then_print_document": (
        ("openai_request", "OPENAI_REQUEST", "openai"),
        ("print_action", "PRINT_DOCUMENT", "printer"),
    ),
    "print_document": (("print_action", "PRINT_DOCUMENT", "printer"),),
    "openai_then_email": (
        ("openai_request", "OPENAI_REQUEST", "openai"),
        ("email_action", "EMAIL_ACTION", "email"),
    ),
}
_UNSET = object()


def _utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _stable_identifier(value: object, *, field_name: str) -> str:
    normalized = _normalize_text(value)
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    if _IDENTIFIER_PATTERN.fullmatch(normalized) is None:
        raise ValueError(f"{field_name} must be a stable identifier")
    return normalized


def _normalize_workflow_type(value: object) -> str:
    normalized = _normalize_text(value).lower()
    if normalized not in OWNER_WORKFLOW_TYPES:
        raise ValueError("workflow_type must be one of: " + ", ".join(sorted(OWNER_WORKFLOW_TYPES)))
    return normalized


def _normalize_workflow_status(value: object) -> str:
    normalized = _normalize_text(value).lower()
    if normalized not in OWNER_WORKFLOW_STATUSES:
        raise ValueError(
            "workflow status must be one of: " + ", ".join(sorted(OWNER_WORKFLOW_STATUSES))
        )
    return normalized


def _normalize_step_status(value: object) -> str:
    normalized = _normalize_text(value).lower()
    if normalized not in OWNER_WORKFLOW_STEP_STATUSES:
        raise ValueError(
            "step status must be one of: " + ", ".join(sorted(OWNER_WORKFLOW_STEP_STATUSES))
        )
    return normalized


def _normalize_sequence_index(value: object) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("sequence_index must be an integer") from exc
    if normalized < 0:
        raise ValueError("sequence_index must be non-negative")
    return normalized


def _normalize_optional_identifier(value: object, *, field_name: str) -> str | None:
    normalized = _normalize_text(value)
    if not normalized:
        return None
    return _stable_identifier(normalized, field_name=field_name)


def _normalize_action_type(value: object) -> str:
    return _stable_identifier(value, field_name="action_type").upper()


def _normalize_action_parameters(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError("action_parameters must be a dict")
    return dict(value)


def _normalize_optional_error(value: object) -> dict[str, str] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise ValueError("last_error must be a dict")
    payload = dict(value)
    unexpected_fields = sorted(set(payload) - {"error_code", "error_message"})
    if unexpected_fields:
        raise ValueError(
            "last_error contains unsupported fields: " + ", ".join(unexpected_fields)
        )
    error_code = _normalize_text(payload.get("error_code")) or None
    error_message = _normalize_text(payload.get("error_message")) or None
    if error_code is None and error_message is None:
        return None
    return {
        "error_code": error_code or "",
        "error_message": error_message or "",
    }


def _normalize_optional_result_payload(value: object) -> dict[str, object] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise ValueError("result_payload must be a dict")
    return dict(value)


def _normalize_attempt_count(value: object) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("attempt_count must be an integer") from exc
    if normalized < 0:
        raise ValueError("attempt_count must be non-negative")
    return normalized


def _normalize_retry_status(value: object) -> str:
    normalized = _normalize_text(value).lower() or "not_needed"
    if normalized not in OWNER_WORKFLOW_RETRY_STATUSES:
        raise ValueError(
            "retry_status must be one of: "
            + ", ".join(sorted(OWNER_WORKFLOW_RETRY_STATUSES))
        )
    return normalized


def _normalize_optional_step_outcome(value: object) -> str | None:
    normalized = _normalize_text(value).lower()
    if not normalized:
        return None
    if normalized not in OWNER_WORKFLOW_STEP_OUTCOMES:
        raise ValueError(
            "step outcome must be one of: "
            + ", ".join(sorted(OWNER_WORKFLOW_STEP_OUTCOMES))
        )
    return normalized


def _normalize_retry_limit(value: object) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("max_retries must be an integer") from exc
    if normalized < 0 or normalized > MAX_STEP_MAX_RETRIES:
        raise ValueError(
            f"max_retries must be between 0 and {MAX_STEP_MAX_RETRIES}"
        )
    return normalized


def _normalize_timeout_seconds(value: object) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("timeout_seconds must be an integer") from exc
    if normalized <= 0 or normalized > MAX_STEP_TIMEOUT_SECONDS:
        raise ValueError(
            f"timeout_seconds must be between 1 and {MAX_STEP_TIMEOUT_SECONDS}"
        )
    return normalized


def _normalize_step_ids(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        candidates = [value]
    elif isinstance(value, (list, tuple, set, frozenset)):
        candidates = list(value)
    else:
        return ()
    normalized: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        stable = _stable_identifier(candidate, field_name="step_ids item")
        if stable in seen:
            continue
        seen.add(stable)
        normalized.append(stable)
    return tuple(normalized)


def _new_identifier(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _default_workflow_title(workflow_type: str) -> str:
    labels = {
        "browser_then_email": "Browser Then Email",
        "browser_open_extract": "Page Review And Extract",
        "browser_open_extract_fill_submit": "Web Form Review And Submit",
        "email_draft_then_send": "Draft Then Send Email",
        "openai_then_print_document": "Generate Review And Print Document",
        "openai_then_browser_open_fill_submit": "Draft Then Browser Update",
        "print_document": "Review And Print Document",
        "openai_then_email": "Generate Then Email",
    }
    return labels.get(workflow_type, workflow_type.replace("_", " ").title())


def _default_step_title(workflow_type: str, step_key: str, action_type: str) -> str:
    labels = {
        ("browser_then_email", "browser_action"): "Browser Step",
        ("browser_then_email", "email_action"): "Email Step",
        ("browser_open_extract", "open_page_action"): "Open Page",
        ("browser_open_extract", "extract_action"): "Extract Page Text",
        ("browser_open_extract_fill_submit", "open_page_action"): "Open Page",
        ("browser_open_extract_fill_submit", "extract_action"): "Review Page",
        ("browser_open_extract_fill_submit", "fill_action"): "Fill Form",
        ("browser_open_extract_fill_submit", "submit_action"): "Submit Form",
        ("email_draft_then_send", "draft_action"): "Draft Email",
        ("email_draft_then_send", "send_action"): "Send Email",
        ("openai_then_print_document", "openai_request"): "Generate Document",
        ("openai_then_print_document", "print_action"): "Print Document",
        ("openai_then_browser_open_fill_submit", "openai_request"): "Draft Content",
        ("openai_then_browser_open_fill_submit", "open_page_action"): "Open Update Page",
        ("openai_then_browser_open_fill_submit", "fill_action"): "Fill Update Form",
        ("openai_then_browser_open_fill_submit", "submit_action"): "Submit Update",
        ("print_document", "print_action"): "Print Document",
        ("openai_then_email", "openai_request"): "Generate Content",
        ("openai_then_email", "email_action"): "Email Step",
    }
    return labels.get((workflow_type, step_key), action_type.replace("_", " ").title())


@dataclass(frozen=True, slots=True)
class OwnerWorkflowStep:
    step_id: str
    workflow_id: str
    owner_id: str
    sequence_index: int
    title: str
    request_text: str
    action_type: str
    target_type: str
    target_ref: str
    action_parameters: dict[str, object]
    status: str = "pending"
    action_id: str | None = None
    approval_id: str | None = None
    result_status: str | None = None
    outcome: str | None = None
    result_summary: str | None = None
    result_payload: dict[str, object] | None = None
    attempt_count: int = 0
    last_error: dict[str, str] | None = None
    retry_status: str = "not_needed"
    max_retries: int = DEFAULT_STEP_MAX_RETRIES
    timeout_seconds: int = DEFAULT_STEP_TIMEOUT_SECONDS
    created_at: str = field(default_factory=_utc_timestamp)
    updated_at: str = field(default_factory=_utc_timestamp)

    def __post_init__(self) -> None:
        object.__setattr__(self, "step_id", _stable_identifier(self.step_id, field_name="step_id"))
        object.__setattr__(
            self,
            "workflow_id",
            _stable_identifier(self.workflow_id, field_name="workflow_id"),
        )
        object.__setattr__(self, "owner_id", _stable_identifier(self.owner_id, field_name="owner_id"))
        object.__setattr__(self, "sequence_index", _normalize_sequence_index(self.sequence_index))
        object.__setattr__(self, "title", _normalize_text(self.title))
        object.__setattr__(self, "request_text", _normalize_text(self.request_text))
        object.__setattr__(self, "action_type", _normalize_action_type(self.action_type))
        object.__setattr__(
            self,
            "target_type",
            _stable_identifier(self.target_type, field_name="target_type").lower(),
        )
        object.__setattr__(self, "target_ref", _normalize_text(self.target_ref))
        object.__setattr__(
            self,
            "action_parameters",
            _normalize_action_parameters(self.action_parameters),
        )
        object.__setattr__(self, "status", _normalize_step_status(self.status))
        object.__setattr__(
            self,
            "action_id",
            _normalize_optional_identifier(self.action_id, field_name="action_id"),
        )
        object.__setattr__(
            self,
            "approval_id",
            _normalize_optional_identifier(self.approval_id, field_name="approval_id"),
        )
        object.__setattr__(self, "result_status", _normalize_text(self.result_status).lower() or None)
        object.__setattr__(self, "outcome", _normalize_optional_step_outcome(self.outcome))
        object.__setattr__(self, "result_summary", _normalize_text(self.result_summary) or None)
        object.__setattr__(
            self,
            "result_payload",
            _normalize_optional_result_payload(self.result_payload),
        )
        object.__setattr__(self, "attempt_count", _normalize_attempt_count(self.attempt_count))
        object.__setattr__(self, "last_error", _normalize_optional_error(self.last_error))
        object.__setattr__(self, "retry_status", _normalize_retry_status(self.retry_status))
        object.__setattr__(self, "max_retries", _normalize_retry_limit(self.max_retries))
        object.__setattr__(self, "timeout_seconds", _normalize_timeout_seconds(self.timeout_seconds))
        object.__setattr__(self, "created_at", _normalize_text(self.created_at) or _utc_timestamp())
        object.__setattr__(self, "updated_at", _normalize_text(self.updated_at) or self.created_at)
        if not self.title:
            raise ValueError("title must not be empty")
        if not self.request_text:
            raise ValueError("request_text must not be empty")
        if not self.target_ref:
            raise ValueError("target_ref must not be empty")

    def to_dict(self) -> dict[str, object]:
        return {
            "step_id": self.step_id,
            "workflow_id": self.workflow_id,
            "owner_id": self.owner_id,
            "sequence_index": self.sequence_index,
            "title": self.title,
            "request_text": self.request_text,
            "action_type": self.action_type,
            "target_type": self.target_type,
            "target_ref": self.target_ref,
            "action_parameters": dict(self.action_parameters),
            "status": self.status,
            "action_id": self.action_id,
            "approval_id": self.approval_id,
            "result_status": self.result_status,
            "outcome": self.outcome,
            "result_summary": self.result_summary,
            "result_payload": None if self.result_payload is None else dict(self.result_payload),
            "attempt_count": self.attempt_count,
            "last_error": None if self.last_error is None else dict(self.last_error),
            "retry_status": self.retry_status,
            "max_retries": self.max_retries,
            "timeout_seconds": self.timeout_seconds,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True, slots=True)
class OwnerWorkflow:
    workflow_id: str
    owner_id: str
    workflow_type: str
    title: str
    step_ids: tuple[str, ...]
    status: str = "pending"
    current_step_id: str | None = None
    completed_step_count: int = 0
    last_action_id: str | None = None
    last_approval_id: str | None = None
    final_result_summary: str | None = None
    created_at: str = field(default_factory=_utc_timestamp)
    updated_at: str = field(default_factory=_utc_timestamp)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "workflow_id",
            _stable_identifier(self.workflow_id, field_name="workflow_id"),
        )
        object.__setattr__(self, "owner_id", _stable_identifier(self.owner_id, field_name="owner_id"))
        object.__setattr__(self, "workflow_type", _normalize_workflow_type(self.workflow_type))
        object.__setattr__(self, "title", _normalize_text(self.title))
        object.__setattr__(self, "step_ids", _normalize_step_ids(self.step_ids))
        object.__setattr__(self, "status", _normalize_workflow_status(self.status))
        object.__setattr__(
            self,
            "current_step_id",
            _normalize_optional_identifier(self.current_step_id, field_name="current_step_id"),
        )
        try:
            completed_step_count = int(self.completed_step_count)
        except (TypeError, ValueError) as exc:
            raise ValueError("completed_step_count must be an integer") from exc
        if completed_step_count < 0:
            raise ValueError("completed_step_count must be non-negative")
        object.__setattr__(self, "completed_step_count", completed_step_count)
        object.__setattr__(
            self,
            "last_action_id",
            _normalize_optional_identifier(self.last_action_id, field_name="last_action_id"),
        )
        object.__setattr__(
            self,
            "last_approval_id",
            _normalize_optional_identifier(self.last_approval_id, field_name="last_approval_id"),
        )
        object.__setattr__(
            self,
            "final_result_summary",
            _normalize_text(self.final_result_summary) or None,
        )
        object.__setattr__(self, "created_at", _normalize_text(self.created_at) or _utc_timestamp())
        object.__setattr__(self, "updated_at", _normalize_text(self.updated_at) or self.created_at)
        if not self.title:
            raise ValueError("title must not be empty")

    def to_dict(self) -> dict[str, object]:
        return {
            "workflow_id": self.workflow_id,
            "owner_id": self.owner_id,
            "workflow_type": self.workflow_type,
            "title": self.title,
            "step_ids": list(self.step_ids),
            "status": self.status,
            "current_step_id": self.current_step_id,
            "completed_step_count": self.completed_step_count,
            "last_action_id": self.last_action_id,
            "last_approval_id": self.last_approval_id,
            "final_result_summary": self.final_result_summary,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


def create_owner_workflow_step(
    *,
    workflow_id: object,
    owner_id: object,
    sequence_index: object,
    title: object,
    request_text: object,
    action_type: object,
    target_type: object,
    target_ref: object,
    action_parameters: Mapping[str, object] | None,
    status: object = "pending",
    action_id: object | None = None,
    approval_id: object | None = None,
    result_status: object | None = None,
    outcome: object | None = None,
    result_summary: object | None = None,
    result_payload: Mapping[str, object] | None = None,
    attempt_count: object = 0,
    last_error: Mapping[str, object] | None = None,
    retry_status: object = "not_needed",
    max_retries: object = DEFAULT_STEP_MAX_RETRIES,
    timeout_seconds: object = DEFAULT_STEP_TIMEOUT_SECONDS,
    created_at: object | None = None,
    updated_at: object | None = None,
    step_id: object | None = None,
) -> OwnerWorkflowStep:
    timestamp = _normalize_text(created_at) or _utc_timestamp()
    resolved_action_parameters = dict(action_parameters or {})
    resolved_timeout_seconds = timeout_seconds
    if timeout_seconds == DEFAULT_STEP_TIMEOUT_SECONDS and "timeout_seconds" in resolved_action_parameters:
        resolved_timeout_seconds = resolved_action_parameters["timeout_seconds"]
    return OwnerWorkflowStep(
        step_id=_normalize_text(step_id) or _new_identifier("owner-workflow-step"),
        workflow_id=_normalize_text(workflow_id),
        owner_id=_normalize_text(owner_id),
        sequence_index=sequence_index,
        title=title,
        request_text=request_text,
        action_type=action_type,
        target_type=target_type,
        target_ref=target_ref,
        action_parameters=resolved_action_parameters,
        status=_normalize_text(status) or "pending",
        action_id=action_id,
        approval_id=approval_id,
        result_status=result_status,
        outcome=outcome,
        result_summary=result_summary,
        result_payload=None if result_payload is None else dict(result_payload),
        attempt_count=attempt_count,
        last_error=None if last_error is None else dict(last_error),
        retry_status=retry_status,
        max_retries=max_retries,
        timeout_seconds=resolved_timeout_seconds,
        created_at=timestamp,
        updated_at=_normalize_text(updated_at) or timestamp,
    )


def update_owner_workflow_step(
    step: OwnerWorkflowStep,
    *,
    status: object | None = None,
    action_id: object = _UNSET,
    approval_id: object = _UNSET,
    result_status: object = _UNSET,
    outcome: object = _UNSET,
    result_summary: object = _UNSET,
    result_payload: Mapping[str, object] | None | object = _UNSET,
    attempt_count: object = _UNSET,
    action_parameters: Mapping[str, object] | object = _UNSET,
    last_error: Mapping[str, object] | None | object = _UNSET,
    retry_status: object = _UNSET,
    max_retries: object = _UNSET,
    timeout_seconds: object = _UNSET,
    updated_at: object | None = None,
) -> OwnerWorkflowStep:
    return OwnerWorkflowStep(
        step_id=step.step_id,
        workflow_id=step.workflow_id,
        owner_id=step.owner_id,
        sequence_index=step.sequence_index,
        title=step.title,
        request_text=step.request_text,
        action_type=step.action_type,
        target_type=step.target_type,
        target_ref=step.target_ref,
        action_parameters=(
            dict(step.action_parameters)
            if action_parameters is _UNSET
            else dict(action_parameters)
        ),
        status=_normalize_text(status) or step.status,
        action_id=step.action_id if action_id is _UNSET else action_id,
        approval_id=step.approval_id if approval_id is _UNSET else approval_id,
        result_status=step.result_status if result_status is _UNSET else result_status,
        outcome=step.outcome if outcome is _UNSET else outcome,
        result_summary=step.result_summary if result_summary is _UNSET else result_summary,
        result_payload=(
            step.result_payload
            if result_payload is _UNSET
            else (None if result_payload is None else dict(result_payload))
        ),
        attempt_count=step.attempt_count if attempt_count is _UNSET else attempt_count,
        last_error=step.last_error if last_error is _UNSET else last_error,
        retry_status=step.retry_status if retry_status is _UNSET else retry_status,
        max_retries=step.max_retries if max_retries is _UNSET else max_retries,
        timeout_seconds=step.timeout_seconds if timeout_seconds is _UNSET else timeout_seconds,
        created_at=step.created_at,
        updated_at=_normalize_text(updated_at) or _utc_timestamp(),
    )


def create_owner_workflow(
    *,
    owner_id: object,
    workflow_type: object,
    title: object | None = None,
    step_ids: object = (),
    status: object = "pending",
    current_step_id: object | None = None,
    completed_step_count: object = 0,
    last_action_id: object | None = None,
    last_approval_id: object | None = None,
    final_result_summary: object | None = None,
    created_at: object | None = None,
    updated_at: object | None = None,
    workflow_id: object | None = None,
) -> OwnerWorkflow:
    timestamp = _normalize_text(created_at) or _utc_timestamp()
    normalized_workflow_type = _normalize_workflow_type(workflow_type)
    return OwnerWorkflow(
        workflow_id=_normalize_text(workflow_id) or _new_identifier("owner-workflow"),
        owner_id=_normalize_text(owner_id),
        workflow_type=normalized_workflow_type,
        title=_normalize_text(title) or _default_workflow_title(normalized_workflow_type),
        step_ids=step_ids,
        status=_normalize_text(status) or "pending",
        current_step_id=current_step_id,
        completed_step_count=completed_step_count,
        last_action_id=last_action_id,
        last_approval_id=last_approval_id,
        final_result_summary=final_result_summary,
        created_at=timestamp,
        updated_at=_normalize_text(updated_at) or timestamp,
    )


def update_owner_workflow(
    workflow: OwnerWorkflow,
    *,
    status: object | None = None,
    current_step_id: object = _UNSET,
    completed_step_count: object | None = None,
    last_action_id: object = _UNSET,
    last_approval_id: object = _UNSET,
    final_result_summary: object = _UNSET,
    updated_at: object | None = None,
) -> OwnerWorkflow:
    return OwnerWorkflow(
        workflow_id=workflow.workflow_id,
        owner_id=workflow.owner_id,
        workflow_type=workflow.workflow_type,
        title=workflow.title,
        step_ids=workflow.step_ids,
        status=_normalize_text(status) or workflow.status,
        current_step_id=workflow.current_step_id if current_step_id is _UNSET else current_step_id,
        completed_step_count=(
            workflow.completed_step_count
            if completed_step_count is None
            else completed_step_count
        ),
        last_action_id=workflow.last_action_id if last_action_id is _UNSET else last_action_id,
        last_approval_id=(
            workflow.last_approval_id if last_approval_id is _UNSET else last_approval_id
        ),
        final_result_summary=(
            workflow.final_result_summary
            if final_result_summary is _UNSET
            else final_result_summary
        ),
        created_at=workflow.created_at,
        updated_at=_normalize_text(updated_at) or _utc_timestamp(),
    )


def summarize_owner_workflow_step(step: OwnerWorkflowStep) -> dict[str, object]:
    return {
        "step_id": step.step_id,
        "sequence_index": step.sequence_index,
        "title": step.title,
        "action_type": step.action_type,
        "status": step.status,
        "action_id": step.action_id,
        "approval_id": step.approval_id,
        "result_status": step.result_status,
        "outcome": step.outcome,
        "result_summary": step.result_summary,
        "attempt_count": step.attempt_count,
        "last_error": None if step.last_error is None else dict(step.last_error),
        "retry_status": step.retry_status,
        "max_retries": step.max_retries,
        "timeout_seconds": step.timeout_seconds,
    }


def _normalize_step_config(
    value: object,
    *,
    workflow_type: str,
    step_key: str,
    action_type: str,
    default_target_ref: str,
) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{workflow_type}.{step_key} must be a dict")
    payload = dict(value)
    unexpected_fields = sorted(set(payload) - _STEP_CONFIG_FIELDS)
    if unexpected_fields:
        raise ValueError(
            f"{workflow_type}.{step_key} contains unsupported fields: "
            + ", ".join(unexpected_fields)
        )
    request_text = _normalize_text(payload.get("request_text"))
    if not request_text:
        raise ValueError(f"{workflow_type}.{step_key}.request_text must not be empty")
    action_parameters = payload.get("action_parameters")
    if not isinstance(action_parameters, Mapping):
        raise ValueError(f"{workflow_type}.{step_key}.action_parameters must be a dict")
    target_type = _normalize_text(payload.get("target_type")) or "adapter"
    target_ref = _normalize_text(payload.get("target_ref")) or default_target_ref
    timeout_candidate = payload.get("timeout_seconds")
    if timeout_candidate is None:
        timeout_candidate = action_parameters.get("timeout_seconds", DEFAULT_STEP_TIMEOUT_SECONDS)
    return {
        "title": _normalize_text(payload.get("title"))
        or _default_step_title(workflow_type, step_key, action_type),
        "request_text": request_text,
        "action_type": action_type,
        "target_type": target_type,
        "target_ref": target_ref,
        "action_parameters": dict(action_parameters),
        "max_retries": _normalize_retry_limit(
            payload.get("max_retries", DEFAULT_STEP_MAX_RETRIES)
        ),
        "timeout_seconds": _normalize_timeout_seconds(timeout_candidate),
    }


def instantiate_workflow_steps(
    *,
    workflow_id: object,
    owner_id: object,
    workflow_type: object,
    workflow_payload: Mapping[str, object],
    created_at: object | None = None,
) -> tuple[OwnerWorkflowStep, ...]:
    normalized_workflow_id = _normalize_text(workflow_id)
    normalized_owner_id = _normalize_text(owner_id)
    normalized_workflow_type = _normalize_workflow_type(workflow_type)
    if not isinstance(workflow_payload, Mapping):
        raise ValueError("workflow_payload must be a dict")
    payload = dict(workflow_payload)
    expected_pattern = _WORKFLOW_STEP_PATTERNS[normalized_workflow_type]
    expected_keys = {item[0] for item in expected_pattern}
    unexpected_keys = sorted(set(payload) - expected_keys)
    if unexpected_keys:
        raise ValueError(
            "workflow_payload contains unsupported fields: " + ", ".join(unexpected_keys)
        )
    missing_keys = [step_key for step_key, _action_type, _ref in expected_pattern if step_key not in payload]
    if missing_keys:
        raise ValueError("workflow_payload missing required fields: " + ", ".join(missing_keys))

    timestamp = _normalize_text(created_at) or _utc_timestamp()
    steps: list[OwnerWorkflowStep] = []
    for index, (step_key, action_type, default_target_ref) in enumerate(expected_pattern):
        normalized_config = _normalize_step_config(
            payload[step_key],
            workflow_type=normalized_workflow_type,
            step_key=step_key,
            action_type=action_type,
            default_target_ref=default_target_ref,
        )
        steps.append(
            create_owner_workflow_step(
                step_id=f"{normalized_workflow_id}.step{index + 1}",
                workflow_id=normalized_workflow_id,
                owner_id=normalized_owner_id,
                sequence_index=index,
                title=normalized_config["title"],
                request_text=normalized_config["request_text"],
                action_type=normalized_config["action_type"],
                target_type=normalized_config["target_type"],
                target_ref=normalized_config["target_ref"],
                action_parameters=normalized_config["action_parameters"],
                max_retries=normalized_config["max_retries"],
                timeout_seconds=normalized_config["timeout_seconds"],
                created_at=timestamp,
                updated_at=timestamp,
            )
        )
    return tuple(steps)
