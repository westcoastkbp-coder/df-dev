from __future__ import annotations

import inspect
import json
import re
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.execution.action_contract import (
    ActionContractViolation,
    build_action_contract,
    build_action_result_contract,
)
from app.execution.paths import OUTPUT_DIR, ROOT_DIR
from app.memory.memory_store import CanonicalMemoryStore
from app.memory.memory_object import make_trace_object
from app.memory.memory_registry import compute_artifact_key, register_artifact
from app.ownerbox.context_boundary import (
    OwnerRequestContextRef,
    assemble_owner_canonical_context,
    assemble_owner_context,
)
from app.ownerbox.domain import (
    OWNERBOX_DOMAIN_TYPE,
    OwnerActionScope,
    OwnerDomain,
    OwnerMemoryScope,
    OwnerTrustProfile,
)
from app.ownerbox.owner_action_queue import (
    OwnerActionQueue,
    OwnerActionQueueEntry,
    create_owner_action_queue_entry,
)
from app.ownerbox.owner_approval import (
    OwnerApproval,
    OwnerApprovalStore,
    create_owner_approval,
    resolve_owner_approval,
)
from app.ownerbox.owner_request import (
    OwnerRequest,
    create_owner_request,
    update_owner_request,
)
from app.ownerbox.owner_response_plan import (
    OwnerResponsePlan,
    create_owner_response_plan,
)
from app.ownerbox.owner_session import (
    OwnerSession,
    create_owner_session,
    increment_owner_session_request,
)
from app.ownerbox.trust_model import (
    ActionRiskProfile,
    classify_action_risk,
    is_known_action_type,
)
from runtime.system_log import log_event


DEFAULT_OWNER_MODEL = "gpt-5-mini"
DEFAULT_MAX_TOKENS = 240
DEFAULT_TEMPERATURE = 0.2
DEFAULT_TARGET_REF = "openai"
DEFAULT_TARGET_TYPE = "adapter"
DEFAULT_REQUESTED_BY = "ownerbox_interaction_v1"
OWNER_APPROVAL_TRACE_ARTIFACT_TYPE = "owner_approval_trace"
OWNER_APPROVAL_TRACE_DIR = OUTPUT_DIR / "traces" / "owner_approvals"
_MAX_PROMPT_LENGTH = 480
_MAX_SUMMARY_LENGTH = 120
_MAX_PREVIEW_LENGTH = 180
_SAFE_FILENAME_PATTERN = re.compile(r"[^A-Za-z0-9._-]+")
RETRYABLE_ACTION_ERROR_CODES = frozenset({"timeout", "transport_error"})
NON_RETRYABLE_ACTION_ERROR_CODES = frozenset(
    {"validation_error", "provider_not_configured", "unsupported_operation"}
)
SAFE_BROWSER_RETRY_OPERATIONS = frozenset({"open_page", "extract_text"})


def _utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _collapse_whitespace(value: object) -> str:
    return " ".join(_normalize_text(value).split())


def _mapping(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _truncate(value: object, *, limit: int) -> str:
    normalized = _collapse_whitespace(value)
    if len(normalized) <= limit:
        return normalized
    return normalized[: max(0, limit - 3)].rstrip() + "..."


def _new_identifier(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _safe_filename(value: object) -> str:
    normalized = _normalize_text(value) or "unknown"
    safe = _SAFE_FILENAME_PATTERN.sub("_", normalized).strip("._")
    return safe or "unknown"


def _dispatcher_supports_kwarg(
    dispatcher: Callable[..., dict[str, object]], name: str
) -> bool:
    try:
        signature = inspect.signature(dispatcher)
    except (TypeError, ValueError):
        return False
    for parameter in signature.parameters.values():
        if parameter.kind == inspect.Parameter.VAR_KEYWORD:
            return True
        if parameter.name == name:
            return True
    return False


def _resolved_memory_summary(record: Mapping[str, object]) -> str:
    payload = _mapping(record.get("payload"))
    summary = (
        payload.get("summary")
        or payload.get("note")
        or payload.get("text")
        or record.get("id")
    )
    return _truncate(
        f"{_normalize_text(record.get('id'))}|{_normalize_text(record.get('type'))}|{summary}",
        limit=90,
    )


def _canonical_context_summary(entry: Mapping[str, object]) -> str:
    return _truncate(
        f"{_normalize_text(entry.get('memory_id'))}|{_normalize_text(entry.get('summary'))}",
        limit=90,
    )


def _build_owner_prompt(
    *,
    session: OwnerSession,
    request: OwnerRequest,
    owner_context: Mapping[str, object],
) -> str:
    resolved_memory = owner_context.get("resolved_memory")
    memory_records = list(resolved_memory) if isinstance(resolved_memory, list) else []
    canonical_memory_context = _mapping(owner_context.get("canonical_memory_context"))
    canonical_facts = canonical_memory_context.get("fact_summaries")
    canonical_preferences = canonical_memory_context.get("preferences")
    canonical_decisions = canonical_memory_context.get("relevant_decisions")
    fact_entries = list(canonical_facts) if isinstance(canonical_facts, list) else []
    preference_entries = (
        list(canonical_preferences) if isinstance(canonical_preferences, list) else []
    )
    decision_entries = (
        list(canonical_decisions) if isinstance(canonical_decisions, list) else []
    )
    lines = [
        "Digital Foreman OwnerBox Interaction Layer v1",
        f"owner_session_id={session.owner_session_id}",
        f"request_id={request.request_id}",
        f"owner_id={request.owner_id}",
        f"request_type={request.request_type}",
        f"priority_class={request.priority_class}",
        f"active_language={session.active_language}",
        f"detected_language={request.detected_language}",
        f"context_ref={request.context_ref or session.context_ref or 'none'}",
        f"resolved_memory_count={len(memory_records)}",
        f"canonical_memory_count={len(fact_entries) + len(preference_entries) + len(decision_entries)}",
    ]
    for index, record in enumerate(memory_records[:3], start=1):
        lines.append(f"memory_{index}={_resolved_memory_summary(record)}")
    for index, entry in enumerate(fact_entries[:2], start=1):
        lines.append(
            f"canonical_fact_{index}={_canonical_context_summary(_mapping(entry))}"
        )
    for index, entry in enumerate(preference_entries[:2], start=1):
        lines.append(
            f"canonical_preference_{index}={_canonical_context_summary(_mapping(entry))}"
        )
    for index, entry in enumerate(decision_entries[:2], start=1):
        lines.append(
            f"canonical_decision_{index}={_canonical_context_summary(_mapping(entry))}"
        )
    lines.extend(
        [
            f"request_summary={_truncate(request.normalized_payload.get('summary'), limit=_MAX_SUMMARY_LENGTH)}",
            "Return concise owner-facing text only.",
        ]
    )
    return _truncate("\n".join(lines), limit=_MAX_PROMPT_LENGTH)


def _result_payload_text(action_result: Mapping[str, object]) -> str:
    payload = _mapping(action_result.get("payload"))
    summary = _collapse_whitespace(payload.get("summary"))
    if summary:
        return summary
    text = _collapse_whitespace(payload.get("text"))
    if text:
        return text
    note = _collapse_whitespace(payload.get("note"))
    if note:
        return note
    error_message = _collapse_whitespace(action_result.get("error_message"))
    if error_message:
        return f"Owner request could not be completed: {error_message}"
    return "Owner request completed without a response summary."


def _response_type_from_result_status(result_status: str) -> str:
    normalized = _normalize_text(result_status).lower()
    if normalized == "blocked":
        return "input_error"
    if normalized == "failed":
        return "error"
    return "summary_text"


def _response_plan_status(result_status: str) -> str:
    normalized = _normalize_text(result_status).lower()
    if normalized == "blocked":
        return "blocked"
    if normalized == "failed":
        return "failed"
    return "planned"


def _request_status(result_status: str) -> str:
    normalized = _normalize_text(result_status).lower()
    if normalized == "blocked":
        return "blocked"
    if normalized == "failed":
        return "failed"
    return "completed"


def _queue_status(result_status: str, *, requires_confirmation: bool) -> str:
    normalized = _normalize_text(result_status).lower()
    if normalized == "success" and requires_confirmation:
        return "awaiting_confirmation"
    if normalized == "success":
        return "success"
    if normalized == "failed":
        return "failed"
    return "blocked"


def _friendly_action_description(action_type: str) -> str:
    mapping = {
        "OPENAI_REQUEST": "generate an owner-facing response",
        "READ_FILE": "read a file",
        "WRITE_FILE": "write a file",
        "EMAIL_ACTION": "execute an email action",
        "BROWSER_ACTION": "execute a browser action",
        "SEND_EMAIL": "send an email",
        "SEND_SMS": "send an SMS",
        "MAKE_CALL": "place a phone call",
        "API_REQUEST": "perform an API request",
        "BROWSER_TOOL": "use the browser tool",
        "PRINT_DOCUMENT": "print a document",
    }
    return mapping.get(action_type.upper(), f"perform {action_type.lower()}")


def _operation_preview_text(
    *,
    action_type: str,
    parameters: Mapping[str, object],
) -> str | None:
    operation = _normalize_text(parameters.get("operation")).lower()
    if action_type == "EMAIL_ACTION":
        recipient_list = (
            list(parameters.get("to")) if isinstance(parameters.get("to"), list) else []
        )
        recipient_summary = ", ".join(str(item) for item in recipient_list[:2])
        if len(recipient_list) > 2:
            recipient_summary += ", ..."
        suffix = f" to {recipient_summary}" if recipient_summary else ""
        if operation == "create_draft":
            return _truncate(
                f"System wants to prepare a draft email{suffix}.",
                limit=_MAX_PREVIEW_LENGTH,
            )
        if operation == "send_email":
            return _truncate(
                f"System wants to send an email{suffix}.", limit=_MAX_PREVIEW_LENGTH
            )
        if operation == "reply_email":
            reply_to_id = _normalize_text(parameters.get("reply_to_id")) or "thread"
            return _truncate(
                f"System wants to reply to email {reply_to_id}.",
                limit=_MAX_PREVIEW_LENGTH,
            )
    if action_type == "BROWSER_ACTION":
        url = _normalize_text(parameters.get("url"))
        selector = _normalize_text(parameters.get("selector"))
        labels = {
            "open_page": f"System wants to open {url}.",
            "extract_text": f"System wants to extract text from {selector or 'page element'} on {url}.",
            "fill_form": f"System wants to fill a form on {url}.",
            "click_element": f"System wants to click {selector or 'page element'} on {url}.",
            "submit_form": f"System wants to submit a form on {url}.",
        }
        if operation in labels:
            return _truncate(labels[operation], limit=_MAX_PREVIEW_LENGTH)
    if action_type == "PRINT_DOCUMENT":
        document_title = (
            _normalize_text(parameters.get("document_title")) or "Untitled Document"
        )
        document_text = _collapse_whitespace(parameters.get("document_text"))
        printer_name = _normalize_text(parameters.get("printer_name"))
        suffix = f" on {printer_name}" if printer_name else ""
        if document_text:
            return _truncate(
                f"System wants to print '{document_title}'{suffix}. Preview: {document_text}",
                limit=_MAX_PREVIEW_LENGTH,
            )
        return _truncate(
            f"System wants to print '{document_title}'{suffix}.",
            limit=_MAX_PREVIEW_LENGTH,
        )
    return None


def _preview_text(
    *,
    request: OwnerRequest,
    action_contract: Mapping[str, object],
) -> str:
    action_type = _normalize_text(action_contract.get("action_type")).upper()
    operation_preview = _operation_preview_text(
        action_type=action_type,
        parameters=_mapping(action_contract.get("parameters")),
    )
    if operation_preview:
        return operation_preview
    return _truncate(
        f"System wants to {_friendly_action_description(action_type)} for request: "
        f"{request.normalized_payload.get('summary') or request.request_text}",
        limit=_MAX_PREVIEW_LENGTH,
    )


def _confirmation_summary(
    *,
    preview_text: str,
    trust_class: str,
    action_contract: Mapping[str, object],
) -> str:
    action_type = _normalize_text(action_contract.get("action_type")).upper()
    operation = _normalize_text(
        _mapping(action_contract.get("parameters")).get("operation")
    ).lower()
    if action_type == "EMAIL_ACTION" and operation == "create_draft":
        return "System prepared a draft email. Review or approve."
    if action_type == "PRINT_DOCUMENT":
        return _truncate(
            f"{preview_text} Physical print approval required.",
            limit=_MAX_PREVIEW_LENGTH,
        )
    if action_type in {"EMAIL_ACTION", "BROWSER_ACTION"}:
        return _truncate(
            f"{preview_text} Approval required.", limit=_MAX_PREVIEW_LENGTH
        )
    return _truncate(
        f"{preview_text} Confirm? trust_class={trust_class}",
        limit=_MAX_PREVIEW_LENGTH,
    )


def _response_metadata(
    *,
    session: OwnerSession,
    request: OwnerRequest,
    action_contract: Mapping[str, object] | None,
    action_result: Mapping[str, object] | None,
    owner_context: Mapping[str, object],
    trace_metadata: Mapping[str, object],
    approval: OwnerApproval | None,
    risk_profile: ActionRiskProfile | None,
    preview_text: str | None,
) -> dict[str, object]:
    action = _mapping(action_contract)
    result = _mapping(action_result)
    payload = _mapping(result.get("payload"))
    result_metadata = _mapping(payload.get("metadata"))
    return {
        "owner_session_id": session.owner_session_id,
        "request_id": request.request_id,
        "owner_id": request.owner_id,
        "domain_type": OWNERBOX_DOMAIN_TYPE,
        "request_type": request.request_type,
        "priority_class": request.priority_class,
        "context_ref": request.context_ref or session.context_ref,
        "action_id": _normalize_text(action.get("action_id")) or None,
        "result_status": _normalize_text(result.get("status")) or "blocked",
        "result_type": _normalize_text(result.get("result_type")) or "owner_response",
        "started_at": request.created_at,
        "completed_at": _normalize_text(result.get("timestamp"))
        or trace_metadata.get("completed_at"),
        "latency_ms": int(result_metadata.get("dispatcher_latency_ms") or 0),
        "approval_id": None if approval is None else approval.approval_id,
        "approval_status": None if approval is None else approval.status,
        "trust_class": None if risk_profile is None else risk_profile.trust_class,
        "preview_text": preview_text,
        "boundary_application": dict(
            _mapping(owner_context.get("boundary_application"))
        ),
        "trace_metadata": dict(trace_metadata),
        "action_risk_profile": None if risk_profile is None else risk_profile.to_dict(),
    }


def _result_metadata(action_result: Mapping[str, object] | None) -> dict[str, object]:
    if action_result is None:
        return {}
    payload = _mapping(action_result.get("payload"))
    return _mapping(payload.get("metadata"))


def _result_attempt_count(action_result: Mapping[str, object] | None) -> int:
    metadata = _result_metadata(action_result)
    try:
        normalized = int(metadata.get("attempt_count", 0))
    except (TypeError, ValueError):
        return 0 if action_result is None else 1
    return max(0, normalized)


def _result_error_code(action_result: Mapping[str, object] | None) -> str:
    if action_result is None:
        return ""
    return _normalize_text(action_result.get("error_code")).lower()


def _validate_owner_scope(
    *,
    owner_id: str,
    session: OwnerSession | None,
    owner_domain: OwnerDomain,
    trust_profile: OwnerTrustProfile,
) -> None:
    if owner_domain.owner_id != owner_id:
        raise ValueError("owner_id must match owner_domain.owner_id")
    if trust_profile.owner_id != owner_id:
        raise ValueError("owner_id must match trust_profile.owner_id")
    if session is not None and session.owner_id != owner_id:
        raise ValueError("owner_id must match session.owner_id")


def create_owner_trace_metadata(
    *,
    owner_session_id: object,
    request_id: object,
    owner_id: object,
    action_id: object,
    result_status: object,
    response_plan_id: object,
    request_created_at: object,
    response_created_at: object,
    completed_at: object,
    approval_id: object = None,
    approval_status: object = None,
    trust_class: object = None,
    approval_created: bool = False,
    approval_resolved: bool = False,
) -> dict[str, object]:
    return {
        "owner_session_id": _normalize_text(owner_session_id),
        "request_id": _normalize_text(request_id),
        "owner_id": _normalize_text(owner_id),
        "domain_type": OWNERBOX_DOMAIN_TYPE,
        "action_id": _normalize_text(action_id) or None,
        "result_status": _normalize_text(result_status).lower() or "blocked",
        "response_plan_id": _normalize_text(response_plan_id),
        "request_created_at": _normalize_text(request_created_at),
        "response_created_at": _normalize_text(response_created_at),
        "completed_at": _normalize_text(completed_at),
        "approval_id": _normalize_text(approval_id) or None,
        "approval_status": _normalize_text(approval_status).lower() or None,
        "trust_class": _normalize_text(trust_class).lower() or None,
        "approval_created": bool(approval_created),
        "approval_resolved": bool(approval_resolved),
    }


@dataclass(frozen=True, slots=True)
class OwnerInteractionResult:
    session: OwnerSession
    request: OwnerRequest
    owner_context: dict[str, object]
    action_contract: dict[str, object] | None
    action_result: dict[str, object] | None
    response_plan: OwnerResponsePlan
    queue_entry: OwnerActionQueueEntry | None
    trace_metadata: dict[str, object]
    approval: OwnerApproval | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "session": self.session.to_dict(),
            "request": self.request.to_dict(),
            "owner_context": dict(self.owner_context),
            "action_contract": None
            if self.action_contract is None
            else dict(self.action_contract),
            "action_result": None
            if self.action_result is None
            else dict(self.action_result),
            "response_plan": self.response_plan.to_dict(),
            "queue_entry": None
            if self.queue_entry is None
            else self.queue_entry.to_dict(),
            "trace_metadata": dict(self.trace_metadata),
            "approval": None if self.approval is None else self.approval.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class OwnerApprovalResolutionResult:
    approval: OwnerApproval | None
    action_contract: dict[str, object] | None
    action_result: dict[str, object] | None
    response_plan: OwnerResponsePlan
    queue_entry: OwnerActionQueueEntry | None
    trace_metadata: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "approval": None if self.approval is None else self.approval.to_dict(),
            "action_contract": None
            if self.action_contract is None
            else dict(self.action_contract),
            "action_result": None
            if self.action_result is None
            else dict(self.action_result),
            "response_plan": self.response_plan.to_dict(),
            "queue_entry": None
            if self.queue_entry is None
            else self.queue_entry.to_dict(),
            "trace_metadata": dict(self.trace_metadata),
        }


@dataclass(slots=True)
class _PendingApprovalState:
    approval: OwnerApproval
    session: OwnerSession
    request: OwnerRequest
    owner_context: dict[str, object]
    action_contract: dict[str, object]
    queue_entry_id: str
    preview_text: str
    risk_profile: ActionRiskProfile
    scenario_type: str | None = None
    workflow_id: str | None = None
    workflow_step_id: str | None = None
    max_retries: int = 0
    step_timeout_seconds: int | None = None
    action_result: dict[str, object] | None = None
    response_plan_id: str | None = None


class OwnerOrchestrator:
    def __init__(
        self,
        *,
        dispatcher: Callable[..., dict[str, object]] | None = None,
        action_queue: OwnerActionQueue | None = None,
        approval_store: OwnerApprovalStore | None = None,
        model: str = DEFAULT_OWNER_MODEL,
        max_tokens: int = DEFAULT_MAX_TOKENS,
        temperature: float = DEFAULT_TEMPERATURE,
    ) -> None:
        if dispatcher is None:
            from app.execution.action_dispatcher import dispatch_action

            dispatcher = dispatch_action
        self._dispatcher = dispatcher
        self._action_queue = action_queue or OwnerActionQueue()
        self._approval_store = approval_store or OwnerApprovalStore()
        self._approval_runtime: dict[str, _PendingApprovalState] = {}
        self._model = model
        self._max_tokens = max_tokens
        self._temperature = temperature

    @property
    def action_queue(self) -> OwnerActionQueue:
        return self._action_queue

    @property
    def approval_store(self) -> OwnerApprovalStore:
        return self._approval_store

    def export_pending_approval_state(
        self, approval_id: object
    ) -> dict[str, object] | None:
        approval = self._approval_store.get(approval_id)
        pending = (
            None
            if approval is None
            else self._approval_runtime.get(approval.approval_id)
        )
        if approval is None or pending is None:
            return None
        queue_entry = self._action_queue.get(pending.queue_entry_id)
        return {
            "approval": approval.to_dict(),
            "session": pending.session.to_dict(),
            "request": pending.request.to_dict(),
            "owner_context": dict(pending.owner_context),
            "action_contract": dict(pending.action_contract),
            "queue_entry": None if queue_entry is None else queue_entry.to_dict(),
            "preview_text": pending.preview_text,
            "risk_profile": pending.risk_profile.to_dict(),
            "scenario_type": pending.scenario_type,
            "workflow_id": pending.workflow_id,
            "workflow_step_id": pending.workflow_step_id,
            "max_retries": pending.max_retries,
            "step_timeout_seconds": pending.step_timeout_seconds,
            "action_result": None
            if pending.action_result is None
            else dict(pending.action_result),
            "response_plan_id": pending.response_plan_id,
        }

    def hydrate_pending_approval_state(
        self, payload: Mapping[str, object]
    ) -> OwnerApproval:
        approval = OwnerApproval(**dict(_mapping(payload.get("approval"))))
        session = OwnerSession(**dict(_mapping(payload.get("session"))))
        request = OwnerRequest(**dict(_mapping(payload.get("request"))))
        owner_context = dict(_mapping(payload.get("owner_context")))
        action_contract = dict(_mapping(payload.get("action_contract")))
        risk_profile = ActionRiskProfile(**dict(_mapping(payload.get("risk_profile"))))
        queue_entry_payload = _mapping(payload.get("queue_entry"))
        queue_entry_id = _normalize_text(queue_entry_payload.get("queue_entry_id"))
        if queue_entry_payload:
            queue_entry = OwnerActionQueueEntry(**dict(queue_entry_payload))
            if self._action_queue.get(queue_entry.queue_entry_id) is None:
                self._action_queue.enqueue(queue_entry)
            queue_entry_id = queue_entry.queue_entry_id
        if not queue_entry_id:
            raise ValueError("pending approval state missing queue_entry_id")
        if approval.status == "pending":
            self._approval_store.add(approval)
        else:
            self._approval_store.replace(approval)
        self._approval_runtime[approval.approval_id] = _PendingApprovalState(
            approval=approval,
            session=session,
            request=request,
            owner_context=owner_context,
            action_contract=action_contract,
            queue_entry_id=queue_entry_id,
            preview_text=_normalize_text(payload.get("preview_text")),
            risk_profile=risk_profile,
            scenario_type=_normalize_text(payload.get("scenario_type")) or None,
            workflow_id=_normalize_text(payload.get("workflow_id")) or None,
            workflow_step_id=_normalize_text(payload.get("workflow_step_id")) or None,
            max_retries=int(payload.get("max_retries") or 0),
            step_timeout_seconds=(
                None
                if payload.get("step_timeout_seconds") in (None, "")
                else int(payload.get("step_timeout_seconds"))
            ),
            action_result=(
                None
                if payload.get("action_result") is None
                else dict(_mapping(payload.get("action_result")))
            ),
            response_plan_id=_normalize_text(payload.get("response_plan_id")) or None,
        )
        return approval

    def _dispatch(
        self,
        action_contract: Mapping[str, object],
        *,
        owner_context: Mapping[str, object],
        risk_profile: ActionRiskProfile | None = None,
        approval: OwnerApproval | None = None,
        scenario_type: object = None,
        workflow_id: object = None,
        workflow_step_id: object = None,
        step_timeout_seconds: int | None = None,
    ) -> dict[str, object]:
        dispatch_kwargs: dict[str, object] = {}
        if _dispatcher_supports_kwarg(self._dispatcher, "memory_domain"):
            dispatch_kwargs["memory_domain"] = OWNERBOX_DOMAIN_TYPE
        if _dispatcher_supports_kwarg(self._dispatcher, "domain_binding"):
            dispatch_kwargs["domain_binding"] = owner_context.get("domain_binding")
        if _dispatcher_supports_kwarg(self._dispatcher, "dispatch_context"):
            domain_binding = _mapping(owner_context.get("domain_binding"))
            dispatch_kwargs["dispatch_context"] = {
                "owner_id": _normalize_text(domain_binding.get("owner_id")),
                "trust_class": None
                if risk_profile is None
                else risk_profile.trust_class,
                "approval_id": None if approval is None else approval.approval_id,
                "scenario_type": _normalize_text(scenario_type) or None,
                "workflow_id": _normalize_text(workflow_id) or None,
                "step_id": _normalize_text(workflow_step_id) or None,
                "step_timeout_seconds": None
                if step_timeout_seconds is None
                else str(step_timeout_seconds),
            }
        return self._dispatcher(action_contract, **dispatch_kwargs)

    def _action_is_safe_to_retry(self, action_contract: Mapping[str, object]) -> bool:
        action_type = _normalize_text(action_contract.get("action_type")).upper()
        parameters = _mapping(action_contract.get("parameters"))
        operation = _normalize_text(parameters.get("operation")).lower()
        if action_type == "OPENAI_REQUEST":
            return True
        if (
            action_type == "BROWSER_ACTION"
            and operation in SAFE_BROWSER_RETRY_OPERATIONS
        ):
            return True
        return False

    def _result_with_retry_metadata(
        self,
        action_result: Mapping[str, object],
        *,
        attempt_count: int,
        max_retries: int,
        retry_status: str,
        step_timeout_seconds: int | None,
    ) -> dict[str, object]:
        payload = dict(_mapping(action_result.get("payload")))
        metadata = dict(_mapping(payload.get("metadata")))
        if "attempt_count" in metadata:
            metadata.setdefault("adapter_attempt_count", metadata["attempt_count"])
        metadata["attempt_count"] = attempt_count
        metadata["max_retries"] = max_retries
        metadata["retry_status"] = retry_status
        if step_timeout_seconds is not None:
            metadata["step_timeout_seconds"] = step_timeout_seconds
        payload["metadata"] = metadata
        return build_action_result_contract(
            action_id=action_result.get("action_id"),
            status=action_result.get("status"),
            result_type=action_result.get("result_type"),
            payload=payload,
            error_code=action_result.get("error_code"),
            error_message=action_result.get("error_message"),
            timestamp=action_result.get("timestamp"),
        )

    def _dispatch_with_retry(
        self,
        action_contract: Mapping[str, object],
        *,
        owner_context: Mapping[str, object],
        risk_profile: ActionRiskProfile | None = None,
        approval: OwnerApproval | None = None,
        scenario_type: object = None,
        workflow_id: object = None,
        workflow_step_id: object = None,
        max_retries: int = 0,
        step_timeout_seconds: int | None = None,
    ) -> dict[str, object]:
        attempts = 0
        while True:
            attempts += 1
            action_result = self._dispatch(
                action_contract,
                owner_context=owner_context,
                risk_profile=risk_profile,
                approval=approval,
                scenario_type=scenario_type,
                workflow_id=workflow_id,
                workflow_step_id=workflow_step_id,
                step_timeout_seconds=step_timeout_seconds,
            )
            error_code = _result_error_code(action_result)
            retryable_error = error_code in RETRYABLE_ACTION_ERROR_CODES
            safe_to_retry = self._action_is_safe_to_retry(action_contract)
            retries_remaining = attempts <= (max_retries or 0)
            if _normalize_text(action_result.get("status")).lower() == "success":
                return self._result_with_retry_metadata(
                    action_result,
                    attempt_count=attempts,
                    max_retries=max_retries,
                    retry_status="succeeded_after_retry"
                    if attempts > 1
                    else "not_needed",
                    step_timeout_seconds=step_timeout_seconds,
                )
            if retryable_error and safe_to_retry and retries_remaining:
                continue
            if retryable_error and safe_to_retry and attempts > 1:
                retry_status = "exhausted"
            elif retryable_error and safe_to_retry:
                retry_status = "not_retryable" if max_retries == 0 else "exhausted"
            elif retryable_error:
                retry_status = "not_retryable"
            else:
                retry_status = "not_retryable"
            return self._result_with_retry_metadata(
                action_result,
                attempt_count=attempts,
                max_retries=max_retries,
                retry_status=retry_status,
                step_timeout_seconds=step_timeout_seconds,
            )

    def _persist_approval_trace(
        self,
        *,
        approval: OwnerApproval,
        event_name: str,
        session: OwnerSession,
        request: OwnerRequest,
        response_plan_id: object,
        result_status: object,
    ) -> str:
        trace_payload = {
            "type": OWNER_APPROVAL_TRACE_ARTIFACT_TYPE,
            "event_name": _normalize_text(event_name),
            "timestamp": _utc_timestamp(),
            "approval_id": approval.approval_id,
            "owner_id": approval.owner_id,
            "action_id": approval.action_id,
            "trust_class": approval.trust_class,
            "approval_status": approval.status,
            "owner_session_id": session.owner_session_id,
            "request_id": request.request_id,
            "response_plan_id": _normalize_text(response_plan_id),
            "result_status": _normalize_text(result_status).lower() or approval.status,
        }
        trace_path = (
            ROOT_DIR
            / OWNER_APPROVAL_TRACE_DIR
            / (
                f"{_safe_filename(approval.approval_id)}_{_safe_filename(event_name)}.json"
            )
        )
        trace_path.parent.mkdir(parents=True, exist_ok=True)
        logical_key = compute_artifact_key(
            OWNERBOX_DOMAIN_TYPE,
            OWNER_APPROVAL_TRACE_ARTIFACT_TYPE,
            f"{approval.approval_id}:{event_name}",
        )
        trace_object = make_trace_object(
            id=f"{approval.approval_id}:{event_name}",
            domain=OWNERBOX_DOMAIN_TYPE,
            payload=trace_payload,
            local_path=trace_path,
            artifact_type=OWNER_APPROVAL_TRACE_ARTIFACT_TYPE,
            logical_key=logical_key,
            refs=[
                f"approval:{approval.approval_id}",
                f"action:{approval.action_id}",
                f"owner:{approval.owner_id}",
                f"session:{session.owner_session_id}",
                f"request:{request.request_id}",
            ],
            tags=["ownerbox", "approval", approval.status, _normalize_text(event_name)],
        )
        trace_path.write_text(
            json.dumps(trace_object.to_dict(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        register_artifact(
            trace_object.id,
            OWNERBOX_DOMAIN_TYPE,
            OWNER_APPROVAL_TRACE_ARTIFACT_TYPE,
            trace_path,
            logical_key=logical_key,
            refs=list(trace_object.refs),
            tags=list(trace_object.tags),
            payload=trace_payload,
            memory_class="trace",
            truth_level="working",
            execution_role="evidence",
            created_at=trace_object.created_at,
            updated_at=trace_object.updated_at,
        )
        log_event(
            "trace",
            trace_payload,
            task_id=approval.action_id,
            status=trace_payload["approval_status"],
        )
        return str(trace_path)

    def _build_action_contract(
        self,
        *,
        session: OwnerSession,
        request: OwnerRequest,
        owner_context: Mapping[str, object],
        action_type: object,
        target_type: object,
        target_ref: object,
        action_parameters: Mapping[str, object] | None,
        execution_mode: str,
    ) -> dict[str, object]:
        normalized_action_type = _normalize_text(action_type) or "openai_request"
        parameters = dict(action_parameters or {})
        if normalized_action_type.upper() == "OPENAI_REQUEST" and not parameters:
            parameters = {
                "model": self._model,
                "prompt": _build_owner_prompt(
                    session=session,
                    request=request,
                    owner_context=owner_context,
                ),
                "max_tokens": self._max_tokens,
                "temperature": self._temperature,
            }
        return build_action_contract(
            action_id=f"owner-action-{request.request_id}",
            action_type=normalized_action_type,
            target_type=target_type,
            target_ref=target_ref,
            parameters=parameters,
            execution_mode=execution_mode,
            confirmation_policy="not_required",
            idempotency_key=f"owner:{session.owner_session_id}:{request.request_id}",
            requested_by=DEFAULT_REQUESTED_BY,
        )

    def _build_blocked_interaction(
        self,
        *,
        session: OwnerSession,
        request: OwnerRequest,
        owner_context: Mapping[str, object],
        summary_text: str,
        response_type: str,
        action_contract: Mapping[str, object] | None,
        approval: OwnerApproval | None,
        risk_profile: ActionRiskProfile | None,
        preview_text: str | None,
    ) -> OwnerInteractionResult:
        blocked_request = update_owner_request(request, status="blocked")
        response_plan_id = _new_identifier("owner-response-plan")
        trace_metadata = create_owner_trace_metadata(
            owner_session_id=session.owner_session_id,
            request_id=blocked_request.request_id,
            owner_id=blocked_request.owner_id,
            action_id=None
            if action_contract is None
            else action_contract.get("action_id"),
            result_status="blocked",
            response_plan_id=response_plan_id,
            request_created_at=blocked_request.created_at,
            response_created_at=blocked_request.created_at,
            completed_at=blocked_request.created_at,
            approval_id=None if approval is None else approval.approval_id,
            approval_status=None if approval is None else approval.status,
            trust_class=None if risk_profile is None else risk_profile.trust_class,
            approval_created=approval is not None,
            approval_resolved=bool(
                approval is not None and approval.status != "pending"
            ),
        )
        response_plan = create_owner_response_plan(
            response_plan_id=response_plan_id,
            owner_session_id=session.owner_session_id,
            owner_id=blocked_request.owner_id,
            response_type=response_type,
            target_language=blocked_request.detected_language,
            summary_text=summary_text,
            action_refs=[]
            if action_contract is None
            else [str(action_contract["action_id"])],
            requires_confirmation=False,
            requires_high_trust=False
            if risk_profile is None
            else risk_profile.requires_high_trust,
            approval_id=None if approval is None else approval.approval_id,
            trust_class=None if risk_profile is None else risk_profile.trust_class,
            preview_text=preview_text,
            metadata=_response_metadata(
                session=session,
                request=blocked_request,
                action_contract=action_contract,
                action_result=None,
                owner_context=owner_context,
                trace_metadata=trace_metadata,
                approval=approval,
                risk_profile=risk_profile,
                preview_text=preview_text,
            ),
            created_at=blocked_request.created_at,
            status="blocked",
        )
        return OwnerInteractionResult(
            session=session,
            request=blocked_request,
            owner_context=dict(owner_context),
            action_contract=None if action_contract is None else dict(action_contract),
            action_result=None,
            response_plan=response_plan,
            queue_entry=None,
            trace_metadata=trace_metadata,
            approval=approval,
        )

    def process_request(
        self,
        *,
        request_text: object,
        owner_id: object,
        owner_domain: OwnerDomain,
        memory_scope: OwnerMemoryScope,
        action_scope: OwnerActionScope,
        trust_profile: OwnerTrustProfile,
        session: OwnerSession | None = None,
        owner_session_id: object | None = None,
        active_language: object = "und",
        detected_language: object | None = None,
        request_type: object | None = None,
        priority_class: object | None = None,
        context_ref: object = None,
        execution_mode: str = "live",
        memory_records: list[Mapping[str, object]] | None = None,
        canonical_memory_store: CanonicalMemoryStore | None = None,
        canonical_memory_context_request: Mapping[str, object] | None = None,
        scenario_type: object = None,
        action_type: object = "openai_request",
        target_type: object = DEFAULT_TARGET_TYPE,
        target_ref: object = DEFAULT_TARGET_REF,
        action_parameters: Mapping[str, object] | None = None,
        workflow_id: object = None,
        workflow_step_id: object = None,
        max_retries: int = 0,
        step_timeout_seconds: int | None = None,
    ) -> OwnerInteractionResult:
        normalized_owner_id = _normalize_text(owner_id)
        _validate_owner_scope(
            owner_id=normalized_owner_id,
            session=session,
            owner_domain=owner_domain,
            trust_profile=trust_profile,
        )
        owner_session = session or create_owner_session(
            owner_id=normalized_owner_id,
            active_language=active_language,
            context_ref=context_ref,
            owner_session_id=owner_session_id,
        )
        resolved_context_ref = _normalize_text(context_ref) or owner_session.context_ref
        resolved_detected_language = (
            _normalize_text(detected_language) or owner_session.active_language
        )
        request = create_owner_request(
            owner_session_id=owner_session.owner_session_id,
            owner_id=normalized_owner_id,
            request_text=request_text,
            detected_language=resolved_detected_language,
            request_type=request_type,
            priority_class=priority_class,
            context_ref=resolved_context_ref,
        )
        owner_session = increment_owner_session_request(
            owner_session,
            active_language=resolved_detected_language,
            context_ref=resolved_context_ref,
            last_request_at=request.created_at,
        )
        trace_id = _new_identifier("owner-trace")
        owner_context = assemble_owner_context(
            request_ref=OwnerRequestContextRef(
                request_ref=request.request_id,
                owner_id=normalized_owner_id,
                session_ref=owner_session.owner_session_id,
                trace_id=trace_id,
                memory_context={},
            ),
            owner_domain=owner_domain,
            memory_scope=memory_scope,
            action_scope=action_scope,
            trust_profile=trust_profile,
            memory_records=memory_records,
        )
        if (
            canonical_memory_store is not None
            and canonical_memory_context_request is not None
        ):
            owner_context["canonical_memory_context"] = (
                assemble_owner_canonical_context(
                    owner_domain=owner_domain,
                    request_ref=OwnerRequestContextRef(
                        request_ref=request.request_id,
                        owner_id=normalized_owner_id,
                        session_ref=owner_session.owner_session_id,
                        trace_id=trace_id,
                        memory_context={},
                    ),
                    memory_store=canonical_memory_store,
                    context_request=canonical_memory_context_request,
                )
            )

        if not request.request_text:
            return self._build_blocked_interaction(
                session=owner_session,
                request=request,
                owner_context=owner_context,
                summary_text="Owner request was empty after normalization.",
                response_type="input_error",
                action_contract=None,
                approval=None,
                risk_profile=None,
                preview_text=None,
            )

        normalized_action_type = _normalize_text(action_type)
        if not normalized_action_type:
            return self._build_blocked_interaction(
                session=owner_session,
                request=request,
                owner_context=owner_context,
                summary_text="Owner action type was missing.",
                response_type="error",
                action_contract=None,
                approval=None,
                risk_profile=None,
                preview_text=None,
            )

        risk_profile = classify_action_risk(
            normalized_action_type,
            action_scope=action_scope,
            action_parameters=dict(action_parameters or {}),
        )
        if not is_known_action_type(risk_profile.action_type):
            return self._build_blocked_interaction(
                session=owner_session,
                request=request,
                owner_context=owner_context,
                summary_text=f"Owner action is blocked: unknown action_type {risk_profile.action_type}.",
                response_type="error",
                action_contract=None,
                approval=None,
                risk_profile=risk_profile,
                preview_text=None,
            )
        if (
            action_scope.blocked_action_types
            and risk_profile.action_type in action_scope.blocked_action_types
        ):
            return self._build_blocked_interaction(
                session=owner_session,
                request=request,
                owner_context=owner_context,
                summary_text=f"Owner action is blocked by scope: {risk_profile.action_type}.",
                response_type="error",
                action_contract=None,
                approval=None,
                risk_profile=risk_profile,
                preview_text=None,
            )
        if (
            action_scope.allowed_action_types
            and risk_profile.action_type not in action_scope.allowed_action_types
        ):
            return self._build_blocked_interaction(
                session=owner_session,
                request=request,
                owner_context=owner_context,
                summary_text=f"Owner action is outside the allowed scope: {risk_profile.action_type}.",
                response_type="error",
                action_contract=None,
                approval=None,
                risk_profile=risk_profile,
                preview_text=None,
            )

        try:
            action_contract = self._build_action_contract(
                session=owner_session,
                request=request,
                owner_context=owner_context,
                action_type=risk_profile.action_type,
                target_type=target_type,
                target_ref=target_ref,
                action_parameters=action_parameters,
                execution_mode=execution_mode,
            )
        except ActionContractViolation as exc:
            return self._build_blocked_interaction(
                session=owner_session,
                request=request,
                owner_context=owner_context,
                summary_text=f"Owner action is blocked: {exc}",
                response_type="error",
                action_contract=None,
                approval=None,
                risk_profile=risk_profile,
                preview_text=None,
            )

        preview_text = _preview_text(request=request, action_contract=action_contract)

        if risk_profile.auto_execute_allowed:
            action_result = self._dispatch_with_retry(
                action_contract,
                owner_context=owner_context,
                risk_profile=risk_profile,
                scenario_type=scenario_type,
                workflow_id=workflow_id,
                workflow_step_id=workflow_step_id,
                max_retries=max_retries,
                step_timeout_seconds=step_timeout_seconds,
            )
            completed_request = update_owner_request(
                request,
                status=_request_status(_normalize_text(action_result.get("status"))),
            )
            response_plan_id = _new_identifier("owner-response-plan")
            trace_metadata = create_owner_trace_metadata(
                owner_session_id=owner_session.owner_session_id,
                request_id=completed_request.request_id,
                owner_id=completed_request.owner_id,
                action_id=action_contract["action_id"],
                result_status=action_result.get("status"),
                response_plan_id=response_plan_id,
                request_created_at=completed_request.created_at,
                response_created_at=_utc_timestamp(),
                completed_at=_normalize_text(action_result.get("timestamp"))
                or _utc_timestamp(),
                trust_class=risk_profile.trust_class,
            )
            response_plan = create_owner_response_plan(
                response_plan_id=response_plan_id,
                owner_session_id=owner_session.owner_session_id,
                owner_id=completed_request.owner_id,
                response_type=_response_type_from_result_status(
                    _normalize_text(action_result.get("status"))
                ),
                target_language=completed_request.detected_language,
                summary_text=_result_payload_text(action_result),
                action_refs=[str(action_contract["action_id"])],
                requires_confirmation=risk_profile.requires_confirmation,
                requires_high_trust=risk_profile.requires_high_trust,
                approval_id=None,
                trust_class=risk_profile.trust_class,
                preview_text=preview_text,
                metadata=_response_metadata(
                    session=owner_session,
                    request=completed_request,
                    action_contract=action_contract,
                    action_result=action_result,
                    owner_context=owner_context,
                    trace_metadata=trace_metadata,
                    approval=None,
                    risk_profile=risk_profile,
                    preview_text=preview_text,
                ),
                status=_response_plan_status(
                    _normalize_text(action_result.get("status"))
                ),
            )
            queue_entry = self._action_queue.enqueue(
                create_owner_action_queue_entry(
                    owner_id=completed_request.owner_id,
                    action_id=action_contract["action_id"],
                    action_type=action_contract["action_type"],
                    action_status=_queue_status(
                        _normalize_text(action_result.get("status")),
                        requires_confirmation=risk_profile.requires_confirmation,
                    ),
                    requires_confirmation=risk_profile.requires_confirmation,
                    requires_high_trust=risk_profile.requires_high_trust,
                    priority_class=completed_request.priority_class,
                    created_at=completed_request.created_at,
                    updated_at=_normalize_text(action_result.get("timestamp"))
                    or response_plan.created_at,
                )
            )
            return OwnerInteractionResult(
                session=owner_session,
                request=completed_request,
                owner_context=owner_context,
                action_contract=action_contract,
                action_result=action_result,
                response_plan=response_plan,
                queue_entry=queue_entry,
                trace_metadata=trace_metadata,
                approval=None,
            )

        approval = self._approval_store.add(
            create_owner_approval(
                owner_id=request.owner_id,
                action_id=action_contract["action_id"],
                trust_class=risk_profile.trust_class,
                created_at=request.created_at,
            )
        )
        gated_request = update_owner_request(request, status="dispatched")
        response_plan_id = _new_identifier("owner-response-plan")
        trace_metadata = create_owner_trace_metadata(
            owner_session_id=owner_session.owner_session_id,
            request_id=gated_request.request_id,
            owner_id=gated_request.owner_id,
            action_id=action_contract["action_id"],
            result_status="blocked",
            response_plan_id=response_plan_id,
            request_created_at=gated_request.created_at,
            response_created_at=gated_request.created_at,
            completed_at=gated_request.created_at,
            approval_id=approval.approval_id,
            approval_status=approval.status,
            trust_class=risk_profile.trust_class,
            approval_created=True,
            approval_resolved=False,
        )
        response_plan = create_owner_response_plan(
            response_plan_id=response_plan_id,
            owner_session_id=owner_session.owner_session_id,
            owner_id=gated_request.owner_id,
            response_type="confirmation_request",
            target_language=gated_request.detected_language,
            summary_text=_confirmation_summary(
                preview_text=preview_text,
                trust_class=risk_profile.trust_class,
                action_contract=action_contract,
            ),
            action_refs=[str(action_contract["action_id"])],
            requires_confirmation=True,
            requires_high_trust=risk_profile.requires_high_trust,
            approval_id=approval.approval_id,
            trust_class=risk_profile.trust_class,
            preview_text=preview_text,
            metadata=_response_metadata(
                session=owner_session,
                request=gated_request,
                action_contract=action_contract,
                action_result=None,
                owner_context=owner_context,
                trace_metadata=trace_metadata,
                approval=approval,
                risk_profile=risk_profile,
                preview_text=preview_text,
            ),
            created_at=gated_request.created_at,
            status="planned",
        )
        queue_entry = self._action_queue.enqueue(
            create_owner_action_queue_entry(
                owner_id=gated_request.owner_id,
                action_id=action_contract["action_id"],
                action_type=action_contract["action_type"],
                action_status="awaiting_confirmation",
                requires_confirmation=True,
                requires_high_trust=risk_profile.requires_high_trust,
                priority_class=gated_request.priority_class,
                created_at=gated_request.created_at,
                updated_at=gated_request.created_at,
            )
        )
        self._approval_runtime[approval.approval_id] = _PendingApprovalState(
            approval=approval,
            session=owner_session,
            request=gated_request,
            owner_context=dict(owner_context),
            action_contract=dict(action_contract),
            queue_entry_id=queue_entry.queue_entry_id,
            preview_text=preview_text,
            risk_profile=risk_profile,
            scenario_type=_normalize_text(scenario_type) or None,
            workflow_id=_normalize_text(workflow_id) or None,
            workflow_step_id=_normalize_text(workflow_step_id) or None,
            max_retries=max_retries,
            step_timeout_seconds=step_timeout_seconds,
            response_plan_id=response_plan.response_plan_id,
        )
        self._persist_approval_trace(
            approval=approval,
            event_name="approval_created",
            session=owner_session,
            request=gated_request,
            response_plan_id=response_plan.response_plan_id,
            result_status="blocked",
        )
        return OwnerInteractionResult(
            session=owner_session,
            request=gated_request,
            owner_context=owner_context,
            action_contract=action_contract,
            action_result=None,
            response_plan=response_plan,
            queue_entry=queue_entry,
            trace_metadata=trace_metadata,
            approval=approval,
        )

    def _missing_approval_resolution(
        self, *, approval_id: object, message: str
    ) -> OwnerApprovalResolutionResult:
        response_plan_id = _new_identifier("owner-response-plan")
        trace_metadata = create_owner_trace_metadata(
            owner_session_id="",
            request_id="",
            owner_id="",
            action_id=None,
            result_status="blocked",
            response_plan_id=response_plan_id,
            request_created_at=_utc_timestamp(),
            response_created_at=_utc_timestamp(),
            completed_at=_utc_timestamp(),
            approval_id=approval_id,
            approval_status="blocked",
            approval_created=False,
            approval_resolved=False,
        )
        response_plan = create_owner_response_plan(
            response_plan_id=response_plan_id,
            owner_session_id="owner-session-missing",
            owner_id="owner-missing",
            response_type="error",
            target_language="und",
            summary_text=message,
            action_refs=[],
            requires_confirmation=False,
            requires_high_trust=False,
            approval_id=_normalize_text(approval_id) or None,
            trust_class=None,
            preview_text=None,
            metadata={"trace_metadata": trace_metadata},
            status="blocked",
        )
        return OwnerApprovalResolutionResult(
            approval=None,
            action_contract=None,
            action_result=None,
            response_plan=response_plan,
            queue_entry=None,
            trace_metadata=trace_metadata,
        )

    def approve_action(self, approval_id: object) -> OwnerApprovalResolutionResult:
        approval = self._approval_store.get(approval_id)
        pending = (
            None
            if approval is None
            else self._approval_runtime.get(approval.approval_id)
        )
        if approval is None or pending is None:
            return self._missing_approval_resolution(
                approval_id=approval_id,
                message="Owner approval is blocked: approval was not found.",
            )
        if approval.status == "rejected":
            return self._missing_approval_resolution(
                approval_id=approval.approval_id,
                message="Owner approval is blocked: approval was already rejected.",
            )
        if approval.status == "approved" and pending.action_result is not None:
            queue_entry = self._action_queue.get(pending.queue_entry_id)
            trace_metadata = create_owner_trace_metadata(
                owner_session_id=pending.session.owner_session_id,
                request_id=pending.request.request_id,
                owner_id=pending.request.owner_id,
                action_id=pending.action_contract["action_id"],
                result_status=_normalize_text(pending.action_result.get("status")),
                response_plan_id=pending.response_plan_id
                or _new_identifier("owner-response-plan"),
                request_created_at=pending.request.created_at,
                response_created_at=_utc_timestamp(),
                completed_at=_normalize_text(pending.action_result.get("timestamp"))
                or _utc_timestamp(),
                approval_id=approval.approval_id,
                approval_status=approval.status,
                trust_class=approval.trust_class,
                approval_created=True,
                approval_resolved=True,
            )
            response_plan = create_owner_response_plan(
                response_plan_id=_new_identifier("owner-response-plan"),
                owner_session_id=pending.session.owner_session_id,
                owner_id=pending.request.owner_id,
                response_type=_response_type_from_result_status(
                    _normalize_text(pending.action_result.get("status"))
                ),
                target_language=pending.request.detected_language,
                summary_text=_result_payload_text(pending.action_result),
                action_refs=[str(pending.action_contract["action_id"])],
                requires_confirmation=False,
                requires_high_trust=pending.risk_profile.requires_high_trust,
                approval_id=approval.approval_id,
                trust_class=approval.trust_class,
                preview_text=pending.preview_text,
                metadata=_response_metadata(
                    session=pending.session,
                    request=pending.request,
                    action_contract=pending.action_contract,
                    action_result=pending.action_result,
                    owner_context=pending.owner_context,
                    trace_metadata=trace_metadata,
                    approval=approval,
                    risk_profile=pending.risk_profile,
                    preview_text=pending.preview_text,
                ),
                status=_response_plan_status(
                    _normalize_text(pending.action_result.get("status"))
                ),
            )
            return OwnerApprovalResolutionResult(
                approval=approval,
                action_contract=dict(pending.action_contract),
                action_result=dict(pending.action_result),
                response_plan=response_plan,
                queue_entry=queue_entry,
                trace_metadata=trace_metadata,
            )

        resolved_approval = self._approval_store.replace(
            resolve_owner_approval(approval, status="approved")
        )
        pending.approval = resolved_approval
        self._persist_approval_trace(
            approval=resolved_approval,
            event_name="approval_resolved",
            session=pending.session,
            request=pending.request,
            response_plan_id=pending.response_plan_id or "",
            result_status="approved",
        )
        action_result = self._dispatch_with_retry(
            pending.action_contract,
            owner_context=pending.owner_context,
            risk_profile=pending.risk_profile,
            approval=resolved_approval,
            scenario_type=pending.scenario_type,
            workflow_id=pending.workflow_id,
            workflow_step_id=pending.workflow_step_id,
            max_retries=pending.max_retries,
            step_timeout_seconds=pending.step_timeout_seconds,
        )
        pending.action_result = dict(action_result)
        queue_entry = self._action_queue.update_status(
            queue_entry_id=pending.queue_entry_id,
            action_status=_queue_status(
                _normalize_text(action_result.get("status")),
                requires_confirmation=False,
            ),
            updated_at=_normalize_text(action_result.get("timestamp"))
            or _utc_timestamp(),
        )
        response_plan_id = _new_identifier("owner-response-plan")
        pending.response_plan_id = response_plan_id
        trace_metadata = create_owner_trace_metadata(
            owner_session_id=pending.session.owner_session_id,
            request_id=pending.request.request_id,
            owner_id=pending.request.owner_id,
            action_id=pending.action_contract["action_id"],
            result_status=action_result.get("status"),
            response_plan_id=response_plan_id,
            request_created_at=pending.request.created_at,
            response_created_at=_utc_timestamp(),
            completed_at=_normalize_text(action_result.get("timestamp"))
            or _utc_timestamp(),
            approval_id=resolved_approval.approval_id,
            approval_status=resolved_approval.status,
            trust_class=resolved_approval.trust_class,
            approval_created=True,
            approval_resolved=True,
        )
        response_plan = create_owner_response_plan(
            response_plan_id=response_plan_id,
            owner_session_id=pending.session.owner_session_id,
            owner_id=pending.request.owner_id,
            response_type=_response_type_from_result_status(
                _normalize_text(action_result.get("status"))
            ),
            target_language=pending.request.detected_language,
            summary_text=_result_payload_text(action_result),
            action_refs=[str(pending.action_contract["action_id"])],
            requires_confirmation=False,
            requires_high_trust=pending.risk_profile.requires_high_trust,
            approval_id=resolved_approval.approval_id,
            trust_class=resolved_approval.trust_class,
            preview_text=pending.preview_text,
            metadata=_response_metadata(
                session=pending.session,
                request=pending.request,
                action_contract=pending.action_contract,
                action_result=action_result,
                owner_context=pending.owner_context,
                trace_metadata=trace_metadata,
                approval=resolved_approval,
                risk_profile=pending.risk_profile,
                preview_text=pending.preview_text,
            ),
            status=_response_plan_status(_normalize_text(action_result.get("status"))),
        )
        return OwnerApprovalResolutionResult(
            approval=resolved_approval,
            action_contract=dict(pending.action_contract),
            action_result=dict(action_result),
            response_plan=response_plan,
            queue_entry=queue_entry,
            trace_metadata=trace_metadata,
        )

    def reject_action(self, approval_id: object) -> OwnerApprovalResolutionResult:
        approval = self._approval_store.get(approval_id)
        pending = (
            None
            if approval is None
            else self._approval_runtime.get(approval.approval_id)
        )
        if approval is None or pending is None:
            return self._missing_approval_resolution(
                approval_id=approval_id,
                message="Owner approval is blocked: approval was not found.",
            )
        if approval.status == "approved":
            return self._missing_approval_resolution(
                approval_id=approval.approval_id,
                message="Owner approval is blocked: approval was already approved.",
            )
        if approval.status == "rejected":
            queue_entry = self._action_queue.get(pending.queue_entry_id)
            trace_metadata = create_owner_trace_metadata(
                owner_session_id=pending.session.owner_session_id,
                request_id=pending.request.request_id,
                owner_id=pending.request.owner_id,
                action_id=pending.action_contract["action_id"],
                result_status="blocked",
                response_plan_id=pending.response_plan_id
                or _new_identifier("owner-response-plan"),
                request_created_at=pending.request.created_at,
                response_created_at=_utc_timestamp(),
                completed_at=_utc_timestamp(),
                approval_id=approval.approval_id,
                approval_status=approval.status,
                trust_class=approval.trust_class,
                approval_created=True,
                approval_resolved=True,
            )
            response_plan = create_owner_response_plan(
                response_plan_id=_new_identifier("owner-response-plan"),
                owner_session_id=pending.session.owner_session_id,
                owner_id=pending.request.owner_id,
                response_type="summary_text",
                target_language=pending.request.detected_language,
                summary_text="Owner approval rejected. Action was not executed.",
                action_refs=[str(pending.action_contract["action_id"])],
                requires_confirmation=False,
                requires_high_trust=pending.risk_profile.requires_high_trust,
                approval_id=approval.approval_id,
                trust_class=approval.trust_class,
                preview_text=pending.preview_text,
                metadata=_response_metadata(
                    session=pending.session,
                    request=pending.request,
                    action_contract=pending.action_contract,
                    action_result=None,
                    owner_context=pending.owner_context,
                    trace_metadata=trace_metadata,
                    approval=approval,
                    risk_profile=pending.risk_profile,
                    preview_text=pending.preview_text,
                ),
                status="blocked",
            )
            return OwnerApprovalResolutionResult(
                approval=approval,
                action_contract=dict(pending.action_contract),
                action_result=None,
                response_plan=response_plan,
                queue_entry=queue_entry,
                trace_metadata=trace_metadata,
            )

        resolved_approval = self._approval_store.replace(
            resolve_owner_approval(approval, status="rejected")
        )
        pending.approval = resolved_approval
        self._persist_approval_trace(
            approval=resolved_approval,
            event_name="approval_resolved",
            session=pending.session,
            request=pending.request,
            response_plan_id=pending.response_plan_id or "",
            result_status="rejected",
        )
        queue_entry = self._action_queue.update_status(
            queue_entry_id=pending.queue_entry_id,
            action_status="blocked",
            updated_at=resolved_approval.resolved_at,
        )
        response_plan_id = _new_identifier("owner-response-plan")
        pending.response_plan_id = response_plan_id
        trace_metadata = create_owner_trace_metadata(
            owner_session_id=pending.session.owner_session_id,
            request_id=pending.request.request_id,
            owner_id=pending.request.owner_id,
            action_id=pending.action_contract["action_id"],
            result_status="blocked",
            response_plan_id=response_plan_id,
            request_created_at=pending.request.created_at,
            response_created_at=_utc_timestamp(),
            completed_at=resolved_approval.resolved_at,
            approval_id=resolved_approval.approval_id,
            approval_status=resolved_approval.status,
            trust_class=resolved_approval.trust_class,
            approval_created=True,
            approval_resolved=True,
        )
        response_plan = create_owner_response_plan(
            response_plan_id=response_plan_id,
            owner_session_id=pending.session.owner_session_id,
            owner_id=pending.request.owner_id,
            response_type="summary_text",
            target_language=pending.request.detected_language,
            summary_text="Owner approval rejected. Action was not executed.",
            action_refs=[str(pending.action_contract["action_id"])],
            requires_confirmation=False,
            requires_high_trust=pending.risk_profile.requires_high_trust,
            approval_id=resolved_approval.approval_id,
            trust_class=resolved_approval.trust_class,
            preview_text=pending.preview_text,
            metadata=_response_metadata(
                session=pending.session,
                request=pending.request,
                action_contract=pending.action_contract,
                action_result=None,
                owner_context=pending.owner_context,
                trace_metadata=trace_metadata,
                approval=resolved_approval,
                risk_profile=pending.risk_profile,
                preview_text=pending.preview_text,
            ),
            status="blocked",
        )
        return OwnerApprovalResolutionResult(
            approval=resolved_approval,
            action_contract=dict(pending.action_contract),
            action_result=None,
            response_plan=response_plan,
            queue_entry=queue_entry,
            trace_metadata=trace_metadata,
        )
