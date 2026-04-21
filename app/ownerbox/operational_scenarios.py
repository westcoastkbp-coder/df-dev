from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Mapping
from urllib.parse import urlparse

from app.ownerbox.domain import (
    OwnerActionScope,
    OwnerDomain,
    OwnerMemoryScope,
    OwnerTrustProfile,
)
from app.ownerbox.owner_session import OwnerSession
from app.ownerbox.workflow import summarize_owner_workflow_step
from app.ownerbox.workflow_orchestrator import (
    OwnerWorkflowOrchestrator,
    OwnerWorkflowRunResult,
)


OPERATIONAL_SCENARIO_TYPES = frozenset(
    {
        "owner_draft_then_browser_update",
        "owner_email_review_and_send",
        "owner_generate_review_and_print_document",
        "owner_page_review_and_extract",
        "owner_web_form_review_and_submit",
    }
)
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9._:@-]+$")
_EMAIL_PATTERN = re.compile(
    r"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+$"
)
_PLACEHOLDER_STEP1_SUMMARY = "{{step1.result_summary}}"


class OperationalScenarioValidationError(ValueError):
    code = "validation_error"

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _stable_identifier(value: object, *, field_name: str) -> str:
    normalized = _normalize_text(value)
    if not normalized:
        raise OperationalScenarioValidationError(f"{field_name} must not be empty")
    if _IDENTIFIER_PATTERN.fullmatch(normalized) is None:
        raise OperationalScenarioValidationError(
            f"{field_name} must be a stable identifier"
        )
    return normalized


def _optional_identifier(value: object, *, field_name: str) -> str | None:
    normalized = _normalize_text(value)
    if not normalized:
        return None
    return _stable_identifier(normalized, field_name=field_name)


def _url(value: object, *, field_name: str, required: bool) -> str | None:
    normalized = _normalize_text(value)
    if not normalized:
        if required:
            raise OperationalScenarioValidationError(f"{field_name} must not be empty")
        return None
    parsed = urlparse(normalized)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise OperationalScenarioValidationError(
            f"{field_name} must be an absolute http or https URL"
        )
    return normalized


def _string_mapping(value: object, *, field_name: str) -> dict[str, str]:
    if value in (None, ""):
        return {}
    if not isinstance(value, Mapping):
        raise OperationalScenarioValidationError(f"{field_name} must be a dict")
    normalized: dict[str, str] = {}
    for key in sorted(value):
        normalized_key = _normalize_text(key)
        normalized_value = _normalize_text(value[key])
        if not normalized_key:
            raise OperationalScenarioValidationError(
                f"{field_name} keys must not be empty"
            )
        if not normalized_value:
            raise OperationalScenarioValidationError(
                f"{field_name}.{normalized_key} must not be empty"
            )
        normalized[normalized_key] = normalized_value
    return normalized


def _object_mapping(value: object, *, field_name: str) -> dict[str, object]:
    if value in (None, ""):
        return {}
    if not isinstance(value, Mapping):
        raise OperationalScenarioValidationError(f"{field_name} must be a dict")
    normalized: dict[str, object] = {}
    for key in sorted(value):
        normalized_key = _normalize_text(key)
        if not normalized_key:
            raise OperationalScenarioValidationError(
                f"{field_name} keys must not be empty"
            )
        normalized[normalized_key] = value[key]
    return normalized


def _string_list(
    value: object, *, field_name: str, required: bool = False
) -> list[str]:
    if value in (None, ""):
        if required:
            raise OperationalScenarioValidationError(f"{field_name} must not be empty")
        return []
    if not isinstance(value, (list, tuple)):
        raise OperationalScenarioValidationError(f"{field_name} must be a list")
    normalized: list[str] = []
    for item in value:
        text = _normalize_text(item)
        if not text:
            raise OperationalScenarioValidationError(
                f"{field_name} items must not be empty"
            )
        normalized.append(text)
    if required and not normalized:
        raise OperationalScenarioValidationError(f"{field_name} must not be empty")
    return normalized


def _email_list(value: object, *, field_name: str) -> list[str]:
    emails = _string_list(value, field_name=field_name, required=True)
    for email in emails:
        if _EMAIL_PATTERN.fullmatch(email.lower()) is None:
            raise OperationalScenarioValidationError(
                f"{field_name} contains invalid recipient: {email}"
            )
    return [email.lower() for email in emails]


def _bool(value: object, *, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise OperationalScenarioValidationError(f"{field_name} must be a boolean")
    return value


def _bounded_positive_int(value: object, *, field_name: str, allowed: set[int]) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise OperationalScenarioValidationError(
            f"{field_name} must be an integer"
        ) from exc
    if normalized not in allowed:
        raise OperationalScenarioValidationError(
            f"{field_name} must be one of: "
            + ", ".join(str(item) for item in sorted(allowed))
        )
    return normalized


@dataclass(frozen=True, slots=True)
class OperationalScenarioDefinition:
    scenario_type: str
    description: str
    required_inputs: tuple[str, ...]
    generated_workflow_type: str
    trust_profile_expectations: tuple[str, ...]
    confirmation_requirements: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "scenario_type": self.scenario_type,
            "description": self.description,
            "required_inputs": list(self.required_inputs),
            "generated_workflow_type": self.generated_workflow_type,
            "trust_profile_expectations": list(self.trust_profile_expectations),
            "confirmation_requirements": list(self.confirmation_requirements),
        }


@dataclass(frozen=True, slots=True)
class OwnerOperationalScenarioRequest:
    scenario_type: str
    owner_id: str
    owner_session_id: str | None = None
    context_ref: str | None = None
    target_url: str | None = None
    target_fields: dict[str, str] = field(default_factory=dict)
    structured_inputs: dict[str, object] = field(default_factory=dict)
    draft_content: str | None = None
    prompt_input: str | None = None
    approval_required: bool = True
    active_language: str = "und"
    detected_language: str | None = None
    execution_mode: str = "live"
    title: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "scenario_type", _normalize_scenario_type(self.scenario_type)
        )
        object.__setattr__(
            self, "owner_id", _stable_identifier(self.owner_id, field_name="owner_id")
        )
        object.__setattr__(
            self,
            "owner_session_id",
            _optional_identifier(self.owner_session_id, field_name="owner_session_id"),
        )
        object.__setattr__(
            self, "context_ref", _normalize_text(self.context_ref) or None
        )
        object.__setattr__(
            self,
            "target_url",
            _url(self.target_url, field_name="target_url", required=False),
        )
        object.__setattr__(
            self,
            "target_fields",
            _string_mapping(self.target_fields, field_name="target_fields"),
        )
        object.__setattr__(
            self,
            "structured_inputs",
            _object_mapping(self.structured_inputs, field_name="structured_inputs"),
        )
        object.__setattr__(
            self, "draft_content", _normalize_text(self.draft_content) or None
        )
        object.__setattr__(
            self, "prompt_input", _normalize_text(self.prompt_input) or None
        )
        object.__setattr__(
            self,
            "approval_required",
            _bool(self.approval_required, field_name="approval_required"),
        )
        object.__setattr__(
            self, "active_language", _normalize_text(self.active_language) or "und"
        )
        object.__setattr__(
            self,
            "detected_language",
            _normalize_text(self.detected_language) or self.active_language,
        )
        object.__setattr__(
            self, "execution_mode", _normalize_text(self.execution_mode) or "live"
        )
        object.__setattr__(self, "title", _normalize_text(self.title) or None)
        self._validate()

    def _validate(self) -> None:
        if self.scenario_type == "owner_email_review_and_send":
            unexpected_keys = sorted(
                set(self.structured_inputs) - {"attachments", "subject", "to"}
            )
            if unexpected_keys:
                raise OperationalScenarioValidationError(
                    "structured_inputs contains unsupported fields: "
                    + ", ".join(unexpected_keys)
                )
            if self.target_url is not None:
                raise OperationalScenarioValidationError(
                    "target_url is not supported for this scenario"
                )
            if self.target_fields:
                raise OperationalScenarioValidationError(
                    "target_fields is not supported for this scenario"
                )
            _email_list(
                self.structured_inputs.get("to"), field_name="structured_inputs.to"
            )
            if not _normalize_text(self.structured_inputs.get("subject")):
                raise OperationalScenarioValidationError(
                    "structured_inputs.subject must not be empty"
                )
            if self.draft_content is None:
                raise OperationalScenarioValidationError(
                    "draft_content must not be empty"
                )
            _string_list(
                self.structured_inputs.get("attachments"),
                field_name="structured_inputs.attachments",
            )
            return

        if self.scenario_type == "owner_web_form_review_and_submit":
            unexpected_keys = sorted(
                set(self.structured_inputs) - {"extract_selector", "form_selector"}
            )
            if unexpected_keys:
                raise OperationalScenarioValidationError(
                    "structured_inputs contains unsupported fields: "
                    + ", ".join(unexpected_keys)
                )
            if self.draft_content is not None or self.prompt_input is not None:
                raise OperationalScenarioValidationError(
                    "draft_content and prompt_input are not supported for this scenario"
                )
            _url(self.target_url, field_name="target_url", required=True)
            if not self.target_fields:
                raise OperationalScenarioValidationError(
                    "target_fields must not be empty"
                )
            if not _normalize_text(self.structured_inputs.get("form_selector")):
                raise OperationalScenarioValidationError(
                    "structured_inputs.form_selector must not be empty"
                )
            return

        if self.scenario_type == "owner_page_review_and_extract":
            unexpected_keys = sorted(set(self.structured_inputs) - {"extract_selector"})
            if unexpected_keys:
                raise OperationalScenarioValidationError(
                    "structured_inputs contains unsupported fields: "
                    + ", ".join(unexpected_keys)
                )
            if self.target_fields:
                raise OperationalScenarioValidationError(
                    "target_fields is not supported for this scenario"
                )
            if self.draft_content is not None or self.prompt_input is not None:
                raise OperationalScenarioValidationError(
                    "draft_content and prompt_input are not supported for this scenario"
                )
            _url(self.target_url, field_name="target_url", required=True)
            return

        if self.scenario_type == "owner_draft_then_browser_update":
            unexpected_keys = sorted(
                set(self.structured_inputs) - {"draft_field_name", "form_selector"}
            )
            if unexpected_keys:
                raise OperationalScenarioValidationError(
                    "structured_inputs contains unsupported fields: "
                    + ", ".join(unexpected_keys)
                )
            _url(self.target_url, field_name="target_url", required=True)
            if not _normalize_text(self.structured_inputs.get("form_selector")):
                raise OperationalScenarioValidationError(
                    "structured_inputs.form_selector must not be empty"
                )
            draft_field_name = _normalize_text(
                self.structured_inputs.get("draft_field_name")
            )
            if not draft_field_name:
                raise OperationalScenarioValidationError(
                    "structured_inputs.draft_field_name must not be empty"
                )
            if self.target_fields.get(draft_field_name):
                raise OperationalScenarioValidationError(
                    "target_fields must not contain structured_inputs.draft_field_name"
                )
            if self.prompt_input is None and self.draft_content is None:
                raise OperationalScenarioValidationError(
                    "prompt_input or draft_content must not be empty"
                )
            return

        if self.scenario_type == "owner_generate_review_and_print_document":
            unexpected_keys = sorted(
                set(self.structured_inputs)
                - {"copies", "document_title", "printer_name"}
            )
            if unexpected_keys:
                raise OperationalScenarioValidationError(
                    "structured_inputs contains unsupported fields: "
                    + ", ".join(unexpected_keys)
                )
            if self.target_url is not None:
                raise OperationalScenarioValidationError(
                    "target_url is not supported for this scenario"
                )
            if self.target_fields:
                raise OperationalScenarioValidationError(
                    "target_fields is not supported for this scenario"
                )
            if not self.approval_required:
                raise OperationalScenarioValidationError(
                    "approval_required must be true for this scenario"
                )
            if not _normalize_text(self.structured_inputs.get("document_title")):
                raise OperationalScenarioValidationError(
                    "structured_inputs.document_title must not be empty"
                )
            _bounded_positive_int(
                self.structured_inputs.get("copies", 1),
                field_name="structured_inputs.copies",
                allowed={1},
            )
            printer_name = _normalize_text(self.structured_inputs.get("printer_name"))
            if printer_name and any(
                character in printer_name for character in ("\x00", "\n", "\r")
            ):
                raise OperationalScenarioValidationError(
                    "structured_inputs.printer_name contains unsupported characters"
                )
            if self.draft_content is not None and self.prompt_input is not None:
                raise OperationalScenarioValidationError(
                    "document_text and generation_prompt must not both be set"
                )
            if self.draft_content is None and self.prompt_input is None:
                raise OperationalScenarioValidationError(
                    "document_text or generation_prompt must not be empty"
                )
            return

        raise OperationalScenarioValidationError(
            f"unsupported scenario_type: {self.scenario_type}"
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "scenario_type": self.scenario_type,
            "owner_id": self.owner_id,
            "owner_session_id": self.owner_session_id,
            "context_ref": self.context_ref,
            "target_url": self.target_url,
            "target_fields": dict(self.target_fields),
            "structured_inputs": dict(self.structured_inputs),
            "draft_content": self.draft_content,
            "prompt_input": self.prompt_input,
            "approval_required": self.approval_required,
            "active_language": self.active_language,
            "detected_language": self.detected_language,
            "execution_mode": self.execution_mode,
            "title": self.title,
        }


@dataclass(frozen=True, slots=True)
class OperationalScenarioCompilation:
    scenario_type: str
    workflow_type: str
    title: str
    workflow_payload: dict[str, object]
    workflow_metadata: dict[str, object]


@dataclass(frozen=True, slots=True)
class OwnerOperationalScenarioResult:
    scenario_type: str
    workflow_id: str | None
    current_step: dict[str, object] | None
    preview_text: str | None
    pending_approval_ids: tuple[str, ...]
    final_result_summary: str | None
    failure_reason: str | None
    workflow_status: str

    def to_dict(self) -> dict[str, object]:
        return {
            "scenario_type": self.scenario_type,
            "workflow_id": self.workflow_id,
            "current_step": None
            if self.current_step is None
            else dict(self.current_step),
            "preview_text": self.preview_text,
            "pending_approval_ids": list(self.pending_approval_ids),
            "final_result_summary": self.final_result_summary,
            "failure_reason": self.failure_reason,
            "workflow_status": self.workflow_status,
        }


SCENARIO_REGISTRY: dict[str, OperationalScenarioDefinition] = {
    "owner_email_review_and_send": OperationalScenarioDefinition(
        scenario_type="owner_email_review_and_send",
        description="Create an email draft, expose the preview, then wait for approval before send.",
        required_inputs=(
            "owner_id",
            "structured_inputs.to",
            "structured_inputs.subject",
            "draft_content",
        ),
        generated_workflow_type="email_draft_then_send",
        trust_profile_expectations=(
            "email drafts may auto-run",
            "send_email remains trust-gated",
        ),
        confirmation_requirements=("preview draft before send approval",),
    ),
    "owner_web_form_review_and_submit": OperationalScenarioDefinition(
        scenario_type="owner_web_form_review_and_submit",
        description="Open a page, review it, fill a form, then wait for approval before submit.",
        required_inputs=(
            "owner_id",
            "target_url",
            "target_fields",
            "structured_inputs.form_selector",
        ),
        generated_workflow_type="browser_open_extract_fill_submit",
        trust_profile_expectations=(
            "read and fill browser actions may auto-run",
            "submit_form remains trust-gated",
        ),
        confirmation_requirements=("preview page state before submit approval",),
    ),
    "owner_page_review_and_extract": OperationalScenarioDefinition(
        scenario_type="owner_page_review_and_extract",
        description="Open a page and extract bounded text without side effects.",
        required_inputs=("owner_id", "target_url"),
        generated_workflow_type="browser_open_extract",
        trust_profile_expectations=("read-only browser actions stay lower-risk",),
        confirmation_requirements=(),
    ),
    "owner_draft_then_browser_update": OperationalScenarioDefinition(
        scenario_type="owner_draft_then_browser_update",
        description="Draft bounded content, open a page, fill an update form, then wait for approval before submit.",
        required_inputs=(
            "owner_id",
            "target_url",
            "structured_inputs.form_selector",
            "structured_inputs.draft_field_name",
            "prompt_input|draft_content",
        ),
        generated_workflow_type="openai_then_browser_open_fill_submit",
        trust_profile_expectations=(
            "draft generation may auto-run",
            "submit_form remains trust-gated",
        ),
        confirmation_requirements=("preview populated form before submit approval",),
    ),
    "owner_generate_review_and_print_document": OperationalScenarioDefinition(
        scenario_type="owner_generate_review_and_print_document",
        description="Prepare a bounded plain-text document, expose the preview, then wait for approval before print.",
        required_inputs=(
            "owner_id",
            "structured_inputs.document_title",
            "draft_content|prompt_input",
        ),
        generated_workflow_type="print_document",
        trust_profile_expectations=(
            "document preparation may auto-run",
            "print_document remains trust-gated",
        ),
        confirmation_requirements=(
            "preview printable text before physical print approval",
        ),
    ),
}


def _normalize_scenario_type(value: object) -> str:
    normalized = _normalize_text(value).lower()
    if normalized not in OPERATIONAL_SCENARIO_TYPES:
        raise OperationalScenarioValidationError(
            "scenario_type must be one of: "
            + ", ".join(sorted(OPERATIONAL_SCENARIO_TYPES))
        )
    return normalized


def create_owner_operational_scenario_request(
    *,
    scenario_type: object,
    owner_id: object,
    owner_session_id: object | None = None,
    context_ref: object = None,
    target_url: object = None,
    target_fields: Mapping[str, object] | None = None,
    structured_inputs: Mapping[str, object] | None = None,
    draft_content: object = None,
    prompt_input: object = None,
    approval_required: bool = True,
    active_language: object = "und",
    detected_language: object | None = None,
    execution_mode: object = "live",
    title: object | None = None,
) -> OwnerOperationalScenarioRequest:
    return OwnerOperationalScenarioRequest(
        scenario_type=_normalize_text(scenario_type),
        owner_id=_normalize_text(owner_id),
        owner_session_id=_normalize_text(owner_session_id) or None,
        context_ref=_normalize_text(context_ref) or None,
        target_url=_normalize_text(target_url) or None,
        target_fields=dict(target_fields or {}),
        structured_inputs=dict(structured_inputs or {}),
        draft_content=_normalize_text(draft_content) or None,
        prompt_input=_normalize_text(prompt_input) or None,
        approval_required=approval_required,
        active_language=_normalize_text(active_language) or "und",
        detected_language=_normalize_text(detected_language) or None,
        execution_mode=_normalize_text(execution_mode) or "live",
        title=_normalize_text(title) or None,
    )


def get_operational_scenario_definition(
    scenario_type: object,
) -> OperationalScenarioDefinition:
    return SCENARIO_REGISTRY[_normalize_scenario_type(scenario_type)]


def compile_owner_operational_scenario(
    request: OwnerOperationalScenarioRequest,
) -> OperationalScenarioCompilation:
    definition = get_operational_scenario_definition(request.scenario_type)
    if request.scenario_type == "owner_email_review_and_send":
        recipients = _email_list(
            request.structured_inputs.get("to"), field_name="structured_inputs.to"
        )
        subject = _normalize_text(request.structured_inputs.get("subject"))
        attachments = _string_list(
            request.structured_inputs.get("attachments"),
            field_name="structured_inputs.attachments",
        )
        body = request.draft_content or ""
        payload = {
            "draft_action": {
                "request_text": "Prepare the owner email draft for review.",
                "action_parameters": {
                    "operation": "create_draft",
                    "to": recipients,
                    "subject": subject,
                    "body": body,
                    "attachments": attachments,
                },
            },
            "send_action": {
                "request_text": "Send the owner email after approval.",
                "action_parameters": {
                    "operation": "send_email",
                    "to": recipients,
                    "subject": subject,
                    "body": body,
                    "attachments": attachments,
                },
            },
        }
        return OperationalScenarioCompilation(
            scenario_type=request.scenario_type,
            workflow_type=definition.generated_workflow_type,
            title=request.title or "Owner Email Review And Send",
            workflow_payload=payload,
            workflow_metadata={
                "scenario_type": request.scenario_type,
                "approval_required": request.approval_required,
            },
        )

    if request.scenario_type == "owner_web_form_review_and_submit":
        form_selector = _normalize_text(request.structured_inputs.get("form_selector"))
        extract_selector = (
            _normalize_text(request.structured_inputs.get("extract_selector")) or "body"
        )
        payload = {
            "open_page_action": {
                "request_text": "Open the target page for owner review.",
                "action_parameters": {
                    "operation": "open_page",
                    "url": request.target_url,
                    "timeout_seconds": 10,
                },
            },
            "extract_action": {
                "request_text": "Extract the visible page content for review.",
                "action_parameters": {
                    "operation": "extract_text",
                    "url": request.target_url,
                    "selector": extract_selector,
                    "timeout_seconds": 10,
                },
            },
            "fill_action": {
                "request_text": "Fill the owner-provided form fields.",
                "action_parameters": {
                    "operation": "fill_form",
                    "url": request.target_url,
                    "selector": form_selector,
                    "fields": dict(request.target_fields),
                    "timeout_seconds": 10,
                },
            },
            "submit_action": {
                "request_text": "Submit the owner-reviewed form after approval.",
                "action_parameters": {
                    "operation": "submit_form",
                    "url": request.target_url,
                    "selector": form_selector,
                    "timeout_seconds": 10,
                },
            },
        }
        return OperationalScenarioCompilation(
            scenario_type=request.scenario_type,
            workflow_type=definition.generated_workflow_type,
            title=request.title or "Owner Web Form Review And Submit",
            workflow_payload=payload,
            workflow_metadata={
                "scenario_type": request.scenario_type,
                "approval_required": request.approval_required,
            },
        )

    if request.scenario_type == "owner_page_review_and_extract":
        extract_selector = (
            _normalize_text(request.structured_inputs.get("extract_selector")) or "body"
        )
        payload = {
            "open_page_action": {
                "request_text": "Open the requested page.",
                "action_parameters": {
                    "operation": "open_page",
                    "url": request.target_url,
                    "timeout_seconds": 10,
                },
            },
            "extract_action": {
                "request_text": "Extract the requested page content.",
                "action_parameters": {
                    "operation": "extract_text",
                    "url": request.target_url,
                    "selector": extract_selector,
                    "timeout_seconds": 10,
                },
            },
        }
        return OperationalScenarioCompilation(
            scenario_type=request.scenario_type,
            workflow_type=definition.generated_workflow_type,
            title=request.title or "Owner Page Review And Extract",
            workflow_payload=payload,
            workflow_metadata={
                "scenario_type": request.scenario_type,
                "approval_required": request.approval_required,
            },
        )

    if request.scenario_type == "owner_draft_then_browser_update":
        form_selector = _normalize_text(request.structured_inputs.get("form_selector"))
        draft_field_name = _normalize_text(
            request.structured_inputs.get("draft_field_name")
        )
        prompt = request.prompt_input or "Return the provided draft content only."
        if request.draft_content:
            prompt = (
                f"{prompt}\n\nDraft content:\n{request.draft_content}\n\n"
                "Return only the final draft text."
            )
        payload = {
            "openai_request": {
                "request_text": "Draft bounded browser update content.",
                "action_parameters": {
                    "model": "gpt-5-mini",
                    "prompt": prompt,
                    "max_tokens": 240,
                    "temperature": 0.0,
                },
            },
            "open_page_action": {
                "request_text": "Open the target page for the owner update.",
                "action_parameters": {
                    "operation": "open_page",
                    "url": request.target_url,
                    "timeout_seconds": 10,
                },
            },
            "fill_action": {
                "request_text": "Fill the update form with the drafted content.",
                "action_parameters": {
                    "operation": "fill_form",
                    "url": request.target_url,
                    "selector": form_selector,
                    "fields": {
                        **dict(request.target_fields),
                        draft_field_name: _PLACEHOLDER_STEP1_SUMMARY,
                    },
                    "timeout_seconds": 10,
                },
            },
            "submit_action": {
                "request_text": "Submit the browser update after approval.",
                "action_parameters": {
                    "operation": "submit_form",
                    "url": request.target_url,
                    "selector": form_selector,
                    "timeout_seconds": 10,
                },
            },
        }
        return OperationalScenarioCompilation(
            scenario_type=request.scenario_type,
            workflow_type=definition.generated_workflow_type,
            title=request.title or "Owner Draft Then Browser Update",
            workflow_payload=payload,
            workflow_metadata={
                "scenario_type": request.scenario_type,
                "approval_required": request.approval_required,
            },
        )

    if request.scenario_type == "owner_generate_review_and_print_document":
        document_title = _normalize_text(
            request.structured_inputs.get("document_title")
        )
        copies = _bounded_positive_int(
            request.structured_inputs.get("copies", 1),
            field_name="structured_inputs.copies",
            allowed={1},
        )
        printer_name = (
            _normalize_text(request.structured_inputs.get("printer_name")) or None
        )
        print_action_parameters: dict[str, object] = {
            "operation": "print_document",
            "document_title": document_title,
            "copies": copies,
        }
        if printer_name is not None:
            print_action_parameters["printer_name"] = printer_name

        if request.draft_content is not None:
            payload = {
                "print_action": {
                    "request_text": "Print the reviewed owner document after approval.",
                    "action_parameters": {
                        **print_action_parameters,
                        "document_text": request.draft_content,
                    },
                }
            }
            workflow_type = "print_document"
        else:
            generation_prompt = request.prompt_input or ""
            payload = {
                "openai_request": {
                    "request_text": "Generate a bounded plain-text document for owner review.",
                    "action_parameters": {
                        "model": "gpt-5-mini",
                        "prompt": (
                            f"Generate a plain-text printable document titled '{document_title}'. "
                            "Return plain text only, no markdown, no commentary, and keep the result concise.\n\n"
                            f"Generation prompt:\n{generation_prompt}"
                        ),
                        "max_tokens": 240,
                        "temperature": 0.0,
                    },
                },
                "print_action": {
                    "request_text": "Print the generated owner document after approval.",
                    "action_parameters": {
                        **print_action_parameters,
                        "document_text": "{{step1.result_payload.text}}",
                    },
                },
            }
            workflow_type = "openai_then_print_document"

        return OperationalScenarioCompilation(
            scenario_type=request.scenario_type,
            workflow_type=workflow_type,
            title=request.title or "Owner Generate Review And Print Document",
            workflow_payload=payload,
            workflow_metadata={
                "scenario_type": request.scenario_type,
                "approval_required": True,
                "document_title": document_title,
            },
        )

    raise OperationalScenarioValidationError(
        f"validation_error: unsupported scenario_type {request.scenario_type}"
    )


def _scenario_type_from_run_result(run_result: OwnerWorkflowRunResult) -> str:
    scenario_type = _normalize_text(run_result.trace_metadata.get("scenario_type"))
    if scenario_type:
        return scenario_type
    workflow_metadata = run_result.response_plan.metadata.get("workflow")
    if isinstance(workflow_metadata, Mapping):
        return _normalize_text(workflow_metadata.get("scenario_type")) or "unknown"
    return "unknown"


def _current_step_payload(
    run_result: OwnerWorkflowRunResult,
) -> dict[str, object] | None:
    if run_result.current_step is None:
        return None
    return summarize_owner_workflow_step(run_result.current_step)


def _failure_reason(run_result: OwnerWorkflowRunResult) -> str | None:
    if run_result.workflow.status not in {"failed", "rejected", "partial_failure"}:
        return None
    if (
        run_result.current_step is not None
        and run_result.current_step.last_error is not None
    ):
        message = _normalize_text(
            run_result.current_step.last_error.get("error_message")
        )
        code = _normalize_text(run_result.current_step.last_error.get("error_code"))
        if code and message:
            return f"{code}: {message}"
        return message or code or run_result.workflow.final_result_summary
    return run_result.workflow.final_result_summary


def scenario_result_from_run_result(
    run_result: OwnerWorkflowRunResult,
) -> OwnerOperationalScenarioResult:
    pending_approval_ids = tuple(
        step.approval_id
        for step in run_result.steps
        if step.status == "awaiting_approval" and step.approval_id
    )
    return OwnerOperationalScenarioResult(
        scenario_type=_scenario_type_from_run_result(run_result),
        workflow_id=run_result.workflow.workflow_id,
        current_step=_current_step_payload(run_result),
        preview_text=run_result.response_plan.preview_text,
        pending_approval_ids=pending_approval_ids,
        final_result_summary=run_result.workflow.final_result_summary
        or run_result.response_plan.summary_text,
        failure_reason=_failure_reason(run_result),
        workflow_status=run_result.workflow.status,
    )


def validation_failure_result(
    scenario_type: object,
    exc: OperationalScenarioValidationError,
) -> OwnerOperationalScenarioResult:
    return OwnerOperationalScenarioResult(
        scenario_type=_normalize_text(scenario_type) or "unknown",
        workflow_id=None,
        current_step=None,
        preview_text=None,
        pending_approval_ids=(),
        final_result_summary=None,
        failure_reason=f"{exc.code}: {exc.reason}",
        workflow_status="failed",
    )


class OwnerOperationalScenarioOrchestrator:
    def __init__(
        self,
        *,
        workflow_orchestrator: OwnerWorkflowOrchestrator | None = None,
    ) -> None:
        self._workflow_orchestrator = (
            workflow_orchestrator or OwnerWorkflowOrchestrator()
        )

    def execute_scenario(
        self,
        request: OwnerOperationalScenarioRequest | Mapping[str, object],
        *,
        owner_domain: OwnerDomain,
        memory_scope: OwnerMemoryScope,
        action_scope: OwnerActionScope,
        trust_profile: OwnerTrustProfile,
        session: OwnerSession | None = None,
    ) -> OwnerOperationalScenarioResult:
        try:
            resolved_request = (
                request
                if isinstance(request, OwnerOperationalScenarioRequest)
                else create_owner_operational_scenario_request(**dict(request))
            )
            compilation = compile_owner_operational_scenario(resolved_request)
        except TypeError as exc:
            raw_scenario_type = (
                request.scenario_type
                if isinstance(request, OwnerOperationalScenarioRequest)
                else dict(request).get("scenario_type")
            )
            return validation_failure_result(
                raw_scenario_type,
                OperationalScenarioValidationError(str(exc)),
            )
        except OperationalScenarioValidationError as exc:
            raw_scenario_type = (
                request.scenario_type
                if isinstance(request, OwnerOperationalScenarioRequest)
                else dict(request).get("scenario_type")
            )
            return validation_failure_result(raw_scenario_type, exc)

        run_result = self._workflow_orchestrator.create_workflow(
            owner_id=resolved_request.owner_id,
            workflow_type=compilation.workflow_type,
            workflow_payload=compilation.workflow_payload,
            owner_domain=owner_domain,
            memory_scope=memory_scope,
            action_scope=action_scope,
            trust_profile=trust_profile,
            title=compilation.title,
            session=session,
            owner_session_id=resolved_request.owner_session_id,
            active_language=resolved_request.active_language,
            detected_language=resolved_request.detected_language,
            context_ref=resolved_request.context_ref,
            execution_mode=resolved_request.execution_mode,
            workflow_metadata=compilation.workflow_metadata,
        )
        return scenario_result_from_run_result(run_result)

    def resume_scenario(self, workflow_id: object) -> OwnerOperationalScenarioResult:
        return scenario_result_from_run_result(
            self._workflow_orchestrator.resume_workflow(workflow_id)
        )

    def approve_scenario(self, approval_id: object) -> OwnerOperationalScenarioResult:
        return scenario_result_from_run_result(
            self._workflow_orchestrator.approve_step(approval_id)
        )

    def reject_scenario(self, approval_id: object) -> OwnerOperationalScenarioResult:
        return scenario_result_from_run_result(
            self._workflow_orchestrator.reject_step(approval_id)
        )
