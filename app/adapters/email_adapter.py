from __future__ import annotations

import base64
import os
import re
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from email.message import EmailMessage
from hashlib import sha1
from typing import Protocol
from urllib.parse import quote

from app.execution.action_contract import (
    ActionContractViolation,
    build_action_result_contract,
    validate_action_contract,
)
from integrations import gmail_tool


SUPPORTED_ACTION_TYPE = "EMAIL_ACTION"
SUPPORTED_TARGET_REFS = frozenset({"email", "email_adapter"})
SUPPORTED_PARAMETER_FIELDS = frozenset(
    {"operation", "to", "subject", "body", "reply_to_id", "attachments"}
)
SUPPORTED_OPERATIONS = frozenset({"create_draft", "send_email", "reply_email"})
SUPPORTED_PROVIDER_MODES = frozenset({"auto", "stub", "gmail"})
DEFAULT_PROVIDER_MODE = "auto"
MAX_RECIPIENTS = 20
MAX_ATTACHMENTS = 10
MAX_SUBJECT_LENGTH = 240
MAX_BODY_LENGTH = 4000
PROVIDER_ENV_VAR = "DIGITAL_FOREMAN_EMAIL_BACKEND"
_EMAIL_PATTERN = re.compile(
    r"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+$"
)


@dataclass(frozen=True, slots=True)
class EmailAdapterConfig:
    provider_mode: str = DEFAULT_PROVIDER_MODE


@dataclass(frozen=True, slots=True)
class EmailExecution:
    result_type: str
    summary: str
    references: dict[str, object]


class EmailAdapterError(RuntimeError):
    def __init__(
        self,
        error_code: str,
        message: str,
        *,
        diagnostic: Mapping[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.message = message
        self.diagnostic = _normalize_mapping(diagnostic)


class EmailRuntime(Protocol):
    def create_draft(
        self,
        *,
        to: list[str],
        subject: str,
        body: str,
        attachments: list[str],
    ) -> EmailExecution: ...

    def send_email(
        self,
        *,
        to: list[str],
        subject: str,
        body: str,
        attachments: list[str],
    ) -> EmailExecution: ...

    def reply_email(
        self,
        *,
        reply_to_id: str,
        body: str,
        to: list[str],
        subject: str | None,
        attachments: list[str],
    ) -> EmailExecution: ...


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_mapping(value: Mapping[str, object] | None) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _normalize_sequence(value: object) -> list[object]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return list(value)
    return []


def _bounded_text(value: object, *, field_name: str, max_length: int) -> str:
    normalized = _normalize_text(value)
    if not normalized:
        raise ActionContractViolation(f"{field_name} must not be empty")
    if len(normalized) > max_length:
        raise ActionContractViolation(f"{field_name} exceeds max length")
    return normalized


def _normalize_recipients(value: object, *, required: bool) -> list[str]:
    recipients = _normalize_sequence(value)
    if not recipients:
        if required:
            raise ActionContractViolation("parameters.to must be a non-empty list")
        return []
    if len(recipients) > MAX_RECIPIENTS:
        raise ActionContractViolation("parameters.to exceeds max size")
    normalized: list[str] = []
    for recipient in recipients:
        email = _bounded_text(
            recipient, field_name="parameters.to item", max_length=320
        ).lower()
        if _EMAIL_PATTERN.fullmatch(email) is None:
            raise ActionContractViolation(f"invalid recipient: {email}")
        if email not in normalized:
            normalized.append(email)
    return normalized


def _normalize_attachments(value: object) -> list[str]:
    attachments = _normalize_sequence(value)
    if not attachments:
        return []
    if len(attachments) > MAX_ATTACHMENTS:
        raise ActionContractViolation("parameters.attachments exceeds max size")
    normalized: list[str] = []
    for item in attachments:
        normalized.append(
            _bounded_text(
                item, field_name="parameters.attachments item", max_length=256
            )
        )
    return normalized


def validate_email_action_parameters(
    parameters: Mapping[str, object],
) -> dict[str, object]:
    normalized = dict(parameters)
    unexpected_fields = sorted(set(normalized) - SUPPORTED_PARAMETER_FIELDS)
    if unexpected_fields:
        raise ActionContractViolation(
            "parameters contains unsupported fields: " + ", ".join(unexpected_fields)
        )

    operation = _bounded_text(
        normalized.get("operation"),
        field_name="parameters.operation",
        max_length=32,
    )
    if operation not in SUPPORTED_OPERATIONS:
        raise ActionContractViolation(f"unsupported email operation: {operation}")

    requires_recipients = operation in {"create_draft", "send_email"}
    recipients = _normalize_recipients(
        normalized.get("to"), required=requires_recipients
    )
    attachments = _normalize_attachments(normalized.get("attachments"))

    if operation in {"create_draft", "send_email"}:
        subject = _bounded_text(
            normalized.get("subject"),
            field_name="parameters.subject",
            max_length=MAX_SUBJECT_LENGTH,
        )
        body = _bounded_text(
            normalized.get("body"),
            field_name="parameters.body",
            max_length=MAX_BODY_LENGTH,
        )
        reply_to_id = None
    else:
        subject = _normalize_text(normalized.get("subject")) or None
        if subject is not None and len(subject) > MAX_SUBJECT_LENGTH:
            raise ActionContractViolation("parameters.subject exceeds max length")
        body = _bounded_text(
            normalized.get("body"),
            field_name="parameters.body",
            max_length=MAX_BODY_LENGTH,
        )
        reply_to_id = _bounded_text(
            normalized.get("reply_to_id"),
            field_name="parameters.reply_to_id",
            max_length=256,
        )

    if operation != "reply_email" and _normalize_text(normalized.get("reply_to_id")):
        raise ActionContractViolation(
            f"parameters.reply_to_id is not supported for {operation}"
        )

    return {
        "operation": operation,
        "to": recipients,
        "subject": subject,
        "body": body,
        "reply_to_id": reply_to_id,
        "attachments": attachments,
    }


class StubEmailRuntime:
    backend_name = "stub"

    def create_draft(
        self,
        *,
        to: list[str],
        subject: str,
        body: str,
        attachments: list[str],
    ) -> EmailExecution:
        draft_id = f"draft-{sha1(f'{to}|{subject}'.encode('utf-8')).hexdigest()[:12]}"
        return EmailExecution(
            result_type="email_draft",
            summary=f"Prepared draft email to {', '.join(to)}",
            references={
                "draft_id": draft_id,
                "recipient_count": len(to),
                "subject": subject,
                "attachment_count": len(attachments),
            },
        )

    def send_email(
        self,
        *,
        to: list[str],
        subject: str,
        body: str,
        attachments: list[str],
    ) -> EmailExecution:
        message_id = (
            f"message-{sha1(f'{to}|{subject}|{body}'.encode('utf-8')).hexdigest()[:12]}"
        )
        return EmailExecution(
            result_type="email_send",
            summary=f"Sent email to {', '.join(to)}",
            references={
                "message_id": message_id,
                "recipient_count": len(to),
                "subject": subject,
                "attachment_count": len(attachments),
            },
        )

    def reply_email(
        self,
        *,
        reply_to_id: str,
        body: str,
        to: list[str],
        subject: str | None,
        attachments: list[str],
    ) -> EmailExecution:
        message_id = (
            f"reply-{sha1(f'{reply_to_id}|{body}'.encode('utf-8')).hexdigest()[:12]}"
        )
        return EmailExecution(
            result_type="email_reply",
            summary=f"Replied to {reply_to_id}",
            references={
                "message_id": message_id,
                "reply_to_id": reply_to_id,
                "recipient_count": len(to),
                "subject": subject,
                "attachment_count": len(attachments),
            },
        )


class GmailEmailRuntime:
    backend_name = "gmail"

    def _ensure_configured(self) -> None:
        if not gmail_tool._gmail_credentials_available():
            raise EmailAdapterError(
                "provider_not_configured",
                "gmail provider is not configured",
            )

    def _ensure_supported_attachments(self, attachments: list[str]) -> None:
        if attachments:
            raise EmailAdapterError(
                "unsupported_operation",
                "gmail email adapter does not support attachments",
                diagnostic={"attachment_count": len(attachments)},
            )

    def _encoded_message(self, message: EmailMessage) -> str:
        return base64.urlsafe_b64encode(message.as_bytes()).decode("ascii")

    def _build_message(
        self,
        *,
        to: list[str],
        subject: str,
        body: str,
        in_reply_to: str | None = None,
        references: str | None = None,
    ) -> EmailMessage:
        message = EmailMessage()
        message["To"] = ", ".join(to)
        message["Subject"] = gmail_tool._safe_header_value(
            "subject",
            subject,
            error_code="GMAIL_SEND_FAILED",
        )
        sender = gmail_tool.gmail_execution_sender()
        if sender:
            message["From"] = sender
        if in_reply_to:
            message["In-Reply-To"] = gmail_tool._safe_header_value(
                "message id",
                in_reply_to,
                error_code="GMAIL_SEND_FAILED",
            )
        if references:
            message["References"] = gmail_tool._safe_header_value(
                "references",
                references,
                error_code="GMAIL_SEND_FAILED",
            )
        message.set_content(gmail_tool._normalized_send_body(body))
        return message

    def _create_draft_payload(
        self,
        *,
        to: list[str],
        subject: str,
        body: str,
    ) -> dict[str, object]:
        message = self._build_message(to=to, subject=subject, body=body)
        return {"message": {"raw": self._encoded_message(message)}}

    def _metadata_headers(
        self,
        message_id: str,
    ) -> tuple[str | None, str | None, str | None, str, str]:
        self._ensure_configured()
        response = gmail_tool._gmail_request_json(
            f"/messages/{quote(message_id)}?format=metadata"
            "&metadataHeaders=Subject"
            "&metadataHeaders=Message-ID"
            "&metadataHeaders=References"
            "&metadataHeaders=Reply-To"
            "&metadataHeaders=From",
            method="GET",
        )
        payload = _normalize_mapping(response.get("payload"))
        headers = payload.get("headers")
        if not isinstance(headers, list):
            raise EmailAdapterError(
                "provider_error",
                "gmail reply lookup returned no message headers",
            )
        subject = _normalize_text(gmail_tool._header_value(headers, "Subject")) or None
        message_header_id = (
            _normalize_text(gmail_tool._header_value(headers, "Message-ID")) or None
        )
        references = (
            _normalize_text(gmail_tool._header_value(headers, "References")) or None
        )
        reply_to = _normalize_text(
            gmail_tool._header_value(headers, "Reply-To")
        ) or _normalize_text(gmail_tool._header_value(headers, "From"))
        recipient = _normalize_text(reply_to)
        if "<" in recipient and ">" in recipient:
            recipient = recipient.split("<", 1)[1].split(">", 1)[0].strip()
        if not recipient or _EMAIL_PATTERN.fullmatch(recipient.lower()) is None:
            raise EmailAdapterError(
                "provider_error",
                "gmail reply lookup returned no valid recipient",
            )
        thread_id = _normalize_text(response.get("threadId"))
        if not thread_id:
            raise EmailAdapterError(
                "provider_error",
                "gmail reply lookup returned no thread id",
            )
        return subject, message_header_id, references, recipient.lower(), thread_id

    def create_draft(
        self,
        *,
        to: list[str],
        subject: str,
        body: str,
        attachments: list[str],
    ) -> EmailExecution:
        self._ensure_configured()
        self._ensure_supported_attachments(attachments)
        response = gmail_tool._gmail_request_json(
            "/drafts",
            method="POST",
            payload=self._create_draft_payload(to=to, subject=subject, body=body),
        )
        draft_id = _normalize_text(response.get("id"))
        if not draft_id:
            raise EmailAdapterError(
                "provider_error", "gmail draft creation returned no draft id"
            )
        response_message = _normalize_mapping(response.get("message"))
        return EmailExecution(
            result_type="email_draft",
            summary=f"Created draft email to {', '.join(to)}",
            references={
                "draft_id": draft_id,
                "thread_id": _normalize_text(response_message.get("threadId")) or None,
                "recipient_count": len(to),
                "subject": subject,
                "attachment_count": 0,
            },
        )

    def send_email(
        self,
        *,
        to: list[str],
        subject: str,
        body: str,
        attachments: list[str],
    ) -> EmailExecution:
        self._ensure_configured()
        self._ensure_supported_attachments(attachments)
        message = self._build_message(to=to, subject=subject, body=body)
        response = gmail_tool._gmail_request_json(
            "/messages/send",
            method="POST",
            payload={"raw": self._encoded_message(message)},
        )
        message_id = _normalize_text(response.get("id"))
        if not message_id:
            raise EmailAdapterError(
                "provider_error", "gmail send returned no message id"
            )
        return EmailExecution(
            result_type="email_send",
            summary=f"Sent email to {', '.join(to)}",
            references={
                "message_id": message_id,
                "thread_id": _normalize_text(response.get("threadId")) or None,
                "recipient_count": len(to),
                "subject": subject,
                "attachment_count": 0,
            },
        )

    def reply_email(
        self,
        *,
        reply_to_id: str,
        body: str,
        to: list[str],
        subject: str | None,
        attachments: list[str],
    ) -> EmailExecution:
        self._ensure_configured()
        self._ensure_supported_attachments(attachments)
        (
            source_subject,
            message_header_id,
            references,
            reply_recipient,
            thread_id,
        ) = self._metadata_headers(reply_to_id)
        recipients = list(to) if to else [reply_recipient]
        resolved_subject = subject or source_subject or "Re:"
        if not resolved_subject.lower().startswith("re:"):
            resolved_subject = f"Re: {resolved_subject}"
        combined_references = (
            " ".join(
                part
                for part in [references, message_header_id]
                if _normalize_text(part)
            ).strip()
            or None
        )
        message = self._build_message(
            to=recipients,
            subject=resolved_subject,
            body=body,
            in_reply_to=message_header_id,
            references=combined_references,
        )
        response = gmail_tool._gmail_request_json(
            "/messages/send",
            method="POST",
            payload={
                "raw": self._encoded_message(message),
                "threadId": thread_id,
            },
        )
        message_id = _normalize_text(response.get("id"))
        if not message_id:
            raise EmailAdapterError(
                "provider_error", "gmail reply returned no message id"
            )
        return EmailExecution(
            result_type="email_reply",
            summary=f"Replied to {reply_to_id}",
            references={
                "message_id": message_id,
                "thread_id": _normalize_text(response.get("threadId")) or None,
                "reply_to_id": reply_to_id,
                "recipient_count": len(recipients),
                "recipient": reply_recipient,
                "subject": resolved_subject,
                "attachment_count": 0,
            },
        )


def _validate_email_action_contract(action_contract: object) -> dict[str, object]:
    validated = validate_action_contract(action_contract)
    if validated["action_type"] != SUPPORTED_ACTION_TYPE:
        raise ActionContractViolation(
            f"unsupported action_type for email adapter: {validated['action_type']}"
        )
    target_ref = _normalize_text(validated.get("target_ref")).lower()
    if target_ref not in SUPPORTED_TARGET_REFS:
        raise ActionContractViolation(
            "target_ref must be one of: " + ", ".join(sorted(SUPPORTED_TARGET_REFS))
        )
    return {
        **validated,
        "parameters": validate_email_action_parameters(
            _normalize_mapping(validated.get("parameters"))
        ),
    }


def _operation_result_type(operation: str) -> str:
    mapping = {
        "create_draft": "email_draft",
        "send_email": "email_send",
        "reply_email": "email_reply",
    }
    return mapping[operation]


def _normalize_provider_mode(value: object) -> str:
    normalized = _normalize_text(value).lower() or DEFAULT_PROVIDER_MODE
    if normalized not in SUPPORTED_PROVIDER_MODES:
        raise EmailAdapterError(
            "validation_error",
            "provider_mode must be one of: "
            + ", ".join(sorted(SUPPORTED_PROVIDER_MODES)),
        )
    return normalized


def _resolve_email_adapter_config(
    config: EmailAdapterConfig | Mapping[str, object] | None,
) -> EmailAdapterConfig:
    if config is None:
        return EmailAdapterConfig()
    if isinstance(config, EmailAdapterConfig):
        return config
    if not isinstance(config, Mapping):
        raise EmailAdapterError(
            "validation_error", "email adapter config must be a mapping"
        )
    payload = dict(config)
    unexpected_fields = sorted(set(payload) - {"provider_mode"})
    if unexpected_fields:
        raise EmailAdapterError(
            "validation_error",
            "email adapter config contains unsupported fields: "
            + ", ".join(unexpected_fields),
        )
    return EmailAdapterConfig(
        provider_mode=_normalize_provider_mode(payload.get("provider_mode"))
    )


def _env_provider() -> str:
    normalized = _normalize_text(os.getenv(PROVIDER_ENV_VAR)).lower()
    if normalized in SUPPORTED_PROVIDER_MODES - {"auto"}:
        return normalized
    return ""


def _build_default_email_runtime(
    config: EmailAdapterConfig,
) -> tuple[EmailRuntime, str]:
    provider = config.provider_mode
    if provider == "auto":
        provider = _env_provider()
        if not provider:
            raise EmailAdapterError(
                "provider_not_configured",
                "email provider is not configured",
            )
    if provider == "stub":
        return StubEmailRuntime(), "stub"
    if provider == "gmail":
        return GmailEmailRuntime(), "gmail"
    raise EmailAdapterError(
        "provider_not_configured",
        f"email provider `{provider}` is not configured",
    )


def _runtime_backend_name(
    runtime: EmailRuntime | None, config: EmailAdapterConfig
) -> str:
    candidate = _normalize_text(getattr(runtime, "backend_name", ""))
    if candidate:
        return candidate
    if config.provider_mode != "auto":
        return config.provider_mode
    return "custom"


def _simulation_payload(
    action_id: str, parameters: Mapping[str, object]
) -> dict[str, object]:
    operation = _normalize_text(parameters.get("operation"))
    return build_action_result_contract(
        action_id=action_id,
        status="success",
        result_type="simulation",
        payload={
            "note": f"dry run: {operation}",
            "summary": f"Simulated email operation {operation}",
            "references": {
                "to": list(parameters.get("to") or []),
                "reply_to_id": parameters.get("reply_to_id"),
                "attachment_count": len(list(parameters.get("attachments") or [])),
            },
            "metadata": {
                "dry_run": True,
                "simulation_mode": "dry_run",
                "operation": operation,
                "provider": "simulation",
                "backend_used": "dry_run",
            },
        },
    )


def _failure_result(
    *,
    action_id: str,
    error_code: str,
    error_message: str,
    backend_used: str,
    parameters: Mapping[str, object] | None = None,
    diagnostic: Mapping[str, object] | None = None,
) -> dict[str, object]:
    metadata = {
        "operation": _normalize_text((parameters or {}).get("operation")),
        "provider": backend_used,
        "backend_used": backend_used,
    }
    payload: dict[str, object] = {"metadata": metadata}
    normalized_diagnostic = _normalize_mapping(diagnostic)
    if normalized_diagnostic:
        payload["diagnostic"] = normalized_diagnostic
    operation = _normalize_text((parameters or {}).get("operation")) or "create_draft"
    result_type = (
        _operation_result_type(operation)
        if operation in SUPPORTED_OPERATIONS
        else "email_action"
    )
    return build_action_result_contract(
        action_id=action_id,
        status="failed",
        result_type=result_type,
        payload=payload,
        error_code=error_code,
        error_message=error_message,
    )


def execute_email_action(
    action_contract: object,
    *,
    runtime: EmailRuntime | None = None,
    config: EmailAdapterConfig | Mapping[str, object] | None = None,
) -> dict[str, object]:
    started_at = time.monotonic()
    action_id = (
        _normalize_text(_normalize_mapping(action_contract).get("action_id"))
        or "unknown_action"
    )
    raw_parameters = _normalize_mapping(
        _normalize_mapping(action_contract).get("parameters")
    )
    effective_config = _resolve_email_adapter_config(config)
    backend_used = _runtime_backend_name(runtime, effective_config)

    try:
        validated = _validate_email_action_contract(action_contract)
        action_id = str(validated["action_id"])
        parameters = dict(validated["parameters"])
        operation = str(parameters["operation"])

        if validated["execution_mode"] == "dry_run":
            return _simulation_payload(action_id, parameters)

        if runtime is None:
            runtime, backend_used = _build_default_email_runtime(effective_config)
        else:
            backend_used = _runtime_backend_name(runtime, effective_config)

        if operation == "create_draft":
            execution = runtime.create_draft(
                to=list(parameters["to"]),
                subject=str(parameters["subject"]),
                body=str(parameters["body"]),
                attachments=list(parameters["attachments"]),
            )
        elif operation == "send_email":
            execution = runtime.send_email(
                to=list(parameters["to"]),
                subject=str(parameters["subject"]),
                body=str(parameters["body"]),
                attachments=list(parameters["attachments"]),
            )
        else:
            execution = runtime.reply_email(
                reply_to_id=str(parameters["reply_to_id"]),
                body=str(parameters["body"]),
                to=list(parameters["to"]),
                subject=(
                    None
                    if parameters.get("subject") is None
                    else str(parameters["subject"])
                ),
                attachments=list(parameters["attachments"]),
            )
    except ActionContractViolation as exc:
        return _failure_result(
            action_id=action_id,
            error_code="validation_error",
            error_message=str(exc),
            backend_used=backend_used,
            parameters=raw_parameters,
        )
    except gmail_tool.GmailToolError as exc:
        message = _normalize_text(exc.message) or "gmail provider failed"
        error_code = "provider_error"
        lowered = message.lower()
        if "credentials are not configured" in lowered:
            error_code = "provider_not_configured"
        elif "timed out" in lowered:
            error_code = "timeout"
        return _failure_result(
            action_id=action_id,
            error_code=error_code,
            error_message=message,
            backend_used=backend_used,
            parameters=raw_parameters,
            diagnostic={"provider_code": exc.code},
        )
    except EmailAdapterError as exc:
        return _failure_result(
            action_id=action_id,
            error_code=exc.error_code,
            error_message=exc.message,
            backend_used=backend_used,
            parameters=raw_parameters,
            diagnostic=exc.diagnostic,
        )
    except TimeoutError:
        return _failure_result(
            action_id=action_id,
            error_code="timeout",
            error_message="email action timed out",
            backend_used=backend_used,
            parameters=raw_parameters,
        )
    except Exception as exc:
        return _failure_result(
            action_id=action_id,
            error_code="unknown_error",
            error_message="email action failed",
            backend_used=backend_used,
            parameters=raw_parameters,
            diagnostic={"exception_type": type(exc).__name__},
        )

    latency_ms = max(0, int(round((time.monotonic() - started_at) * 1000)))
    return build_action_result_contract(
        action_id=action_id,
        status="success",
        result_type=execution.result_type,
        payload={
            "summary": execution.summary,
            "references": dict(execution.references),
            "metadata": {
                "operation": operation,
                "provider": backend_used,
                "backend_used": backend_used,
                "latency_ms": latency_ms,
            },
        },
    )
