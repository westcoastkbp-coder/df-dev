from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Final

from app.execution.action_result import build_action_result
from app.execution.execution_boundary import current_execution_scope, require_execution_boundary
from app.execution.external_modules import (
    ExternalModuleRegistry,
    build_external_module_request,
    execute_external_module,
)
from app.policy.policy_gate import evaluate_external_action_policy
from integrations.gmail_gateway import send_email
from integrations.phone_gateway import schedule_call
from integrations.sms_gateway import send_sms


SEND_SMS: Final[str] = "SEND_SMS"
MAKE_CALL: Final[str] = "MAKE_CALL"
SEND_EMAIL: Final[str] = "SEND_EMAIL"
API_REQUEST: Final[str] = "API_REQUEST"

ALLOWED_EXTERNAL_ACTION_TYPES: Final[tuple[str, ...]] = (
    SEND_SMS,
    MAKE_CALL,
    SEND_EMAIL,
    API_REQUEST,
)
STATIC_ACTION_DESTINATIONS: Final[dict[str, frozenset[str]]] = {
    SEND_SMS: frozenset({"sms_gateway"}),
    MAKE_CALL: frozenset({"phone_gateway"}),
    SEND_EMAIL: frozenset({"gmail_gateway"}),
    API_REQUEST: frozenset({"estimate_service"}),
}


class ExternalActionValidationError(ValueError):
    """Raised when an external action payload violates the fixed contract."""


@dataclass(frozen=True, slots=True)
class ExternalActionRequest:
    action_type: str
    destination: str
    payload: dict[str, object]


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_mapping(value: object, *, field_name: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise ExternalActionValidationError(f"{field_name} must be a dict")
    return dict(value)


def _normalize_string_list(value: object, *, field_name: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ExternalActionValidationError(f"{field_name} must be a list")
    normalized = [_normalize_text(item) for item in value]
    if any(not item for item in normalized):
        raise ExternalActionValidationError(f"{field_name} must not contain empty values")
    return normalized


def _validate_destination(action_type: str, destination: object) -> str:
    normalized_destination = _normalize_text(destination)
    if not normalized_destination:
        raise ExternalActionValidationError("destination must not be empty")
    allowed_destinations = STATIC_ACTION_DESTINATIONS.get(action_type, frozenset())
    if normalized_destination not in allowed_destinations:
        raise ExternalActionValidationError(
            f"destination `{normalized_destination}` is not allowed for {action_type}"
        )
    return normalized_destination


def build_external_action_request(action_type: object, payload: object) -> ExternalActionRequest:
    normalized_action_type = _normalize_text(action_type).upper()
    if normalized_action_type not in ALLOWED_EXTERNAL_ACTION_TYPES:
        raise ExternalActionValidationError(
            f"unsupported external action type: {normalized_action_type or '(empty)'}"
        )
    normalized_payload = _normalize_mapping(payload, field_name="payload")
    destination = _validate_destination(
        normalized_action_type,
        normalized_payload.get("destination"),
    )

    if normalized_action_type == SEND_SMS:
        request_payload = {
            "destination": destination,
            "contact_id": _normalize_text(normalized_payload.get("contact_id")),
            "text": _normalize_text(normalized_payload.get("text")),
            "confirmed": bool(normalized_payload.get("confirmed")),
        }
        if not request_payload["contact_id"]:
            raise ExternalActionValidationError("payload.contact_id must not be empty")
        if not request_payload["text"]:
            raise ExternalActionValidationError("payload.text must not be empty")
        return ExternalActionRequest(
            action_type=normalized_action_type,
            destination=destination,
            payload=request_payload,
        )

    if normalized_action_type == MAKE_CALL:
        request_payload = {
            "destination": destination,
            "contact_id": _normalize_text(normalized_payload.get("contact_id")),
            "phone_number": _normalize_text(normalized_payload.get("phone_number")),
            "script": _normalize_text(normalized_payload.get("script")),
            "confirmed": bool(normalized_payload.get("confirmed")),
        }
        for field_name in ("contact_id", "phone_number", "script"):
            if not request_payload[field_name]:
                raise ExternalActionValidationError(f"payload.{field_name} must not be empty")
        return ExternalActionRequest(
            action_type=normalized_action_type,
            destination=destination,
            payload=request_payload,
        )

    if normalized_action_type == SEND_EMAIL:
        request_payload = {
            "destination": destination,
            "to": _normalize_text(normalized_payload.get("to")),
            "subject": _normalize_text(normalized_payload.get("subject")),
            "body": _normalize_text(normalized_payload.get("body")),
            "link": _normalize_text(normalized_payload.get("link")),
            "attachments": _normalize_string_list(
                normalized_payload.get("attachments"),
                field_name="payload.attachments",
            ),
            "confirmed": bool(normalized_payload.get("confirmed")),
        }
        if "@" not in request_payload["to"]:
            raise ExternalActionValidationError("payload.to must be a valid email destination")
        if not request_payload["subject"]:
            raise ExternalActionValidationError("payload.subject must not be empty")
        return ExternalActionRequest(
            action_type=normalized_action_type,
            destination=destination,
            payload=request_payload,
        )

    request_payload = {
        "destination": destination,
        "request_id": _normalize_text(normalized_payload.get("request_id")),
        "operation": _normalize_text(normalized_payload.get("operation")),
        "request_payload": _normalize_mapping(
            normalized_payload.get("request_payload"),
            field_name="payload.request_payload",
        ),
        "timeout_ms": normalized_payload.get("timeout_ms", 15000),
        "correlation_id": _normalize_text(
            normalized_payload.get("correlation_id") or normalized_payload.get("request_id")
        ),
        "confirmed": bool(normalized_payload.get("confirmed")),
    }
    if not request_payload["request_id"]:
        raise ExternalActionValidationError("payload.request_id must not be empty")
    if not request_payload["operation"]:
        raise ExternalActionValidationError("payload.operation must not be empty")
    if not isinstance(request_payload["timeout_ms"], int):
        raise ExternalActionValidationError("payload.timeout_ms must be an integer")
    return ExternalActionRequest(
        action_type=normalized_action_type,
        destination=destination,
        payload=request_payload,
    )


def _policy_blocked_result(
    *,
    task_id: str,
    action_type: str,
    reason: str,
    policy_trace: dict[str, object],
) -> dict[str, object]:
    return build_action_result(
        status="policy_blocked",
        task_id=task_id,
        action_type=action_type,
        result_payload={
            "result_type": "POLICY_VIOLATION",
            "policy_trace": dict(policy_trace),
        },
        error_code="POLICY_VIOLATION",
        error_message=reason,
        source="app.execution.external_actions",
    )


def _gateway_failure_result(
    *,
    task_id: str,
    action_type: str,
    provider_result: Mapping[str, object],
) -> dict[str, object]:
    return build_action_result(
        status="failed",
        task_id=task_id,
        action_type=action_type,
        result_payload={"provider_result": dict(provider_result)},
        error_code="external_action_failed",
        error_message=_normalize_text(provider_result.get("error")) or "external action failed",
        source="app.execution.external_actions",
    )


def execute_external(
    action_type: object,
    payload: object,
    *,
    registry: ExternalModuleRegistry | None = None,
) -> dict[str, object]:
    scope = require_execution_boundary(
        component="external_actions.execute_external",
        reason="direct_external_action_call_blocked",
    )
    request = build_external_action_request(action_type, payload)
    policy_result = evaluate_external_action_policy(
        request.action_type,
        request.payload,
        {"task_id": scope.task_id, "status": "running"},
    )
    if not policy_result.execution_allowed:
        return _policy_blocked_result(
            task_id=scope.task_id,
            action_type=request.action_type,
            reason=policy_result.reason,
            policy_trace=policy_result.policy_trace,
        )

    if request.action_type == SEND_SMS:
        provider_result = send_sms(
            contact_id=str(request.payload["contact_id"]),
            text=str(request.payload["text"]),
        )
        if not bool(provider_result.get("ok")):
            return _gateway_failure_result(
                task_id=scope.task_id,
                action_type=request.action_type,
                provider_result=provider_result,
            )
        return build_action_result(
            status="completed",
            task_id=scope.task_id,
            action_type=request.action_type,
            result_payload={"provider_result": dict(provider_result)},
            error_code="",
            error_message="",
            source="app.execution.external_actions",
        )

    if request.action_type == MAKE_CALL:
        provider_result = schedule_call(
            contact_id=str(request.payload["contact_id"]),
            phone_number=str(request.payload["phone_number"]),
            script=str(request.payload["script"]),
        )
        if not bool(provider_result.get("ok")):
            return _gateway_failure_result(
                task_id=scope.task_id,
                action_type=request.action_type,
                provider_result=provider_result,
            )
        return build_action_result(
            status="completed",
            task_id=scope.task_id,
            action_type=request.action_type,
            result_payload={"provider_result": dict(provider_result)},
            error_code="",
            error_message="",
            source="app.execution.external_actions",
        )

    if request.action_type == SEND_EMAIL:
        provider_result = send_email(
            to=str(request.payload["to"]),
            subject=str(request.payload["subject"]),
            body=str(request.payload["body"]),
            link=str(request.payload.get("link", "")).strip() or None,
            attachments=list(request.payload.get("attachments", [])),
        )
        if not bool(provider_result.get("ok")):
            return _gateway_failure_result(
                task_id=scope.task_id,
                action_type=request.action_type,
                provider_result=provider_result,
            )
        return build_action_result(
            status="completed",
            task_id=scope.task_id,
            action_type=request.action_type,
            result_payload={"provider_result": dict(provider_result)},
            error_code="",
            error_message="",
            source="app.execution.external_actions",
        )

    effective_registry = registry or ExternalModuleRegistry()
    external_request = build_external_module_request(
        {
            "request_id": request.payload["request_id"],
            "task_id": scope.task_id,
            "task_type": current_execution_scope().intent or "external_action",
            "module_type": request.destination,
            "operation": request.payload["operation"],
            "payload": request.payload["request_payload"],
            "correlation_id": request.payload["correlation_id"],
            "timeout_ms": request.payload["timeout_ms"],
            "metadata": {
                "schema_version": "v1",
                "request_source": "external_actions",
                "capability": "api_request",
                "priority": "normal",
                "tags": ["external_action", "api_request"],
            },
        }
    )
    module_result = execute_external_module(external_request, registry=effective_registry)
    if module_result.status != "success":
        return build_action_result(
            status="failed",
            task_id=scope.task_id,
            action_type=request.action_type,
            result_payload={"external_module_result": module_result.to_dict()},
            error_code=module_result.error_code or "external_action_failed",
            error_message=module_result.error_message or "external action failed",
            source="app.execution.external_actions",
        )
    return build_action_result(
        status="completed",
        task_id=scope.task_id,
        action_type=request.action_type,
        result_payload={"external_module_result": module_result.to_dict()},
        error_code="",
        error_message="",
        source="app.execution.external_actions",
    )
