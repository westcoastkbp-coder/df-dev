from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from app.policy.policy_gate import ALLOWED_ACTION_TYPES, ALLOWED_EXTERNAL_ACTION_TYPES


ACTION_CONTRACT_SCHEMA_VERSION = "v1"
ACTION_RESULT_SCHEMA_VERSION = "v1"
ACTION_EXECUTION_MODES = {"dry_run", "live"}
ACTION_CONFIRMATION_POLICIES = {"required", "not_required"}
ACTION_RESULT_STATUSES = {"success", "failed", "blocked"}
KNOWN_ACTION_TYPES = frozenset(sorted(ALLOWED_ACTION_TYPES | ALLOWED_EXTERNAL_ACTION_TYPES))
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9._:@-]+$")
_ACTION_REQUIRED_FIELDS = {
    "action_id",
    "action_type",
    "target_type",
    "target_ref",
    "parameters",
    "execution_mode",
    "confirmation_policy",
    "idempotency_key",
    "requested_by",
    "timestamp",
    "schema_version",
}
_ACTION_RESULT_REQUIRED_FIELDS = {
    "action_id",
    "status",
    "result_type",
    "payload",
    "error_code",
    "error_message",
    "timestamp",
    "schema_version",
}
_MAX_STRING_LENGTH = 512
_MAX_MAPPING_KEYS = 32
_MAX_SEQUENCE_ITEMS = 64
_MAX_NESTING_DEPTH = 4
_OPENAI_REQUEST_ACTION_TYPE = "OPENAI_REQUEST"
_BROWSER_ACTION_TYPE = "BROWSER_ACTION"
_EMAIL_ACTION_TYPE = "EMAIL_ACTION"
_PRINT_DOCUMENT_ACTION_TYPE = "PRINT_DOCUMENT"


@dataclass(frozen=True, slots=True)
class ActionContractViolation(ValueError):
    reason: str

    def __str__(self) -> str:
        return self.reason


def _utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_nullable_text(value: object) -> str | None:
    normalized = _normalize_text(value)
    return normalized or None


def _stable_identifier(value: object, *, field_name: str) -> str:
    normalized = _normalize_text(value)
    if not normalized:
        raise ActionContractViolation(f"{field_name} must not be empty")
    if len(normalized) > _MAX_STRING_LENGTH:
        raise ActionContractViolation(f"{field_name} exceeds max length")
    if _IDENTIFIER_PATTERN.fullmatch(normalized) is None:
        raise ActionContractViolation(f"{field_name} must be a stable identifier")
    return normalized


def _bounded_text(value: object, *, field_name: str) -> str:
    normalized = _normalize_text(value)
    if not normalized:
        raise ActionContractViolation(f"{field_name} must not be empty")
    if len(normalized) > _MAX_STRING_LENGTH:
        raise ActionContractViolation(f"{field_name} exceeds max length")
    return normalized


def _validate_timestamp(value: object, *, field_name: str) -> str:
    normalized = _bounded_text(value, field_name=field_name)
    try:
        datetime.fromisoformat(normalized.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError as exc:
        raise ActionContractViolation(f"{field_name} must be a valid ISO-8601 timestamp") from exc
    return normalized


def _validate_known_fields(
    payload: dict[str, Any],
    *,
    required_fields: set[str],
    contract_name: str,
) -> None:
    missing_fields = sorted(required_fields - set(payload))
    if missing_fields:
        raise ActionContractViolation(
            f"{contract_name} missing required fields: {', '.join(missing_fields)}"
        )
    extra_fields = sorted(set(payload) - required_fields)
    if extra_fields:
        raise ActionContractViolation(
            f"{contract_name} contains unsupported fields: {', '.join(extra_fields)}"
        )


def _bounded_json_like(
    value: object,
    *,
    field_name: str,
    depth: int = 0,
) -> object:
    if depth > _MAX_NESTING_DEPTH:
        raise ActionContractViolation(f"{field_name} exceeds max nesting depth")
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ActionContractViolation(f"{field_name} must contain only finite numbers")
        return value
    if isinstance(value, str):
        if len(value) > _MAX_STRING_LENGTH:
            raise ActionContractViolation(f"{field_name} contains an overlong string")
        return value
    if isinstance(value, dict):
        if len(value) > _MAX_MAPPING_KEYS:
            raise ActionContractViolation(f"{field_name} exceeds max object size")
        normalized: dict[str, object] = {}
        for key in sorted(value):
            normalized_key = _stable_identifier(key, field_name=f"{field_name} key")
            normalized[normalized_key] = _bounded_json_like(
                value[key],
                field_name=f"{field_name}.{normalized_key}",
                depth=depth + 1,
            )
        return normalized
    if isinstance(value, (list, tuple)):
        if len(value) > _MAX_SEQUENCE_ITEMS:
            raise ActionContractViolation(f"{field_name} exceeds max array size")
        return [
            _bounded_json_like(item, field_name=f"{field_name}[{index}]", depth=depth + 1)
            for index, item in enumerate(value)
        ]
    raise ActionContractViolation(f"{field_name} must contain only JSON-safe values")


def _validate_action_parameters(
    action_type: str,
    parameters: dict[str, object],
) -> dict[str, object]:
    if action_type == _OPENAI_REQUEST_ACTION_TYPE:
        from app.adapters.openai_adapter import validate_openai_action_parameters

        return validate_openai_action_parameters(parameters)
    if action_type == _BROWSER_ACTION_TYPE:
        from app.adapters.browser_adapter import validate_browser_action_parameters

        return validate_browser_action_parameters(parameters)
    if action_type == _EMAIL_ACTION_TYPE:
        from app.adapters.email_adapter import validate_email_action_parameters

        return validate_email_action_parameters(parameters)
    if action_type == _PRINT_DOCUMENT_ACTION_TYPE:
        from app.adapters.printer_adapter import validate_printer_action_parameters

        return validate_printer_action_parameters(parameters)
    return parameters


def validate_action_contract(payload: object) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise ActionContractViolation("action contract must be a dict")
    _validate_known_fields(payload, required_fields=_ACTION_REQUIRED_FIELDS, contract_name="action contract")

    schema_version = _bounded_text(payload.get("schema_version"), field_name="schema_version")
    if schema_version != ACTION_CONTRACT_SCHEMA_VERSION:
        raise ActionContractViolation(f"unsupported schema_version: {schema_version}")

    action_type = _stable_identifier(payload.get("action_type"), field_name="action_type").upper()
    if action_type not in KNOWN_ACTION_TYPES:
        raise ActionContractViolation(f"unsupported action_type: {action_type}")

    execution_mode = _bounded_text(payload.get("execution_mode"), field_name="execution_mode").lower()
    if execution_mode not in ACTION_EXECUTION_MODES:
        raise ActionContractViolation(f"unsupported execution_mode: {execution_mode}")

    confirmation_policy = _bounded_text(
        payload.get("confirmation_policy"),
        field_name="confirmation_policy",
    ).lower()
    if confirmation_policy not in ACTION_CONFIRMATION_POLICIES:
        raise ActionContractViolation(f"unsupported confirmation_policy: {confirmation_policy}")

    parameters = payload.get("parameters")
    if not isinstance(parameters, dict):
        raise ActionContractViolation("parameters must be a dict")
    normalized_parameters = _bounded_json_like(parameters, field_name="parameters")
    if not isinstance(normalized_parameters, dict):
        raise ActionContractViolation("parameters must be a dict")

    return {
        "action_id": _stable_identifier(payload.get("action_id"), field_name="action_id"),
        "action_type": action_type,
        "target_type": _stable_identifier(payload.get("target_type"), field_name="target_type").lower(),
        "target_ref": _bounded_text(payload.get("target_ref"), field_name="target_ref"),
        "parameters": _validate_action_parameters(action_type, dict(normalized_parameters)),
        "execution_mode": execution_mode,
        "confirmation_policy": confirmation_policy,
        "idempotency_key": _stable_identifier(payload.get("idempotency_key"), field_name="idempotency_key"),
        "requested_by": _bounded_text(payload.get("requested_by"), field_name="requested_by"),
        "timestamp": _validate_timestamp(payload.get("timestamp"), field_name="timestamp"),
        "schema_version": schema_version,
    }


def build_action_contract(
    *,
    action_id: object,
    action_type: object,
    target_type: object,
    target_ref: object,
    parameters: dict[str, object] | None,
    execution_mode: object,
    confirmation_policy: object,
    idempotency_key: object,
    requested_by: object,
    timestamp: object | None = None,
    schema_version: object = ACTION_CONTRACT_SCHEMA_VERSION,
) -> dict[str, object]:
    return validate_action_contract(
        {
            "action_id": action_id,
            "action_type": action_type,
            "target_type": target_type,
            "target_ref": target_ref,
            "parameters": dict(parameters or {}),
            "execution_mode": execution_mode,
            "confirmation_policy": confirmation_policy,
            "idempotency_key": idempotency_key,
            "requested_by": requested_by,
            "timestamp": timestamp or _utc_timestamp(),
            "schema_version": schema_version,
        }
    )


def validate_action_result_contract(payload: object) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise ActionContractViolation("action result contract must be a dict")
    _validate_known_fields(
        payload,
        required_fields=_ACTION_RESULT_REQUIRED_FIELDS,
        contract_name="action result contract",
    )

    schema_version = _bounded_text(payload.get("schema_version"), field_name="schema_version")
    if schema_version != ACTION_RESULT_SCHEMA_VERSION:
        raise ActionContractViolation(f"unsupported schema_version: {schema_version}")

    status = _bounded_text(payload.get("status"), field_name="status").lower()
    if status not in ACTION_RESULT_STATUSES:
        raise ActionContractViolation(f"unsupported action result status: {status}")

    result_payload = payload.get("payload")
    if not isinstance(result_payload, dict):
        raise ActionContractViolation("payload must be a dict")

    error_code = _normalize_nullable_text(payload.get("error_code"))
    error_message = _normalize_nullable_text(payload.get("error_message"))
    if status == "success" and (error_code is not None or error_message is not None):
        raise ActionContractViolation(
            "successful action result must not include error_code or error_message"
        )

    return {
        "action_id": _stable_identifier(payload.get("action_id"), field_name="action_id"),
        "status": status,
        "result_type": _stable_identifier(payload.get("result_type"), field_name="result_type").lower(),
        "payload": _bounded_json_like(result_payload, field_name="payload"),
        "error_code": error_code,
        "error_message": error_message,
        "timestamp": _validate_timestamp(payload.get("timestamp"), field_name="timestamp"),
        "schema_version": schema_version,
    }


def build_action_result_contract(
    *,
    action_id: object,
    status: object,
    result_type: object,
    payload: dict[str, object] | None,
    error_code: object = None,
    error_message: object = None,
    timestamp: object | None = None,
    schema_version: object = ACTION_RESULT_SCHEMA_VERSION,
) -> dict[str, object]:
    return validate_action_result_contract(
        {
            "action_id": action_id,
            "status": status,
            "result_type": result_type,
            "payload": dict(payload or {}),
            "error_code": error_code,
            "error_message": error_message,
            "timestamp": timestamp or _utc_timestamp(),
            "schema_version": schema_version,
        }
    )


def precheck_action_contract(
    payload: object,
    *,
    confirmation_received: bool = False,
    existing_idempotency_keys: set[str] | None = None,
) -> dict[str, object]:
    try:
        normalized_action = validate_action_contract(payload)
    except ActionContractViolation as exc:
        return {
            "allowed": False,
            "status": "blocked",
            "reason": str(exc),
            "action": None,
        }

    idempotency_key = str(normalized_action["idempotency_key"])
    if existing_idempotency_keys and idempotency_key in existing_idempotency_keys:
        return {
            "allowed": False,
            "status": "blocked",
            "reason": f"duplicate idempotency_key: {idempotency_key}",
            "action": normalized_action,
        }

    if normalized_action["confirmation_policy"] == "required" and not confirmation_received:
        return {
            "allowed": False,
            "status": "blocked",
            "reason": f"confirmation required: {normalized_action['action_type']}",
            "action": normalized_action,
        }

    return {
        "allowed": True,
        "status": "allowed",
        "reason": "action contract valid",
        "action": normalized_action,
    }


def serialize_action_contract(payload: object) -> str:
    return json.dumps(
        validate_action_contract(payload),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def serialize_action_result_contract(payload: object) -> str:
    return json.dumps(
        validate_action_result_contract(payload),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
