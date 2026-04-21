from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone

from app.execution.decision_trace import ensure_decision_trace, infer_confidence, infer_policy_result, summarize_context_reference
from app.execution.paths import LOGS_DIR, ROOT_DIR


ACTION_RESULT_VIOLATIONS_LOG = ROOT_DIR / LOGS_DIR / "action_result_violations.jsonl"
REQUIRED_ACTION_RESULT_FIELDS = {
    "status",
    "action_type",
    "result_payload",
    "error_code",
    "error_message",
    "source",
    "decision_trace",
}
OPTIONAL_ACTION_RESULT_FIELDS = {
    "task_id",
    "diagnostic_message",
}
ACTION_RESULT_FIELDS = REQUIRED_ACTION_RESULT_FIELDS | OPTIONAL_ACTION_RESULT_FIELDS
ALLOWED_ACTION_RESULT_STATUSES = {
    "success",
    "completed",
    "failed",
    "partial",
    "unknown",
    "error",
    "policy_blocked",
    "execution_boundary_violation",
    "invalid_action_result",
}


@dataclass(frozen=True, slots=True)
class ActionResultViolation(ValueError):
    reason: str

    def __str__(self) -> str:
        return self.reason


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_mapping(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _is_json_like(value: object) -> bool:
    if value is None or isinstance(value, (str, int, float, bool)):
        return True
    if isinstance(value, Mapping):
        return all(
            _normalize_text(key) != "" and _is_json_like(item)
            for key, item in value.items()
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return all(_is_json_like(item) for item in value)
    return False


def _clone_json_like(value: object) -> object:
    if isinstance(value, Mapping):
        return {
            _normalize_text(key): _clone_json_like(item)
            for key, item in value.items()
            if _normalize_text(key)
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_clone_json_like(item) for item in value]
    return value


def build_action_result(
    *,
    status: object,
    action_type: object,
    result_payload: object,
    error_code: object,
    error_message: object,
    source: object,
    task_id: object = "",
    diagnostic_message: object = "",
    decision_trace: object = None,
) -> dict[str, object]:
    normalized_status = _normalize_text(status).lower()
    normalized_action_type = _normalize_text(action_type)
    normalized_error_code = _normalize_text(error_code)
    normalized_error_message = _normalize_text(error_message)
    normalized_source = _normalize_text(source)
    payload = {
        "status": normalized_status,
        "action_type": normalized_action_type,
        "result_payload": _clone_json_like(result_payload),
        "error_code": normalized_error_code,
        "error_message": normalized_error_message,
        "source": normalized_source,
    }
    normalized_task_id = _normalize_text(task_id)
    if normalized_task_id:
        payload["task_id"] = normalized_task_id
    normalized_diagnostic = _normalize_text(diagnostic_message)
    if normalized_diagnostic:
        payload["diagnostic_message"] = normalized_diagnostic
    try:
        from app.execution.execution_boundary import current_execution_scope

        scope = current_execution_scope()
    except Exception:
        scope = None
    payload["decision_trace"] = ensure_decision_trace(
        decision_trace,
        reason=(
            normalized_diagnostic
            or normalized_error_message
            or _normalize_text(_normalize_mapping(result_payload).get("result_type"))
            or f"{normalized_action_type or 'UNKNOWN_ACTION'} {normalized_status or 'observed'}"
        ),
        context_used=summarize_context_reference(
            task_id=normalized_task_id or getattr(scope, "task_id", ""),
            intent=getattr(scope, "intent", ""),
            payload=result_payload,
            source=normalized_source,
        ),
        action_type=normalized_action_type,
        policy_result=infer_policy_result(
            status=normalized_status,
            error_code=normalized_error_code,
            error_message=normalized_error_message,
        ),
        confidence=infer_confidence(status=normalized_status),
    )
    return validate_action_result(payload)


def build_invalid_action_result_signal(
    *,
    task_id: object,
    action_type: object,
    reason: object,
    source: object,
) -> dict[str, object]:
    signal = {
        "status": "invalid_action_result",
        "task_id": _normalize_text(task_id),
        "action_type": _normalize_text(action_type),
        "reason": _normalize_text(reason),
        "source": _normalize_text(source),
        "decision_trace": ensure_decision_trace(
            None,
            reason=_normalize_text(reason) or "invalid action result",
            context_used=summarize_context_reference(
                task_id=task_id,
                source=source,
            ),
            action_type=_normalize_text(action_type),
            policy_result="blocked: invalid action result contract",
            confidence="high",
        ),
    }
    return signal


def log_invalid_action_result_violation(
    *,
    task_id: object,
    action_type: object,
    reason: object,
    source: object,
) -> dict[str, object]:
    signal = build_invalid_action_result_signal(
        task_id=task_id,
        action_type=action_type,
        reason=reason,
        source=source,
    )
    entry = {
        "timestamp": _timestamp(),
        **signal,
    }
    ACTION_RESULT_VIOLATIONS_LOG.parent.mkdir(parents=True, exist_ok=True)
    with ACTION_RESULT_VIOLATIONS_LOG.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=True, separators=(",", ":")) + "\n")
    return signal


def validate_action_result(
    payload: object,
    *,
    expected_task_id: object = "",
) -> dict[str, object]:
    if not isinstance(payload, Mapping):
        raise ActionResultViolation("action result must be a dict")
    normalized_payload = dict(payload)
    missing_fields = sorted(REQUIRED_ACTION_RESULT_FIELDS - set(normalized_payload))
    if missing_fields:
        raise ActionResultViolation(
            "action result missing required fields: " + ", ".join(missing_fields)
        )
    extra_fields = sorted(set(normalized_payload) - ACTION_RESULT_FIELDS)
    if extra_fields:
        raise ActionResultViolation(
            "action result contains unsupported fields: " + ", ".join(extra_fields)
        )

    status = _normalize_text(normalized_payload.get("status")).lower()
    if status not in ALLOWED_ACTION_RESULT_STATUSES:
        raise ActionResultViolation(f"unsupported action result status: {status or '(empty)'}")

    action_type = _normalize_text(normalized_payload.get("action_type"))
    if not action_type:
        raise ActionResultViolation("action_type must not be empty")

    source = _normalize_text(normalized_payload.get("source"))
    if not source:
        raise ActionResultViolation("source must not be empty")

    if not isinstance(normalized_payload.get("result_payload"), Mapping):
        raise ActionResultViolation("result_payload must be a dict")
    if not _is_json_like(normalized_payload.get("result_payload")):
        raise ActionResultViolation("result_payload must contain only structured JSON-like values")
    decision_trace = normalized_payload.get("decision_trace")
    if not isinstance(decision_trace, Mapping):
        raise ActionResultViolation("decision_trace must be a dict")

    error_code = _normalize_text(normalized_payload.get("error_code"))
    error_message = _normalize_text(normalized_payload.get("error_message"))
    if status in {"completed", "success"} and (error_code or error_message):
        raise ActionResultViolation(
            "successful action result must not include error_code or error_message"
        )

    normalized_task_id = _normalize_text(normalized_payload.get("task_id"))
    normalized_expected_task_id = _normalize_text(expected_task_id)
    if normalized_expected_task_id and normalized_task_id != normalized_expected_task_id:
        raise ActionResultViolation("task_id does not match execution task")

    try:
        normalized_trace = ensure_decision_trace(
            decision_trace,
            reason="",
            context_used="",
            action_type=action_type,
            policy_result="",
            confidence="medium",
        )
    except ValueError as exc:
        raise ActionResultViolation(str(exc)) from exc

    validated = {
        "status": status,
        "action_type": action_type,
        "result_payload": _clone_json_like(normalized_payload.get("result_payload")),
        "error_code": error_code,
        "error_message": error_message,
        "source": source,
        "decision_trace": normalized_trace,
    }
    if normalized_task_id:
        validated["task_id"] = normalized_task_id
    diagnostic_message = _normalize_text(normalized_payload.get("diagnostic_message"))
    if diagnostic_message:
        validated["diagnostic_message"] = diagnostic_message
    return validated
