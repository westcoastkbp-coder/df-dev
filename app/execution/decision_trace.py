from __future__ import annotations

from collections.abc import Mapping, Sequence

from app.execution.vendor_router import DEFAULT_VENDOR, normalize_vendor


DECISION_TRACE_REQUIRED_FIELDS = {
    "reason",
    "context_used",
    "action_type",
    "policy_result",
    "confidence",
}
DECISION_TRACE_CONFIDENCE = {"low", "medium", "high"}
TRACE_MAX_CHARS = 180


def _normalize_text(value: object) -> str:
    return " ".join(str(value or "").strip().split())


def _short_text(
    value: object, *, default: str = "", max_chars: int = TRACE_MAX_CHARS
) -> str:
    normalized = _normalize_text(value) or default
    if len(normalized) <= max_chars:
        return normalized
    return normalized[: max_chars - 3].rstrip() + "..."


def _normalize_mapping(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _normalize_sequence(value: object) -> list[object]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return list(value)
    return []


def _first_non_empty(*values: object) -> str:
    for value in values:
        normalized = _normalize_text(value)
        if normalized:
            return normalized
    return ""


def infer_policy_result(
    *,
    status: object,
    error_code: object = "",
    error_message: object = "",
    explicit: object = "",
) -> str:
    normalized_explicit = _normalize_text(explicit)
    if normalized_explicit:
        return _short_text(normalized_explicit)

    normalized_status = _normalize_text(status).lower()
    detail = _first_non_empty(error_message, error_code)
    if normalized_status in {"policy_blocked", "execution_boundary_violation"}:
        return _short_text(f"blocked: {detail or normalized_status}")
    if normalized_status in {"failed", "error", "invalid_action_result"}:
        return _short_text(f"allowed_then_failed: {detail or normalized_status}")
    return _short_text(f"allowed: {detail or 'policy gate passed'}")


def infer_confidence(*, status: object, explicit: object = "") -> str:
    normalized_explicit = _normalize_text(explicit).lower()
    if normalized_explicit in DECISION_TRACE_CONFIDENCE:
        return normalized_explicit

    normalized_status = _normalize_text(status).lower()
    if normalized_status in {"completed", "success"}:
        return "high"
    if normalized_status in {
        "failed",
        "error",
        "policy_blocked",
        "execution_boundary_violation",
    }:
        return "medium"
    return "low"


def summarize_context_reference(
    *,
    task_id: object = "",
    intent: object = "",
    payload: object = None,
    source: object = "",
    context_summary: object = None,
    command_name: object = "",
    mode: object = "",
) -> str:
    normalized_payload = _normalize_mapping(payload)
    normalized_summary = _normalize_mapping(context_summary)
    parts: list[str] = []

    normalized_command = _normalize_text(command_name)
    if normalized_command:
        parts.append(f"command={normalized_command}")

    normalized_mode = _normalize_text(mode)
    if normalized_mode:
        parts.append(f"mode={normalized_mode}")

    normalized_task_id = _normalize_text(task_id)
    if normalized_task_id:
        parts.append(f"task={normalized_task_id}")

    normalized_intent = _normalize_text(intent)
    if normalized_intent:
        parts.append(f"intent={normalized_intent}")

    path = _first_non_empty(
        normalized_payload.get("path"),
        normalized_payload.get("output_path"),
        normalized_payload.get("filename"),
    )
    if path:
        parts.append(f"path={path}")

    destination = _first_non_empty(
        normalized_payload.get("destination"),
        normalized_payload.get("module_type"),
        normalized_payload.get("operation"),
    )
    if destination:
        parts.append(f"target={destination}")

    steps = _normalize_sequence(normalized_payload.get("steps"))
    if steps:
        parts.append(f"steps={len(steps)}")

    current_stage = _normalize_mapping(normalized_summary.get("current_stage"))
    stage_phase = _normalize_text(current_stage.get("phase"))
    if stage_phase:
        parts.append(f"stage={stage_phase}")

    active_flows = _normalize_sequence(normalized_summary.get("active_flows"))
    if active_flows:
        parts.append(f"flow={_normalize_text(active_flows[0])}")

    normalized_source = _normalize_text(source)
    if normalized_source and len(parts) < 3:
        parts.append(f"source={normalized_source}")

    if not parts:
        return "bounded execution context"
    return _short_text("; ".join(parts[:4]))


def build_decision_trace(
    *,
    reason: object,
    context_used: object,
    action_type: object,
    policy_result: object,
    confidence: object = "medium",
    vendor: object = DEFAULT_VENDOR,
) -> dict[str, str]:
    normalized_confidence = _normalize_text(confidence).lower()
    if normalized_confidence not in DECISION_TRACE_CONFIDENCE:
        normalized_confidence = "medium"
    return {
        "reason": _short_text(reason, default="execution decision recorded"),
        "context_used": _short_text(context_used, default="bounded execution context"),
        "action_type": _short_text(action_type, default="UNKNOWN_ACTION"),
        "policy_result": _short_text(
            policy_result, default="allowed: policy gate passed"
        ),
        "confidence": normalized_confidence,
        "vendor": normalize_vendor(vendor),
    }


def ensure_decision_trace(
    trace: object,
    *,
    reason: object,
    context_used: object,
    action_type: object,
    policy_result: object,
    confidence: object = "medium",
    vendor: object = DEFAULT_VENDOR,
) -> dict[str, str]:
    if isinstance(trace, Mapping):
        candidate = dict(trace)
        missing_fields = sorted(DECISION_TRACE_REQUIRED_FIELDS - set(candidate))
        if missing_fields:
            raise ValueError(
                "decision_trace missing required fields: " + ", ".join(missing_fields)
            )
        normalized_trace = build_decision_trace(
            reason=candidate.get("reason"),
            context_used=candidate.get("context_used"),
            action_type=candidate.get("action_type"),
            policy_result=candidate.get("policy_result"),
            confidence=candidate.get("confidence"),
            vendor=candidate.get("vendor", vendor),
        )
        return normalized_trace

    return build_decision_trace(
        reason=reason,
        context_used=context_used,
        action_type=action_type,
        policy_result=policy_result,
        confidence=confidence,
        vendor=vendor,
    )
