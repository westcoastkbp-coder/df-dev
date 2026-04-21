from __future__ import annotations

from collections.abc import Mapping

from app.execution.paths import LOGS_DIR, ROOT_DIR
from runtime.system_log import write_json_log


ESCALATION_LOG_FILE = ROOT_DIR / LOGS_DIR / "escalations.jsonl"
EXTERNAL_MODULE_FAILURE_STATUSES = {
    "external_module_failed",
    "external_module_timeout",
    "external_module_unavailable",
    "external_module_invalid_result",
}


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_priority(value: object) -> str:
    normalized = _normalize_text(value).upper()
    if normalized in {"CRITICAL", "P0"}:
        return "CRITICAL"
    if normalized in {"HIGH", "P1"}:
        return "HIGH"
    return normalized or "NORMAL"


def _task_priority(task_data: dict[str, object]) -> str:
    payload = dict(task_data.get("payload", {}) or {})
    return (
        _normalize_priority(task_data.get("priority"))
        or _normalize_priority(payload.get("priority"))
        or _normalize_priority(task_data.get("mvp_priority"))
        or _normalize_priority(payload.get("mvp_priority"))
        or "NORMAL"
    )


def _is_critical_task(task_data: dict[str, object]) -> bool:
    return _task_priority(task_data) == "CRITICAL"


def _history_event_count(task_data: dict[str, object], event_name: str) -> int:
    normalized_event_name = _normalize_text(event_name)
    return sum(
        1
        for item in list(task_data.get("history", []) or [])
        if _normalize_text(dict(item).get("event")) == normalized_event_name
    )


def _result_status(
    task_data: dict[str, object], signal: dict[str, object] | None = None
) -> str:
    if signal is not None:
        normalized_signal_status = _normalize_text(signal.get("status"))
        if normalized_signal_status:
            return normalized_signal_status
    result = task_data.get("result")
    if isinstance(result, Mapping):
        return _normalize_text(result.get("status"))
    return ""


def build_escalation_signal(
    *,
    task_id: object,
    reason: object,
    severity: object,
) -> dict[str, object]:
    return {
        "status": "escalation_required",
        "task_id": _normalize_text(task_id),
        "reason": _normalize_text(reason),
        "severity": _normalize_text(severity).lower() or "medium",
    }


def decide_escalation_action(
    task_data: dict[str, object],
    *,
    reason: object = "",
    signal: dict[str, object] | None = None,
) -> dict[str, object]:
    normalized_reason = _normalize_text(reason)
    result_status = _result_status(task_data, signal=signal)

    if result_status == "invalid_action_result" or normalized_reason.startswith(
        "invalid action result:"
    ):
        escalation_signal = build_escalation_signal(
            task_id=task_data.get("task_id"),
            reason="invalid_action_result",
            severity="critical",
        )
        return {"action": "escalate", "signal": escalation_signal}

    if (
        result_status in EXTERNAL_MODULE_FAILURE_STATUSES
        or normalized_reason.startswith("external module")
    ):
        escalation_signal = build_escalation_signal(
            task_id=task_data.get("task_id"),
            reason="external_module_failure",
            severity="high",
        )
        return {"action": "escalate", "signal": escalation_signal}

    if (
        result_status == "task_stuck"
        and _history_event_count(task_data, "execution_failed") >= 1
    ):
        escalation_signal = build_escalation_signal(
            task_id=task_data.get("task_id"),
            reason="task_stuck_after_retry",
            severity="high",
        )
        return {"action": "escalate", "signal": escalation_signal}

    if normalized_reason.startswith("policy gate blocked") and _is_critical_task(
        task_data
    ):
        escalation_signal = build_escalation_signal(
            task_id=task_data.get("task_id"),
            reason="policy_rejection_on_critical_task",
            severity="high",
        )
        return {"action": "escalate", "signal": escalation_signal}

    if _history_event_count(task_data, "execution_failed") >= 2:
        escalation_signal = build_escalation_signal(
            task_id=task_data.get("task_id"),
            reason="repeated_task_failure",
            severity="medium",
        )
        return {"action": "escalate", "signal": escalation_signal}

    return {"action": "fail", "signal": None}


def record_escalation(
    task_data: dict[str, object], signal: dict[str, object]
) -> dict[str, object]:
    verdict = dict(task_data.get("runtime_verdict", {}) or {})
    verdict["escalation_signal"] = dict(signal)
    task_data["runtime_verdict"] = verdict
    write_json_log(
        ESCALATION_LOG_FILE,
        task_id=signal.get("task_id", ""),
        event_type="escalation_required",
        status=signal.get("status", "escalation_required"),
        details=signal,
    )
    return dict(signal)
