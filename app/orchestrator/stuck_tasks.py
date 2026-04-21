from __future__ import annotations

from datetime import datetime, timezone

from app.execution.paths import LOGS_DIR, ROOT_DIR
from app.orchestrator.escalation import decide_escalation_action, record_escalation
from app.orchestrator.task_lifecycle import normalize_task_status, set_task_state
from runtime.system_log import write_json_log


STUCK_TASK_LOG_FILE = ROOT_DIR / LOGS_DIR / "stuck_tasks.jsonl"
STUCK_TIMEOUT_SECONDS = {
    "CREATED": 60,
    "VALIDATED": 120,
    "EXECUTING": 300,
    "DEFERRED": 180,
}
STUCK_ACTIVE_STATUSES = frozenset(STUCK_TIMEOUT_SECONDS)


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _parse_timestamp(value: object) -> datetime | None:
    normalized = _normalize_text(value)
    if not normalized:
        return None
    try:
        return datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None


def _last_updated_at(task_data: dict[str, object]) -> str:
    normalized_last_updated_at = _normalize_text(task_data.get("last_updated_at"))
    if normalized_last_updated_at:
        return normalized_last_updated_at
    history = list(task_data.get("history", []) or [])
    if history:
        latest_event = dict(history[-1])
        return _normalize_text(latest_event.get("timestamp"))
    return _normalize_text(task_data.get("created_at"))


def _duration_seconds(*, since: datetime, now_timestamp: datetime) -> int:
    return max(0, int((now_timestamp - since).total_seconds()))


def build_task_stuck_signal(
    task_data: dict[str, object],
    *,
    now_timestamp: str,
) -> dict[str, object] | None:
    normalized_status = normalize_task_status(task_data.get("status"))
    if normalized_status not in STUCK_ACTIVE_STATUSES:
        return None
    parsed_now = _parse_timestamp(now_timestamp)
    parsed_last_updated_at = _parse_timestamp(_last_updated_at(task_data))
    if parsed_now is None or parsed_last_updated_at is None:
        return None
    duration = _duration_seconds(since=parsed_last_updated_at, now_timestamp=parsed_now)
    threshold_seconds = STUCK_TIMEOUT_SECONDS[normalized_status]
    if duration < threshold_seconds:
        return None
    return {
        "status": "task_stuck",
        "task_id": _normalize_text(task_data.get("task_id")),
        "state": normalized_status,
        "duration": duration,
        "threshold_seconds": threshold_seconds,
    }


def detect_stuck_tasks(
    tasks: list[dict[str, object]],
    *,
    now_timestamp: str,
) -> list[dict[str, object]]:
    detected = []
    for task_data in sorted(
        (dict(task) for task in tasks),
        key=lambda item: (
            _normalize_text(item.get("task_id")),
            _normalize_text(item.get("created_at")),
        ),
    ):
        signal = build_task_stuck_signal(task_data, now_timestamp=now_timestamp)
        if signal is not None:
            detected.append(signal)
    return detected


def log_stuck_task(signal: dict[str, object]) -> dict[str, object]:
    return write_json_log(
        STUCK_TASK_LOG_FILE,
        task_id=signal.get("task_id", ""),
        event_type="task_stuck",
        status=signal.get("status", "task_stuck"),
        details=signal,
    )


def enforce_stuck_tasks(
    tasks: list[dict[str, object]],
    *,
    now_timestamp: str,
    persist,
) -> list[dict[str, object]]:
    enforced_signals: list[dict[str, object]] = []
    for task_data in sorted(
        (dict(task) for task in tasks),
        key=lambda item: (
            _normalize_text(item.get("task_id")),
            _normalize_text(item.get("created_at")),
        ),
    ):
        signal = build_task_stuck_signal(task_data, now_timestamp=now_timestamp)
        if signal is None:
            continue
        task_data["result"] = dict(signal)
        task_data["error"] = "task exceeded deterministic stuck threshold"
        set_task_state(
            task_data,
            "FAILED",
            timestamp=now_timestamp,
            details=dict(signal),
        )
        task_data["result"] = dict(signal)
        escalation_decision = decide_escalation_action(task_data, signal=signal)
        if escalation_decision.get("action") == "escalate" and isinstance(
            escalation_decision.get("signal"), dict
        ):
            record_escalation(task_data, escalation_decision["signal"])
        persist(task_data)
        log_stuck_task(signal)
        enforced_signals.append(signal)
    return enforced_signals
