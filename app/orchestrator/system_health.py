from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path

from app.execution.paths import LOGS_DIR, ROOT_DIR
from app.orchestrator.escalation import ESCALATION_LOG_FILE, record_escalation
from app.orchestrator.stuck_tasks import detect_stuck_tasks
from runtime.system_log import write_json_log


SYSTEM_HEALTH_LOG_FILE = ROOT_DIR / LOGS_DIR / "system_health.jsonl"
SYSTEM_HEALTH_TASK_ID = "SYSTEM-HEALTH-GUARD"
DEGRADED_NO_SUCCESS_SECONDS = 300
CRITICAL_NO_SUCCESS_SECONDS = 900
DEGRADED_FAILURE_COUNT = 2
CRITICAL_FAILURE_COUNT = 5
DEGRADED_STUCK_COUNT = 1
CRITICAL_STUCK_COUNT = 3
DEGRADED_ESCALATION_COUNT = 1
CRITICAL_ESCALATION_COUNT = 3
DEGRADED_LATENCY_SECONDS = 5.0
CRITICAL_LATENCY_SECONDS = 15.0
DEGRADED_RESPONSIVENESS_SECONDS = 120
CRITICAL_RESPONSIVENESS_SECONDS = 600
ESCALATION_LOOKBACK_SECONDS = 900


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _parse_timestamp(value: object) -> datetime | None:
    normalized = _normalize_text(value)
    if not normalized:
        return None
    try:
        return datetime.fromisoformat(normalized.replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
    except ValueError:
        return None


def _seconds_between(*, earlier: datetime | None, later: datetime | None) -> int | None:
    if earlier is None or later is None:
        return None
    return max(0, int((later - earlier).total_seconds()))


def _task_mapping_sequence(tasks: object) -> list[dict[str, object]]:
    if not isinstance(tasks, Sequence) or isinstance(tasks, (str, bytes, bytearray)):
        return []
    normalized: list[dict[str, object]] = []
    for item in tasks:
        if isinstance(item, Mapping):
            normalized.append(dict(item))
    return normalized


def _latest_task_timestamp(task_data: Mapping[str, object]) -> datetime | None:
    candidates = [
        _parse_timestamp(task_data.get("last_updated_at")),
        _parse_timestamp(task_data.get("completed_at")),
        _parse_timestamp(task_data.get("failed_at")),
        _parse_timestamp(task_data.get("started_at")),
        _parse_timestamp(task_data.get("created_at")),
    ]
    history = list(task_data.get("history", []) or [])
    for entry in history:
        if isinstance(entry, Mapping):
            candidates.append(_parse_timestamp(entry.get("timestamp")))
    resolved = [candidate for candidate in candidates if candidate is not None]
    return max(resolved) if resolved else None


def _last_success_timestamp(tasks: list[dict[str, object]]) -> str:
    timestamps = [
        timestamp
        for task in tasks
        for timestamp in (
            _parse_timestamp(task.get("completed_at")),
            _latest_task_timestamp(task)
            if _normalize_text(task.get("status")).upper() == "COMPLETED"
            else None,
        )
        if timestamp is not None
        and _normalize_text(task.get("status")).upper() == "COMPLETED"
    ]
    if not timestamps:
        return ""
    return max(timestamps).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _failed_task_count(tasks: list[dict[str, object]]) -> int:
    return sum(
        1 for task in tasks if _normalize_text(task.get("status")).upper() == "FAILED"
    )


def _execution_latency_seconds(tasks: list[dict[str, object]]) -> float:
    durations: list[float] = []
    for task in tasks:
        started_at = _parse_timestamp(task.get("started_at"))
        finished_at = _parse_timestamp(task.get("completed_at")) or _parse_timestamp(
            task.get("failed_at")
        )
        if started_at is not None and finished_at is not None:
            durations.append(max(0.0, (finished_at - started_at).total_seconds()))
            continue
        offload_latency = task.get("offload_latency")
        if isinstance(offload_latency, (int, float)) and float(offload_latency) >= 0.0:
            durations.append(float(offload_latency))
    if not durations:
        return 0.0
    return round(sum(durations) / len(durations), 4)


def _system_responsiveness_seconds(
    tasks: list[dict[str, object]], *, now_timestamp: str
) -> int:
    now_dt = _parse_timestamp(now_timestamp)
    latest_activity = max(
        (_latest_task_timestamp(task) for task in tasks),
        default=None,
    )
    delta = _seconds_between(earlier=latest_activity, later=now_dt)
    return 0 if delta is None else delta


def _recent_escalation_count(
    *, now_timestamp: str, log_file: Path | None = None
) -> int:
    target = Path(log_file) if log_file is not None else ESCALATION_LOG_FILE
    now_dt = _parse_timestamp(now_timestamp)
    if now_dt is None or not target.exists():
        return 0
    count = 0
    for line in target.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        entry_timestamp = _parse_timestamp(payload.get("timestamp"))
        age_seconds = _seconds_between(earlier=entry_timestamp, later=now_dt)
        if age_seconds is None:
            continue
        if age_seconds <= ESCALATION_LOOKBACK_SECONDS:
            count += 1
    return count


def build_system_health_signal(
    *,
    state: str,
    reason: str,
    metrics: Mapping[str, object],
) -> dict[str, object]:
    return {
        "status": "system_health",
        "state": _normalize_text(state).lower(),
        "reason": _normalize_text(reason),
        "metrics": dict(metrics),
    }


def evaluate_system_health(
    tasks: object,
    *,
    now_timestamp: str,
    escalation_log_file: Path | None = None,
) -> dict[str, object]:
    normalized_tasks = _task_mapping_sequence(tasks)
    last_successful_execution_timestamp = _last_success_timestamp(normalized_tasks)
    last_success_dt = _parse_timestamp(last_successful_execution_timestamp)
    now_dt = _parse_timestamp(now_timestamp)
    seconds_since_last_success = _seconds_between(earlier=last_success_dt, later=now_dt)
    failed_tasks_count = _failed_task_count(normalized_tasks)
    stuck_tasks_count = len(
        detect_stuck_tasks(normalized_tasks, now_timestamp=now_timestamp)
    )
    execution_latency_seconds = _execution_latency_seconds(normalized_tasks)
    system_responsiveness_seconds = _system_responsiveness_seconds(
        normalized_tasks,
        now_timestamp=now_timestamp,
    )
    escalation_frequency = _recent_escalation_count(
        now_timestamp=now_timestamp,
        log_file=escalation_log_file,
    )
    metrics = {
        "last_successful_execution_timestamp": last_successful_execution_timestamp,
        "failed_tasks_count": failed_tasks_count,
        "stuck_tasks_count": stuck_tasks_count,
        "execution_latency_seconds": execution_latency_seconds,
        "system_responsiveness_seconds": system_responsiveness_seconds,
        "escalation_frequency": escalation_frequency,
        "seconds_since_last_success": 0
        if seconds_since_last_success is None
        else seconds_since_last_success,
    }

    critical_reasons: list[str] = []
    degraded_reasons: list[str] = []
    if (
        seconds_since_last_success is not None
        and seconds_since_last_success >= CRITICAL_NO_SUCCESS_SECONDS
    ):
        critical_reasons.append("no_success_within_critical_threshold")
    elif (
        seconds_since_last_success is not None
        and seconds_since_last_success >= DEGRADED_NO_SUCCESS_SECONDS
    ):
        degraded_reasons.append("no_success_within_degraded_threshold")

    if failed_tasks_count >= CRITICAL_FAILURE_COUNT:
        critical_reasons.append("repeated_failures")
    elif failed_tasks_count >= DEGRADED_FAILURE_COUNT:
        degraded_reasons.append("repeated_failures")

    if stuck_tasks_count >= CRITICAL_STUCK_COUNT:
        critical_reasons.append("repeated_stuck_tasks")
    elif stuck_tasks_count >= DEGRADED_STUCK_COUNT:
        degraded_reasons.append("repeated_stuck_tasks")

    if escalation_frequency >= CRITICAL_ESCALATION_COUNT:
        critical_reasons.append("escalation_frequency_high")
    elif escalation_frequency >= DEGRADED_ESCALATION_COUNT:
        degraded_reasons.append("escalation_frequency_high")

    if execution_latency_seconds >= CRITICAL_LATENCY_SECONDS:
        critical_reasons.append("execution_latency_high")
    elif execution_latency_seconds >= DEGRADED_LATENCY_SECONDS:
        degraded_reasons.append("execution_latency_high")

    if system_responsiveness_seconds >= CRITICAL_RESPONSIVENESS_SECONDS:
        critical_reasons.append("system_responsiveness_low")
    elif system_responsiveness_seconds >= DEGRADED_RESPONSIVENESS_SECONDS:
        degraded_reasons.append("system_responsiveness_low")

    if critical_reasons:
        return build_system_health_signal(
            state="critical",
            reason="; ".join(sorted(set(critical_reasons))),
            metrics=metrics,
        )
    if degraded_reasons:
        return build_system_health_signal(
            state="degraded",
            reason="; ".join(sorted(set(degraded_reasons))),
            metrics=metrics,
        )
    return build_system_health_signal(
        state="healthy",
        reason="system operating within deterministic thresholds",
        metrics=metrics,
    )


def log_system_health(
    signal: Mapping[str, object],
    *,
    phase: str,
    task_id: object = "",
    log_file: Path | None = None,
) -> dict[str, object]:
    target = Path(log_file) if log_file is not None else SYSTEM_HEALTH_LOG_FILE
    details = dict(signal)
    details["phase"] = _normalize_text(phase)
    return write_json_log(
        target,
        task_id=_normalize_text(task_id) or SYSTEM_HEALTH_TASK_ID,
        event_type="system_health",
        status=_normalize_text(signal.get("state")) or "healthy",
        details=details,
    )


def enforce_system_health_response(
    signal: Mapping[str, object],
    *,
    task_data: dict[str, object] | None = None,
) -> dict[str, object] | None:
    if _normalize_text(signal.get("state")).lower() != "critical":
        return None
    target_task = dict(task_data or {})
    target_task.setdefault("task_id", SYSTEM_HEALTH_TASK_ID)
    return record_escalation(
        target_task,
        {
            "status": "escalation_required",
            "task_id": _normalize_text(target_task.get("task_id"))
            or SYSTEM_HEALTH_TASK_ID,
            "reason": "system_health_critical",
            "severity": "critical",
        },
    )


def assess_system_health(
    tasks: object,
    *,
    now_timestamp: str,
    phase: str,
    task_data: dict[str, object] | None = None,
    escalation_log_file: Path | None = None,
    health_log_file: Path | None = None,
) -> dict[str, object]:
    signal = evaluate_system_health(
        tasks,
        now_timestamp=now_timestamp,
        escalation_log_file=escalation_log_file,
    )
    log_system_health(
        signal,
        phase=phase,
        task_id=(task_data or {}).get("task_id", ""),
        log_file=health_log_file,
    )
    enforce_system_health_response(signal, task_data=task_data)
    return signal
