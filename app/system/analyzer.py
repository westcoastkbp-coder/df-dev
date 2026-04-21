from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path

from app.config.hybrid_runtime import HybridRuntimeConfig, load_runtime_config
from app.execution.paths import ROOT_DIR as DEFAULT_ROOT_DIR


FAILURE_THRESHOLD = 2
DELAY_THRESHOLD = 2
HIGH_DELAY_SECONDS = 300.0
STALE_FLOW_SECONDS = 600
TERMINAL_STATUSES = {"COMPLETED", "FAILED", "CANCELLED"}
ACTIVE_STATUSES = {"CREATED", "VALIDATED", "EXECUTING", "DEFERRED", "AWAITING_APPROVAL"}
FAILURE_EVENT_TYPES = {"execution_failed", "task_failed", "workflow_failed"}
DELAY_EVENT_TYPES = {"execution_deferred", "task_stuck", "execution_delayed"}
START_EVENT_TYPES = {
    "execution_started",
    "workflow_started",
    "offload_started",
    "step_started",
}
TERMINAL_EVENT_TYPES = {
    "execution_completed",
    "execution_failed",
    "workflow_completed",
    "workflow_failed",
    "offload_completed",
}
SEVERITY_SCORES = {"low": 1, "medium": 2, "high": 3}
IMPACT_SCORES = {"low": 1, "medium": 2, "high": 3}
LOW_PRIORITY = "low"
MEDIUM_PRIORITY = "medium"
HIGH_PRIORITY = "high"
PRIORITY_URGENCY = {
    LOW_PRIORITY: "delayed",
    MEDIUM_PRIORITY: "normal",
    HIGH_PRIORITY: "immediate",
}


def _current_config(
    *,
    root_dir: Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> HybridRuntimeConfig:
    return load_runtime_config(
        root_dir=root_dir or DEFAULT_ROOT_DIR,
        environ=None if environ is None else dict(environ),
    )


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_frequency(value: object) -> int:
    if isinstance(value, bool):
        return 1
    if isinstance(value, int):
        return max(1, value)
    text = _normalize_text(value)
    if not text:
        return 1
    try:
        return max(1, int(text))
    except ValueError:
        return 1


def _frequency_score(frequency: int) -> int:
    if frequency >= 4:
        return 3
    if frequency >= 2:
        return 2
    return 1


def _priority_level_from_score(priority_score: int) -> str:
    if priority_score >= 7:
        return HIGH_PRIORITY
    if priority_score >= 5:
        return MEDIUM_PRIORITY
    return LOW_PRIORITY


def build_gap_priority(
    *,
    severity: object,
    impact_score: object,
    frequency: object,
) -> dict[str, object]:
    normalized_severity = _normalize_text(severity).lower() or LOW_PRIORITY
    normalized_impact_score = _normalize_text(impact_score).lower() or LOW_PRIORITY
    normalized_frequency = _normalize_frequency(frequency)
    severity_score = SEVERITY_SCORES.get(
        normalized_severity, SEVERITY_SCORES[LOW_PRIORITY]
    )
    impact_value = IMPACT_SCORES.get(
        normalized_impact_score, IMPACT_SCORES[LOW_PRIORITY]
    )
    priority_score = (
        severity_score + _frequency_score(normalized_frequency) + impact_value
    )
    priority_level = _priority_level_from_score(priority_score)
    return {
        "severity": normalized_severity,
        "impact_score": normalized_impact_score,
        "frequency": normalized_frequency,
        "urgency": PRIORITY_URGENCY[priority_level],
        "priority_score": priority_score,
        "priority_level": priority_level,
    }


def ensure_gap_priority_metadata(gap: Mapping[str, object]) -> dict[str, object]:
    normalized_gap = dict(gap)
    priority = build_gap_priority(
        severity=normalized_gap.get("severity"),
        impact_score=normalized_gap.get("impact_score") or LOW_PRIORITY,
        frequency=normalized_gap.get("frequency") or 1,
    )
    normalized_gap.update(priority)
    return normalized_gap


def _parse_json_object(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _parse_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    entries: list[dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            entries.append(dict(payload))
    return entries


def _task_contexts(active_threads_dir: Path) -> list[dict[str, object]]:
    if not active_threads_dir.exists():
        return []
    contexts: list[dict[str, object]] = []
    for path in sorted(active_threads_dir.glob("task-*.json")):
        payload = _parse_json_object(path)
        if payload:
            value = _context_value(payload)
            contexts.append(value or payload)
    return contexts


def _context_value(payload: Mapping[str, object]) -> dict[str, object]:
    value = payload.get("value")
    return dict(value) if isinstance(value, Mapping) else {}


def _event_payload(record: Mapping[str, object]) -> dict[str, object]:
    payload = record.get("payload")
    return dict(payload) if isinstance(payload, Mapping) else {}


def _task_id(record: Mapping[str, object]) -> str:
    direct = _normalize_text(record.get("task_id"))
    if direct:
        return direct
    payload_task_id = _normalize_text(_event_payload(record).get("task_id"))
    if payload_task_id:
        return payload_task_id
    return _normalize_text(record.get("id"))


def _task_summary(task_context: Mapping[str, object]) -> str:
    summary = _normalize_text(task_context.get("summary"))
    if summary:
        return summary
    payload = task_context.get("payload")
    if isinstance(payload, Mapping):
        for key in ("summary", "text", "intent"):
            value = _normalize_text(payload.get(key))
            if value:
                return value
    return (
        _normalize_text(task_context.get("intent")) or _task_id(task_context) or "task"
    )


def _task_signature(task_context: Mapping[str, object]) -> str:
    payload = task_context.get("payload")
    if isinstance(payload, Mapping):
        for key in (
            "workflow_type",
            "step_name",
            "intent",
            "summary",
            "idempotency_key",
        ):
            value = _normalize_text(payload.get(key))
            if value:
                return value.lower()
    return (
        _normalize_text(task_context.get("intent"))
        or _task_summary(task_context)
        or _task_id(task_context)
    ).lower()


def _as_float(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    text = _normalize_text(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _extract_delay_seconds(record: Mapping[str, object]) -> float | None:
    payload = _event_payload(record)
    for source in (record, payload):
        for key in ("delay_seconds", "latency_seconds", "offload_latency", "seconds"):
            value = _as_float(source.get(key))
            if value is not None and value >= 0.0:
                return value
    return None


def _latest_timestamp(records: Sequence[Mapping[str, object]]) -> str:
    timestamps = [_normalize_text(record.get("timestamp")) for record in records]
    normalized = [timestamp for timestamp in timestamps if timestamp]
    return max(normalized) if normalized else ""


def _gap(
    *,
    severity: str,
    problem: str,
    impact: str,
    proposed_fix: str,
    impact_score: str,
    frequency: int,
    task_id: object = "",
    interaction_id: object = "",
    context_reference: object = "",
    dedupe_key: object = "",
) -> dict[str, object]:
    priority = build_gap_priority(
        severity=severity,
        impact_score=impact_score,
        frequency=frequency,
    )
    return {
        "type": "system_gap",
        "severity": priority["severity"],
        "problem": problem,
        "impact": impact,
        "proposed_fix": proposed_fix,
        "impact_score": priority["impact_score"],
        "frequency": priority["frequency"],
        "urgency": priority["urgency"],
        "priority_score": priority["priority_score"],
        "priority_level": priority["priority_level"],
        "task_id": _normalize_text(task_id),
        "interaction_id": _normalize_text(interaction_id),
        "context_reference": _normalize_text(context_reference),
        "dedupe_key": _normalize_text(dedupe_key),
    }


def _collect_repeated_failures(
    task_contexts: Sequence[Mapping[str, object]],
    audit_events: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    failures = Counter()
    labels: dict[str, str] = {}
    task_ids: dict[str, str] = {}
    interaction_ids: dict[str, str] = {}
    task_by_id = {
        _task_id(task): dict(task) for task in task_contexts if _task_id(task)
    }
    for task in task_contexts:
        if _normalize_text(task.get("status")).upper() == "FAILED":
            signature = _task_signature(task)
            failures[signature] += 1
            labels.setdefault(signature, _task_summary(task))
            task_ids.setdefault(signature, _task_id(task))
            interaction_ids.setdefault(
                signature, _normalize_text(task.get("interaction_id"))
            )
    for event in audit_events:
        if _normalize_text(event.get("event_type")).lower() not in FAILURE_EVENT_TYPES:
            continue
        task = task_by_id.get(_task_id(event), {})
        signature = _task_signature(task or _event_payload(event))
        failures[signature] += 1
        labels.setdefault(signature, _task_summary(task or _event_payload(event)))
        task_ids.setdefault(signature, _task_id(task or event))
        interaction_ids.setdefault(
            signature,
            _normalize_text((task or event).get("interaction_id")),
        )

    gaps: list[dict[str, object]] = []
    for signature, count in sorted(failures.items()):
        if count < FAILURE_THRESHOLD:
            continue
        severity = "high" if count >= 4 else "medium"
        label = labels.get(signature) or signature
        gaps.append(
            _gap(
                severity=severity,
                problem=f"Repeated failures detected for `{label}` ({count} occurrences).",
                impact="The same execution path is failing often enough to reduce reliability and waste retries.",
                proposed_fix="Add a guarded recovery path or validation step for this flow before execution is retried again.",
                impact_score="high",
                frequency=count,
                task_id=task_ids.get(signature),
                interaction_id=interaction_ids.get(signature),
                context_reference=f"active_task:{task_ids.get(signature)}"
                if task_ids.get(signature)
                else "",
                dedupe_key=f"repeated_failures:{signature}",
            )
        )
    return gaps


def _collect_repeated_delays(
    task_contexts: Sequence[Mapping[str, object]],
    audit_events: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    delayed = Counter()
    labels: dict[str, str] = {}
    task_ids: dict[str, str] = {}
    interaction_ids: dict[str, str] = {}
    task_by_id = {
        _task_id(task): dict(task) for task in task_contexts if _task_id(task)
    }

    for task in task_contexts:
        status = _normalize_text(task.get("status")).upper()
        delay_seconds = _extract_delay_seconds(task)
        if status == "DEFERRED" or (
            delay_seconds is not None and delay_seconds >= HIGH_DELAY_SECONDS
        ):
            signature = _task_signature(task)
            delayed[signature] += 1
            labels.setdefault(signature, _task_summary(task))
            task_ids.setdefault(signature, _task_id(task))
            interaction_ids.setdefault(
                signature, _normalize_text(task.get("interaction_id"))
            )

    for event in audit_events:
        event_type = _normalize_text(event.get("event_type")).lower()
        delay_seconds = _extract_delay_seconds(event)
        if event_type not in DELAY_EVENT_TYPES and not (
            delay_seconds is not None and delay_seconds >= HIGH_DELAY_SECONDS
        ):
            continue
        task = task_by_id.get(_task_id(event), {})
        signature = _task_signature(task or _event_payload(event))
        delayed[signature] += 1
        labels.setdefault(signature, _task_summary(task or _event_payload(event)))
        task_ids.setdefault(signature, _task_id(task or event))
        interaction_ids.setdefault(
            signature,
            _normalize_text((task or event).get("interaction_id")),
        )

    gaps: list[dict[str, object]] = []
    for signature, count in sorted(delayed.items()):
        if count < DELAY_THRESHOLD:
            continue
        severity = "high" if count >= 4 else "medium"
        label = labels.get(signature) or signature
        gaps.append(
            _gap(
                severity=severity,
                problem=f"Repeated delays detected for `{label}` ({count} occurrences).",
                impact="Slow or deferred handling is likely causing queue buildup and weak operator responsiveness.",
                proposed_fix="Introduce a dedicated fast-path, timeout guard, or offload rule for this delayed flow.",
                impact_score="medium",
                frequency=count,
                task_id=task_ids.get(signature),
                interaction_id=interaction_ids.get(signature),
                context_reference=f"active_task:{task_ids.get(signature)}"
                if task_ids.get(signature)
                else "",
                dedupe_key=f"repeated_delays:{signature}",
            )
        )
    return gaps


def _collect_missing_execution_paths(
    task_contexts: Sequence[Mapping[str, object]],
    audit_events: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    events_by_task: dict[str, set[str]] = {}
    for event in audit_events:
        task_id = _task_id(event)
        if not task_id:
            continue
        events_by_task.setdefault(task_id, set()).add(
            _normalize_text(event.get("event_type")).lower()
        )

    gaps: list[dict[str, object]] = []
    for task in task_contexts:
        task_id = _task_id(task)
        if not task_id:
            continue
        status = _normalize_text(task.get("status")).upper()
        approval_status = _normalize_text(task.get("approval_status")).lower()
        if status not in ACTIVE_STATUSES:
            continue
        task_events = events_by_task.get(task_id, set())
        has_terminal = bool(task_events & TERMINAL_EVENT_TYPES)
        has_started = bool(task_events & START_EVENT_TYPES)
        if has_started or has_terminal:
            continue
        if approval_status == "pending" and status == "AWAITING_APPROVAL":
            continue
        summary = _task_summary(task)
        severity = (
            "high"
            if approval_status == "approved" or status == "VALIDATED"
            else "medium"
        )
        gaps.append(
            _gap(
                severity=severity,
                problem=f"Missing execution path detected for task `{summary}` ({task_id}).",
                impact="The task exists in active context but no execution-start event was recorded, which can leave work stranded.",
                proposed_fix="Register or route this task type to an execution handler and emit an `execution_started` audit event when work begins.",
                impact_score="high",
                frequency=1,
                task_id=task_id,
                interaction_id=_normalize_text(task.get("interaction_id")),
                context_reference=f"active_task:{task_id}",
                dedupe_key=f"missing_execution_path:{task_id}",
            )
        )
    return gaps


def _collect_incomplete_flows(
    task_contexts: Sequence[Mapping[str, object]],
    audit_events: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    latest_timestamp = _latest_timestamp(audit_events)
    if not latest_timestamp:
        return []
    task_by_id = {
        _task_id(task): dict(task) for task in task_contexts if _task_id(task)
    }
    latest_by_task: dict[str, str] = {}
    started_without_terminal: dict[str, bool] = {}
    for event in audit_events:
        task_id = _task_id(event)
        if not task_id:
            continue
        event_type = _normalize_text(event.get("event_type")).lower()
        timestamp = _normalize_text(event.get("timestamp"))
        if timestamp:
            previous = latest_by_task.get(task_id, "")
            latest_by_task[task_id] = (
                max(previous, timestamp) if previous else timestamp
            )
        if event_type in START_EVENT_TYPES:
            started_without_terminal[task_id] = True
        if event_type in TERMINAL_EVENT_TYPES:
            started_without_terminal[task_id] = False

    gaps: list[dict[str, object]] = []
    for task_id, started in sorted(started_without_terminal.items()):
        if not started:
            continue
        latest_task_timestamp = latest_by_task.get(task_id, "")
        if latest_timestamp <= latest_task_timestamp:
            continue
        task = task_by_id.get(task_id, {})
        status = _normalize_text(task.get("status")).upper()
        if status in TERMINAL_STATUSES:
            continue
        summary = _task_summary(task or {"task_id": task_id})
        severity = "high" if status == "EXECUTING" else "medium"
        gaps.append(
            _gap(
                severity=severity,
                problem=f"Incomplete flow detected for task `{summary}` ({task_id}).",
                impact="A workflow started but no terminal event was recorded, which can leave the system in a partial state.",
                proposed_fix="Add completion or failure bookkeeping for this flow so every started execution closes with a terminal audit event.",
                impact_score="high",
                frequency=1,
                task_id=task_id,
                interaction_id=_normalize_text(task.get("interaction_id")),
                context_reference=f"active_task:{task_id}",
                dedupe_key=f"incomplete_flow:{task_id}",
            )
        )
    return gaps


def analyze_system_gap_inputs(
    *,
    shared_context: Mapping[str, object] | None = None,
    task_contexts: Sequence[Mapping[str, object]] | None = None,
    audit_events: Sequence[Mapping[str, object]] | None = None,
    interaction_events: Sequence[Mapping[str, object]] | None = None,
) -> list[dict[str, object]]:
    normalized_shared_context = dict(shared_context or {})
    normalized_task_contexts = [
        dict(item) for item in (task_contexts or []) if isinstance(item, Mapping)
    ]
    normalized_audit_events = [
        dict(item) for item in (audit_events or []) if isinstance(item, Mapping)
    ]
    normalized_interaction_events = [
        dict(item) for item in (interaction_events or []) if isinstance(item, Mapping)
    ]

    combined_events = [*normalized_interaction_events, *normalized_audit_events]
    combined_events.sort(key=lambda item: _normalize_text(item.get("timestamp")))

    gaps = [
        *_collect_repeated_failures(normalized_task_contexts, combined_events),
        *_collect_repeated_delays(normalized_task_contexts, combined_events),
        *_collect_missing_execution_paths(normalized_task_contexts, combined_events),
        *_collect_incomplete_flows(normalized_task_contexts, combined_events),
    ]

    system_value = _context_value(normalized_shared_context)
    flagged_gap = _normalize_text(system_value.get("known_gap"))
    if flagged_gap:
        gaps.append(
            _gap(
                severity="low",
                problem=f"Shared context reports a known gap: {flagged_gap}",
                impact="Operators already identified a system weakness that may not yet be represented as executable work.",
                proposed_fix="Convert the known gap in shared context into a tracked remediation task with acceptance criteria.",
                impact_score="low",
                frequency=1,
                context_reference="system_context",
                dedupe_key=f"known_gap:{flagged_gap.lower()}",
            )
        )

    deduped: list[dict[str, object]] = []
    seen: set[tuple[str, str]] = set()
    for gap in gaps:
        marker = (gap["severity"], gap["problem"])
        if marker in seen:
            continue
        seen.add(marker)
        deduped.append(gap)
    return deduped


def analyze_system(
    *,
    root_dir: Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> list[dict[str, str]]:
    config = _current_config(root_dir=root_dir, environ=environ)
    storage_paths = config.storage_paths
    return analyze_system_gap_inputs(
        shared_context=_parse_json_object(storage_paths.system_context_file),
        task_contexts=_task_contexts(storage_paths.active_threads_dir),
        audit_events=_parse_jsonl(storage_paths.audit_file),
        interaction_events=_parse_jsonl(storage_paths.interactions_file),
    )
