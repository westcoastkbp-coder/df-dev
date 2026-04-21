from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path

from app.context.shared_context_store import set_context
from app.orchestrator.task_factory import create_task, now, save_task
from app.orchestrator.task_lifecycle import transition_task_status
from app.orchestrator.task_queue import InMemoryTaskQueue, task_queue
from app.system.analyzer import analyze_system, ensure_gap_priority_metadata


LOW_SEVERITY = "low"
MEDIUM_SEVERITY = "medium"
HIGH_SEVERITY = "high"
SUPPORTED_SEVERITIES = {LOW_SEVERITY, MEDIUM_SEVERITY, HIGH_SEVERITY}
LOW_PRIORITY = "low"
MEDIUM_PRIORITY = "medium"
HIGH_PRIORITY = "high"
SUPPORTED_IMPACT_SCORES = {LOW_PRIORITY, MEDIUM_PRIORITY, HIGH_PRIORITY}
SUPPORTED_URGENCY = {"delayed", "normal", "immediate"}
MAX_AUTO_TASKS_PER_CYCLE = 2
BLOCKED_TASKS_SAFETY_THRESHOLD = 3


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalized_gap(gap: Mapping[str, object]) -> dict[str, object]:
    priority_enriched_gap = ensure_gap_priority_metadata(gap)
    severity = _normalize_text(priority_enriched_gap.get("severity")).lower()
    if severity not in SUPPORTED_SEVERITIES:
        raise ValueError(f"unsupported system gap severity: {severity or '(empty)'}")
    impact_score = _normalize_text(priority_enriched_gap.get("impact_score")).lower()
    if impact_score not in SUPPORTED_IMPACT_SCORES:
        raise ValueError(
            f"unsupported system gap impact score: {impact_score or '(empty)'}"
        )
    urgency = _normalize_text(priority_enriched_gap.get("urgency")).lower()
    if urgency not in SUPPORTED_URGENCY:
        raise ValueError(f"unsupported system gap urgency: {urgency or '(empty)'}")
    normalized = {
        "type": _normalize_text(priority_enriched_gap.get("type")) or "system_gap",
        "severity": severity,
        "problem": _normalize_text(priority_enriched_gap.get("problem")),
        "impact": _normalize_text(priority_enriched_gap.get("impact")),
        "proposed_fix": _normalize_text(priority_enriched_gap.get("proposed_fix")),
        "impact_score": impact_score,
        "frequency": int(priority_enriched_gap.get("frequency") or 1),
        "urgency": urgency,
        "priority_score": int(priority_enriched_gap.get("priority_score") or 0),
        "priority_level": _normalize_text(
            priority_enriched_gap.get("priority_level")
        ).lower(),
        "interaction_id": _normalize_text(priority_enriched_gap.get("interaction_id")),
        "task_id": _normalize_text(priority_enriched_gap.get("task_id")),
        "context_reference": _normalize_text(
            priority_enriched_gap.get("context_reference")
        ),
        "dedupe_key": _normalize_text(priority_enriched_gap.get("dedupe_key")),
    }
    if (
        not normalized["problem"]
        or not normalized["impact"]
        or not normalized["proposed_fix"]
    ):
        raise ValueError("system gap missing required fields")
    return normalized


def _gap_identity(normalized_gap: Mapping[str, object]) -> str:
    explicit_key = _normalize_text(normalized_gap.get("dedupe_key"))
    if explicit_key:
        return explicit_key
    payload = {
        "problem": _normalize_text(normalized_gap.get("problem")),
        "impact": _normalize_text(normalized_gap.get("impact")),
        "proposed_fix": _normalize_text(normalized_gap.get("proposed_fix")),
        "severity": _normalize_text(normalized_gap.get("severity")),
    }
    digest = hashlib.sha256(
        json.dumps(
            payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()
    return f"system_gap:{digest}"


def _priority_route(
    normalized_gap: Mapping[str, object],
    *,
    allow_auto_task: bool,
) -> dict[str, object]:
    severity = _normalize_text(normalized_gap.get("severity")).lower()
    priority_level = _normalize_text(normalized_gap.get("priority_level")).lower()
    if severity == LOW_SEVERITY:
        if priority_level == LOW_PRIORITY or not allow_auto_task:
            return {
                "status": "created",
                "approval_status": "approved",
                "requires_approval": False,
                "route_target": "batch_queue",
                "priority": "LOW",
                "auto_task_mode": "batched",
            }
        return {
            "status": "created",
            "approval_status": "approved",
            "requires_approval": False,
            "route_target": "immediate_execution"
            if priority_level == HIGH_PRIORITY
            else "execution",
            "priority": "HIGH" if priority_level == HIGH_PRIORITY else "NORMAL",
            "auto_task_mode": "queued",
        }
    return {
        "status": "awaiting_approval",
        "approval_status": "pending",
        "requires_approval": True,
        "route_target": "approval_queue"
        if priority_level == HIGH_PRIORITY
        else "manual_review",
        "priority": "HIGH" if priority_level == HIGH_PRIORITY else "NORMAL",
        "auto_task_mode": "approval_queue",
    }


def gap_to_task_input(
    gap: Mapping[str, object],
    *,
    allow_auto_task: bool = True,
    safety_mode: bool = False,
) -> dict[str, object]:
    normalized_gap = _normalized_gap(gap)
    routing = _priority_route(normalized_gap, allow_auto_task=allow_auto_task)
    dedupe_key = _gap_identity(normalized_gap)
    source_task_id = _normalize_text(normalized_gap.get("task_id"))
    interaction_id = _normalize_text(normalized_gap.get("interaction_id"))
    context_reference = (
        _normalize_text(normalized_gap.get("context_reference")) or "system_context"
    )
    priority_metadata = {
        "severity": normalized_gap["severity"],
        "impact_score": normalized_gap["impact_score"],
        "frequency": normalized_gap["frequency"],
        "urgency": normalized_gap["urgency"],
        "priority_score": normalized_gap["priority_score"],
        "priority_level": normalized_gap["priority_level"],
        "core_impact": False,
    }
    payload = {
        "type": "system_improvement_task",
        "summary": normalized_gap["problem"],
        "problem": normalized_gap["problem"],
        "impact": normalized_gap["impact"],
        "proposed_fix": normalized_gap["proposed_fix"],
        **priority_metadata,
        "affected_files": [],
        "source": "analyzer",
        "requires_approval": bool(routing["requires_approval"]),
        "route_target": routing["route_target"],
        "auto_task_mode": routing["auto_task_mode"],
        "context_reference": context_reference,
        "lineage": {
            "source_task_id": source_task_id,
            "source_type": "system_gap",
            "context_reference": context_reference,
        },
        "idempotency_key": dedupe_key,
        "dedupe_key": dedupe_key,
        "priority": routing["priority"],
        "safety_mode": bool(safety_mode),
    }
    if safety_mode and not payload["core_impact"] and not payload["requires_approval"]:
        payload["route_target"] = "batch_queue"
        payload["auto_task_mode"] = "batched"
        payload["priority"] = "LOW"
    task_input = {
        "source": "internal",
        "intent": "system_improvement_task",
        "status": routing["status"],
        "approval_status": routing["approval_status"],
        "interaction_id": interaction_id,
        "parent_task_id": source_task_id,
        "payload": payload,
        "notes": [
            f"system_gap:{normalized_gap['severity']}",
            f"priority_level:{normalized_gap['priority_level']}",
            f"priority_score:{normalized_gap['priority_score']}",
            f"route_target:{routing['route_target']}",
        ],
        "idempotency_key": dedupe_key,
    }
    return task_input


def _auto_validate_for_execution(
    task_data: dict[str, object],
    *,
    store_path: Path | None = None,
) -> dict[str, object]:
    if _normalize_text(task_data.get("status")) == "VALIDATED":
        return task_data
    transition_task_status(
        task_data,
        "VALIDATED",
        timestamp=now(),
        details="system improvement task auto-approved for low-severity analyzer gap",
    )
    task_data["approval_status"] = "approved"
    return save_task(task_data, store_path=store_path)


def ingest_system_gap(
    gap: Mapping[str, object],
    *,
    queue: InMemoryTaskQueue = task_queue,
    store_path: Path | None = None,
    allow_auto_task: bool = True,
    safety_mode: bool = False,
) -> dict[str, object]:
    normalized_gap = _normalized_gap(gap)
    task_input = gap_to_task_input(
        normalized_gap,
        allow_auto_task=allow_auto_task,
        safety_mode=safety_mode,
    )
    created_task = create_task(task_input, store_path=store_path)
    payload = dict(created_task.get("payload", {}) or {})
    routed_task = dict(created_task)
    if (
        _normalize_text(payload.get("requires_approval")).lower() != "true"
        and _normalize_text(payload.get("auto_task_mode")) == "queued"
    ):
        routed_task = _auto_validate_for_execution(routed_task, store_path=store_path)
        queue.enqueue(routed_task["task_id"])
    _write_priority_context(
        normalized_gap,
        task_data=routed_task,
        max_auto_tasks_per_cycle=MAX_AUTO_TASKS_PER_CYCLE,
    )
    return routed_task


def ingest_system_gaps(
    gaps: Sequence[Mapping[str, object]],
    *,
    queue: InMemoryTaskQueue = task_queue,
    store_path: Path | None = None,
) -> list[dict[str, object]]:
    created_tasks: list[dict[str, object]] = []
    auto_tasks_used = 0
    blocked_tasks = 0
    seen_identities: set[str] = set()
    for gap in gaps:
        if not isinstance(gap, Mapping):
            continue
        normalized_gap = _normalized_gap(gap)
        gap_identity = _gap_identity(normalized_gap)
        auto_eligible = (
            _normalize_text(normalized_gap.get("severity")) == LOW_SEVERITY
            and _normalize_text(normalized_gap.get("priority_level")) != LOW_PRIORITY
        )
        allow_auto_task = (
            not auto_eligible
        ) or auto_tasks_used < MAX_AUTO_TASKS_PER_CYCLE
        safety_mode = blocked_tasks > BLOCKED_TASKS_SAFETY_THRESHOLD
        try:
            created_tasks.append(
                ingest_system_gap(
                    normalized_gap,
                    queue=queue,
                    store_path=store_path,
                    allow_auto_task=allow_auto_task,
                    safety_mode=safety_mode,
                )
            )
        except ValueError:
            blocked_tasks += 1
            if safety_mode:
                created_tasks.append(
                    ingest_system_gap(
                        normalized_gap,
                        queue=queue,
                        store_path=store_path,
                        allow_auto_task=False,
                        safety_mode=True,
                    )
                )
            else:
                raise
        if auto_eligible and allow_auto_task and gap_identity not in seen_identities:
            auto_tasks_used += 1
        seen_identities.add(gap_identity)
    return created_tasks


def _write_priority_context(
    normalized_gap: Mapping[str, object],
    *,
    task_data: Mapping[str, object],
    max_auto_tasks_per_cycle: int,
) -> None:
    payload = dict(task_data.get("payload", {}) or {})
    dedupe_key = _gap_identity(normalized_gap)
    context_reference = (
        _normalize_text(normalized_gap.get("context_reference")) or "system_context"
    )
    set_context(
        "system_context",
        {
            "system_improvement_priorities": {
                dedupe_key: {
                    "task_id": _normalize_text(task_data.get("task_id")),
                    "context_reference": context_reference,
                    "severity": _normalize_text(normalized_gap.get("severity")),
                    "impact_score": _normalize_text(normalized_gap.get("impact_score")),
                    "frequency": int(normalized_gap.get("frequency") or 1),
                    "urgency": _normalize_text(normalized_gap.get("urgency")),
                    "priority_score": int(normalized_gap.get("priority_score") or 0),
                    "priority_level": _normalize_text(
                        normalized_gap.get("priority_level")
                    ),
                    "core_impact": _normalize_text(payload.get("core_impact")).lower()
                    == "true",
                    "route_target": _normalize_text(payload.get("route_target")),
                    "auto_task_mode": _normalize_text(payload.get("auto_task_mode")),
                    "approval_status": _normalize_text(
                        task_data.get("approval_status")
                    ),
                }
            },
            "system_improvement_limits": {
                "max_auto_tasks_per_cycle": max_auto_tasks_per_cycle,
            },
        },
        task_id=_normalize_text(task_data.get("task_id")),
        interaction_id=_normalize_text(task_data.get("interaction_id")),
        timestamp=_normalize_text(task_data.get("last_updated_at")) or now(),
    )


def analyze_and_create_system_improvement_tasks(
    *,
    root_dir: Path | None = None,
    environ: Mapping[str, str] | None = None,
    queue: InMemoryTaskQueue = task_queue,
    store_path: Path | None = None,
) -> list[dict[str, object]]:
    return ingest_system_gaps(
        analyze_system(root_dir=root_dir, environ=environ),
        queue=queue,
        store_path=store_path,
    )
