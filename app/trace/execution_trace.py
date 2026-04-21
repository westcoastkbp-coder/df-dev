from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.ownerbox.domain import (
    build_ownerbox_trace_metadata,
    normalize_ownerbox_domain_binding,
)


def _utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _mapping(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {}


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _task_domain(task_packet: dict[str, Any]) -> str:
    payload = _mapping(task_packet.get("payload"))
    memory_context = _mapping(task_packet.get("memory_context"))
    domain_binding = normalize_ownerbox_domain_binding(
        task_packet.get("domain_binding") or payload.get("domain_binding")
    )
    normalized = _normalize_text(
        payload.get("domain")
        or task_packet.get("domain")
        or memory_context.get("domain")
        or domain_binding.get("domain_type")
        or "dev"
    ).lower()
    if normalized in {"owner", "ownerbox"}:
        return "ownerbox"
    return "dev"


def _result_artifact_id(
    task_packet: dict[str, Any],
    execution_status: str,
    final_decision: dict[str, Any],
) -> str | None:
    if execution_status != "executed":
        return None

    artifact_id = _normalize_text(
        final_decision.get("artifact_id")
        or task_packet.get("artifact_id")
        or task_packet.get("doc_id")
    )
    return artifact_id or None


def create_execution_trace(
    task_packet: dict[str, Any] | None,
    stages: dict[str, Any] | None,
) -> dict[str, Any]:
    task = _mapping(task_packet)
    stage_payload = _mapping(stages)
    final_decision = _mapping(stage_payload.get("final_decision"))
    execution_status = (
        _normalize_text(stage_payload.get("execution_status")) or "blocked"
    )
    domain_binding = normalize_ownerbox_domain_binding(
        task.get("domain_binding")
        or _mapping(task.get("payload")).get("domain_binding")
    )

    trace = {
        "type": "execution_trace",
        "task_id": _normalize_text(task.get("task_id")),
        "domain": _task_domain(task),
        "timestamp": _utc_timestamp(),
        "stages": {
            "resolver": _mapping(stage_payload.get("resolver")),
            "memory_policy": _mapping(stage_payload.get("memory_policy")),
            "conflict_gate": _mapping(stage_payload.get("conflict_gate")),
            "replay_protection": _mapping(stage_payload.get("replay_protection")),
            "execution_invariants": _mapping(stage_payload.get("execution_invariants")),
            "final_decision": final_decision,
        },
        "result": {
            "status": execution_status,
            "artifact_id": _result_artifact_id(task, execution_status, final_decision),
        },
    }
    trace_metadata = build_ownerbox_trace_metadata(domain_binding)
    if trace_metadata:
        trace["domain_metadata"] = trace_metadata
    return trace
