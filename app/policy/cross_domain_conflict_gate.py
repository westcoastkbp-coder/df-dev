from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from app.memory.memory_object import effective_status
from app.memory.memory_registry import (
    MemoryRegistryError,
    compute_artifact_key,
    get_artifact_by_logical_key,
)


CONFLICT_ESCALATION_TYPE = "conflict_escalation"
PENDING_RESOLUTION_STATUS = "pending_resolution"
RESOLVED_STATUS = "resolved"


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_mapping(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _first_non_empty(*values: object) -> str:
    for value in values:
        normalized = _normalize_text(value)
        if normalized:
            return normalized
    return ""


def _payload(entity: object) -> dict[str, Any]:
    return _normalize_mapping(_normalize_mapping(entity).get("payload"))


def _memory_context(entity: object) -> dict[str, Any]:
    return _normalize_mapping(_normalize_mapping(entity).get("memory_context"))


def _logical_id_from_logical_key(value: object) -> str:
    logical_key = _normalize_text(value)
    if not logical_key:
        return ""
    _, _, logical_id = logical_key.partition(":")
    if not logical_id:
        return ""
    _, _, logical_id = logical_id.partition(":")
    return _normalize_text(logical_id)


def _resource_key(entity: object) -> str:
    mapped = _normalize_mapping(entity)
    payload = _payload(entity)
    return _first_non_empty(
        payload.get("resource_id"),
        mapped.get("resource_id"),
        payload.get("logical_id"),
        mapped.get("logical_id"),
        _logical_id_from_logical_key(mapped.get("logical_key")),
        payload.get("idempotency_key"),
        mapped.get("idempotency_key"),
    )


def _domain(entity: object) -> str:
    mapped = _normalize_mapping(entity)
    payload = _payload(entity)
    memory_context = _memory_context(entity)
    return _first_non_empty(
        payload.get("domain"),
        mapped.get("domain"),
        memory_context.get("domain"),
    )


def _artifact_type(entity: object) -> str:
    mapped = _normalize_mapping(entity)
    payload = _payload(entity)
    memory_context = _memory_context(entity)
    return _first_non_empty(
        mapped.get("type"),
        payload.get("type"),
        mapped.get("task_type"),
        memory_context.get("type"),
    )


def _status(entity: object) -> str:
    mapped = _normalize_mapping(entity)
    payload = _payload(entity)
    result = _normalize_mapping(mapped.get("result"))
    normalized = effective_status(mapped)
    if normalized:
        return normalized
    normalized_payload = effective_status(payload)
    if normalized_payload:
        return normalized_payload
    return _first_non_empty(result.get("status")).lower()


def _is_completed(entity: object) -> bool:
    return _status(entity) == "completed"


def _conflict_resolution_entry(resource_key: str) -> dict[str, Any] | None:
    normalized_resource_key = _normalize_text(resource_key)
    if not normalized_resource_key:
        return None

    logical_key = compute_artifact_key(
        "ownerbox",
        CONFLICT_ESCALATION_TYPE,
        normalized_resource_key,
    )
    try:
        entry = get_artifact_by_logical_key(logical_key)
    except MemoryRegistryError:
        return None
    if not isinstance(entry, dict):
        return None
    if _normalize_text(entry.get("type")) != CONFLICT_ESCALATION_TYPE:
        return None
    if _status(entry) == RESOLVED_STATUS:
        return dict(entry)

    local_path = _normalize_text(entry.get("local_path"))
    if not local_path:
        return None
    try:
        artifact_payload = json.loads(Path(local_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(artifact_payload, dict):
        return None
    if _normalize_text(artifact_payload.get("type")) != CONFLICT_ESCALATION_TYPE:
        return None
    if _status(artifact_payload) != RESOLVED_STATUS:
        return None

    resolved_entry = dict(entry)
    resolved_entry["status"] = RESOLVED_STATUS
    resolved_entry["resolution"] = artifact_payload.get("resolution")
    return resolved_entry


def _find_cross_domain_conflict(
    task_packet: dict[str, Any] | None,
    resolved_memory: list[dict[str, Any]] | None,
) -> dict[str, Any] | None:
    task = task_packet if isinstance(task_packet, dict) else {}
    task_resource_key = _resource_key(task)
    task_domain = _domain(task)
    task_type = _artifact_type(task)
    if not task_resource_key or not task_domain or not task_type or _is_completed(task):
        return None

    for raw_artifact in resolved_memory or []:
        if not isinstance(raw_artifact, dict):
            continue

        artifact = dict(raw_artifact)
        if _resource_key(artifact) != task_resource_key:
            continue

        artifact_domain = _domain(artifact)
        if not artifact_domain or artifact_domain == task_domain:
            continue

        if _is_completed(artifact):
            continue

        if _artifact_type(artifact) != task_type:
            continue

        return artifact

    return None


def evaluate_cross_domain_conflict(
    task_packet: dict[str, Any] | None,
    resolved_memory: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    task = task_packet if isinstance(task_packet, dict) else {}
    conflict = _find_cross_domain_conflict(task, resolved_memory)
    if conflict is None:
        return {
            "allowed": True,
            "reason": "no_cross_domain_conflict",
            "conflict_with": None,
            "action": "continue",
        }

    resolved_conflict = _conflict_resolution_entry(_resource_key(task))
    if resolved_conflict is not None:
        return {
            "allowed": True,
            "reason": "previously_resolved_conflict",
            "conflict_with": _normalize_text(resolved_conflict.get("id")) or None,
            "action": "continue",
        }

    return {
        "allowed": False,
        "reason": "cross_domain_conflict_detected",
        "conflict_with": _normalize_text(conflict.get("id")) or None,
        "action": "block",
    }
