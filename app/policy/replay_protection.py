from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.memory.memory_registry import MemoryRegistryError, list_by_type


TRACE_ARTIFACT_TYPE = "execution_trace"
EXECUTED_STATUS = "executed"


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _mapping(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {}


def _trace_payload_from_entry(entry: dict[str, Any]) -> dict[str, Any]:
    local_path = _normalize_text(entry.get("local_path"))
    if not local_path:
        return {}

    try:
        record = json.loads(Path(local_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}

    if not isinstance(record, dict):
        return {}

    payload = _mapping(record.get("payload"))
    if payload:
        return payload
    return record


def _previous_trace_id(entry: dict[str, Any], payload: dict[str, Any]) -> str:
    return _normalize_text(entry.get("id")) or _normalize_text(payload.get("id"))


def _is_executed_trace_for_task(payload: dict[str, Any], task_id: str) -> bool:
    if _normalize_text(payload.get("type")) != TRACE_ARTIFACT_TYPE:
        return False
    if _normalize_text(payload.get("task_id")) != task_id:
        return False

    result = _mapping(payload.get("result"))
    return _normalize_text(result.get("status")) == EXECUTED_STATUS


def check_replay(task_packet: dict[str, Any] | None) -> dict[str, Any]:
    task = _mapping(task_packet)
    task_id = _normalize_text(task.get("task_id"))
    if not task_id:
        return {
            "allowed": True,
            "reason": "missing_task_id",
            "previous_trace_id": "",
            "action": "allow",
        }

    try:
        trace_entries = list_by_type(TRACE_ARTIFACT_TYPE)
    except MemoryRegistryError:
        return {
            "allowed": True,
            "reason": "trace_registry_unavailable",
            "previous_trace_id": "",
            "action": "allow",
        }

    for raw_entry in reversed(trace_entries):
        if not isinstance(raw_entry, dict):
            continue

        entry = dict(raw_entry)
        payload = _trace_payload_from_entry(entry)
        if not _is_executed_trace_for_task(payload, task_id):
            continue

        return {
            "allowed": False,
            "reason": "already_executed",
            "previous_trace_id": _previous_trace_id(entry, payload),
            "action": "block",
        }

    return {
        "allowed": True,
        "reason": "not_previously_executed",
        "previous_trace_id": "",
        "action": "allow",
    }
