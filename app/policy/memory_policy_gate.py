from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


RECENT_DUPLICATE_WINDOW = timedelta(hours=24)


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalized_tags(raw_tags: Any) -> set[str]:
    if isinstance(raw_tags, str):
        candidates = [raw_tags]
    elif isinstance(raw_tags, (list, tuple)):
        candidates = list(raw_tags)
    else:
        return set()

    return {
        _normalize_text(item)
        for item in candidates
        if _normalize_text(item)
    }


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_timestamp(value: object) -> datetime | None:
    normalized = _normalize_text(value)
    if not normalized:
        return None
    try:
        return datetime.fromisoformat(normalized.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def evaluate_memory_policy(
    task_packet: dict[str, Any] | None,
    resolved_memory: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    task = task_packet if isinstance(task_packet, dict) else {}
    memory_context = task.get("memory_context")
    if not isinstance(memory_context, dict):
        memory_context = {}

    required_domain = _normalize_text(memory_context.get("domain"))
    required_type = _normalize_text(memory_context.get("type"))
    required_tags = _normalized_tags(memory_context.get("tags"))

    latest_match: dict[str, Any] | None = None
    latest_match_timestamp: datetime | None = None

    for raw_artifact in resolved_memory or []:
        if not isinstance(raw_artifact, dict):
            continue

        artifact = dict(raw_artifact)
        if required_domain and _normalize_text(artifact.get("domain")) != required_domain:
            continue
        if required_type and _normalize_text(artifact.get("type")) != required_type:
            continue

        if required_tags and _normalized_tags(artifact.get("tags")) != required_tags:
            continue

        artifact_timestamp = _parse_timestamp(artifact.get("timestamp"))
        if artifact_timestamp is None:
            continue

        if latest_match_timestamp is None or artifact_timestamp > latest_match_timestamp:
            latest_match = artifact
            latest_match_timestamp = artifact_timestamp

    if latest_match_timestamp is None:
        return {
            "allowed": True,
            "reason": "no_recent_duplicate",
            "matched_artifact_id": None,
            "action": "continue",
        }

    if latest_match_timestamp >= (_utc_now() - RECENT_DUPLICATE_WINDOW):
        return {
            "allowed": False,
            "reason": "recent_duplicate_detected",
            "matched_artifact_id": _normalize_text(latest_match.get("id")),
            "action": "block",
        }

    return {
        "allowed": True,
        "reason": "no_recent_duplicate",
        "matched_artifact_id": None,
        "action": "continue",
    }
