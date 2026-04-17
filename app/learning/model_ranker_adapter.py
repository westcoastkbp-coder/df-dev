from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.learning.model_loader import REQUIRED_MODEL_FEATURES, load_model
from app.learning.model_output_contract import build_model_output


DEFAULT_MODEL_ID = "memory_ranker_v1"


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalized_tags(raw_tags: object) -> set[str]:
    if isinstance(raw_tags, str):
        candidates = [raw_tags]
    elif isinstance(raw_tags, (list, tuple, set, frozenset)):
        candidates = list(raw_tags)
    else:
        return set()
    return {
        _normalize_text(item)
        for item in candidates
        if _normalize_text(item)
    }


def _context_mapping(task_packet: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(task_packet, dict):
        return {}
    memory_context = task_packet.get("memory_context")
    if isinstance(memory_context, dict):
        merged = dict(task_packet)
        merged.update(memory_context)
        return merged
    return dict(task_packet)


def _parse_timestamp(value: object) -> datetime | None:
    normalized = _normalize_text(value)
    if not normalized:
        return None
    try:
        return datetime.fromisoformat(normalized.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _infer_memory_class(artifact_type: object) -> str:
    normalized_type = _normalize_text(artifact_type).lower()
    if normalized_type == "execution_trace" or normalized_type.endswith("_trace"):
        return "trace"
    if normalized_type == "conflict_escalation" or "conflict" in normalized_type:
        return "conflict"
    return "artifact"


def _normalized_memory_object(raw_object: dict[str, Any]) -> dict[str, Any]:
    artifact = dict(raw_object)
    artifact_type = _normalize_text(artifact.get("type"))
    memory_class = _normalize_text(artifact.get("memory_class")) or _infer_memory_class(artifact_type)
    updated_at = _normalize_text(
        artifact.get("updated_at") or artifact.get("timestamp") or artifact.get("created_at")
    )
    return {
        "id": _normalize_text(artifact.get("id")),
        "domain": _normalize_text(artifact.get("domain")),
        "type": artifact_type,
        "memory_class": memory_class,
        "tags": sorted(_normalized_tags(artifact.get("tags"))),
        "updated_at": updated_at,
        "timestamp": updated_at,
        "created_at": _normalize_text(artifact.get("created_at")) or updated_at,
    }


def _timestamp_score(
    artifact: dict[str, Any],
    *,
    newest: datetime | None,
    oldest: datetime | None,
) -> float:
    artifact_timestamp = _parse_timestamp(
        artifact.get("updated_at") or artifact.get("timestamp") or artifact.get("created_at")
    )
    if artifact_timestamp is None or newest is None or oldest is None:
        return 0.0
    if newest == oldest:
        return 1.0
    span_seconds = (newest - oldest).total_seconds()
    if span_seconds <= 0:
        return 1.0
    return max(0.0, min(1.0, (artifact_timestamp - oldest).total_seconds() / span_seconds))


def _requested_memory_class(context: dict[str, Any]) -> str:
    explicit_memory_class = _normalize_text(context.get("memory_class"))
    if explicit_memory_class:
        return explicit_memory_class
    requested_type = _normalize_text(context.get("type"))
    if requested_type:
        return _infer_memory_class(requested_type)
    return ""


def _tag_score(required_tags: set[str], artifact_tags: set[str]) -> float:
    if not required_tags or not artifact_tags:
        return 0.0
    intersection = required_tags & artifact_tags
    if not intersection:
        return 0.0
    union = required_tags | artifact_tags
    if not union:
        return 0.0
    return len(intersection) / len(union)


def score_with_model(
    task_packet: dict[str, Any] | None,
    memory_objects: list[dict[str, Any]] | None,
    *,
    model_id: str | None = None,
) -> dict[str, Any]:
    resolved_model_id = _normalize_text(model_id) or DEFAULT_MODEL_ID
    try:
        model = load_model(resolved_model_id)
    except Exception:
        print("[MODEL] fallback -> heuristic")
        return {}

    print(f"[MODEL] loaded id={model['model_id']}")
    context = _context_mapping(task_packet)
    normalized_objects: list[dict[str, Any]] = []
    timestamps: list[datetime] = []

    for raw_object in memory_objects or []:
        if not isinstance(raw_object, dict):
            continue
        artifact = _normalized_memory_object(raw_object)
        normalized_objects.append(artifact)
        parsed_timestamp = _parse_timestamp(
            artifact.get("updated_at") or artifact.get("timestamp") or artifact.get("created_at")
        )
        if parsed_timestamp is not None:
            timestamps.append(parsed_timestamp)

    newest = max(timestamps) if timestamps else None
    oldest = min(timestamps) if timestamps else None
    required_tags = _normalized_tags(context.get("tags"))
    requested_memory_class = _requested_memory_class(context)
    requested_domain = _normalize_text(context.get("domain"))
    weights = dict(model.get("weights") or {})

    scored: list[dict[str, Any]] = []
    for artifact in normalized_objects:
        artifact_domain = _normalize_text(artifact.get("domain"))
        artifact_memory_class = _normalize_text(artifact.get("memory_class"))
        artifact_tags = _normalized_tags(artifact.get("tags"))
        feature_values = {
            "recency": _timestamp_score(artifact, newest=newest, oldest=oldest),
            "tag_overlap": _tag_score(required_tags, artifact_tags),
            "domain_match": 1.0 if requested_domain and artifact_domain == requested_domain else 0.0,
            "memory_class": (
                1.0
                if requested_memory_class and artifact_memory_class == requested_memory_class
                else 0.0
            ),
            "conflict_flag": 1.0 if artifact_memory_class == "conflict" else 0.0,
            "state_flag": 1.0 if artifact_memory_class == "state" else 0.0,
        }
        model_score = round(
            sum(
                float(weights[feature_name]) * float(feature_values[feature_name])
                for feature_name in REQUIRED_MODEL_FEATURES
            ),
            6,
        )
        scored.append(
            {
                "entity_id": _normalize_text(artifact.get("id")),
                "score": max(0.0, min(1.0, model_score)),
                "confidence": max(0.0, min(1.0, model_score)),
            }
        )

    try:
        return build_model_output(
            model_id=model["model_id"],
            output_type="ranking",
            items=scored,
        )
    except Exception:
        print("[MODEL] invalid output -> heuristic")
        return {}
