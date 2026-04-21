from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.learning.model_output_contract import normalize_model_output
from app.learning.model_ranker_adapter import DEFAULT_MODEL_ID, score_with_model


_CONFLICT_PRIORITY_BOOST = 0.55
_STATE_PRIORITY_BOOST = 0.5
_RECENCY_WEIGHT = 0.25
_DOMAIN_BOOST = 0.1
_TAG_WEIGHT = 0.15
_MEMORY_CLASS_BOOST = 0.1
_MODEL_WEIGHT = 0.6
_HEURISTIC_WEIGHT = 0.4


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalized_tags(raw_tags: object) -> set[str]:
    if isinstance(raw_tags, str):
        candidates = [raw_tags]
    elif isinstance(raw_tags, (list, tuple, set, frozenset)):
        candidates = list(raw_tags)
    else:
        return set()
    return {_normalize_text(item) for item in candidates if _normalize_text(item)}


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
        return datetime.fromisoformat(normalized.replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
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
    memory_class = _normalize_text(artifact.get("memory_class")) or _infer_memory_class(
        artifact_type
    )
    updated_at = _normalize_text(
        artifact.get("updated_at")
        or artifact.get("timestamp")
        or artifact.get("created_at")
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
        artifact.get("updated_at")
        or artifact.get("timestamp")
        or artifact.get("created_at")
    )
    if artifact_timestamp is None or newest is None or oldest is None:
        return 0.0
    if newest == oldest:
        return 1.0
    span_seconds = (newest - oldest).total_seconds()
    if span_seconds <= 0:
        return 1.0
    return max(
        0.0, min(1.0, (artifact_timestamp - oldest).total_seconds() / span_seconds)
    )


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


def _score_memory_object(
    context: dict[str, Any],
    artifact: dict[str, Any],
    *,
    newest: datetime | None,
    oldest: datetime | None,
) -> float:
    score = _RECENCY_WEIGHT * _timestamp_score(artifact, newest=newest, oldest=oldest)

    artifact_domain = _normalize_text(artifact.get("domain"))
    if artifact_domain and artifact_domain == _normalize_text(context.get("domain")):
        score += _DOMAIN_BOOST

    required_tags = _normalized_tags(context.get("tags"))
    artifact_tags = _normalized_tags(artifact.get("tags"))
    score += _TAG_WEIGHT * _tag_score(required_tags, artifact_tags)

    requested_memory_class = _requested_memory_class(context)
    artifact_memory_class = _normalize_text(artifact.get("memory_class"))
    if requested_memory_class and artifact_memory_class == requested_memory_class:
        score += _MEMORY_CLASS_BOOST

    if artifact_memory_class == "conflict":
        score += _CONFLICT_PRIORITY_BOOST
    elif artifact_memory_class == "state":
        score += _STATE_PRIORITY_BOOST

    return round(max(0.0, min(1.0, score)), 6)


def _coerce_model_scores(
    model_output: dict[str, Any] | None,
    *,
    expected_ids: set[str],
    expected_model_id: str,
) -> dict[str, float]:
    if not isinstance(model_output, dict):
        return {}

    try:
        normalized_output = normalize_model_output(
            model_output,
            expected_model_id=expected_model_id,
            allowed_output_types={"ranking"},
        )
    except Exception:
        return {}

    coerced: dict[str, float] = {}
    for entry in normalized_output["items"]:
        memory_id = _normalize_text(entry.get("entity_id"))
        if not memory_id or memory_id not in expected_ids:
            return {}
        coerced[memory_id] = round(float(entry["score"]), 6)
    if expected_ids != set(coerced):
        return {}
    return coerced


def rank_memory(
    task_packet: dict[str, Any] | None,
    memory_objects: list[dict[str, Any]] | None,
    *,
    model_enabled: bool = False,
    model_id: str | None = None,
) -> list[dict[str, Any]]:
    context = _context_mapping(task_packet)
    normalized_objects: list[dict[str, Any]] = []
    timestamps: list[datetime] = []

    for raw_object in memory_objects or []:
        if not isinstance(raw_object, dict):
            continue
        artifact = _normalized_memory_object(raw_object)
        normalized_objects.append(artifact)
        parsed_timestamp = _parse_timestamp(
            artifact.get("updated_at")
            or artifact.get("timestamp")
            or artifact.get("created_at")
        )
        if parsed_timestamp is not None:
            timestamps.append(parsed_timestamp)

    newest = max(timestamps) if timestamps else None
    oldest = min(timestamps) if timestamps else None
    expected_ids = {
        _normalize_text(artifact.get("id"))
        for artifact in normalized_objects
        if _normalize_text(artifact.get("id"))
    }
    resolved_model_id = _normalize_text(model_id) or DEFAULT_MODEL_ID
    model_scores: dict[str, float] = {}
    combined_scoring_applied = False

    if model_enabled:
        print("[RANKER] model_enabled")
        try:
            model_scores = _coerce_model_scores(
                score_with_model(task_packet, normalized_objects, model_id=model_id),
                expected_ids=expected_ids,
                expected_model_id=resolved_model_id,
            )
        except Exception:
            model_scores = {}
        if model_scores:
            combined_scoring_applied = True
            print("[RANKER] combined scoring applied")
        else:
            print("[RANKER] heuristic_only")
    else:
        print("[RANKER] heuristic_only")

    scored: list[dict[str, Any]] = []
    for artifact in normalized_objects:
        parsed_timestamp = _parse_timestamp(
            artifact.get("updated_at")
            or artifact.get("timestamp")
            or artifact.get("created_at")
        )
        heuristic_score = _score_memory_object(
            context, artifact, newest=newest, oldest=oldest
        )
        model_score = model_scores.get(_normalize_text(artifact.get("id")))
        final_score = heuristic_score
        if combined_scoring_applied and model_score is not None:
            final_score = round(
                (_MODEL_WEIGHT * model_score) + (_HEURISTIC_WEIGHT * heuristic_score),
                6,
            )
        scored.append(
            {
                "memory_id": _normalize_text(artifact.get("id")),
                "score": final_score,
                "timestamp_value": parsed_timestamp.timestamp()
                if parsed_timestamp is not None
                else float("-inf"),
            }
        )

    scored.sort(
        key=lambda item: (
            -float(item["score"]),
            -float(item["timestamp_value"]),
            str(item["memory_id"]),
        ),
        reverse=False,
    )

    ranked: list[dict[str, Any]] = []
    for index, item in enumerate(scored, start=1):
        ranked.append(
            {
                "memory_id": str(item["memory_id"]),
                "score": float(item["score"]),
                "rank": index,
            }
        )

    print(f"[RANKER] scored {len(ranked)} objects")
    return ranked
