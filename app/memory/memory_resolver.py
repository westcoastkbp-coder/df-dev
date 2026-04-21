from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.learning.model_evaluator import deterministic_sample_selected, evaluate_models
from app.learning.memory_ranker import rank_memory
from app.learning.model_update_config import load_model_update_config
from app.memory.memory_registry import list_by_domain
from app.memory.memory_object import memory_object_from_mapping


REPO_ROOT = Path(__file__).resolve().parents[2]
MEMORY_RANKING_CONFIG_FILE = REPO_ROOT / "config" / "memory_ranking.json"
DEFAULT_MEMORY_RANKING_CONFIG = {
    "enabled": True,
    "use_model": False,
    "model_id": "",
    "top_k": 10,
}


def _normalized_tags(raw_tags: Any) -> list[str]:
    if isinstance(raw_tags, str):
        candidates = [raw_tags]
    elif isinstance(raw_tags, (list, tuple)):
        candidates = list(raw_tags)
    else:
        return []

    normalized_tags: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        tag = str(item or "").strip()
        if not tag or tag in seen:
            continue
        seen.add(tag)
        normalized_tags.append(tag)
    return normalized_tags


def _load_ranking_config() -> dict[str, Any]:
    config = dict(DEFAULT_MEMORY_RANKING_CONFIG)
    if not MEMORY_RANKING_CONFIG_FILE.exists():
        return config
    try:
        payload = json.loads(MEMORY_RANKING_CONFIG_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return config
    if not isinstance(payload, dict):
        return config
    if "enabled" in payload:
        config["enabled"] = bool(payload["enabled"])
    if "use_model" in payload:
        config["use_model"] = bool(payload["use_model"])
    if "model_id" in payload:
        config["model_id"] = str(payload["model_id"] or "").strip()
    if "top_k" in payload:
        try:
            config["top_k"] = max(0, int(payload["top_k"]))
        except (TypeError, ValueError):
            config["top_k"] = DEFAULT_MEMORY_RANKING_CONFIG["top_k"]
    return config


def _timestamp_sort_key(item: dict[str, Any]) -> str:
    return str(item.get("updated_at") or item.get("timestamp") or "")


def _run_passive_evaluation(
    context: dict[str, Any],
    filtered: list[dict[str, Any]],
) -> None:
    update_config = load_model_update_config()
    if not bool(update_config.get("evaluation_mode", False)):
        return
    if not deterministic_sample_selected(
        context, float(update_config.get("evaluation_sample_rate", 0.0))
    ):
        return
    evaluate_models(context, filtered)


def resolve_memory(context: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(context, dict):
        return []

    domain = str(context.get("domain") or "").strip()
    artifact_type = str(context.get("type") or "").strip()
    memory_class = str(context.get("memory_class") or "").strip()
    if not domain or (not artifact_type and not memory_class):
        return []

    required_tags = set(_normalized_tags(context.get("tags")))
    artifacts = list_by_domain(domain)
    filtered: list[dict[str, Any]] = []
    for artifact in artifacts:
        normalized_artifact = memory_object_from_mapping(artifact).to_dict()
        if (
            artifact_type
            and str(normalized_artifact.get("type") or "").strip() != artifact_type
        ):
            continue
        if (
            memory_class
            and str(normalized_artifact.get("memory_class") or "").strip()
            != memory_class
        ):
            continue
        artifact_tags = {
            str(item or "").strip()
            for item in normalized_artifact.get("tags", [])
            if str(item or "").strip()
        }
        if required_tags and not required_tags.issubset(artifact_tags):
            continue
        filtered.append(normalized_artifact)

    filtered.sort(key=_timestamp_sort_key, reverse=True)

    config = _load_ranking_config()
    if not bool(config.get("enabled", True)):
        return filtered

    rankings = rank_memory(
        context,
        filtered,
        model_enabled=bool(
            config.get("use_model", DEFAULT_MEMORY_RANKING_CONFIG["use_model"])
        ),
        model_id=str(
            config.get("model_id", DEFAULT_MEMORY_RANKING_CONFIG["model_id"]) or ""
        ).strip()
        or None,
    )
    if not rankings:
        print("[RANKER] top selected ids=[]")
        return []

    artifacts_by_id = {
        str(item.get("id") or "").strip(): item
        for item in filtered
        if str(item.get("id") or "").strip()
    }
    ranked_objects = [
        artifacts_by_id[str(entry["memory_id"])]
        for entry in rankings
        if str(entry.get("memory_id") or "").strip() in artifacts_by_id
    ]

    top_k = max(0, int(config.get("top_k", DEFAULT_MEMORY_RANKING_CONFIG["top_k"])))
    selected = ranked_objects[:top_k] if top_k else []
    _run_passive_evaluation(context, filtered)
    print(
        f"[RANKER] top selected ids={[str(item.get('id') or '').strip() for item in selected]}"
    )
    return selected
