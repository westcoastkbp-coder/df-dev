from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.learning.memory_ranker import rank_memory
from app.learning.model_update_config import load_model_update_config


REPO_ROOT = Path(__file__).resolve().parents[2]
EVALS_ROOT = REPO_ROOT / "DF" / "shared" / "evals"
DEFAULT_TOP_K = 10


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _rank_memory_ids(rankings: list[dict[str, Any]]) -> list[str]:
    return [
        _normalize_text(entry.get("memory_id"))
        for entry in rankings
        if _normalize_text(entry.get("memory_id"))
    ]


def _agreement_score(active_ids: list[str], candidate_ids: list[str]) -> float:
    union_ids = sorted(set(active_ids) | set(candidate_ids))
    if not union_ids:
        return 1.0

    active_ranks = {memory_id: index + 1 for index, memory_id in enumerate(active_ids)}
    candidate_ranks = {
        memory_id: index + 1 for index, memory_id in enumerate(candidate_ids)
    }
    default_rank = len(union_ids) + 1
    total_diff = sum(
        abs(
            active_ranks.get(memory_id, default_rank)
            - candidate_ranks.get(memory_id, default_rank)
        )
        for memory_id in union_ids
    )
    max_diff = len(union_ids) * len(union_ids)
    if max_diff <= 0:
        return 1.0
    return round(max(0.0, min(1.0, 1.0 - (total_diff / max_diff))), 6)


def _top_k_overlap(
    active_ids: list[str], candidate_ids: list[str], *, top_k: int = DEFAULT_TOP_K
) -> float:
    effective_k = min(top_k, max(len(active_ids), len(candidate_ids)))
    if effective_k <= 0:
        return 1.0
    active_top = set(active_ids[:effective_k])
    candidate_top = set(candidate_ids[:effective_k])
    return round(len(active_top & candidate_top) / effective_k, 6)


def _write_eval_artifact(payload: dict[str, Any]) -> Path:
    timestamp = (
        _normalize_text(payload.get("evaluated_at")).replace(":", "").replace("-", "")
    )
    safe_timestamp = timestamp.replace("+00", "Z")
    target_path = EVALS_ROOT / f"{safe_timestamp}.json"
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return target_path


def deterministic_sample_selected(
    task_packet: dict[str, Any] | None, sample_rate: float
) -> bool:
    bounded_rate = max(0.0, min(1.0, float(sample_rate)))
    if bounded_rate <= 0.0:
        return False
    if bounded_rate >= 1.0:
        return True

    serialized = json.dumps(
        task_packet if isinstance(task_packet, dict) else {},
        sort_keys=True,
        default=str,
    )
    digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
    bucket = int(digest[:8], 16) / 0xFFFFFFFF
    return bucket < bounded_rate


def evaluate_models(
    task_packet: dict[str, Any] | None,
    memory_objects: list[dict[str, Any]] | None,
) -> dict[str, Any] | None:
    config = load_model_update_config()
    active_model = _normalize_text(config.get("active_model"))
    candidate_model = _normalize_text(config.get("candidate_model"))
    if not active_model or not candidate_model or active_model == candidate_model:
        return None

    active_rankings = rank_memory(
        task_packet, memory_objects, model_enabled=True, model_id=active_model
    )
    candidate_rankings = rank_memory(
        task_packet, memory_objects, model_enabled=True, model_id=candidate_model
    )
    active_ids = _rank_memory_ids(active_rankings)
    candidate_ids = _rank_memory_ids(candidate_rankings)
    top1_match = bool(
        active_ids and candidate_ids and active_ids[0] == candidate_ids[0]
    )
    metrics = {
        "active_model": active_model,
        "candidate_model": candidate_model,
        "agreement_score": _agreement_score(active_ids, candidate_ids),
        "top1_match": top1_match,
        "top_k_overlap": _top_k_overlap(active_ids, candidate_ids),
        "evaluated_at": _utc_timestamp(),
    }
    artifact_payload = {
        "active_model": active_model,
        "candidate_model": candidate_model,
        "active_rankings": active_rankings,
        "candidate_rankings": candidate_rankings,
        "agreement_score": metrics["agreement_score"],
        "top1_match": metrics["top1_match"],
        "top_k_overlap": metrics["top_k_overlap"],
        "evaluated_at": metrics["evaluated_at"],
        "task_packet": dict(task_packet) if isinstance(task_packet, dict) else {},
    }
    artifact_path = _write_eval_artifact(artifact_payload)
    metrics["artifact_path"] = str(artifact_path)
    print(
        "[MODEL_EVAL] "
        f"active={active_model} candidate={candidate_model} "
        f"agreement={metrics['agreement_score']:.2f} top1_match={str(top1_match).lower()}"
    )
    return metrics
