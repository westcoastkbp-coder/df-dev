from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.learning.model_update_config import load_model_update_config, write_model_update_config
from app.learning.promotion_audit import (
    build_promotion_audit_record,
    candidate_model_artifact_refs,
    config_version,
    path_ref,
    persist_promotion_audit_record,
    replace_promotion_audit_record,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
EVALS_ROOT = REPO_ROOT / "DF" / "shared" / "evals"
REQUIRED_METRICS = ("agreement_score", "top1_match", "top_k_overlap")


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _deep_copy(value: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(value))


def _empty_metrics(*, active_model: str, candidate_model: str, sample_count: int = 0) -> dict[str, Any]:
    return {
        "active_model": active_model,
        "candidate_model": candidate_model,
        "agreement_score": None,
        "top1_match": None,
        "top_k_overlap": None,
        "sample_count": sample_count,
    }


def _decision(
    *,
    promote: bool,
    reason: str,
    metrics: dict[str, Any],
) -> dict[str, Any]:
    return {
        "promote": promote,
        "reason": reason,
        "metrics": metrics,
    }


def _promotion_rules(config: dict[str, Any]) -> dict[str, Any]:
    raw_rules = config.get("promotion_rules")
    if not isinstance(raw_rules, dict):
        return {}
    return raw_rules


def _thresholds(rules: dict[str, Any]) -> dict[str, Any]:
    return {
        "min_agreement_score": float(rules.get("min_agreement", 0.7)),
        "min_top1_match": float(rules.get("min_top1_match", 0.6)),
        "min_top_k_overlap": float(rules.get("min_overlap", 0.7)),
        "min_samples": int(rules.get("min_samples", 20)),
    }


def _load_relevant_artifacts(
    evals_root: Path,
    *,
    active_model: str,
    candidate_model: str,
) -> tuple[list[dict[str, Any]], list[str], bool]:
    if not evals_root.exists():
        return [], [], False

    relevant_artifacts: list[dict[str, Any]] = []
    evaluation_refs: list[str] = []
    missing_metrics = False
    for artifact_path in sorted(evals_root.glob("*.json")):
        try:
            payload = json.loads(artifact_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        if _normalize_text(payload.get("active_model")) != active_model:
            continue
        if _normalize_text(payload.get("candidate_model")) != candidate_model:
            continue
        evaluation_refs.append(path_ref(artifact_path))
        if any(metric_name not in payload for metric_name in REQUIRED_METRICS):
            missing_metrics = True
            continue
        relevant_artifacts.append(payload)
    return relevant_artifacts, evaluation_refs, missing_metrics


def _float_metric(payload: dict[str, Any], metric_name: str) -> float | None:
    try:
        value = float(payload.get(metric_name))
    except (TypeError, ValueError):
        return None
    if not 0.0 <= value <= 1.0:
        return None
    return value


def _top1_metric(payload: dict[str, Any]) -> float | None:
    value = payload.get("top1_match")
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)) and value in {0, 1, 0.0, 1.0}:
        return float(value)
    return None


def _evaluate_promotion_details(config: dict[str, Any]) -> dict[str, Any]:
    active_model = _normalize_text(config.get("active_model"))
    candidate_model = _normalize_text(config.get("candidate_model"))
    metrics = _empty_metrics(active_model=active_model, candidate_model=candidate_model)
    thresholds = _thresholds(_promotion_rules(config))

    if not candidate_model:
        return {
            "decision": _decision(promote=False, reason="missing_candidate_model", metrics=metrics),
            "thresholds": thresholds,
            "evaluation_refs": [],
        }
    if not active_model:
        return {
            "decision": _decision(promote=False, reason="missing_active_model", metrics=metrics),
            "thresholds": thresholds,
            "evaluation_refs": [],
        }
    if active_model == candidate_model:
        return {
            "decision": _decision(promote=False, reason="candidate_matches_active", metrics=metrics),
            "thresholds": thresholds,
            "evaluation_refs": [],
        }

    relevant_artifacts, evaluation_refs, missing_metrics = _load_relevant_artifacts(
        EVALS_ROOT,
        active_model=active_model,
        candidate_model=candidate_model,
    )
    metrics["sample_count"] = len(relevant_artifacts)
    if missing_metrics:
        return {
            "decision": _decision(promote=False, reason="missing_metrics", metrics=metrics),
            "thresholds": thresholds,
            "evaluation_refs": evaluation_refs,
        }

    agreement_scores: list[float] = []
    top1_scores: list[float] = []
    overlap_scores: list[float] = []
    for artifact in relevant_artifacts:
        agreement_score = _float_metric(artifact, "agreement_score")
        top1_score = _top1_metric(artifact)
        overlap_score = _float_metric(artifact, "top_k_overlap")
        if agreement_score is None or top1_score is None or overlap_score is None:
            return {
                "decision": _decision(promote=False, reason="missing_metrics", metrics=metrics),
                "thresholds": thresholds,
                "evaluation_refs": evaluation_refs,
            }
        agreement_scores.append(agreement_score)
        top1_scores.append(top1_score)
        overlap_scores.append(overlap_score)

    if len(relevant_artifacts) < int(thresholds["min_samples"]):
        return {
            "decision": _decision(promote=False, reason="low_samples", metrics=metrics),
            "thresholds": thresholds,
            "evaluation_refs": evaluation_refs,
        }

    metrics["agreement_score"] = round(sum(agreement_scores) / len(agreement_scores), 6)
    metrics["top1_match"] = round(sum(top1_scores) / len(top1_scores), 6)
    metrics["top_k_overlap"] = round(sum(overlap_scores) / len(overlap_scores), 6)

    if metrics["agreement_score"] < float(thresholds["min_agreement_score"]):
        return {
            "decision": _decision(promote=False, reason="low_agreement", metrics=metrics),
            "thresholds": thresholds,
            "evaluation_refs": evaluation_refs,
        }
    if metrics["top1_match"] < float(thresholds["min_top1_match"]):
        return {
            "decision": _decision(promote=False, reason="low_top1_match", metrics=metrics),
            "thresholds": thresholds,
            "evaluation_refs": evaluation_refs,
        }
    if metrics["top_k_overlap"] < float(thresholds["min_top_k_overlap"]):
        return {
            "decision": _decision(promote=False, reason="low_overlap", metrics=metrics),
            "thresholds": thresholds,
            "evaluation_refs": evaluation_refs,
        }

    return {
        "decision": _decision(promote=True, reason="thresholds_met", metrics=metrics),
        "thresholds": thresholds,
        "evaluation_refs": evaluation_refs,
    }


def _build_audit_record(
    *,
    decision: dict[str, Any],
    thresholds: dict[str, Any],
    evaluation_refs: list[str],
    config_version_before: str,
    config_version_after: str,
    status: str,
    promotion_id: str | None = None,
    timestamp: str | None = None,
) -> dict[str, Any]:
    return build_promotion_audit_record(
        active_model=_normalize_text(decision["metrics"].get("active_model")),
        candidate_model=_normalize_text(decision["metrics"].get("candidate_model")),
        accepted=bool(decision.get("promote")),
        reason=_normalize_text(decision.get("reason")),
        metrics=decision["metrics"],
        thresholds=thresholds,
        evaluation_refs=evaluation_refs,
        config_version_before=config_version_before,
        config_version_after=config_version_after,
        artifact_refs=candidate_model_artifact_refs(_normalize_text(decision["metrics"].get("candidate_model"))),
        status=status,
        promotion_id=promotion_id,
        timestamp=timestamp,
    )


def evaluate_promotion() -> dict[str, Any]:
    return _evaluate_promotion_details(load_model_update_config())["decision"]


def promote_model() -> dict[str, Any]:
    config_before = load_model_update_config()
    details = _evaluate_promotion_details(config_before)
    decision = details["decision"]
    before_version = config_version(config_before)

    if not bool(decision.get("promote")):
        audit_record = _build_audit_record(
            decision=decision,
            thresholds=details["thresholds"],
            evaluation_refs=details["evaluation_refs"],
            config_version_before=before_version,
            config_version_after=before_version,
            status="committed",
        )
        persist_promotion_audit_record(audit_record)
        print(f"[MODEL_PROMOTION] rejected → reason={decision['reason']}")
        return decision

    config_after = _deep_copy(config_before)
    candidate_model = _normalize_text(config_before.get("candidate_model"))
    config_after["active_model"] = candidate_model
    config_after["candidate_model"] = ""
    after_version = config_version(config_after)

    pending_record = _build_audit_record(
        decision=decision,
        thresholds=details["thresholds"],
        evaluation_refs=details["evaluation_refs"],
        config_version_before=before_version,
        config_version_after=before_version,
        status="pending",
    )
    persist_promotion_audit_record(pending_record)

    try:
        write_model_update_config(config_after)
    except Exception:
        failed_record = _build_audit_record(
            decision=decision,
            thresholds=details["thresholds"],
            evaluation_refs=details["evaluation_refs"],
            config_version_before=before_version,
            config_version_after=before_version,
            status="failed",
            promotion_id=_normalize_text(pending_record.get("promotion_id")),
            timestamp=_normalize_text(pending_record.get("timestamp")),
        )
        replace_promotion_audit_record(failed_record)
        raise

    try:
        committed_record = _build_audit_record(
            decision=decision,
            thresholds=details["thresholds"],
            evaluation_refs=details["evaluation_refs"],
            config_version_before=before_version,
            config_version_after=after_version,
            status="committed",
            promotion_id=_normalize_text(pending_record.get("promotion_id")),
            timestamp=_normalize_text(pending_record.get("timestamp")),
        )
        replace_promotion_audit_record(committed_record)
    except Exception:
        write_model_update_config(config_before)
        failed_record = _build_audit_record(
            decision=decision,
            thresholds=details["thresholds"],
            evaluation_refs=details["evaluation_refs"],
            config_version_before=before_version,
            config_version_after=before_version,
            status="failed",
            promotion_id=_normalize_text(pending_record.get("promotion_id")),
            timestamp=_normalize_text(pending_record.get("timestamp")),
        )
        replace_promotion_audit_record(failed_record)
        raise

    print(f"[MODEL_PROMOTION] accepted → {candidate_model}")
    return decision
