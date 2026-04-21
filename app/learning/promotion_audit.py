from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4


REPO_ROOT = Path(__file__).resolve().parents[2]
PROMOTION_AUDITS_ROOT = REPO_ROOT / "DF" / "shared" / "promotion_audits"
MODELS_ROOT = REPO_ROOT / "DF" / "shared" / "models"
VALID_STATUSES = {"pending", "committed", "failed"}


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_status(value: object) -> str:
    normalized = _normalize_text(value).lower()
    if normalized not in VALID_STATUSES:
        raise ValueError(f"unsupported promotion audit status: {value!r}")
    return normalized


def _utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _canonical_json(payload: object) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def path_ref(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def _record_path(record: dict[str, Any]) -> Path:
    return PROMOTION_AUDITS_ROOT / f"{_normalize_text(record.get('promotion_id'))}.json"


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temp_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        os.replace(temp_path, path)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def config_version(config: dict[str, Any]) -> str:
    return (
        f"sha256:{hashlib.sha256(_canonical_json(config).encode('utf-8')).hexdigest()}"
    )


def candidate_model_artifact_refs(candidate_model: str) -> list[str]:
    normalized_candidate = _normalize_text(candidate_model)
    if not normalized_candidate:
        return []

    candidate_path = MODELS_ROOT / f"{normalized_candidate}.json"
    if not candidate_path.exists():
        return []
    return [path_ref(candidate_path)]


def build_promotion_audit_record(
    *,
    active_model: str,
    candidate_model: str,
    accepted: bool,
    reason: str,
    metrics: dict[str, Any],
    thresholds: dict[str, Any],
    evaluation_refs: list[str],
    config_version_before: str,
    config_version_after: str,
    artifact_refs: list[str] | None = None,
    status: str,
    promotion_id: str | None = None,
    timestamp: str | None = None,
) -> dict[str, Any]:
    normalized_status = _normalize_status(status)
    record_timestamp = _normalize_text(timestamp) or _utc_timestamp()
    resolved_promotion_id = _normalize_text(promotion_id)
    if not resolved_promotion_id:
        promotion_seed = {
            "timestamp": record_timestamp,
            "active_model": active_model,
            "candidate_model": candidate_model,
            "decision": "accepted" if accepted else "rejected",
            "reason": reason,
            "config_version_before": config_version_before,
            "config_version_after": config_version_after,
        }
        digest = hashlib.sha256(
            _canonical_json(promotion_seed).encode("utf-8")
        ).hexdigest()[:12]
        resolved_promotion_id = f"promotion-{record_timestamp.replace('-', '').replace(':', '').replace('.', '')}-{digest}"

    return {
        "promotion_id": resolved_promotion_id,
        "timestamp": record_timestamp,
        "status": normalized_status,
        "active_model": active_model,
        "candidate_model": candidate_model,
        "decision": "accepted" if accepted else "rejected",
        "reason": _normalize_text(reason),
        "metrics": {
            "agreement_score": metrics.get("agreement_score"),
            "top1_match": metrics.get("top1_match"),
            "top_k_overlap": metrics.get("top_k_overlap"),
            "sample_count": int(metrics.get("sample_count", 0) or 0),
        },
        "thresholds": {
            "min_agreement_score": thresholds.get("min_agreement_score"),
            "min_top1_match": thresholds.get("min_top1_match"),
            "min_top_k_overlap": thresholds.get("min_top_k_overlap"),
            "min_samples": int(thresholds.get("min_samples", 0) or 0),
        },
        "evaluation_refs": list(evaluation_refs),
        "config_version_before": _normalize_text(config_version_before),
        "config_version_after": _normalize_text(config_version_after),
        "artifact_refs": list(artifact_refs or []),
    }


def persist_promotion_audit_record(record: dict[str, Any]) -> Path:
    target_path = _record_path(record)
    if target_path.exists():
        raise FileExistsError(f"promotion audit already exists: {target_path}")
    _atomic_write_json(target_path, record)
    return target_path


def replace_promotion_audit_record(record: dict[str, Any]) -> Path:
    target_path = _record_path(record)
    if not target_path.exists():
        raise FileNotFoundError(f"promotion audit does not exist: {target_path}")
    _atomic_write_json(target_path, record)
    return target_path


def read_promotion_audit_records(*, status: str | None = None) -> list[dict[str, Any]]:
    if not PROMOTION_AUDITS_ROOT.exists():
        return []

    normalized_status = _normalize_status(status) if status is not None else None
    records: list[dict[str, Any]] = []
    for path in sorted(PROMOTION_AUDITS_ROOT.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        if (
            normalized_status is not None
            and _normalize_text(payload.get("status")).lower() != normalized_status
        ):
            continue
        records.append(payload)
    return records


def read_committed_promotion_audits() -> list[dict[str, Any]]:
    return read_promotion_audit_records(status="committed")


def read_pending_promotion_audits() -> list[dict[str, Any]]:
    return read_promotion_audit_records(status="pending")


__all__ = [
    "PROMOTION_AUDITS_ROOT",
    "build_promotion_audit_record",
    "candidate_model_artifact_refs",
    "config_version",
    "path_ref",
    "persist_promotion_audit_record",
    "read_committed_promotion_audits",
    "read_pending_promotion_audits",
    "read_promotion_audit_records",
    "replace_promotion_audit_record",
]
