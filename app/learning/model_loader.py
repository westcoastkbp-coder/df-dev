from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
MODELS_ROOT = REPO_ROOT / "DF" / "shared" / "models"
REQUIRED_MODEL_FEATURES = (
    "recency",
    "tag_overlap",
    "domain_match",
    "memory_class",
    "conflict_flag",
    "state_flag",
)


class ModelLoaderError(RuntimeError):
    """Raised when a stored ranking model cannot be loaded or validated."""


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _safe_model_id(model_id: object) -> str:
    normalized = _normalize_text(model_id)
    if not normalized:
        raise ModelLoaderError("model_id must not be empty.")
    return normalized


def _parse_timestamp(value: object) -> str:
    normalized = _normalize_text(value)
    if not normalized:
        raise ModelLoaderError("created_at must not be empty.")
    try:
        datetime.fromisoformat(normalized.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError as exc:
        raise ModelLoaderError("created_at must be a valid ISO-8601 timestamp.") from exc
    return normalized


def model_artifact_path(model_id: object) -> Path:
    return MODELS_ROOT / f"{_safe_model_id(model_id)}.json"


def _validate_features(value: object) -> list[str]:
    if not isinstance(value, list):
        raise ModelLoaderError("features must be a list.")
    features = [_normalize_text(item) for item in value]
    if any(not feature for feature in features):
        raise ModelLoaderError("features must not contain empty values.")
    if len(features) != len(set(features)):
        raise ModelLoaderError("features must not contain duplicates.")
    if tuple(features) != REQUIRED_MODEL_FEATURES:
        raise ModelLoaderError(
            "features must match the required memory ranker feature order."
        )
    return features


def _validate_weights(value: object) -> dict[str, float]:
    if not isinstance(value, dict):
        raise ModelLoaderError("weights must be a JSON object.")
    weights: dict[str, float] = {}
    for feature_name in REQUIRED_MODEL_FEATURES:
        if feature_name not in value:
            raise ModelLoaderError(f"weights missing required feature '{feature_name}'.")
        raw_weight = value[feature_name]
        try:
            normalized_weight = float(raw_weight)
        except (TypeError, ValueError) as exc:
            raise ModelLoaderError(
                f"weight '{feature_name}' must be numeric."
            ) from exc
        if not math.isfinite(normalized_weight):
            raise ModelLoaderError(
                f"weight '{feature_name}' must be finite."
            )
        weights[feature_name] = normalized_weight
    extra_keys = set(value) - set(REQUIRED_MODEL_FEATURES)
    if extra_keys:
        extras = ", ".join(sorted(str(item) for item in extra_keys))
        raise ModelLoaderError(f"weights contains unsupported features: {extras}")
    return weights


def load_model(model_id: object) -> dict[str, Any]:
    normalized_model_id = _safe_model_id(model_id)
    model_path = model_artifact_path(normalized_model_id)
    try:
        payload = json.loads(model_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ModelLoaderError(f"model not found: {normalized_model_id}") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise ModelLoaderError(f"model is not readable: {normalized_model_id}") from exc

    if not isinstance(payload, dict):
        raise ModelLoaderError("model artifact must contain a JSON object.")

    normalized_type = _normalize_text(payload.get("model_type"))
    if normalized_type != "memory_ranker":
        raise ModelLoaderError("model_type must be 'memory_ranker'.")

    try:
        version = int(payload.get("version"))
    except (TypeError, ValueError) as exc:
        raise ModelLoaderError("version must be an integer.") from exc
    if version != 1:
        raise ModelLoaderError("version must be 1.")

    artifact_model_id = _safe_model_id(payload.get("model_id"))
    if artifact_model_id != normalized_model_id:
        raise ModelLoaderError("model artifact id does not match requested model_id.")

    model = {
        "model_id": artifact_model_id,
        "model_type": normalized_type,
        "version": version,
        "created_at": _parse_timestamp(payload.get("created_at")),
        "features": _validate_features(payload.get("features")),
        "weights": _validate_weights(payload.get("weights")),
    }
    return model
