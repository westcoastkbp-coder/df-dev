from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from uuid import uuid4


REPO_ROOT = Path(__file__).resolve().parents[2]
MODEL_UPDATE_CONFIG_FILE = REPO_ROOT / "config" / "model_update.json"
DEFAULT_MODEL_UPDATE_CONFIG = {
    "active_model": "memory_ranker_v1",
    "candidate_model": "",
    "evaluation_mode": True,
    "evaluation_sample_rate": 0.2,
    "promotion_rules": {
        "min_agreement": 0.7,
        "min_top1_match": 0.6,
        "min_overlap": 0.7,
        "min_samples": 20,
    },
    "memory_ranking": {
        "min_new_records": 50,
        "last_dataset_size": 0,
    },
}


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _bounded_float(value: object, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = float(default)
    return max(0.0, min(1.0, parsed))


def _non_negative_int(value: object, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(0, parsed)


def load_model_update_config() -> dict[str, Any]:
    config = json.loads(json.dumps(DEFAULT_MODEL_UPDATE_CONFIG))
    if not MODEL_UPDATE_CONFIG_FILE.exists():
        return config
    try:
        payload = json.loads(MODEL_UPDATE_CONFIG_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return config
    if not isinstance(payload, dict):
        return config

    config["active_model"] = _normalize_text(payload.get("active_model")) or config["active_model"]
    config["candidate_model"] = _normalize_text(payload.get("candidate_model"))
    config["evaluation_mode"] = bool(payload.get("evaluation_mode", config["evaluation_mode"]))
    config["evaluation_sample_rate"] = _bounded_float(
        payload.get("evaluation_sample_rate", config["evaluation_sample_rate"]),
        float(config["evaluation_sample_rate"]),
    )

    raw_promotion_rules = payload.get("promotion_rules")
    if isinstance(raw_promotion_rules, dict):
        config["promotion_rules"] = {
            "min_agreement": _bounded_float(
                raw_promotion_rules.get("min_agreement"),
                config["promotion_rules"]["min_agreement"],
            ),
            "min_top1_match": _bounded_float(
                raw_promotion_rules.get("min_top1_match"),
                config["promotion_rules"]["min_top1_match"],
            ),
            "min_overlap": _bounded_float(
                raw_promotion_rules.get("min_overlap"),
                config["promotion_rules"]["min_overlap"],
            ),
            "min_samples": _non_negative_int(
                raw_promotion_rules.get("min_samples"),
                config["promotion_rules"]["min_samples"],
            ),
        }

    for raw_dataset_type, raw_settings in payload.items():
        normalized_type = _normalize_text(raw_dataset_type)
        if normalized_type in {
            "active_model",
            "candidate_model",
            "evaluation_mode",
            "evaluation_sample_rate",
            "promotion_rules",
        }:
            continue
        if not isinstance(raw_settings, dict):
            continue
        dataset_settings = dict(config.get(normalized_type) or {"min_new_records": 50, "last_dataset_size": 0})
        dataset_settings["min_new_records"] = _non_negative_int(raw_settings.get("min_new_records", 50), 50)
        dataset_settings["last_dataset_size"] = _non_negative_int(raw_settings.get("last_dataset_size", 0), 0)
        config[normalized_type] = dataset_settings

    return config


def write_model_update_config(config: dict[str, Any]) -> None:
    MODEL_UPDATE_CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(config, indent=2, sort_keys=True) + "\n"
    temp_path = MODEL_UPDATE_CONFIG_FILE.with_name(f".{MODEL_UPDATE_CONFIG_FILE.name}.{uuid4().hex}.tmp")
    try:
        temp_path.write_text(payload, encoding="utf-8")
        os.replace(temp_path, MODEL_UPDATE_CONFIG_FILE)
    finally:
        if temp_path.exists():
            temp_path.unlink()
