from __future__ import annotations

import json
import math
import re
from typing import Any


SUPPORTED_SCHEMA_VERSION = "v1"
SUPPORTED_OUTPUT_TYPES = {"ranking", "classification", "scoring"}
_REQUIRED_TOP_LEVEL_KEYS = {"schema_version", "model_id", "output_type", "items"}
_REQUIRED_ITEM_KEYS = {"entity_id", "score", "confidence"}
_STABLE_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:-]+$")


class ModelOutputContractError(RuntimeError):
    """Raised when model output fails DF contract validation."""


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _stable_identifier(value: object, *, field_name: str) -> str:
    normalized = _normalize_text(value)
    if not normalized:
        raise ModelOutputContractError(f"{field_name} must not be empty.")
    if _STABLE_ID_PATTERN.fullmatch(normalized) is None:
        raise ModelOutputContractError(f"{field_name} must use a stable identifier.")
    return normalized


def _bounded_float(value: object, *, field_name: str) -> float:
    try:
        normalized = float(value)
    except (TypeError, ValueError) as exc:
        raise ModelOutputContractError(f"{field_name} must be numeric.") from exc
    if not math.isfinite(normalized):
        raise ModelOutputContractError(f"{field_name} must be finite.")
    if normalized < 0.0 or normalized > 1.0:
        raise ModelOutputContractError(f"{field_name} must be within [0, 1].")
    return round(normalized, 6)


def _validate_top_level_keys(payload: dict[str, Any]) -> None:
    extra_keys = set(payload) - _REQUIRED_TOP_LEVEL_KEYS
    missing_keys = _REQUIRED_TOP_LEVEL_KEYS - set(payload)
    if missing_keys:
        missing = ", ".join(sorted(missing_keys))
        raise ModelOutputContractError(
            f"model_output missing required fields: {missing}"
        )
    if extra_keys:
        extras = ", ".join(sorted(extra_keys))
        raise ModelOutputContractError(
            f"model_output contains unsupported fields: {extras}"
        )


def _validate_item_keys(item: dict[str, Any]) -> None:
    extra_keys = set(item) - _REQUIRED_ITEM_KEYS
    missing_keys = _REQUIRED_ITEM_KEYS - set(item)
    if missing_keys:
        missing = ", ".join(sorted(missing_keys))
        raise ModelOutputContractError(
            f"model_output item missing required fields: {missing}"
        )
    if extra_keys:
        extras = ", ".join(sorted(extra_keys))
        raise ModelOutputContractError(
            f"model_output item contains unsupported fields: {extras}"
        )


def normalize_model_output(
    payload: object,
    *,
    expected_model_id: str | None = None,
    allowed_output_types: set[str] | None = None,
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ModelOutputContractError("model_output must be a JSON object.")
    _validate_top_level_keys(payload)

    schema_version = _normalize_text(payload.get("schema_version"))
    if schema_version != SUPPORTED_SCHEMA_VERSION:
        raise ModelOutputContractError(
            f"unsupported schema_version: {schema_version or '<empty>'}"
        )

    model_id = _stable_identifier(payload.get("model_id"), field_name="model_id")
    expected = _normalize_text(expected_model_id)
    if expected and model_id != expected:
        raise ModelOutputContractError(
            "model_output model_id does not match expected model."
        )

    output_type = _normalize_text(payload.get("output_type")).lower()
    if output_type not in SUPPORTED_OUTPUT_TYPES:
        raise ModelOutputContractError(
            f"unsupported output_type: {output_type or '<empty>'}"
        )
    if allowed_output_types is not None and output_type not in allowed_output_types:
        raise ModelOutputContractError(
            f"output_type is not allowed here: {output_type}"
        )

    raw_items = payload.get("items")
    if not isinstance(raw_items, list):
        raise ModelOutputContractError("items must be a list.")

    deduplicated: dict[str, dict[str, Any]] = {}
    for raw_item in raw_items:
        if not isinstance(raw_item, dict):
            raise ModelOutputContractError("items must contain JSON objects.")
        _validate_item_keys(raw_item)
        entity_id = _stable_identifier(
            raw_item.get("entity_id"), field_name="entity_id"
        )
        normalized_item = {
            "entity_id": entity_id,
            "score": _bounded_float(raw_item.get("score"), field_name="score"),
            "confidence": _bounded_float(
                raw_item.get("confidence"), field_name="confidence"
            ),
        }
        current_item = deduplicated.get(entity_id)
        if current_item is None:
            deduplicated[entity_id] = normalized_item
            continue
        current_sort = (
            float(current_item["score"]),
            float(current_item["confidence"]),
            str(current_item["entity_id"]),
        )
        candidate_sort = (
            float(normalized_item["score"]),
            float(normalized_item["confidence"]),
            str(normalized_item["entity_id"]),
        )
        if candidate_sort > current_sort:
            deduplicated[entity_id] = normalized_item

    normalized_items = sorted(
        deduplicated.values(),
        key=lambda item: (
            -float(item["score"]),
            -float(item["confidence"]),
            str(item["entity_id"]),
        ),
    )
    return {
        "schema_version": SUPPORTED_SCHEMA_VERSION,
        "model_id": model_id,
        "output_type": output_type,
        "items": normalized_items,
    }


def build_model_output(
    *,
    model_id: object,
    output_type: object,
    items: list[dict[str, Any]] | None,
    schema_version: object = SUPPORTED_SCHEMA_VERSION,
) -> dict[str, Any]:
    return normalize_model_output(
        {
            "schema_version": _normalize_text(schema_version),
            "model_id": _normalize_text(model_id),
            "output_type": _normalize_text(output_type).lower(),
            "items": list(items or []),
        },
        expected_model_id=_normalize_text(model_id),
        allowed_output_types=SUPPORTED_OUTPUT_TYPES,
    )


def model_output_json(payload: dict[str, Any]) -> str:
    normalized = normalize_model_output(payload)
    return json.dumps(normalized, indent=2, sort_keys=True) + "\n"
