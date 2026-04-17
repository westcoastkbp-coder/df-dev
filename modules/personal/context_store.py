from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
PERSONAL_CONTEXT_TASK_TYPE = "personal_context_update"
DEFAULT_PERSONAL_CONTEXT_PATH = REPO_ROOT / "personal" / "personal_context.json"

_DEFAULT_PERSONAL_CONTEXT = {
    "owner": {
        "name": "",
        "notes": "",
    },
    "vehicles": [],
    "dmv": [],
    "immigration": [],
    "reminders": [],
}


def default_personal_context() -> dict[str, Any]:
    return copy.deepcopy(_DEFAULT_PERSONAL_CONTEXT)


def _coerce_text(value: Any) -> str:
    return "" if value is None else str(value)


def _coerce_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return [copy.deepcopy(item) for item in value]
    return [copy.deepcopy(value)]


def _normalize_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _normalize_json_value(item)
            for key, item in sorted(value.items(), key=lambda entry: str(entry[0]))
        }
    if isinstance(value, list):
        return [_normalize_json_value(item) for item in value]
    return copy.deepcopy(value)


def _normalize_owner(value: Any) -> dict[str, str]:
    record = value if isinstance(value, dict) else {}
    return {
        "name": _coerce_text(record.get("name")),
        "notes": _coerce_text(record.get("notes")),
    }


def _normalize_vehicle(value: Any) -> dict[str, Any]:
    record = value if isinstance(value, dict) else {}
    return {
        "name": _coerce_text(record.get("name")),
        "model": _coerce_text(record.get("model")),
        "year": _coerce_text(record.get("year")),
        "vin": _coerce_text(record.get("vin")),
        "maintenance": _merge_generic_list([], record.get("maintenance", [])),
    }


def normalize_personal_context(value: Any) -> dict[str, Any]:
    source = value if isinstance(value, dict) else {}
    return {
        "owner": _normalize_owner(source.get("owner")),
        "vehicles": [
            _normalize_vehicle(vehicle)
            for vehicle in _coerce_list(source.get("vehicles"))
            if isinstance(vehicle, dict)
        ],
        "dmv": _merge_generic_list([], source.get("dmv", [])),
        "immigration": _merge_generic_list([], source.get("immigration", [])),
        "reminders": _merge_generic_list([], source.get("reminders", [])),
    }


def load_personal_context(
    context_path: Path | str | None = None,
) -> dict[str, Any]:
    path = Path(context_path) if context_path is not None else DEFAULT_PERSONAL_CONTEXT_PATH
    if not path.exists():
        return default_personal_context()
    return normalize_personal_context(json.loads(path.read_text(encoding="utf-8")))


def save_personal_context(
    context: dict[str, Any],
    context_path: Path | str | None = None,
) -> Path:
    path = Path(context_path) if context_path is not None else DEFAULT_PERSONAL_CONTEXT_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = normalize_personal_context(context)
    path.write_text(
        json.dumps(normalized, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def _vehicle_identity(vehicle: dict[str, Any]) -> tuple[str, str]:
    vin = _coerce_text(vehicle.get("vin")).strip()
    if vin:
        return ("vin", vin.upper())

    composite = "|".join(
        (
            _coerce_text(vehicle.get("name")).strip().lower(),
            _coerce_text(vehicle.get("model")).strip().lower(),
            _coerce_text(vehicle.get("year")).strip().lower(),
        )
    )
    return ("composite", composite)


def _generic_identity(item: Any) -> tuple[str, str] | None:
    if isinstance(item, dict):
        for key in ("id", "record_id"):
            value = _coerce_text(item.get(key)).strip()
            if value:
                return (key, value)
        signature = [
            _coerce_text(item.get(key)).strip().lower()
            for key in ("type", "name", "title", "date", "due_date", "expires_on")
        ]
        joined = "|".join(signature).strip("|")
        if joined:
            return ("fields", joined)
    return ("json", json.dumps(_normalize_json_value(item), sort_keys=True))


def _merge_json_dict(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = dict(existing)
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_json_dict(dict(merged[key]), value)
            continue
        if isinstance(value, list) and isinstance(merged.get(key), list):
            merged[key] = _merge_generic_list(list(merged[key]), value)
            continue
        merged[key] = _normalize_json_value(value)
    return {
        str(key): _normalize_json_value(item)
        for key, item in sorted(merged.items(), key=lambda entry: str(entry[0]))
    }


def _merge_generic_list(existing: Any, incoming: Any) -> list[Any]:
    merged = [_normalize_json_value(item) for item in _coerce_list(existing)]
    for item in _coerce_list(incoming):
        normalized_item = _normalize_json_value(item)
        identity = _generic_identity(normalized_item)
        matched_index = next(
            (
                index
                for index, existing_item in enumerate(merged)
                if _generic_identity(existing_item) == identity
            ),
            None,
        )
        if matched_index is None:
            merged.append(normalized_item)
            continue
        existing_item = merged[matched_index]
        if isinstance(existing_item, dict) and isinstance(normalized_item, dict):
            merged[matched_index] = _merge_json_dict(existing_item, normalized_item)
        else:
            merged[matched_index] = normalized_item
    return merged


def _merge_owner(existing: dict[str, Any], incoming: Any) -> dict[str, str]:
    merged = _normalize_owner(existing)
    if not isinstance(incoming, dict):
        return merged

    for key in ("name", "notes"):
        if key in incoming:
            merged[key] = _coerce_text(incoming.get(key))
    return merged


def _merge_vehicles(existing: list[Any], incoming: Any) -> list[dict[str, Any]]:
    merged = [_normalize_vehicle(item) for item in _coerce_list(existing) if isinstance(item, dict)]
    for vehicle_update in _coerce_list(incoming):
        if not isinstance(vehicle_update, dict):
            continue

        match_identity = _vehicle_identity(vehicle_update)
        matched_index = next(
            (
                index
                for index, vehicle in enumerate(merged)
                if _vehicle_identity(vehicle) == match_identity
            ),
            None,
        )

        if matched_index is None:
            next_vehicle = _normalize_vehicle(vehicle_update)
            merged.append(next_vehicle)
            continue

        current_vehicle = dict(merged[matched_index])
        for key in ("name", "model", "year", "vin"):
            if key in vehicle_update:
                current_vehicle[key] = _coerce_text(vehicle_update.get(key))
        if "maintenance" in vehicle_update:
            current_vehicle["maintenance"] = _merge_generic_list(
                current_vehicle.get("maintenance", []),
                vehicle_update.get("maintenance"),
            )
        merged[matched_index] = _normalize_vehicle(current_vehicle)
    return merged


def apply_personal_context_update(
    existing_context: dict[str, Any] | None,
    update_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    context = normalize_personal_context(existing_context)
    update = update_payload if isinstance(update_payload, dict) else {}

    if "owner" in update:
        context["owner"] = _merge_owner(context.get("owner", {}), update.get("owner"))
    if "vehicles" in update:
        context["vehicles"] = _merge_vehicles(context.get("vehicles", []), update.get("vehicles"))
    for key in ("dmv", "immigration", "reminders"):
        if key in update:
            context[key] = _merge_generic_list(context.get(key, []), update.get(key))

    return normalize_personal_context(context)


def extract_personal_context_update(source: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(source, dict):
        return {}
    if isinstance(source.get("personal_context_update"), dict):
        return dict(source["personal_context_update"])
    return {
        key: copy.deepcopy(source[key])
        for key in ("owner", "vehicles", "dmv", "immigration", "reminders")
        if key in source
    }


def update_personal_context_file(
    update_payload: dict[str, Any] | None,
    *,
    context_path: Path | str | None = None,
) -> tuple[dict[str, Any], Path]:
    path = Path(context_path) if context_path is not None else DEFAULT_PERSONAL_CONTEXT_PATH
    current_context = load_personal_context(path)
    updated_context = apply_personal_context_update(current_context, update_payload)
    saved_path = save_personal_context(updated_context, path)
    return updated_context, saved_path
