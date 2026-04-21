from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
CONTEXT_DIR = REPO_ROOT / "context"
SYSTEM_CONTEXT_FILE = CONTEXT_DIR / "system_context.json"
OWNER_CONTEXT_FILE = CONTEXT_DIR / "owner_context.json"
BUSINESS_CONTEXT_FILE = CONTEXT_DIR / "business_context.json"
ALLOWED_MODES = {"dev", "owner", "business"}

DEFAULT_SYSTEM_CONTEXT = {
    "active_mode": "dev",
    "current_focus": "",
    "last_update": "",
}
DEFAULT_OWNER_CONTEXT = {
    "identity": {},
    "immigration": {},
    "permits": {},
    "notes": "",
}
DEFAULT_BUSINESS_CONTEXT = {
    "projects": [],
    "clients": [],
    "status": "",
}
DEFAULT_CONTEXTS = {
    "system_context": DEFAULT_SYSTEM_CONTEXT,
    "owner_context": DEFAULT_OWNER_CONTEXT,
    "business_context": DEFAULT_BUSINESS_CONTEXT,
}
CONTEXT_FILES = {
    "system_context": SYSTEM_CONTEXT_FILE,
    "owner_context": OWNER_CONTEXT_FILE,
    "business_context": BUSINESS_CONTEXT_FILE,
}


def _utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _resolve_context_name(context_name: str) -> str:
    normalized = str(context_name or "").strip().lower()
    aliases = {
        "system": "system_context",
        "system_context": "system_context",
        "owner": "owner_context",
        "owner_context": "owner_context",
        "business": "business_context",
        "business_context": "business_context",
    }
    resolved = aliases.get(normalized, "")
    if not resolved:
        raise ValueError("INVALID_CONTEXT_NAME")
    return resolved


def _context_path(context_name: str, context_dir: Path | str | None = None) -> Path:
    resolved_name = _resolve_context_name(context_name)
    if context_dir is None:
        return CONTEXT_FILES[resolved_name]
    return Path(context_dir) / f"{resolved_name}.json"


def _default_payload(context_name: str) -> dict[str, Any]:
    return deepcopy(DEFAULT_CONTEXTS[_resolve_context_name(context_name)])


def _load_single_context(
    context_name: str,
    *,
    context_dir: Path | str | None = None,
) -> dict[str, Any]:
    path = _context_path(context_name, context_dir)
    default_payload = _default_payload(context_name)

    if not path.is_file():
        _write_single_context(context_name, default_payload, context_dir=context_dir)
        return default_payload

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        _write_single_context(context_name, default_payload, context_dir=context_dir)
        return default_payload

    if not isinstance(payload, dict):
        _write_single_context(context_name, default_payload, context_dir=context_dir)
        return default_payload
    return payload


def _write_single_context(
    context_name: str,
    payload: dict[str, Any],
    *,
    context_dir: Path | str | None = None,
) -> Path:
    path = _context_path(context_name, context_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return path


def load_context(context_dir: Path | str | None = None) -> dict[str, dict[str, Any]]:
    return {
        "system_context": _load_single_context(
            "system_context", context_dir=context_dir
        ),
        "owner_context": _load_single_context("owner_context", context_dir=context_dir),
        "business_context": _load_single_context(
            "business_context", context_dir=context_dir
        ),
    }


def get_active_context(context_dir: Path | str | None = None) -> dict[str, Any]:
    context_payload = load_context(context_dir)
    system_context = context_payload["system_context"]
    active_mode = str(system_context.get("active_mode") or "dev").strip().lower()
    if active_mode not in ALLOWED_MODES:
        active_mode = "dev"

    active_context = (
        system_context
        if active_mode == "dev"
        else context_payload[f"{active_mode}_context"]
    )
    return {
        "active_mode": active_mode,
        "system_context": system_context,
        "active_context": active_context,
    }


def update_context(
    context_name: str,
    updates: dict[str, Any] | None = None,
    *,
    context_dir: Path | str | None = None,
) -> dict[str, Any]:
    resolved_name = _resolve_context_name(context_name)
    payload = _load_single_context(resolved_name, context_dir=context_dir)
    payload.update(dict(updates or {}))
    if resolved_name == "system_context":
        payload["last_update"] = _utc_now_iso()
    _write_single_context(resolved_name, payload, context_dir=context_dir)
    return payload


def set_active_mode(
    mode: str,
    *,
    current_focus: str | None = None,
    context_dir: Path | str | None = None,
) -> dict[str, Any]:
    normalized_mode = str(mode or "").strip().lower()
    if normalized_mode not in ALLOWED_MODES:
        raise ValueError("INVALID_MODE")

    system_context = _load_single_context("system_context", context_dir=context_dir)
    system_context["active_mode"] = normalized_mode
    if current_focus is not None:
        system_context["current_focus"] = str(current_focus)
    system_context["last_update"] = _utc_now_iso()
    _write_single_context("system_context", system_context, context_dir=context_dir)
    return system_context
