import copy
import json
from datetime import datetime
from pathlib import Path

CTX_PATH = Path(__file__).resolve().with_name("system_context.json")


def default_context():
    return {
        "system": "Digital Foreman",
        "status": "kernel_debug",
        "modules": {},
        "broken_modules": [],
        "broken": {},
        "known_issues": [],
        "history": [],
        "last_update": "",
        "modules_state": {},
        "last_codex_loop": {},
        "next_required": ""
    }


def _merge_defaults(value, default):
    if isinstance(default, dict):
        base = dict(value) if isinstance(value, dict) else {}
        merged = {}
        for key, default_value in default.items():
            merged[key] = _merge_defaults(base.get(key), default_value)
        for key, item in base.items():
            if key not in merged:
                merged[key] = item
        return merged
    if isinstance(default, list):
        if isinstance(value, list):
            return list(value)
        return copy.deepcopy(default)
    if isinstance(default, str):
        if isinstance(value, str):
            return value
        return copy.deepcopy(default)
    if value is None:
        return copy.deepcopy(default)
    if default is not None and not isinstance(value, type(default)):
        return copy.deepcopy(default)
    return value


def load_context():
    if not CTX_PATH.exists():
        return default_context()

    try:
        loaded = json.loads(CTX_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return default_context()

    return _merge_defaults(loaded, default_context())


def save_context(ctx):
    ctx["last_update"] = datetime.utcnow().isoformat()
    CTX_PATH.parent.mkdir(parents=True, exist_ok=True)
    CTX_PATH.write_text(json.dumps(ctx, indent=2), encoding="utf-8")


def record_event(event: str):
    ctx = load_context()
    ctx["history"].append({
        "time": datetime.utcnow().isoformat(),
        "event": event
    })
    save_context(ctx)


def record_issue(issue: str):
    ctx = load_context()
    ctx["known_issues"].append(issue)
    save_context(ctx)
