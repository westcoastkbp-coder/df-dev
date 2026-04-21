from __future__ import annotations

import json
import os
import time
from collections import OrderedDict
from collections.abc import Mapping
from pathlib import Path

from app.config.hybrid_runtime import HybridRuntimeConfig, load_runtime_config
from app.execution.paths import ROOT_DIR as DEFAULT_ROOT_DIR


CONTEXT_STORE_SCHEMA_VERSION = "v1"
GLOBAL_CONTEXT_KEY = "global_context"
SYSTEM_CONTEXT_KEY = "system_context"
ACTIVE_TASK_PREFIX = "active_task:"
THREAD_PREFIX = "thread:"
_PREPARED_CONTEXT_ROOTS: set[str] = set()
_JSON_FILE_CACHE: OrderedDict[str, dict[str, object]] = OrderedDict()
_MAX_JSON_FILE_CACHE_ENTRIES = 32
MAX_JSON_FILE_BYTES = 1_048_576


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _current_config(
    *,
    root_dir: Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> HybridRuntimeConfig:
    return load_runtime_config(
        root_dir=root_dir or DEFAULT_ROOT_DIR,
        environ=dict(os.environ if environ is None else environ),
    )


def shared_context_contract(config: HybridRuntimeConfig) -> dict[str, object]:
    paths = config.storage_paths
    return {
        "schema_version": CONTEXT_STORE_SCHEMA_VERSION,
        "role": config.role,
        "storage_root": paths.storage_root.as_posix(),
        "global_context": {
            "path": paths.global_context_file.as_posix(),
            "kind": "json",
        },
        "active_thread_context": {
            "path": paths.active_threads_dir.as_posix(),
            "kind": "json-per-thread",
        },
        "decisions": {
            "path": paths.decisions_file.as_posix(),
            "kind": "jsonl",
        },
        "interaction_history": {
            "path": paths.interactions_file.as_posix(),
            "kind": "jsonl",
        },
        "audit_trail": {
            "path": paths.audit_file.as_posix(),
            "kind": "jsonl",
        },
        "system_context": {
            "path": paths.system_context_file.as_posix(),
            "kind": "json",
        },
    }


def prepare_shared_context_store(config: HybridRuntimeConfig) -> dict[str, object]:
    paths = config.storage_paths
    storage_root_key = str(paths.storage_root.resolve())
    if storage_root_key in _PREPARED_CONTEXT_ROOTS:
        return shared_context_contract(config)
    for directory in (
        paths.storage_root,
        paths.runtime_root,
        paths.logs_dir,
        paths.state_dir,
        paths.verification_dir,
        paths.shared_context_dir,
        paths.active_threads_dir,
    ):
        directory.mkdir(parents=True, exist_ok=True)

    _ensure_json_file(
        paths.global_context_file,
        {
            "schema_version": CONTEXT_STORE_SCHEMA_VERSION,
            "scope": "global",
            "updated_at": "",
            "value": {},
        },
    )
    _ensure_json_file(
        paths.system_context_file,
        {
            "schema_version": CONTEXT_STORE_SCHEMA_VERSION,
            "scope": "system",
            "updated_at": "",
            "value": {},
        },
    )
    _ensure_jsonl_file(paths.interactions_file)
    _ensure_jsonl_file(paths.decisions_file)
    _ensure_jsonl_file(paths.audit_file)
    _PREPARED_CONTEXT_ROOTS.add(storage_root_key)
    return shared_context_contract(config)


def get_context(
    key: str,
    *,
    root_dir: Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> object:
    config = _current_config(root_dir=root_dir, environ=environ)
    prepare_shared_context_store(config)
    target = _context_path(config, key)
    if not target.exists() or target.suffix.lower() != ".json":
        return {}
    payload = _read_json_file(target)
    if key in {GLOBAL_CONTEXT_KEY, SYSTEM_CONTEXT_KEY}:
        return dict(payload.get("value", {}) or {})
    return dict(payload)


def set_context(
    key: str,
    value: object,
    *,
    task_id: object = "",
    interaction_id: object = "",
    root_dir: Path | None = None,
    environ: Mapping[str, str] | None = None,
    timestamp: str | None = None,
) -> dict[str, object]:
    config = _current_config(root_dir=root_dir, environ=environ)
    prepare_shared_context_store(config)
    target = _context_path(config, key)
    effective_timestamp = str(timestamp or "").strip() or _now()
    normalized_task_id = str(task_id or "").strip()
    normalized_interaction_id = str(interaction_id or "").strip()
    existing = get_context(key, root_dir=root_dir, environ=environ)
    if key in {GLOBAL_CONTEXT_KEY, SYSTEM_CONTEXT_KEY}:
        payload = {
            "schema_version": CONTEXT_STORE_SCHEMA_VERSION,
            "scope": key.replace("_context", ""),
            "updated_at": effective_timestamp,
            "task_id": normalized_task_id,
            "interaction_id": normalized_interaction_id,
            "value": _merge_json_value(existing, value),
        }
    else:
        existing_value = (
            dict(existing.get("value", {}))
            if isinstance(existing, Mapping)
            and isinstance(existing.get("value", {}), Mapping)
            else {}
        )
        payload = {
            "schema_version": CONTEXT_STORE_SCHEMA_VERSION,
            "key": key,
            "updated_at": effective_timestamp,
            "task_id": normalized_task_id or _normalize_link(existing, "task_id"),
            "interaction_id": normalized_interaction_id
            or _normalize_link(existing, "interaction_id"),
            "value": _merge_json_value(existing_value, value),
        }
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    _cache_json_file(target, payload)
    _append_audit_record(
        config=config,
        event_type="context_set",
        payload={
            "key": key,
            "path": target.as_posix(),
            "value": payload.get("value", {}),
        },
        task_id=payload.get("task_id", ""),
        interaction_id=payload.get("interaction_id", ""),
        timestamp=effective_timestamp,
    )
    return payload


def append_event(
    event_type: str,
    payload: object,
    *,
    task_id: object = "",
    interaction_id: object = "",
    root_dir: Path | None = None,
    environ: Mapping[str, str] | None = None,
    timestamp: str | None = None,
) -> dict[str, object]:
    config = _current_config(root_dir=root_dir, environ=environ)
    prepare_shared_context_store(config)
    effective_timestamp = str(timestamp or "").strip() or _now()
    record = {
        "schema_version": CONTEXT_STORE_SCHEMA_VERSION,
        "timestamp": effective_timestamp,
        "event_type": str(event_type or "").strip() or "observed",
        "task_id": str(task_id or "").strip(),
        "interaction_id": str(interaction_id or "").strip(),
        "payload": _clone_json_like(payload),
    }
    target = _event_path(config, record["event_type"])
    _append_jsonl_record(target, record)
    _append_audit_record(
        config=config,
        event_type=record["event_type"],
        payload=dict(record),
        task_id=record["task_id"],
        interaction_id=record["interaction_id"],
        timestamp=effective_timestamp,
    )
    return record


def _append_audit_record(
    *,
    config: HybridRuntimeConfig,
    event_type: str,
    payload: object,
    task_id: object,
    interaction_id: object,
    timestamp: str,
) -> None:
    _append_jsonl_record(
        config.storage_paths.audit_file,
        {
            "schema_version": CONTEXT_STORE_SCHEMA_VERSION,
            "timestamp": timestamp,
            "event_type": str(event_type or "").strip() or "audit",
            "task_id": str(task_id or "").strip(),
            "interaction_id": str(interaction_id or "").strip(),
            "payload": _clone_json_like(payload),
        },
    )


def _context_path(config: HybridRuntimeConfig, key: str) -> Path:
    normalized_key = str(key or "").strip()
    if normalized_key == GLOBAL_CONTEXT_KEY:
        return config.storage_paths.global_context_file
    if normalized_key == SYSTEM_CONTEXT_KEY:
        return config.storage_paths.system_context_file
    if normalized_key.startswith(ACTIVE_TASK_PREFIX):
        task_id = normalized_key.partition(":")[2].strip()
        if not task_id:
            raise ValueError("active_task context key requires task id")
        return config.storage_paths.active_threads_dir / f"task-{task_id}.json"
    if normalized_key.startswith(THREAD_PREFIX):
        thread_id = normalized_key.partition(":")[2].strip()
        if not thread_id:
            raise ValueError("thread context key requires thread id")
        return config.storage_paths.active_threads_dir / f"thread-{thread_id}.json"
    raise ValueError(f"unsupported context key `{normalized_key}`")


def _event_path(config: HybridRuntimeConfig, event_type: str) -> Path:
    normalized_event_type = str(event_type or "").strip().lower()
    if normalized_event_type.startswith("decision"):
        return config.storage_paths.decisions_file
    return config.storage_paths.interactions_file


def _cache_key(path: Path) -> str:
    return str(path.resolve(strict=False))


def _cache_json_file(path: Path, payload: dict[str, object]) -> None:
    key = _cache_key(path)
    _JSON_FILE_CACHE[key] = dict(_clone_json_like(payload))
    _JSON_FILE_CACHE.move_to_end(key)
    while len(_JSON_FILE_CACHE) > _MAX_JSON_FILE_CACHE_ENTRIES:
        _JSON_FILE_CACHE.popitem(last=False)


def _ensure_json_file(path: Path, payload: dict[str, object]) -> None:
    if path.exists():
        return
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    _cache_json_file(path, payload)


def _ensure_jsonl_file(path: Path) -> None:
    if path.exists():
        return
    path.write_text("", encoding="utf-8")


def _append_jsonl_record(path: Path, record: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(record, ensure_ascii=True, separators=(",", ":")) + "\n"
        )


def _read_json_file(path: Path) -> dict[str, object]:
    cached_payload = _JSON_FILE_CACHE.get(_cache_key(path))
    if isinstance(cached_payload, dict):
        return dict(_clone_json_like(cached_payload))
    try:
        if path.stat().st_size > MAX_JSON_FILE_BYTES:
            return {}
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    _cache_json_file(path, payload)
    return dict(_clone_json_like(payload))


def _normalize_link(value: object, field: str) -> str:
    if not isinstance(value, Mapping):
        return ""
    return str(value.get(field, "")).strip()


def _clone_json_like(value: object) -> object:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {
            str(key).strip(): _clone_json_like(item)
            for key, item in dict(value).items()
            if str(key).strip()
        }
    if isinstance(value, (list, tuple)):
        return [_clone_json_like(item) for item in value]
    return str(value).strip()


def _merge_json_value(existing: object, incoming: object) -> object:
    if isinstance(existing, Mapping) and isinstance(incoming, Mapping):
        merged = {
            str(key).strip(): _clone_json_like(item)
            for key, item in dict(existing).items()
            if str(key).strip()
        }
        for key, item in dict(incoming).items():
            normalized_key = str(key).strip()
            if not normalized_key:
                continue
            if normalized_key in {"decision_context", "previous_context"}:
                merged[normalized_key] = _clone_json_like(item)
                continue
            current = merged.get(normalized_key)
            merged[normalized_key] = _merge_json_value(current, item)
        return merged
    return _clone_json_like(incoming)
