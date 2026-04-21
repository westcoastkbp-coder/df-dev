from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.memory.memory_object import (
    infer_execution_role,
    infer_memory_class,
    memory_object_from_mapping,
)


REGISTRY_FILE = Path("/home/avoro/df-system/memory_registry.json")


class MemoryRegistryError(RuntimeError):
    """Raised when the artifact registry cannot be read or written."""


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _ensure_registry_file() -> None:
    try:
        REGISTRY_FILE.parent.mkdir(parents=True, exist_ok=True)
        if not REGISTRY_FILE.exists():
            REGISTRY_FILE.write_text('{\n  "artifacts": []\n}\n', encoding="utf-8")
    except OSError as exc:
        raise MemoryRegistryError(
            f"Memory registry is not writable: {REGISTRY_FILE}"
        ) from exc


def _load_registry() -> dict[str, list[dict[str, Any]]]:
    _ensure_registry_file()
    try:
        payload = json.loads(REGISTRY_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise MemoryRegistryError(
            f"Memory registry is not valid JSON: {REGISTRY_FILE}"
        ) from exc
    except OSError as exc:
        raise MemoryRegistryError(
            f"Memory registry is not readable: {REGISTRY_FILE}"
        ) from exc

    if not isinstance(payload, dict):
        raise MemoryRegistryError(
            f"Memory registry must contain a JSON object: {REGISTRY_FILE}"
        )

    artifacts = payload.get("artifacts")
    if artifacts is None:
        payload["artifacts"] = []
        return payload

    if not isinstance(artifacts, list):
        raise MemoryRegistryError(
            f"Memory registry artifacts index must be a list: {REGISTRY_FILE}"
        )
    return payload


def _save_registry(payload: dict[str, list[dict[str, Any]]]) -> None:
    try:
        REGISTRY_FILE.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    except OSError as exc:
        raise MemoryRegistryError(
            f"Memory registry is not writable: {REGISTRY_FILE}"
        ) from exc


def compute_artifact_key(domain: str, artifact_type: str, logical_id: str) -> str:
    normalized_domain = _normalize_text(domain)
    normalized_type = _normalize_text(artifact_type)
    normalized_logical_id = _normalize_text(logical_id)
    if not normalized_domain:
        raise MemoryRegistryError("domain must not be empty.")
    if not normalized_type:
        raise MemoryRegistryError("artifact_type must not be empty.")
    if not normalized_logical_id:
        raise MemoryRegistryError("logical_id must not be empty.")
    return f"{normalized_domain}:{normalized_type}:{normalized_logical_id}"


def register_artifact(
    artifact_id: str,
    domain: str,
    artifact_type: str,
    local_path: Path | str,
    *,
    logical_key: str | None = None,
    remote_path: str | None = None,
    timestamp: str | None = None,
    tags: list[str] | None = None,
    status: str | None = None,
    resolution: dict[str, Any] | None = None,
    refs: list[str] | None = None,
    memory_class: str | None = None,
    truth_level: str | None = None,
    execution_role: str | None = None,
    created_at: str | None = None,
    updated_at: str | None = None,
    state: str | None = None,
    payload: Any = None,
) -> dict[str, Any]:
    registry = _load_registry()
    resolved_logical_key = (
        _normalize_text(logical_key)
        if logical_key is not None
        else compute_artifact_key(domain, artifact_type, artifact_id)
    )
    normalized_type = _normalize_text(artifact_type)
    normalized_timestamp = _normalize_text(timestamp) if timestamp else _utc_timestamp()
    canonical_memory_class = _normalize_text(memory_class) or infer_memory_class(normalized_type)
    entry = memory_object_from_mapping(
        {
            "id": _normalize_text(artifact_id),
            "domain": _normalize_text(domain),
            "memory_class": canonical_memory_class,
            "status": _normalize_text(status) or "active",
            "truth_level": _normalize_text(truth_level) or "working",
            "execution_role": _normalize_text(execution_role)
            or infer_execution_role(canonical_memory_class),
            "created_at": _normalize_text(created_at) or normalized_timestamp,
            "updated_at": _normalize_text(updated_at) or normalized_timestamp,
            "tags": [_normalize_text(tag) for tag in (tags or []) if _normalize_text(tag)],
            "refs": [_normalize_text(ref) for ref in (refs or []) if _normalize_text(ref)],
            "local_path": str(Path(local_path)),
            "remote_path": _normalize_text(remote_path) if remote_path else None,
            "payload": payload,
            "type": normalized_type,
            "logical_key": resolved_logical_key,
            "state": _normalize_text(state),
            "resolution": dict(resolution) if isinstance(resolution, dict) else None,
        },
        artifact_type=normalized_type,
        logical_key=resolved_logical_key,
        local_path=str(Path(local_path)),
        remote_path=_normalize_text(remote_path) if remote_path else None,
    ).to_dict()
    if not entry["id"]:
        raise MemoryRegistryError("artifact_id must not be empty.")
    if not entry["domain"]:
        raise MemoryRegistryError("domain must not be empty.")
    if not entry["type"]:
        raise MemoryRegistryError("artifact_type must not be empty.")
    if not entry["logical_key"]:
        raise MemoryRegistryError("logical_key must not be empty.")
    if not entry["local_path"]:
        raise MemoryRegistryError("local_path must not be empty.")
    artifacts = registry["artifacts"]
    replaced = False
    for index, existing in enumerate(artifacts):
        if (
            existing.get("logical_key") == entry["logical_key"]
            or (
                existing.get("id") == entry["id"]
                and existing.get("domain") == entry["domain"]
                and existing.get("type") == entry["type"]
            )
        ):
            artifacts[index] = entry
            replaced = True
            break
    if not replaced:
        artifacts.append(entry)

    _save_registry(registry)
    return entry


def get_artifact_by_id(artifact_id: str) -> dict[str, Any] | None:
    normalized_id = _normalize_text(artifact_id)
    if not normalized_id:
        return None

    registry = _load_registry()
    for entry in reversed(registry["artifacts"]):
        if entry.get("id") == normalized_id:
            return memory_object_from_mapping(entry).to_dict()
    return None


def get_artifact_by_logical_key(logical_key: str) -> dict[str, Any] | None:
    normalized_logical_key = _normalize_text(logical_key)
    if not normalized_logical_key:
        return None

    registry = _load_registry()
    for entry in reversed(registry["artifacts"]):
        if entry.get("logical_key") == normalized_logical_key:
            return memory_object_from_mapping(entry).to_dict()
    return None


def list_by_domain(domain: str) -> list[dict[str, Any]]:
    normalized_domain = _normalize_text(domain)
    if not normalized_domain:
        return []

    registry = _load_registry()
    return [
        memory_object_from_mapping(entry).to_dict()
        for entry in registry["artifacts"]
        if entry.get("domain") == normalized_domain
    ]


def list_by_type(artifact_type: str) -> list[dict[str, Any]]:
    normalized_type = _normalize_text(artifact_type)
    if not normalized_type:
        return []

    registry = _load_registry()
    return [
        memory_object_from_mapping(entry).to_dict()
        for entry in registry["artifacts"]
        if entry.get("type") == normalized_type
    ]
