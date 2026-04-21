from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.storage.storage_adapter import (
    resolve_path,
    save_artifact,
)


STATE_ARTIFACT_TYPE = "state"
ALLOWED_DOMAINS = frozenset({"dev", "ownerbox"})
ALLOWED_ENTITY_TYPES = frozenset(
    {"task", "resource", "conflict", "owner_object", "compute_job"}
)
HISTORY_DIRNAME = "history"
_SAFE_COMPONENT_PATTERN = re.compile(r"[^A-Za-z0-9._-]+")


class StateStoreError(RuntimeError):
    """Raised when the canonical state layer cannot persist or load state."""


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _normalize_domain(domain: object) -> str:
    normalized = _normalize_text(domain).lower()
    if normalized not in ALLOWED_DOMAINS:
        allowed = ", ".join(sorted(ALLOWED_DOMAINS))
        raise StateStoreError(f"domain must be one of: {allowed}")
    return normalized


def _normalize_entity_type(entity_type: object) -> str:
    normalized = _normalize_text(entity_type).lower()
    if normalized not in ALLOWED_ENTITY_TYPES:
        allowed = ", ".join(sorted(ALLOWED_ENTITY_TYPES))
        raise StateStoreError(f"entity_type must be one of: {allowed}")
    return normalized


def _safe_component(value: object, *, field_name: str) -> str:
    normalized = _normalize_text(value)
    if not normalized:
        raise StateStoreError(f"{field_name} must not be empty.")
    safe_value = _SAFE_COMPONENT_PATTERN.sub("_", normalized).strip("._")
    if not safe_value:
        raise StateStoreError(f"{field_name} must resolve to a safe path component.")
    return safe_value


def _artifact_root(domain: str) -> Path:
    return resolve_path(domain, "_state_probe").parent


def _base_root(domain: str, *, base_root_override: Path | None = None) -> Path:
    if isinstance(base_root_override, Path):
        return base_root_override
    return _artifact_root(domain).parent


def _state_root(domain: str, *, base_root_override: Path | None = None) -> Path:
    return _base_root(domain, base_root_override=base_root_override) / "state"


def _active_state_relative_path(entity_type: str, entity_id: str) -> Path:
    return (
        Path("state")
        / _safe_component(entity_type, field_name="entity_type")
        / (f"{_safe_component(entity_id, field_name='entity_id')}.json")
    )


def _history_state_relative_path(
    entity_type: str,
    entity_id: str,
    *,
    updated_at: str,
) -> Path:
    timestamp_component = (
        _normalize_text(updated_at).replace(":", "-").replace("+", "_")
    )
    return (
        Path("state")
        / _safe_component(entity_type, field_name="entity_type")
        / HISTORY_DIRNAME
        / f"{_safe_component(entity_id, field_name='entity_id')}-{_safe_component(timestamp_component, field_name='updated_at')}.json"
    )


def _state_id(entity_type: str, entity_id: str) -> str:
    return f"state-{_safe_component(entity_type, field_name='entity_type')}-{_safe_component(entity_id, field_name='entity_id')}"


def _history_state_id(entity_type: str, entity_id: str, updated_at: str) -> str:
    return f"{_state_id(entity_type, entity_id)}-{_safe_component(updated_at, field_name='updated_at')}"


def _coerce_state_payload(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise StateStoreError("state payload must be a dict.")
    payload = dict(value)
    source_artifact = _normalize_text(payload.get("source_artifact"))
    if not source_artifact:
        raise StateStoreError("source_artifact must not be empty.")
    return {
        "id": _normalize_text(payload.get("id")),
        "domain": _normalize_domain(payload.get("domain")),
        "entity_type": _normalize_entity_type(payload.get("entity_type")),
        "entity_id": _normalize_text(payload.get("entity_id")),
        "state": _normalize_text(payload.get("state")),
        "updated_at": _normalize_text(payload.get("updated_at")),
        "source_artifact": source_artifact,
        "version": int(payload.get("version", 1) or 1),
    }


def _state_payload(
    *,
    domain: str,
    entity_type: str,
    entity_id: str,
    state: str,
    source_artifact: str,
    payload_id: str,
    logical_id: str,
    updated_at: str,
) -> dict[str, Any]:
    return {
        "id": payload_id,
        "logical_id": logical_id,
        "domain": domain,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "state": _normalize_text(state),
        "updated_at": updated_at,
        "source_artifact": _normalize_text(source_artifact),
        "version": 1,
    }


def _state_record_to_object(record: dict[str, Any]) -> dict[str, Any]:
    payload = record.get("payload")
    if isinstance(payload, dict) and payload:
        return _coerce_state_payload(payload)
    return _coerce_state_payload(record)


def _load_active_state_record(
    domain: str,
    entity_type: str,
    entity_id: str,
    *,
    base_root_override: Path | None = None,
) -> dict[str, Any] | None:
    path = _state_root(
        domain, base_root_override=base_root_override
    ) / _active_state_relative_path(entity_type, entity_id).relative_to("state")
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise StateStoreError(f"state file is not readable: {path}") from exc
    if not isinstance(payload, dict):
        raise StateStoreError(f"state file must contain a JSON object: {path}")
    return payload


def _persist_historical_snapshot(
    domain: str,
    current_record: dict[str, Any],
    *,
    base_root_override: Path | None = None,
) -> None:
    current_state = _state_record_to_object(current_record)
    history_updated_at = (
        _normalize_text(current_state.get("updated_at")) or _utc_timestamp()
    )
    history_payload = _state_payload(
        domain=domain,
        entity_type=str(current_state["entity_type"]),
        entity_id=str(current_state["entity_id"]),
        state=str(current_state["state"]),
        source_artifact=str(current_state["source_artifact"]),
        payload_id=_history_state_id(
            str(current_state["entity_type"]),
            str(current_state["entity_id"]),
            history_updated_at,
        ),
        logical_id=_history_state_id(
            str(current_state["entity_type"]),
            str(current_state["entity_id"]),
            history_updated_at,
        ),
        updated_at=history_updated_at,
    )
    save_artifact(
        domain,
        STATE_ARTIFACT_TYPE,
        history_payload,
        overwrite=True,
        artifact_status="superseded",
        relative_path=_history_state_relative_path(
            str(current_state["entity_type"]),
            str(current_state["entity_id"]),
            updated_at=history_updated_at,
        ),
        domain_root_override=_base_root(domain, base_root_override=base_root_override),
        memory_class_override="state",
        truth_level_override="canonical",
        execution_role_override="state_holder",
        refs_override=[str(current_state["source_artifact"])],
    )


def _base_root_from_source_artifact(source_artifact: str) -> Path | None:
    candidate = Path(str(source_artifact or "").strip())
    if not candidate.is_absolute():
        return None
    parts = list(candidate.parts)
    for marker in ("artifacts", "state"):
        if marker in parts:
            return Path(*parts[: parts.index(marker)])
    return None


@dataclass(frozen=True, slots=True)
class StateObject:
    id: str
    domain: str
    entity_type: str
    entity_id: str
    state: str
    updated_at: str
    source_artifact: str
    version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "domain": self.domain,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "state": self.state,
            "updated_at": self.updated_at,
            "source_artifact": self.source_artifact,
            "version": self.version,
        }


def set_state(
    entity_type: str,
    entity_id: str,
    state: str,
    source_artifact: str,
    *,
    domain: str | None = None,
) -> dict[str, Any]:
    normalized_entity_type = _normalize_entity_type(entity_type)
    normalized_entity_id = _normalize_text(entity_id)
    if not normalized_entity_id:
        raise StateStoreError("entity_id must not be empty.")
    normalized_state = _normalize_text(state)
    if not normalized_state:
        raise StateStoreError("state must not be empty.")
    normalized_source_artifact = _normalize_text(source_artifact)
    if not normalized_source_artifact:
        raise StateStoreError("source_artifact must not be empty.")

    resolved_domain = (
        _normalize_domain(domain)
        if domain is not None
        else _infer_domain_from_source(normalized_source_artifact)
    )
    base_root_override = _base_root_from_source_artifact(normalized_source_artifact)
    current_record = _load_active_state_record(
        resolved_domain,
        normalized_entity_type,
        normalized_entity_id,
        base_root_override=base_root_override,
    )
    if isinstance(current_record, dict):
        _persist_historical_snapshot(
            resolved_domain,
            current_record,
            base_root_override=base_root_override,
        )

    updated_at = _utc_timestamp()
    payload = _state_payload(
        domain=resolved_domain,
        entity_type=normalized_entity_type,
        entity_id=normalized_entity_id,
        state=normalized_state,
        source_artifact=normalized_source_artifact,
        payload_id=_state_id(normalized_entity_type, normalized_entity_id),
        logical_id=normalized_entity_id,
        updated_at=updated_at,
    )
    save_artifact(
        resolved_domain,
        STATE_ARTIFACT_TYPE,
        payload,
        overwrite=True,
        artifact_status="active",
        relative_path=_active_state_relative_path(
            normalized_entity_type, normalized_entity_id
        ),
        domain_root_override=_base_root(
            resolved_domain,
            base_root_override=base_root_override,
        ),
        memory_class_override="state",
        truth_level_override="canonical",
        execution_role_override="state_holder",
        refs_override=[normalized_source_artifact],
    )
    return StateObject(
        id=_state_id(normalized_entity_type, normalized_entity_id),
        domain=resolved_domain,
        entity_type=normalized_entity_type,
        entity_id=normalized_entity_id,
        state=normalized_state,
        updated_at=updated_at,
        source_artifact=normalized_source_artifact,
    ).to_dict()


def _infer_domain_from_source(source_artifact: str) -> str:
    normalized = _normalize_text(source_artifact)
    if (
        "/ownerbox/" in normalized
        or normalized.startswith("ownerbox/")
        or "DF/owner/" in normalized
    ):
        return "ownerbox"
    if (
        "/df-dev/" in normalized
        or normalized.startswith("exports/dev/")
        or "DF/dev/" in normalized
    ):
        return "dev"
    raise StateStoreError(
        "domain is required when it cannot be inferred from source_artifact."
    )


def get_state(
    entity_type: str,
    entity_id: str,
    *,
    domain: str | None = None,
) -> dict[str, Any] | None:
    normalized_entity_type = _normalize_entity_type(entity_type)
    normalized_entity_id = _normalize_text(entity_id)
    if not normalized_entity_id:
        return None

    if domain is not None:
        record = _load_active_state_record(
            _normalize_domain(domain),
            normalized_entity_type,
            normalized_entity_id,
        )
        return _state_record_to_object(record) if isinstance(record, dict) else None

    matches: list[dict[str, Any]] = []
    for candidate_domain in sorted(ALLOWED_DOMAINS):
        record = _load_active_state_record(
            candidate_domain,
            normalized_entity_type,
            normalized_entity_id,
        )
        if isinstance(record, dict):
            matches.append(_state_record_to_object(record))
    if not matches:
        return None
    if len(matches) > 1:
        raise StateStoreError(
            f"state lookup is ambiguous for entity_type='{normalized_entity_type}' entity_id='{normalized_entity_id}'"
        )
    return matches[0]


def list_active_states(domain: str) -> list[dict[str, Any]]:
    normalized_domain = _normalize_domain(domain)
    root = _state_root(normalized_domain)
    if not root.exists():
        return []

    states: list[dict[str, Any]] = []
    for path in sorted(root.glob("*/*.json")):
        if path.parent.name == HISTORY_DIRNAME:
            continue
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(record, dict):
            continue
        states.append(_state_record_to_object(record))
    states.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
    return states
