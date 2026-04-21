from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ALLOWED_DOMAINS = frozenset({"dev", "ownerbox"})
ALLOWED_MEMORY_CLASSES = frozenset(
    {"artifact", "trace", "conflict", "evidence", "context", "state"}
)
ALLOWED_STATUSES = frozenset({"active", "resolved", "archived", "superseded"})
ALLOWED_TRUTH_LEVELS = frozenset({"working", "validated", "canonical"})
ALLOWED_EXECUTION_ROLES = frozenset(
    {"input", "output", "blocker", "evidence", "context_only", "state_holder"}
)
LEGACY_ACTIVE_STATUSES = frozenset({"", "active", "pending", "pending_resolution"})
TRACE_MEMORY_CLASS = "trace"
CONFLICT_MEMORY_CLASS = "conflict"
ARTIFACT_MEMORY_CLASS = "artifact"


class MemoryObjectError(ValueError):
    """Raised when a canonical memory object is malformed."""


def _utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_optional_path(value: object) -> str | None:
    normalized = _normalize_text(value)
    if not normalized:
        return None
    return str(Path(normalized))


def _normalize_string_list(value: object) -> list[str]:
    if isinstance(value, str):
        candidates = [value]
    elif isinstance(value, (list, tuple, set, frozenset)):
        candidates = list(value)
    else:
        return []

    normalized_items: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        normalized = _normalize_text(item)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        normalized_items.append(normalized)
    return normalized_items


def _validate_enum(value: object, *, field_name: str, allowed: frozenset[str]) -> str:
    normalized = _normalize_text(value).lower()
    if normalized not in allowed:
        allowed_display = ", ".join(sorted(allowed))
        raise MemoryObjectError(f"{field_name} must be one of: {allowed_display}")
    return normalized


def infer_memory_class(artifact_type: object) -> str:
    normalized_type = _normalize_text(artifact_type).lower()
    if normalized_type == "execution_trace" or normalized_type.endswith("_trace"):
        return TRACE_MEMORY_CLASS
    if normalized_type == "conflict_escalation" or "conflict" in normalized_type:
        return CONFLICT_MEMORY_CLASS
    return ARTIFACT_MEMORY_CLASS


def infer_execution_role(memory_class: object) -> str:
    normalized_memory_class = _normalize_text(memory_class).lower()
    if normalized_memory_class == TRACE_MEMORY_CLASS:
        return "evidence"
    if normalized_memory_class == CONFLICT_MEMORY_CLASS:
        return "blocker"
    if normalized_memory_class == "context":
        return "context_only"
    if normalized_memory_class == "state":
        return "state_holder"
    if normalized_memory_class == "evidence":
        return "evidence"
    return "output"


def canonical_status(value: object) -> str:
    normalized = _normalize_text(value).lower()
    if normalized in LEGACY_ACTIVE_STATUSES:
        return "active"
    return _validate_enum(
        normalized,
        field_name="status",
        allowed=ALLOWED_STATUSES,
    )


def compatibility_state(value: object, *, memory_class: object) -> str:
    normalized = _normalize_text(value).lower()
    normalized_memory_class = _normalize_text(memory_class).lower()
    if normalized in {"pending", "pending_resolution"}:
        return normalized
    if normalized_memory_class == CONFLICT_MEMORY_CLASS:
        return "pending_resolution"
    return ""


def effective_status(entity: object) -> str:
    if not isinstance(entity, dict):
        return ""
    state = _normalize_text(entity.get("state")).lower()
    if state:
        return state
    return _normalize_text(entity.get("status")).lower()


@dataclass(frozen=True, slots=True)
class MemoryObject:
    id: str
    domain: str
    memory_class: str
    status: str
    truth_level: str
    execution_role: str
    created_at: str
    updated_at: str
    tags: list[str]
    refs: list[str]
    local_path: str | None
    remote_path: str | None
    payload: Any
    artifact_type: str = ""
    logical_key: str = ""
    state: str = ""
    resolution: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "id",
            _normalize_text(self.id),
        )
        object.__setattr__(
            self,
            "domain",
            _validate_enum(self.domain, field_name="domain", allowed=ALLOWED_DOMAINS),
        )
        object.__setattr__(
            self,
            "memory_class",
            _validate_enum(
                self.memory_class,
                field_name="memory_class",
                allowed=ALLOWED_MEMORY_CLASSES,
            ),
        )
        object.__setattr__(
            self,
            "status",
            _validate_enum(self.status, field_name="status", allowed=ALLOWED_STATUSES),
        )
        object.__setattr__(
            self,
            "truth_level",
            _validate_enum(
                self.truth_level,
                field_name="truth_level",
                allowed=ALLOWED_TRUTH_LEVELS,
            ),
        )
        object.__setattr__(
            self,
            "execution_role",
            _validate_enum(
                self.execution_role,
                field_name="execution_role",
                allowed=ALLOWED_EXECUTION_ROLES,
            ),
        )
        object.__setattr__(self, "created_at", _normalize_text(self.created_at))
        object.__setattr__(self, "updated_at", _normalize_text(self.updated_at))
        object.__setattr__(self, "tags", _normalize_string_list(self.tags))
        object.__setattr__(self, "refs", _normalize_string_list(self.refs))
        object.__setattr__(
            self, "local_path", _normalize_optional_path(self.local_path)
        )
        object.__setattr__(
            self, "remote_path", _normalize_optional_path(self.remote_path)
        )
        object.__setattr__(self, "artifact_type", _normalize_text(self.artifact_type))
        object.__setattr__(self, "logical_key", _normalize_text(self.logical_key))
        object.__setattr__(self, "state", _normalize_text(self.state).lower())
        object.__setattr__(
            self,
            "resolution",
            dict(self.resolution) if isinstance(self.resolution, dict) else None,
        )

        if not self.id:
            raise MemoryObjectError("id must not be empty.")
        if not self.created_at:
            raise MemoryObjectError("created_at must not be empty.")
        if not self.updated_at:
            raise MemoryObjectError("updated_at must not be empty.")

    def to_dict(self, *, include_legacy: bool = True) -> dict[str, Any]:
        payload = {
            "id": self.id,
            "domain": self.domain,
            "memory_class": self.memory_class,
            "status": self.status,
            "truth_level": self.truth_level,
            "execution_role": self.execution_role,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "tags": list(self.tags),
            "refs": list(self.refs),
            "local_path": self.local_path,
            "remote_path": self.remote_path,
            "payload": self.payload,
        }
        if not include_legacy:
            return payload

        payload["type"] = self.artifact_type or self.memory_class
        payload["timestamp"] = self.updated_at
        if self.logical_key:
            payload["logical_key"] = self.logical_key
        if self.state:
            payload["state"] = self.state
        if self.resolution is not None or self.state or self.status != "active":
            payload["resolution"] = (
                dict(self.resolution) if isinstance(self.resolution, dict) else None
            )
        return payload


def memory_object_from_mapping(
    value: object,
    *,
    artifact_type: object | None = None,
    logical_key: object | None = None,
    local_path: object | None = None,
    remote_path: object | None = None,
) -> MemoryObject:
    if not isinstance(value, dict):
        raise MemoryObjectError("memory object source must be a dict.")

    mapped = dict(value)
    resolved_artifact_type = _normalize_text(
        artifact_type if artifact_type is not None else mapped.get("type")
    )
    resolved_memory_class = _normalize_text(
        mapped.get("memory_class")
    ) or infer_memory_class(resolved_artifact_type)
    raw_state = _normalize_text(mapped.get("state"))
    raw_status = _normalize_text(mapped.get("status")) or raw_state
    normalized_updated_at = (
        _normalize_text(mapped.get("updated_at") or mapped.get("timestamp"))
        or _utc_timestamp()
    )
    normalized_created_at = (
        _normalize_text(mapped.get("created_at") or mapped.get("timestamp"))
        or normalized_updated_at
    )

    return MemoryObject(
        id=_normalize_text(mapped.get("id")),
        domain=_normalize_text(mapped.get("domain")),
        memory_class=resolved_memory_class,
        status=canonical_status(raw_status),
        truth_level=_normalize_text(mapped.get("truth_level")) or "working",
        execution_role=_normalize_text(mapped.get("execution_role"))
        or infer_execution_role(resolved_memory_class),
        created_at=normalized_created_at,
        updated_at=normalized_updated_at,
        tags=_normalize_string_list(mapped.get("tags")),
        refs=_normalize_string_list(mapped.get("refs")),
        local_path=local_path if local_path is not None else mapped.get("local_path"),
        remote_path=remote_path
        if remote_path is not None
        else mapped.get("remote_path"),
        payload=mapped.get("payload"),
        artifact_type=resolved_artifact_type,
        logical_key=logical_key
        if logical_key is not None
        else mapped.get("logical_key"),
        state=raw_state
        or compatibility_state(raw_status, memory_class=resolved_memory_class),
        resolution=mapped.get("resolution"),
    )


def make_artifact_object(
    *,
    id: str,
    domain: str,
    payload: Any,
    local_path: str | Path | None = None,
    remote_path: str | Path | None = None,
    status: str = "active",
    truth_level: str = "working",
    execution_role: str = "output",
    created_at: str | None = None,
    updated_at: str | None = None,
    tags: list[str] | None = None,
    refs: list[str] | None = None,
    artifact_type: str = "",
    logical_key: str = "",
    state: str | None = None,
    resolution: dict[str, Any] | None = None,
) -> MemoryObject:
    resolved_updated_at = _normalize_text(updated_at) or _utc_timestamp()
    resolved_created_at = _normalize_text(created_at) or resolved_updated_at
    return MemoryObject(
        id=id,
        domain=domain,
        memory_class=ARTIFACT_MEMORY_CLASS,
        status=canonical_status(status),
        truth_level=truth_level,
        execution_role=execution_role,
        created_at=resolved_created_at,
        updated_at=resolved_updated_at,
        tags=tags or [],
        refs=refs or [],
        local_path=str(local_path) if local_path is not None else None,
        remote_path=str(remote_path) if remote_path is not None else None,
        payload=payload,
        artifact_type=artifact_type,
        logical_key=logical_key,
        state=_normalize_text(state)
        or compatibility_state(status, memory_class=ARTIFACT_MEMORY_CLASS),
        resolution=resolution,
    )


def make_trace_object(
    *,
    id: str,
    domain: str,
    payload: Any,
    local_path: str | Path | None = None,
    remote_path: str | Path | None = None,
    status: str = "active",
    truth_level: str = "working",
    execution_role: str = "evidence",
    created_at: str | None = None,
    updated_at: str | None = None,
    tags: list[str] | None = None,
    refs: list[str] | None = None,
    artifact_type: str = "execution_trace",
    logical_key: str = "",
    state: str | None = None,
    resolution: dict[str, Any] | None = None,
) -> MemoryObject:
    resolved_updated_at = _normalize_text(updated_at) or _utc_timestamp()
    resolved_created_at = _normalize_text(created_at) or resolved_updated_at
    return MemoryObject(
        id=id,
        domain=domain,
        memory_class=TRACE_MEMORY_CLASS,
        status=canonical_status(status),
        truth_level=truth_level,
        execution_role=execution_role,
        created_at=resolved_created_at,
        updated_at=resolved_updated_at,
        tags=tags or [],
        refs=refs or [],
        local_path=str(local_path) if local_path is not None else None,
        remote_path=str(remote_path) if remote_path is not None else None,
        payload=payload,
        artifact_type=artifact_type,
        logical_key=logical_key,
        state=_normalize_text(state),
        resolution=resolution,
    )


def make_conflict_object(
    *,
    id: str,
    domain: str,
    payload: Any,
    local_path: str | Path | None = None,
    remote_path: str | Path | None = None,
    status: str = "active",
    truth_level: str = "working",
    execution_role: str = "blocker",
    created_at: str | None = None,
    updated_at: str | None = None,
    tags: list[str] | None = None,
    refs: list[str] | None = None,
    artifact_type: str = "conflict_escalation",
    logical_key: str = "",
    state: str | None = None,
    resolution: dict[str, Any] | None = None,
) -> MemoryObject:
    resolved_updated_at = _normalize_text(updated_at) or _utc_timestamp()
    resolved_created_at = _normalize_text(created_at) or resolved_updated_at
    return MemoryObject(
        id=id,
        domain=domain,
        memory_class=CONFLICT_MEMORY_CLASS,
        status=canonical_status(status),
        truth_level=truth_level,
        execution_role=execution_role,
        created_at=resolved_created_at,
        updated_at=resolved_updated_at,
        tags=tags or [],
        refs=refs or [],
        local_path=str(local_path) if local_path is not None else None,
        remote_path=str(remote_path) if remote_path is not None else None,
        payload=payload,
        artifact_type=artifact_type,
        logical_key=logical_key,
        state=_normalize_text(state)
        or compatibility_state(status, memory_class=CONFLICT_MEMORY_CLASS),
        resolution=resolution,
    )
