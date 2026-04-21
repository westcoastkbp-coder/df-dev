from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import re
from typing import Any, Mapping
from uuid import UUID


SUPPORTED_MEMORY_DOMAINS = frozenset({"ownerbox"})
SUPPORTED_MEMORY_TYPES = frozenset(
    {"fact", "decision", "preference", "entity_ref", "document_ref", "relationship"}
)
SUPPORTED_TRUST_LEVELS = frozenset({"working", "validated", "canonical"})
SUPPORTED_STATUSES = frozenset({"active", "superseded", "deprecated", "archived"})
SUPPORTED_AUDIT_OPERATIONS = frozenset(
    {"create", "activate", "supersede", "deprecate", "archive", "restore"}
)
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9._:@/-]+$")


class CanonicalMemoryError(ValueError):
    """Raised when a canonical memory object is malformed."""


def utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _is_uuid(value: str) -> bool:
    try:
        UUID(value)
    except (TypeError, ValueError, AttributeError):
        return False
    return True


def _normalize_memory_id(value: object, *, field_name: str) -> str:
    normalized = _normalize_text(value)
    if not normalized:
        raise CanonicalMemoryError(f"{field_name} must not be empty")
    if not _is_uuid(normalized) and _IDENTIFIER_PATTERN.fullmatch(normalized) is None:
        raise CanonicalMemoryError(f"{field_name} must be a UUID or stable identifier")
    return normalized


def _normalize_identifier(value: object, *, field_name: str, required: bool = False) -> str | None:
    normalized = _normalize_text(value)
    if not normalized:
        if required:
            raise CanonicalMemoryError(f"{field_name} must not be empty")
        return None
    if _IDENTIFIER_PATTERN.fullmatch(normalized) is None:
        raise CanonicalMemoryError(f"{field_name} must be a stable identifier")
    return normalized


def _normalize_enum(
    value: object,
    *,
    field_name: str,
    allowed: frozenset[str],
) -> str:
    normalized = _normalize_text(value).lower()
    if normalized not in allowed:
        allowed_values = ", ".join(sorted(allowed))
        raise CanonicalMemoryError(f"{field_name} must be one of: {allowed_values}")
    return normalized


def _normalize_payload(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return {str(key).strip(): value[key] for key in sorted(value) if str(key).strip()}
    raise CanonicalMemoryError("structured_payload must be a mapping")


def _normalize_confidence(value: object) -> float:
    if value is None or _normalize_text(value) == "":
        raise CanonicalMemoryError("confidence must not be empty")
    try:
        normalized = float(value)
    except (TypeError, ValueError) as exc:
        raise CanonicalMemoryError("confidence must be a number") from exc
    if normalized < 0.0 or normalized > 1.0:
        raise CanonicalMemoryError("confidence must be between 0.0 and 1.0")
    return round(normalized, 6)


def _normalize_version(value: object) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise CanonicalMemoryError("version must be a positive integer") from exc
    if normalized < 1:
        raise CanonicalMemoryError("version must be a positive integer")
    return normalized


def build_audit_metadata(
    *,
    operation_type: object,
    source_type: object,
    source_ref: object,
    timestamp: object | None = None,
    prior_memory_id: object | None = None,
    actor_scope: object | None = None,
    lifecycle_from: object | None = None,
    lifecycle_to: object | None = None,
) -> dict[str, Any]:
    normalized_operation_type = _normalize_enum(
        operation_type,
        field_name="audit_metadata.operation_type",
        allowed=SUPPORTED_AUDIT_OPERATIONS,
    )
    normalized_source_type = _normalize_identifier(
        source_type,
        field_name="audit_metadata.source_type",
        required=True,
    )
    normalized_source_ref = _normalize_text(source_ref)
    if not normalized_source_ref:
        raise CanonicalMemoryError("audit_metadata.source_ref must not be empty")
    metadata: dict[str, Any] = {
        "operation_type": normalized_operation_type,
        "source_type": normalized_source_type,
        "source_ref": normalized_source_ref,
        "prior_memory_id": (
            _normalize_memory_id(prior_memory_id, field_name="audit_metadata.prior_memory_id")
            if _normalize_text(prior_memory_id)
            else None
        ),
        "actor_scope": _normalize_identifier(
            actor_scope,
            field_name="audit_metadata.actor_scope",
        ),
        "timestamp": _normalize_text(timestamp) or utc_timestamp(),
    }
    normalized_from = _normalize_text(lifecycle_from).lower()
    if normalized_from:
        metadata["lifecycle_from"] = normalized_from
    normalized_to = _normalize_text(lifecycle_to).lower()
    if normalized_to:
        metadata["lifecycle_to"] = normalized_to
    return metadata


def _normalize_audit_metadata(
    value: object,
    *,
    source_type: object,
    source_ref: object,
    timestamp: object,
    default_operation: str,
    prior_memory_id: str | None,
) -> dict[str, Any]:
    payload = _normalize_payload(value) if isinstance(value, Mapping) else {}
    return build_audit_metadata(
        operation_type=payload.get("operation_type") or default_operation,
        source_type=payload.get("source_type") or source_type,
        source_ref=payload.get("source_ref") or source_ref,
        timestamp=payload.get("timestamp") or timestamp,
        prior_memory_id=payload.get("prior_memory_id") or prior_memory_id,
        actor_scope=payload.get("actor_scope"),
        lifecycle_from=payload.get("lifecycle_from"),
        lifecycle_to=payload.get("lifecycle_to"),
    )


@dataclass(frozen=True, slots=True)
class MemoryObject:
    memory_id: str
    memory_type: str
    domain_scope: str = ""
    domain_type: str = ""
    owner_ref: str | None = None
    subject_ref: str | None = None
    content_summary: str = ""
    structured_payload: dict[str, Any] = field(default_factory=dict)
    status: str = "active"
    created_at: str = field(default_factory=utc_timestamp)
    updated_at: str = field(default_factory=utc_timestamp)
    source_trace_id: str = ""
    evidence_ref: str = ""
    version: int = 1
    confidence: float | None = None
    trust_level: str = "validated"
    trust_class: str = "governed"
    source_type: str = ""
    source_ref: str = ""
    audit_metadata: dict[str, Any] = field(default_factory=dict)
    superseded_by_memory_id: str | None = None
    previous_version_id: str | None = None
    conflict_key: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "memory_id",
            _normalize_memory_id(self.memory_id, field_name="memory_id"),
        )
        object.__setattr__(
            self,
            "memory_type",
            _normalize_enum(
                self.memory_type,
                field_name="memory_type",
                allowed=SUPPORTED_MEMORY_TYPES,
            ),
        )
        resolved_domain_scope = self.domain_scope or self.domain_type
        object.__setattr__(
            self,
            "domain_scope",
            _normalize_enum(
                resolved_domain_scope,
                field_name="domain_scope",
                allowed=SUPPORTED_MEMORY_DOMAINS,
            ),
        )
        object.__setattr__(self, "domain_type", self.domain_scope)
        object.__setattr__(
            self,
            "owner_ref",
            _normalize_identifier(self.owner_ref, field_name="owner_ref"),
        )
        object.__setattr__(
            self,
            "subject_ref",
            _normalize_identifier(self.subject_ref, field_name="subject_ref"),
        )
        object.__setattr__(self, "content_summary", _normalize_text(self.content_summary))
        object.__setattr__(self, "structured_payload", _normalize_payload(self.structured_payload))
        object.__setattr__(
            self,
            "status",
            _normalize_enum(self.status, field_name="status", allowed=SUPPORTED_STATUSES),
        )
        object.__setattr__(self, "created_at", _normalize_text(self.created_at) or utc_timestamp())
        object.__setattr__(self, "updated_at", _normalize_text(self.updated_at) or self.created_at)
        resolved_source_trace_id = self.source_trace_id or f"trace:{_normalize_text(self.source_ref)}"
        object.__setattr__(
            self,
            "source_trace_id",
            _normalize_identifier(
                resolved_source_trace_id,
                field_name="source_trace_id",
                required=True,
            ),
        )
        object.__setattr__(
            self,
            "evidence_ref",
            _normalize_text(self.evidence_ref) or f"evidence:{_normalize_text(self.source_ref)}",
        )
        object.__setattr__(self, "version", _normalize_version(self.version))
        object.__setattr__(
            self,
            "confidence",
            _normalize_confidence(1.0 if self.confidence is None else self.confidence),
        )
        object.__setattr__(
            self,
            "trust_level",
            _normalize_enum(
                self.trust_level,
                field_name="trust_level",
                allowed=SUPPORTED_TRUST_LEVELS,
            ),
        )
        object.__setattr__(
            self,
            "trust_class",
            _normalize_identifier(self.trust_class, field_name="trust_class", required=True),
        )
        object.__setattr__(
            self,
            "source_type",
            _normalize_identifier(self.source_type, field_name="source_type", required=True),
        )
        object.__setattr__(self, "source_ref", _normalize_text(self.source_ref))
        object.__setattr__(
            self,
            "superseded_by_memory_id",
            (
                _normalize_memory_id(
                    self.superseded_by_memory_id,
                    field_name="superseded_by_memory_id",
                )
                if _normalize_text(self.superseded_by_memory_id)
                else None
            ),
        )
        object.__setattr__(
            self,
            "previous_version_id",
            (
                _normalize_memory_id(
                    self.previous_version_id,
                    field_name="previous_version_id",
                )
                if _normalize_text(self.previous_version_id)
                else None
            ),
        )
        object.__setattr__(
            self,
            "conflict_key",
            _normalize_identifier(self.conflict_key, field_name="conflict_key"),
        )
        default_operation = "create" if self.version == 1 else "activate"
        object.__setattr__(
            self,
            "audit_metadata",
            _normalize_audit_metadata(
                self.audit_metadata,
                source_type=self.source_type,
                source_ref=self.source_ref,
                timestamp=self.updated_at,
                default_operation=default_operation,
                prior_memory_id=self.previous_version_id,
            ),
        )

        if not self.evidence_ref:
            raise CanonicalMemoryError("evidence_ref must not be empty")
        if not self.source_ref:
            raise CanonicalMemoryError("source_ref must not be empty")
        if not self.owner_ref and not self.subject_ref:
            raise CanonicalMemoryError("owner_ref or subject_ref must be provided")
        if self.domain_scope == "ownerbox" and not self.owner_ref:
            raise CanonicalMemoryError("ownerbox memory requires owner_ref")
        if not self.content_summary and not self.structured_payload:
            raise CanonicalMemoryError(
                "canonical memory requires content_summary or structured_payload"
            )
        if self.status == "superseded" and not self.superseded_by_memory_id:
            raise CanonicalMemoryError(
                "superseded memory requires superseded_by_memory_id"
            )

    @property
    def type(self) -> str:
        return self.memory_type

    def to_dict(self) -> dict[str, Any]:
        return {
            "memory_id": self.memory_id,
            "type": self.memory_type,
            "memory_type": self.memory_type,
            "domain_scope": self.domain_scope,
            "domain_type": self.domain_scope,
            "owner_ref": self.owner_ref,
            "subject_ref": self.subject_ref,
            "content_summary": self.content_summary,
            "structured_payload": dict(self.structured_payload),
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "source_trace_id": self.source_trace_id,
            "evidence_ref": self.evidence_ref,
            "version": self.version,
            "confidence": self.confidence,
            "trust_level": self.trust_level,
            "trust_class": self.trust_class,
            "source_type": self.source_type,
            "source_ref": self.source_ref,
            "audit_metadata": dict(self.audit_metadata),
            "superseded_by_memory_id": self.superseded_by_memory_id,
            "previous_version_id": self.previous_version_id,
            "conflict_key": self.conflict_key,
        }


class MemoryFact(MemoryObject):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(memory_type="fact", **kwargs)

    def __post_init__(self) -> None:
        super().__post_init__()
        if not self.structured_payload.get("fact_key"):
            raise CanonicalMemoryError("fact memory requires structured_payload.fact_key")


class MemoryDecision(MemoryObject):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(memory_type="decision", **kwargs)

    def __post_init__(self) -> None:
        super().__post_init__()
        if not (
            self.structured_payload.get("decision_ref")
            or self.structured_payload.get("decision_key")
        ):
            raise CanonicalMemoryError(
                "decision memory requires structured_payload.decision_ref or decision_key"
            )


class MemoryPreference(MemoryObject):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(memory_type="preference", **kwargs)

    def __post_init__(self) -> None:
        super().__post_init__()
        if not self.structured_payload.get("preference_key"):
            raise CanonicalMemoryError(
                "preference memory requires structured_payload.preference_key"
            )


class MemoryEntityRef(MemoryObject):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(memory_type="entity_ref", **kwargs)

    def __post_init__(self) -> None:
        super().__post_init__()
        if not self.structured_payload.get("entity_type"):
            raise CanonicalMemoryError(
                "entity_ref memory requires structured_payload.entity_type"
            )


class MemoryDocumentRef(MemoryObject):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(memory_type="document_ref", **kwargs)

    def __post_init__(self) -> None:
        super().__post_init__()
        if not (
            self.structured_payload.get("document_id")
            or self.structured_payload.get("document_locator")
        ):
            raise CanonicalMemoryError(
                "document_ref memory requires structured_payload.document_id or document_locator"
            )


class MemoryRelationship(MemoryObject):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(memory_type="relationship", **kwargs)

    def __post_init__(self) -> None:
        super().__post_init__()
        if not self.structured_payload.get("relationship_type"):
            raise CanonicalMemoryError(
                "relationship memory requires structured_payload.relationship_type"
            )
        if not self.structured_payload.get("related_ref"):
            raise CanonicalMemoryError(
                "relationship memory requires structured_payload.related_ref"
            )


def memory_object_from_mapping(payload: Mapping[str, Any]) -> MemoryObject:
    memory_type = _normalize_text(
        payload.get("memory_type") or payload.get("type")
    ).lower()
    common_kwargs = {
        "memory_id": payload.get("memory_id"),
        "domain_scope": payload.get("domain_scope") or payload.get("domain_type"),
        "owner_ref": payload.get("owner_ref"),
        "subject_ref": payload.get("subject_ref"),
        "content_summary": payload.get("content_summary"),
        "structured_payload": payload.get("structured_payload") or {},
        "status": payload.get("status") or "active",
        "created_at": payload.get("created_at") or utc_timestamp(),
        "updated_at": payload.get("updated_at") or payload.get("created_at") or utc_timestamp(),
        "source_trace_id": payload.get("source_trace_id") or payload.get("source_ref"),
        "evidence_ref": payload.get("evidence_ref") or payload.get("source_ref"),
        "version": payload.get("version") or 1,
        "confidence": payload.get("confidence") if payload.get("confidence") is not None else 1.0,
        "trust_level": payload.get("trust_level") or "validated",
        "trust_class": payload.get("trust_class") or "governed",
        "source_type": payload.get("source_type"),
        "source_ref": payload.get("source_ref"),
        "audit_metadata": payload.get("audit_metadata") or {},
        "superseded_by_memory_id": payload.get("superseded_by_memory_id"),
        "previous_version_id": payload.get("previous_version_id"),
        "conflict_key": payload.get("conflict_key"),
    }
    if memory_type == "fact":
        return MemoryFact(**common_kwargs)
    if memory_type == "decision":
        return MemoryDecision(**common_kwargs)
    if memory_type == "preference":
        return MemoryPreference(**common_kwargs)
    if memory_type == "entity_ref":
        return MemoryEntityRef(**common_kwargs)
    if memory_type == "document_ref":
        return MemoryDocumentRef(**common_kwargs)
    if memory_type == "relationship":
        return MemoryRelationship(**common_kwargs)
    raise CanonicalMemoryError(f"unsupported memory_type: {memory_type}")
