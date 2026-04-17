from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping


OWNERBOX_DOMAIN_TYPE = "ownerbox"
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9._:@-]+$")
_OWNERBOX_STATUSES = frozenset({"active", "inactive", "suspended", "archived"})
_OWNERBOX_TRUST_LEVELS = frozenset({"standard", "elevated", "high"})
_OWNERBOX_TRUST_CLASSES = frozenset({"standard_owner", "high_trust_owner"})
_OWNERBOX_APPROVAL_MODES = frozenset({"manual_confirmation", "structured_confirmation"})
_DEFAULT_ALLOWED_MEMORY_CLASSES = ("artifact", "trace", "evidence", "context", "state")
_DEFAULT_BLOCKED_MEMORY_CLASSES = ("conflict",)
_DEFAULT_ALLOWED_ACTION_TYPES = (
    "OPENAI_REQUEST",
    "READ_FILE",
    "WRITE_FILE",
    "BROWSER_ACTION",
    "EMAIL_ACTION",
    "PRINT_DOCUMENT",
)
_DEFAULT_BLOCKED_ACTION_TYPES = (
    "BROWSER_TOOL",
    "SEND_EMAIL",
    "SEND_SMS",
    "MAKE_CALL",
    "API_REQUEST",
)
_DEFAULT_CONFIRMATION_ACTIONS = ("WRITE_FILE", "PRINT_DOCUMENT")
_DEFAULT_HIGH_TRUST_ACTIONS = (
    "BROWSER_TOOL",
    "SEND_EMAIL",
    "SEND_SMS",
    "MAKE_CALL",
    "API_REQUEST",
    "PRINT_DOCUMENT",
)
_DEFAULT_TRUTH_CONSTRAINTS = {
    "allow_authoritative_transcripts": False,
    "authoritative_memory_classes": list(_DEFAULT_ALLOWED_MEMORY_CLASSES),
    "max_resolved_entries": 5,
    "require_explicit_refs": True,
}


class OwnerBoxBoundaryError(ValueError):
    """Raised when an OwnerBox boundary structure is malformed."""


def _utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_mapping(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return {str(key).strip(): value[key] for key in sorted(value) if str(key).strip()}
    return {}


def _stable_identifier(value: object, *, field_name: str) -> str:
    normalized = _normalize_text(value)
    if not normalized:
        raise OwnerBoxBoundaryError(f"{field_name} must not be empty")
    if _IDENTIFIER_PATTERN.fullmatch(normalized) is None:
        raise OwnerBoxBoundaryError(f"{field_name} must be a stable identifier")
    return normalized


def _optional_identifier(value: object, *, field_name: str) -> str | None:
    normalized = _normalize_text(value)
    if not normalized:
        return None
    return _stable_identifier(normalized, field_name=field_name)


def _normalized_identifiers(
    value: object,
    *,
    field_name: str,
    upper: bool = False,
) -> tuple[str, ...]:
    if isinstance(value, str):
        candidates = [value]
    elif isinstance(value, (list, tuple, set, frozenset)):
        candidates = list(value)
    else:
        return ()

    normalized: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        text = _stable_identifier(item, field_name=field_name)
        if upper:
            text = text.upper()
        if text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return tuple(normalized)


def _normalized_status(value: object, *, field_name: str = "status") -> str:
    normalized = _normalize_text(value).lower()
    if normalized not in _OWNERBOX_STATUSES:
        allowed = ", ".join(sorted(_OWNERBOX_STATUSES))
        raise OwnerBoxBoundaryError(f"{field_name} must be one of: {allowed}")
    return normalized


def _normalized_domain_type(value: object) -> str:
    normalized = _normalize_text(value).lower() or OWNERBOX_DOMAIN_TYPE
    if normalized != OWNERBOX_DOMAIN_TYPE:
        raise OwnerBoxBoundaryError("domain_type must equal ownerbox")
    return normalized


def _normalized_trust_level(value: object) -> str:
    normalized = _normalize_text(value).lower()
    if normalized not in _OWNERBOX_TRUST_LEVELS:
        allowed = ", ".join(sorted(_OWNERBOX_TRUST_LEVELS))
        raise OwnerBoxBoundaryError(f"trust_level must be one of: {allowed}")
    return normalized


def _normalized_trust_class(value: object) -> str:
    normalized = _normalize_text(value).lower()
    if normalized not in _OWNERBOX_TRUST_CLASSES:
        allowed = ", ".join(sorted(_OWNERBOX_TRUST_CLASSES))
        raise OwnerBoxBoundaryError(f"trust_class must be one of: {allowed}")
    return normalized


def _normalized_approval_mode(value: object) -> str:
    normalized = _normalize_text(value).lower()
    if normalized not in _OWNERBOX_APPROVAL_MODES:
        allowed = ", ".join(sorted(_OWNERBOX_APPROVAL_MODES))
        raise OwnerBoxBoundaryError(f"approval_mode must be one of: {allowed}")
    return normalized


def _normalize_truth_constraints(value: object) -> dict[str, Any]:
    payload = dict(_DEFAULT_TRUTH_CONSTRAINTS)
    provided = _normalize_mapping(value)
    if "allow_authoritative_transcripts" in provided:
        payload["allow_authoritative_transcripts"] = bool(
            provided["allow_authoritative_transcripts"]
        )
    if "require_explicit_refs" in provided:
        payload["require_explicit_refs"] = bool(provided["require_explicit_refs"])
    if "max_resolved_entries" in provided:
        try:
            payload["max_resolved_entries"] = max(0, int(provided["max_resolved_entries"]))
        except (TypeError, ValueError) as exc:
            raise OwnerBoxBoundaryError(
                "truth_constraints.max_resolved_entries must be an integer"
            ) from exc
    if "authoritative_memory_classes" in provided:
        payload["authoritative_memory_classes"] = list(
            _normalized_identifiers(
                provided["authoritative_memory_classes"],
                field_name="truth_constraints.authoritative_memory_classes",
            )
        )
    return payload


@dataclass(frozen=True, slots=True)
class OwnerDomain:
    domain_id: str
    owner_id: str
    trust_level: str
    memory_scope_ref: str
    action_scope_ref: str
    policy_scope_ref: str
    created_at: str = field(default_factory=_utc_timestamp)
    status: str = "active"
    domain_type: str = OWNERBOX_DOMAIN_TYPE

    def __post_init__(self) -> None:
        object.__setattr__(self, "domain_id", _stable_identifier(self.domain_id, field_name="domain_id"))
        object.__setattr__(self, "owner_id", _stable_identifier(self.owner_id, field_name="owner_id"))
        object.__setattr__(self, "trust_level", _normalized_trust_level(self.trust_level))
        object.__setattr__(
            self,
            "memory_scope_ref",
            _stable_identifier(self.memory_scope_ref, field_name="memory_scope_ref"),
        )
        object.__setattr__(
            self,
            "action_scope_ref",
            _stable_identifier(self.action_scope_ref, field_name="action_scope_ref"),
        )
        object.__setattr__(
            self,
            "policy_scope_ref",
            _stable_identifier(self.policy_scope_ref, field_name="policy_scope_ref"),
        )
        object.__setattr__(self, "created_at", _normalize_text(self.created_at) or _utc_timestamp())
        object.__setattr__(self, "status", _normalized_status(self.status))
        object.__setattr__(self, "domain_type", _normalized_domain_type(self.domain_type))

    def to_dict(self) -> dict[str, object]:
        return {
            "domain_id": self.domain_id,
            "domain_type": self.domain_type,
            "owner_id": self.owner_id,
            "trust_level": self.trust_level,
            "memory_scope_ref": self.memory_scope_ref,
            "action_scope_ref": self.action_scope_ref,
            "policy_scope_ref": self.policy_scope_ref,
            "created_at": self.created_at,
            "status": self.status,
        }


@dataclass(frozen=True, slots=True)
class OwnerMemoryScope:
    scope_id: str
    allowed_memory_classes: tuple[str, ...] = field(default_factory=lambda: _DEFAULT_ALLOWED_MEMORY_CLASSES)
    blocked_memory_classes: tuple[str, ...] = field(default_factory=lambda: _DEFAULT_BLOCKED_MEMORY_CLASSES)
    allowed_refs: tuple[str, ...] = field(default_factory=tuple)
    blocked_refs: tuple[str, ...] = field(default_factory=tuple)
    truth_constraints: dict[str, Any] = field(default_factory=lambda: dict(_DEFAULT_TRUTH_CONSTRAINTS))
    status: str = "active"
    domain_type: str = OWNERBOX_DOMAIN_TYPE

    def __post_init__(self) -> None:
        object.__setattr__(self, "scope_id", _stable_identifier(self.scope_id, field_name="scope_id"))
        object.__setattr__(self, "domain_type", _normalized_domain_type(self.domain_type))
        object.__setattr__(
            self,
            "allowed_memory_classes",
            _normalized_identifiers(
                self.allowed_memory_classes,
                field_name="allowed_memory_classes",
            ),
        )
        object.__setattr__(
            self,
            "blocked_memory_classes",
            _normalized_identifiers(
                self.blocked_memory_classes,
                field_name="blocked_memory_classes",
            ),
        )
        object.__setattr__(
            self,
            "allowed_refs",
            _normalized_identifiers(self.allowed_refs, field_name="allowed_refs"),
        )
        object.__setattr__(
            self,
            "blocked_refs",
            _normalized_identifiers(self.blocked_refs, field_name="blocked_refs"),
        )
        object.__setattr__(
            self,
            "truth_constraints",
            _normalize_truth_constraints(self.truth_constraints),
        )
        object.__setattr__(self, "status", _normalized_status(self.status))

    def to_dict(self) -> dict[str, object]:
        return {
            "scope_id": self.scope_id,
            "domain_type": self.domain_type,
            "allowed_memory_classes": list(self.allowed_memory_classes),
            "blocked_memory_classes": list(self.blocked_memory_classes),
            "allowed_refs": list(self.allowed_refs),
            "blocked_refs": list(self.blocked_refs),
            "truth_constraints": dict(self.truth_constraints),
            "status": self.status,
        }


@dataclass(frozen=True, slots=True)
class OwnerActionScope:
    scope_id: str
    allowed_action_types: tuple[str, ...] = field(default_factory=lambda: _DEFAULT_ALLOWED_ACTION_TYPES)
    blocked_action_types: tuple[str, ...] = field(default_factory=lambda: _DEFAULT_BLOCKED_ACTION_TYPES)
    requires_confirmation_for: tuple[str, ...] = field(default_factory=lambda: _DEFAULT_CONFIRMATION_ACTIONS)
    requires_high_trust_for: tuple[str, ...] = field(default_factory=lambda: _DEFAULT_HIGH_TRUST_ACTIONS)
    status: str = "active"
    domain_type: str = OWNERBOX_DOMAIN_TYPE

    def __post_init__(self) -> None:
        object.__setattr__(self, "scope_id", _stable_identifier(self.scope_id, field_name="scope_id"))
        object.__setattr__(self, "domain_type", _normalized_domain_type(self.domain_type))
        object.__setattr__(
            self,
            "allowed_action_types",
            _normalized_identifiers(
                self.allowed_action_types,
                field_name="allowed_action_types",
                upper=True,
            ),
        )
        object.__setattr__(
            self,
            "blocked_action_types",
            _normalized_identifiers(
                self.blocked_action_types,
                field_name="blocked_action_types",
                upper=True,
            ),
        )
        object.__setattr__(
            self,
            "requires_confirmation_for",
            _normalized_identifiers(
                self.requires_confirmation_for,
                field_name="requires_confirmation_for",
                upper=True,
            ),
        )
        object.__setattr__(
            self,
            "requires_high_trust_for",
            _normalized_identifiers(
                self.requires_high_trust_for,
                field_name="requires_high_trust_for",
                upper=True,
            ),
        )
        object.__setattr__(self, "status", _normalized_status(self.status))

    def to_dict(self) -> dict[str, object]:
        return {
            "scope_id": self.scope_id,
            "domain_type": self.domain_type,
            "allowed_action_types": list(self.allowed_action_types),
            "blocked_action_types": list(self.blocked_action_types),
            "requires_confirmation_for": list(self.requires_confirmation_for),
            "requires_high_trust_for": list(self.requires_high_trust_for),
            "status": self.status,
        }


@dataclass(frozen=True, slots=True)
class OwnerTrustProfile:
    trust_profile_id: str
    owner_id: str
    confirmation_policy_ref: str
    approval_mode: str
    trust_class: str = "high_trust_owner"
    device_binding_ref: str | None = None
    status: str = "active"

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "trust_profile_id",
            _stable_identifier(self.trust_profile_id, field_name="trust_profile_id"),
        )
        object.__setattr__(self, "owner_id", _stable_identifier(self.owner_id, field_name="owner_id"))
        object.__setattr__(
            self,
            "confirmation_policy_ref",
            _stable_identifier(
                self.confirmation_policy_ref,
                field_name="confirmation_policy_ref",
            ),
        )
        object.__setattr__(self, "approval_mode", _normalized_approval_mode(self.approval_mode))
        object.__setattr__(self, "trust_class", _normalized_trust_class(self.trust_class))
        object.__setattr__(
            self,
            "device_binding_ref",
            _optional_identifier(self.device_binding_ref, field_name="device_binding_ref"),
        )
        object.__setattr__(self, "status", _normalized_status(self.status))

    def to_dict(self) -> dict[str, object]:
        return {
            "trust_profile_id": self.trust_profile_id,
            "owner_id": self.owner_id,
            "trust_class": self.trust_class,
            "confirmation_policy_ref": self.confirmation_policy_ref,
            "approval_mode": self.approval_mode,
            "device_binding_ref": self.device_binding_ref,
            "status": self.status,
        }


def create_owner_domain(
    *,
    domain_id: object,
    owner_id: object,
    trust_level: object,
    memory_scope_ref: object,
    action_scope_ref: object,
    policy_scope_ref: object,
    created_at: object | None = None,
    status: object = "active",
) -> OwnerDomain:
    return OwnerDomain(
        domain_id=_normalize_text(domain_id),
        owner_id=_normalize_text(owner_id),
        trust_level=_normalize_text(trust_level),
        memory_scope_ref=_normalize_text(memory_scope_ref),
        action_scope_ref=_normalize_text(action_scope_ref),
        policy_scope_ref=_normalize_text(policy_scope_ref),
        created_at=_normalize_text(created_at) or _utc_timestamp(),
        status=_normalize_text(status) or "active",
    )


def create_owner_memory_scope(
    *,
    scope_id: object,
    allowed_memory_classes: object | None = None,
    blocked_memory_classes: object | None = None,
    allowed_refs: object | None = None,
    blocked_refs: object | None = None,
    truth_constraints: Mapping[str, object] | None = None,
    status: object = "active",
) -> OwnerMemoryScope:
    return OwnerMemoryScope(
        scope_id=_normalize_text(scope_id),
        allowed_memory_classes=(
            _DEFAULT_ALLOWED_MEMORY_CLASSES
            if allowed_memory_classes is None
            else _normalized_identifiers(
                allowed_memory_classes,
                field_name="allowed_memory_classes",
            )
        ),
        blocked_memory_classes=(
            _DEFAULT_BLOCKED_MEMORY_CLASSES
            if blocked_memory_classes is None
            else _normalized_identifiers(
                blocked_memory_classes,
                field_name="blocked_memory_classes",
            )
        ),
        allowed_refs=_normalized_identifiers(allowed_refs or (), field_name="allowed_refs"),
        blocked_refs=_normalized_identifiers(blocked_refs or (), field_name="blocked_refs"),
        truth_constraints=_normalize_truth_constraints(truth_constraints),
        status=_normalize_text(status) or "active",
    )


def create_owner_action_scope(
    *,
    scope_id: object,
    allowed_action_types: object | None = None,
    blocked_action_types: object | None = None,
    requires_confirmation_for: object | None = None,
    requires_high_trust_for: object | None = None,
    status: object = "active",
) -> OwnerActionScope:
    return OwnerActionScope(
        scope_id=_normalize_text(scope_id),
        allowed_action_types=(
            _DEFAULT_ALLOWED_ACTION_TYPES
            if allowed_action_types is None
            else _normalized_identifiers(
                allowed_action_types,
                field_name="allowed_action_types",
                upper=True,
            )
        ),
        blocked_action_types=(
            _DEFAULT_BLOCKED_ACTION_TYPES
            if blocked_action_types is None
            else _normalized_identifiers(
                blocked_action_types,
                field_name="blocked_action_types",
                upper=True,
            )
        ),
        requires_confirmation_for=(
            _DEFAULT_CONFIRMATION_ACTIONS
            if requires_confirmation_for is None
            else _normalized_identifiers(
                requires_confirmation_for,
                field_name="requires_confirmation_for",
                upper=True,
            )
        ),
        requires_high_trust_for=(
            _DEFAULT_HIGH_TRUST_ACTIONS
            if requires_high_trust_for is None
            else _normalized_identifiers(
                requires_high_trust_for,
                field_name="requires_high_trust_for",
                upper=True,
            )
        ),
        status=_normalize_text(status) or "active",
    )


def create_owner_trust_profile(
    *,
    trust_profile_id: object,
    owner_id: object,
    confirmation_policy_ref: object,
    approval_mode: object = "structured_confirmation",
    trust_class: object = "high_trust_owner",
    device_binding_ref: object | None = None,
    status: object = "active",
) -> OwnerTrustProfile:
    return OwnerTrustProfile(
        trust_profile_id=_normalize_text(trust_profile_id),
        owner_id=_normalize_text(owner_id),
        trust_class=_normalize_text(trust_class),
        confirmation_policy_ref=_normalize_text(confirmation_policy_ref),
        approval_mode=_normalize_text(approval_mode),
        device_binding_ref=_normalize_text(device_binding_ref) or None,
        status=_normalize_text(status) or "active",
    )


def normalize_ownerbox_domain_binding(value: object) -> dict[str, object]:
    payload = _normalize_mapping(value)
    domain_type = _normalize_text(payload.get("domain_type") or payload.get("domain")).lower()
    if domain_type != OWNERBOX_DOMAIN_TYPE:
        return {}
    normalized: dict[str, object] = {
        "domain_type": OWNERBOX_DOMAIN_TYPE,
        "domain_id": _stable_identifier(payload.get("domain_id"), field_name="domain_id"),
        "owner_id": _stable_identifier(payload.get("owner_id"), field_name="owner_id"),
        "trust_level": _normalized_trust_level(payload.get("trust_level")),
        "memory_scope_ref": _stable_identifier(
            payload.get("memory_scope_ref"),
            field_name="memory_scope_ref",
        ),
        "action_scope_ref": _stable_identifier(
            payload.get("action_scope_ref"),
            field_name="action_scope_ref",
        ),
        "policy_scope_ref": _stable_identifier(
            payload.get("policy_scope_ref"),
            field_name="policy_scope_ref",
        ),
    }
    trust_profile_id = _optional_identifier(
        payload.get("trust_profile_id"),
        field_name="trust_profile_id",
    )
    if trust_profile_id is not None:
        normalized["trust_profile_id"] = trust_profile_id
    trust_class = _normalize_text(payload.get("trust_class")).lower()
    if trust_class:
        normalized["trust_class"] = _normalized_trust_class(trust_class)
    request_ref = _optional_identifier(payload.get("request_ref"), field_name="request_ref")
    if request_ref is not None:
        normalized["request_ref"] = request_ref
    session_ref = _optional_identifier(payload.get("session_ref"), field_name="session_ref")
    if session_ref is not None:
        normalized["session_ref"] = session_ref
    trace_id = _optional_identifier(payload.get("trace_id"), field_name="trace_id")
    if trace_id is not None:
        normalized["trace_id"] = trace_id
    return normalized


def build_ownerbox_domain_binding(
    owner_domain: OwnerDomain,
    *,
    trust_profile: OwnerTrustProfile | None = None,
    request_ref: object | None = None,
    session_ref: object | None = None,
    trace_id: object | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "domain_id": owner_domain.domain_id,
        "domain_type": owner_domain.domain_type,
        "owner_id": owner_domain.owner_id,
        "trust_level": owner_domain.trust_level,
        "memory_scope_ref": owner_domain.memory_scope_ref,
        "action_scope_ref": owner_domain.action_scope_ref,
        "policy_scope_ref": owner_domain.policy_scope_ref,
    }
    if trust_profile is not None:
        payload["trust_profile_id"] = trust_profile.trust_profile_id
        payload["trust_class"] = trust_profile.trust_class
    if _normalize_text(request_ref):
        payload["request_ref"] = _normalize_text(request_ref)
    if _normalize_text(session_ref):
        payload["session_ref"] = _normalize_text(session_ref)
    if _normalize_text(trace_id):
        payload["trace_id"] = _normalize_text(trace_id)
    return normalize_ownerbox_domain_binding(payload)


def build_ownerbox_trace_metadata(value: object) -> dict[str, object]:
    binding = normalize_ownerbox_domain_binding(value)
    if not binding:
        return {}
    scope_refs = [
        str(binding[key])
        for key in ("memory_scope_ref", "action_scope_ref", "policy_scope_ref")
        if str(binding.get(key) or "").strip()
    ]
    trace_metadata = dict(binding)
    trace_metadata["scope_refs"] = scope_refs
    return trace_metadata
