from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.ownerbox.trust_model import TRUST_CLASSES


OWNER_RESPONSE_TYPES = frozenset(
    {"summary_text", "confirmation_request", "input_error", "error"}
)
OWNER_RESPONSE_PLAN_STATUSES = frozenset({"planned", "blocked", "failed"})
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9._:@-]+$")
_LANGUAGE_TAG_PATTERN = re.compile(r"^[A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,8}){0,2}$")
_MAX_SUMMARY_LENGTH = 512


def _utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _collapse_whitespace(value: object) -> str:
    return " ".join(_normalize_text(value).split())


def _stable_identifier(value: object, *, field_name: str) -> str:
    normalized = _normalize_text(value)
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    if _IDENTIFIER_PATTERN.fullmatch(normalized) is None:
        raise ValueError(f"{field_name} must be a stable identifier")
    return normalized


def _normalize_language_tag(value: object, *, default: str) -> str:
    normalized = _normalize_text(value) or default
    if _LANGUAGE_TAG_PATTERN.fullmatch(normalized) is None:
        raise ValueError(f"invalid language tag: {normalized}")
    parts = normalized.split("-")
    canonical: list[str] = []
    for index, part in enumerate(parts):
        if index == 0:
            canonical.append(part.lower())
        elif len(part) == 2 and part.isalpha():
            canonical.append(part.upper())
        else:
            canonical.append(part.lower())
    return "-".join(canonical)


def _normalize_response_type(value: object) -> str:
    normalized = _normalize_text(value).lower()
    if normalized not in OWNER_RESPONSE_TYPES:
        raise ValueError(
            "response_type must be one of: " + ", ".join(sorted(OWNER_RESPONSE_TYPES))
        )
    return normalized


def _normalize_status(value: object) -> str:
    normalized = _normalize_text(value).lower()
    if normalized not in OWNER_RESPONSE_PLAN_STATUSES:
        raise ValueError(
            "status must be one of: " + ", ".join(sorted(OWNER_RESPONSE_PLAN_STATUSES))
        )
    return normalized


def _normalize_summary(value: object) -> str:
    normalized = _collapse_whitespace(value)
    if not normalized:
        raise ValueError("summary_text must not be empty")
    if len(normalized) > _MAX_SUMMARY_LENGTH:
        raise ValueError("summary_text exceeds max length")
    return normalized


def _normalize_optional_identifier(value: object, *, field_name: str) -> str | None:
    normalized = _normalize_text(value)
    if not normalized:
        return None
    return _stable_identifier(normalized, field_name=field_name)


def _normalize_optional_trust_class(value: object) -> str | None:
    normalized = _normalize_text(value).lower()
    if not normalized:
        return None
    if normalized not in TRUST_CLASSES:
        raise ValueError(
            "trust_class must be one of: " + ", ".join(sorted(TRUST_CLASSES))
        )
    return normalized


def _normalize_optional_preview_text(value: object) -> str | None:
    normalized = _collapse_whitespace(value)
    if not normalized:
        return None
    if len(normalized) > _MAX_SUMMARY_LENGTH:
        raise ValueError("preview_text exceeds max length")
    return normalized


def _normalize_mapping(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, Any] = {}
    for key in sorted(value):
        normalized_key = _normalize_text(key)
        if normalized_key:
            normalized[normalized_key] = value[key]
    return normalized


def _normalize_action_refs(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        candidates = [value]
    elif isinstance(value, (list, tuple, set, frozenset)):
        candidates = list(value)
    else:
        return ()
    refs: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        normalized = _normalize_text(item)
        if not normalized:
            continue
        stable = _stable_identifier(normalized, field_name="action_refs item")
        if stable in seen:
            continue
        seen.add(stable)
        refs.append(stable)
    return tuple(refs)


def _new_identifier(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


@dataclass(frozen=True, slots=True)
class OwnerResponsePlan:
    response_plan_id: str
    owner_session_id: str
    owner_id: str
    response_type: str
    target_language: str
    summary_text: str
    action_refs: tuple[str, ...] = field(default_factory=tuple)
    requires_confirmation: bool = False
    requires_high_trust: bool = False
    approval_id: str | None = None
    trust_class: str | None = None
    preview_text: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)
    created_at: str = field(default_factory=_utc_timestamp)
    status: str = "planned"

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "response_plan_id",
            _stable_identifier(self.response_plan_id, field_name="response_plan_id"),
        )
        object.__setattr__(
            self,
            "owner_session_id",
            _stable_identifier(self.owner_session_id, field_name="owner_session_id"),
        )
        object.__setattr__(
            self, "owner_id", _stable_identifier(self.owner_id, field_name="owner_id")
        )
        object.__setattr__(
            self, "response_type", _normalize_response_type(self.response_type)
        )
        object.__setattr__(
            self,
            "target_language",
            _normalize_language_tag(self.target_language, default="und"),
        )
        object.__setattr__(self, "summary_text", _normalize_summary(self.summary_text))
        object.__setattr__(
            self, "action_refs", _normalize_action_refs(self.action_refs)
        )
        object.__setattr__(
            self,
            "approval_id",
            _normalize_optional_identifier(self.approval_id, field_name="approval_id"),
        )
        object.__setattr__(
            self, "trust_class", _normalize_optional_trust_class(self.trust_class)
        )
        object.__setattr__(
            self, "preview_text", _normalize_optional_preview_text(self.preview_text)
        )
        object.__setattr__(self, "metadata", _normalize_mapping(self.metadata))
        object.__setattr__(
            self, "created_at", _normalize_text(self.created_at) or _utc_timestamp()
        )
        object.__setattr__(self, "status", _normalize_status(self.status))

    def to_dict(self) -> dict[str, object]:
        return {
            "response_plan_id": self.response_plan_id,
            "owner_session_id": self.owner_session_id,
            "owner_id": self.owner_id,
            "response_type": self.response_type,
            "target_language": self.target_language,
            "summary_text": self.summary_text,
            "action_refs": list(self.action_refs),
            "requires_confirmation": self.requires_confirmation,
            "requires_high_trust": self.requires_high_trust,
            "approval_id": self.approval_id,
            "trust_class": self.trust_class,
            "preview_text": self.preview_text,
            "metadata": dict(self.metadata),
            "created_at": self.created_at,
            "status": self.status,
        }


def create_owner_response_plan(
    *,
    owner_session_id: object,
    owner_id: object,
    response_type: object,
    target_language: object,
    summary_text: object,
    action_refs: object = (),
    requires_confirmation: bool = False,
    requires_high_trust: bool = False,
    approval_id: object = None,
    trust_class: object = None,
    preview_text: object = None,
    metadata: dict[str, object] | None = None,
    created_at: object | None = None,
    status: object = "planned",
    response_plan_id: object | None = None,
) -> OwnerResponsePlan:
    return OwnerResponsePlan(
        response_plan_id=_normalize_text(response_plan_id)
        or _new_identifier("owner-response-plan"),
        owner_session_id=_normalize_text(owner_session_id),
        owner_id=_normalize_text(owner_id),
        response_type=_normalize_text(response_type),
        target_language=_normalize_text(target_language) or "und",
        summary_text=summary_text,
        action_refs=action_refs,
        requires_confirmation=bool(requires_confirmation),
        requires_high_trust=bool(requires_high_trust),
        approval_id=approval_id,
        trust_class=trust_class,
        preview_text=preview_text,
        metadata=dict(metadata or {}),
        created_at=_normalize_text(created_at) or _utc_timestamp(),
        status=_normalize_text(status) or "planned",
    )
