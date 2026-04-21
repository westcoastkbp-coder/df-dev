from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from app.ownerbox.domain import OWNERBOX_DOMAIN_TYPE


OWNER_SESSION_STATUSES = frozenset({"active", "paused", "completed", "failed"})
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9._:@-]+$")
_LANGUAGE_TAG_PATTERN = re.compile(r"^[A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,8}){0,2}$")
_MAX_CONTEXT_REF_LENGTH = 256


def _utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


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


def _normalize_context_ref(value: object) -> str | None:
    normalized = _normalize_text(value)
    if not normalized:
        return None
    if len(normalized) > _MAX_CONTEXT_REF_LENGTH:
        raise ValueError("context_ref exceeds max length")
    return normalized


def _normalize_status(value: object) -> str:
    normalized = _normalize_text(value).lower()
    if normalized not in OWNER_SESSION_STATUSES:
        raise ValueError(
            "status must be one of: " + ", ".join(sorted(OWNER_SESSION_STATUSES))
        )
    return normalized


def _normalize_request_count(value: object) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("request_count must be an integer") from exc
    if normalized < 0:
        raise ValueError("request_count must be non-negative")
    return normalized


def _new_identifier(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


@dataclass(frozen=True, slots=True)
class OwnerSession:
    owner_session_id: str
    owner_id: str
    domain_type: str
    active_language: str
    context_ref: str | None
    request_count: int
    started_at: str
    last_request_at: str
    status: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "owner_session_id",
            _stable_identifier(self.owner_session_id, field_name="owner_session_id"),
        )
        object.__setattr__(self, "owner_id", _stable_identifier(self.owner_id, field_name="owner_id"))
        if _normalize_text(self.domain_type).lower() != OWNERBOX_DOMAIN_TYPE:
            raise ValueError("domain_type must equal ownerbox")
        object.__setattr__(self, "domain_type", OWNERBOX_DOMAIN_TYPE)
        object.__setattr__(
            self,
            "active_language",
            _normalize_language_tag(self.active_language, default="und"),
        )
        object.__setattr__(self, "context_ref", _normalize_context_ref(self.context_ref))
        object.__setattr__(self, "request_count", _normalize_request_count(self.request_count))
        object.__setattr__(self, "started_at", _normalize_text(self.started_at) or _utc_timestamp())
        object.__setattr__(
            self,
            "last_request_at",
            _normalize_text(self.last_request_at) or self.started_at,
        )
        object.__setattr__(self, "status", _normalize_status(self.status))

    def to_dict(self) -> dict[str, object]:
        return {
            "owner_session_id": self.owner_session_id,
            "owner_id": self.owner_id,
            "domain_type": self.domain_type,
            "active_language": self.active_language,
            "context_ref": self.context_ref,
            "request_count": self.request_count,
            "started_at": self.started_at,
            "last_request_at": self.last_request_at,
            "status": self.status,
        }


def create_owner_session(
    *,
    owner_id: object,
    active_language: object = "und",
    context_ref: object = None,
    owner_session_id: object | None = None,
    started_at: object | None = None,
    status: object = "active",
) -> OwnerSession:
    timestamp = _normalize_text(started_at) or _utc_timestamp()
    return OwnerSession(
        owner_session_id=_normalize_text(owner_session_id) or _new_identifier("owner-session"),
        owner_id=_normalize_text(owner_id),
        domain_type=OWNERBOX_DOMAIN_TYPE,
        active_language=_normalize_text(active_language) or "und",
        context_ref=context_ref,
        request_count=0,
        started_at=timestamp,
        last_request_at=timestamp,
        status=_normalize_text(status) or "active",
    )


def update_owner_session(
    session: OwnerSession,
    *,
    active_language: object | None = None,
    context_ref: object | None = None,
    request_count: object | None = None,
    last_request_at: object | None = None,
    status: object | None = None,
) -> OwnerSession:
    return OwnerSession(
        owner_session_id=session.owner_session_id,
        owner_id=session.owner_id,
        domain_type=session.domain_type,
        active_language=_normalize_text(active_language) or session.active_language,
        context_ref=session.context_ref if context_ref is None else context_ref,
        request_count=session.request_count if request_count is None else request_count,
        started_at=session.started_at,
        last_request_at=_normalize_text(last_request_at) or session.last_request_at,
        status=_normalize_text(status) or session.status,
    )


def increment_owner_session_request(
    session: OwnerSession,
    *,
    active_language: object | None = None,
    context_ref: object | None = None,
    last_request_at: object | None = None,
) -> OwnerSession:
    return update_owner_session(
        session,
        active_language=active_language,
        context_ref=context_ref,
        request_count=session.request_count + 1,
        last_request_at=last_request_at,
    )
