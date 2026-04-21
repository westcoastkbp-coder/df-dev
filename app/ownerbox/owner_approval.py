from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from app.ownerbox.trust_model import TRUST_CLASSES


OWNER_APPROVAL_STATUSES = frozenset({"pending", "approved", "rejected"})
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9._:@-]+$")


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


def _normalize_status(value: object) -> str:
    normalized = _normalize_text(value).lower()
    if normalized not in OWNER_APPROVAL_STATUSES:
        raise ValueError(
            "status must be one of: " + ", ".join(sorted(OWNER_APPROVAL_STATUSES))
        )
    return normalized


def _normalize_trust_class(value: object) -> str:
    normalized = _normalize_text(value).lower()
    if normalized not in TRUST_CLASSES:
        raise ValueError(
            "trust_class must be one of: " + ", ".join(sorted(TRUST_CLASSES))
        )
    return normalized


def _new_identifier(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


@dataclass(frozen=True, slots=True)
class OwnerApproval:
    approval_id: str
    owner_id: str
    action_id: str
    trust_class: str
    status: str
    created_at: str
    resolved_at: str | None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "approval_id",
            _stable_identifier(self.approval_id, field_name="approval_id"),
        )
        object.__setattr__(
            self, "owner_id", _stable_identifier(self.owner_id, field_name="owner_id")
        )
        object.__setattr__(
            self,
            "action_id",
            _stable_identifier(self.action_id, field_name="action_id"),
        )
        object.__setattr__(
            self, "trust_class", _normalize_trust_class(self.trust_class)
        )
        object.__setattr__(self, "status", _normalize_status(self.status))
        object.__setattr__(
            self, "created_at", _normalize_text(self.created_at) or _utc_timestamp()
        )
        resolved_at = _normalize_text(self.resolved_at)
        object.__setattr__(self, "resolved_at", resolved_at or None)
        if self.status == "pending" and self.resolved_at is not None:
            raise ValueError("pending approval must not have resolved_at")
        if self.status != "pending" and self.resolved_at is None:
            raise ValueError("resolved approval must include resolved_at")

    def to_dict(self) -> dict[str, object]:
        return {
            "approval_id": self.approval_id,
            "owner_id": self.owner_id,
            "action_id": self.action_id,
            "trust_class": self.trust_class,
            "status": self.status,
            "created_at": self.created_at,
            "resolved_at": self.resolved_at,
        }


def create_owner_approval(
    *,
    owner_id: object,
    action_id: object,
    trust_class: object,
    approval_id: object | None = None,
    created_at: object | None = None,
) -> OwnerApproval:
    return OwnerApproval(
        approval_id=_normalize_text(approval_id) or _new_identifier("owner-approval"),
        owner_id=_normalize_text(owner_id),
        action_id=_normalize_text(action_id),
        trust_class=_normalize_text(trust_class),
        status="pending",
        created_at=_normalize_text(created_at) or _utc_timestamp(),
        resolved_at=None,
    )


def resolve_owner_approval(
    approval: OwnerApproval,
    *,
    status: object,
    resolved_at: object | None = None,
) -> OwnerApproval:
    normalized_status = _normalize_status(status)
    if normalized_status == "pending":
        raise ValueError("approval resolution status must not remain pending")
    return OwnerApproval(
        approval_id=approval.approval_id,
        owner_id=approval.owner_id,
        action_id=approval.action_id,
        trust_class=approval.trust_class,
        status=normalized_status,
        created_at=approval.created_at,
        resolved_at=_normalize_text(resolved_at) or _utc_timestamp(),
    )


class OwnerApprovalStore:
    def __init__(self, approvals: list[OwnerApproval] | None = None) -> None:
        self._approvals = {
            approval.approval_id: approval for approval in approvals or []
        }

    def add(self, approval: OwnerApproval) -> OwnerApproval:
        self._approvals[approval.approval_id] = approval
        return approval

    def get(self, approval_id: object) -> OwnerApproval | None:
        normalized = _normalize_text(approval_id)
        if not normalized:
            return None
        return self._approvals.get(normalized)

    def replace(self, approval: OwnerApproval) -> OwnerApproval:
        self._approvals[approval.approval_id] = approval
        return approval

    def list(
        self,
        *,
        owner_id: object | None = None,
        status: object | None = None,
    ) -> list[OwnerApproval]:
        normalized_owner_id = _normalize_text(owner_id)
        normalized_status = _normalize_text(status).lower()
        approvals = list(self._approvals.values())
        if normalized_owner_id:
            approvals = [
                approval
                for approval in approvals
                if approval.owner_id == normalized_owner_id
            ]
        if normalized_status:
            approvals = [
                approval
                for approval in approvals
                if approval.status == normalized_status
            ]
        return approvals
