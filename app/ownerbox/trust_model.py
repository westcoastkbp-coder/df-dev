from __future__ import annotations

import re
from dataclasses import dataclass

from app.execution.action_contract import KNOWN_ACTION_TYPES
from app.ownerbox.domain import OwnerActionScope


TRUST_CLASSES = frozenset({"low", "medium", "high", "critical"})
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9._:@-]+$")


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _stable_identifier(value: object, *, field_name: str) -> str:
    normalized = _normalize_text(value)
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    if _IDENTIFIER_PATTERN.fullmatch(normalized) is None:
        raise ValueError(f"{field_name} must be a stable identifier")
    return normalized.upper()


def _normalize_trust_class(value: object) -> str:
    normalized = _normalize_text(value).lower()
    if normalized not in TRUST_CLASSES:
        raise ValueError("trust_class must be one of: " + ", ".join(sorted(TRUST_CLASSES)))
    return normalized


@dataclass(frozen=True, slots=True)
class ActionRiskProfile:
    action_type: str
    trust_class: str
    requires_confirmation: bool
    requires_high_trust: bool

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "action_type",
            _stable_identifier(self.action_type, field_name="action_type"),
        )
        object.__setattr__(self, "trust_class", _normalize_trust_class(self.trust_class))

    @property
    def auto_execute_allowed(self) -> bool:
        return self.trust_class == "low"

    @property
    def requires_explicit_approval(self) -> bool:
        return self.trust_class in {"medium", "high", "critical"}

    def to_dict(self) -> dict[str, object]:
        return {
            "action_type": self.action_type,
            "trust_class": self.trust_class,
            "requires_confirmation": self.requires_confirmation,
            "requires_high_trust": self.requires_high_trust,
        }


def is_known_action_type(action_type: object) -> bool:
    normalized = _normalize_text(action_type).upper()
    return normalized in KNOWN_ACTION_TYPES


def classify_action_risk(
    action_type: object,
    *,
    action_scope: OwnerActionScope | None = None,
    action_parameters: dict[str, object] | None = None,
) -> ActionRiskProfile:
    normalized_action_type = _stable_identifier(action_type, field_name="action_type")
    normalized_parameters = dict(action_parameters or {})
    operation = _normalize_text(normalized_parameters.get("operation")).lower()
    requires_high_trust = bool(
        action_scope is not None
        and normalized_action_type in action_scope.requires_high_trust_for
    )
    requires_confirmation = bool(
        action_scope is not None
        and normalized_action_type in action_scope.requires_confirmation_for
    )

    if requires_high_trust:
        trust_class = "critical"
    elif normalized_action_type == "OPENAI_REQUEST":
        trust_class = "low"
    elif normalized_action_type == "EMAIL_ACTION" and operation == "create_draft":
        trust_class = "low"
    elif normalized_action_type == "BROWSER_ACTION" and operation in {
        "open_page",
        "extract_text",
        "fill_form",
    }:
        trust_class = "low"
    elif requires_confirmation:
        trust_class = "medium"
    elif normalized_action_type in {"BROWSER_ACTION", "EMAIL_ACTION"}:
        trust_class = "high"
    else:
        trust_class = "high"

    return ActionRiskProfile(
        action_type=normalized_action_type,
        trust_class=trust_class,
        requires_confirmation=(trust_class != "low"),
        requires_high_trust=requires_high_trust or trust_class == "critical",
    )
