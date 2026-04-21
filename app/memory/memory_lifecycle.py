from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from app.memory.canonical_memory import MemoryObject
from app.memory.memory_policy import MemoryPromotionCandidate

if TYPE_CHECKING:
    from app.memory.memory_store import CanonicalMemoryStore


SUPERSEDABLE_MEMORY_TYPES = frozenset(
    {"fact", "decision", "preference", "document_ref"}
)
SUPPORTED_LIFECYCLE_STATES = frozenset(
    {"proposed", "validated", "active", "superseded", "deprecated", "archived"}
)
ALLOWED_TRANSITIONS = {
    "proposed": frozenset({"validated"}),
    "validated": frozenset({"active"}),
    "active": frozenset({"superseded", "deprecated", "archived"}),
    "superseded": frozenset({"archived"}),
    "deprecated": frozenset({"archived"}),
    "archived": frozenset(),
}


class MemoryLifecycleError(RuntimeError):
    """Raised when canonical lifecycle transitions cannot be applied safely."""


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _memory_key(memory_type: object, structured_payload: dict[str, Any]) -> str:
    normalized_memory_type = _normalize_text(memory_type).lower()
    if normalized_memory_type == "preference":
        return _normalize_text(structured_payload.get("preference_key"))
    if normalized_memory_type == "decision":
        return (
            _normalize_text(structured_payload.get("decision_scope"))
            or _normalize_text(structured_payload.get("decision_key"))
            or _normalize_text(structured_payload.get("decision_ref"))
        )
    if normalized_memory_type == "fact":
        return _normalize_text(structured_payload.get("fact_key"))
    if normalized_memory_type == "document_ref":
        return _normalize_text(
            structured_payload.get("document_id")
        ) or _normalize_text(structured_payload.get("document_locator"))
    if normalized_memory_type == "entity_ref":
        return _normalize_text(structured_payload.get("entity_ref"))
    if normalized_memory_type == "relationship":
        return (
            _normalize_text(structured_payload.get("relationship_type"))
            + ":"
            + _normalize_text(structured_payload.get("related_ref"))
        ).strip(":")
    return ""


def build_conflict_key(
    *,
    memory_type: object,
    domain_scope: object,
    owner_ref: object,
    subject_ref: object,
    structured_payload: dict[str, Any],
) -> str | None:
    key = _memory_key(memory_type, structured_payload)
    if not key:
        return None
    normalized_memory_type = _normalize_text(memory_type).lower()
    normalized_domain_scope = _normalize_text(domain_scope).lower()
    normalized_owner_ref = _normalize_text(owner_ref) or "_"
    normalized_subject_ref = _normalize_text(subject_ref) or "_"
    return (
        f"{normalized_memory_type}/"
        f"{normalized_domain_scope}/"
        f"{normalized_owner_ref}/"
        f"{normalized_subject_ref}/"
        f"{key}"
    )


def candidate_conflict_key(candidate: MemoryPromotionCandidate) -> str | None:
    return build_conflict_key(
        memory_type=candidate.memory_type,
        domain_scope=candidate.domain_scope,
        owner_ref=candidate.owner_ref,
        subject_ref=candidate.subject_ref,
        structured_payload=candidate.structured_payload,
    )


def memory_conflict_key(memory_object: MemoryObject) -> str | None:
    return build_conflict_key(
        memory_type=memory_object.memory_type,
        domain_scope=memory_object.domain_scope,
        owner_ref=memory_object.owner_ref,
        subject_ref=memory_object.subject_ref,
        structured_payload=memory_object.structured_payload,
    )


def _comparable_value(memory_type: object, structured_payload: dict[str, Any]) -> Any:
    normalized_memory_type = _normalize_text(memory_type).lower()
    if normalized_memory_type == "preference":
        return structured_payload.get("preference_value")
    if normalized_memory_type == "decision":
        return structured_payload.get("decision_payload") or structured_payload
    if normalized_memory_type == "fact":
        if "fact_value" in structured_payload:
            return structured_payload.get("fact_value")
        if "result_payload" in structured_payload:
            return structured_payload.get("result_payload")
        if "evidence_payload" in structured_payload:
            return structured_payload.get("evidence_payload")
    if normalized_memory_type == "document_ref":
        return {
            "document_id": structured_payload.get("document_id"),
            "document_locator": structured_payload.get("document_locator"),
        }
    if normalized_memory_type == "entity_ref":
        return {
            "entity_type": structured_payload.get("entity_type"),
            "entity_ref": structured_payload.get("entity_ref"),
        }
    if normalized_memory_type == "relationship":
        return {
            "relationship_type": structured_payload.get("relationship_type"),
            "related_ref": structured_payload.get("related_ref"),
        }
    return structured_payload


def transition_lifecycle_state(current_state: object, next_state: object) -> str:
    normalized_current = _normalize_text(current_state).lower()
    normalized_next = _normalize_text(next_state).lower()
    if normalized_current not in SUPPORTED_LIFECYCLE_STATES:
        raise MemoryLifecycleError("unsupported_lifecycle_state")
    if normalized_next not in SUPPORTED_LIFECYCLE_STATES:
        raise MemoryLifecycleError("unsupported_lifecycle_state")
    if normalized_next not in ALLOWED_TRANSITIONS.get(normalized_current, frozenset()):
        raise MemoryLifecycleError(
            f"invalid_lifecycle_transition:{normalized_current}->{normalized_next}"
        )
    return normalized_next


@dataclass(frozen=True, slots=True)
class MemoryPromotionPlan:
    conflict_key: str | None
    prior_active_records: tuple[MemoryObject, ...]
    previous_version_id: str | None
    next_version: int
    lifecycle_path: tuple[str, ...]

    @property
    def prior_memory_id(self) -> str | None:
        if not self.prior_active_records:
            return None
        return self.prior_active_records[0].memory_id


def plan_promotion(
    candidate: MemoryPromotionCandidate,
    *,
    store: CanonicalMemoryStore,
) -> MemoryPromotionPlan:
    validated_state = transition_lifecycle_state(candidate.lifecycle_state, "validated")
    active_state = transition_lifecycle_state(validated_state, "active")
    conflict_key = candidate_conflict_key(candidate)

    all_conflict_records: tuple[MemoryObject, ...] = ()
    if conflict_key:
        all_conflict_records = tuple(
            store.list_memory_objects(
                domain_scope=candidate.domain_scope,
                memory_type=candidate.memory_type,
                owner_ref=candidate.owner_ref,
                subject_ref=candidate.subject_ref,
                status=("active", "superseded", "deprecated", "archived"),
                conflict_key=conflict_key,
                limit=100,
            )
        )
    active_records = tuple(
        item for item in all_conflict_records if item.status == "active"
    )
    if active_records:
        candidate_value = _comparable_value(
            candidate.memory_type, candidate.structured_payload
        )
        for record in active_records:
            if (
                _comparable_value(record.memory_type, record.structured_payload)
                == candidate_value
            ):
                raise MemoryLifecycleError(
                    "duplicate_active_memory: matching active canonical memory already exists"
                )
        if candidate.memory_type not in SUPERSEDABLE_MEMORY_TYPES:
            raise MemoryLifecycleError(
                "conflicting_active_memory: active canonical memory conflict must be resolved explicitly"
            )

    latest_record = None
    if all_conflict_records:
        latest_record = max(
            all_conflict_records,
            key=lambda item: (item.version, item.updated_at, item.memory_id),
        )
    next_version = 1 if latest_record is None else latest_record.version + 1
    previous_version_id = None if latest_record is None else latest_record.memory_id
    return MemoryPromotionPlan(
        conflict_key=conflict_key,
        prior_active_records=tuple(
            sorted(
                active_records,
                key=lambda item: (item.version, item.updated_at, item.memory_id),
                reverse=True,
            )
        ),
        previous_version_id=previous_version_id,
        next_version=next_version,
        lifecycle_path=("proposed", validated_state, active_state),
    )
