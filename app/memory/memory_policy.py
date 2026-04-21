from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping
from uuid import UUID, uuid4

from app.memory.canonical_memory import (
    SUPPORTED_MEMORY_DOMAINS,
    SUPPORTED_MEMORY_TYPES,
    SUPPORTED_TRUST_LEVELS,
    CanonicalMemoryError,
    utc_timestamp,
)


ALLOWED_CANDIDATE_KINDS = frozenset(
    {
        "owner_fact",
        "owner_preference",
        "execution_result",
        "decision",
        "evidence_summary",
        "document_reference",
    }
)
ALLOWED_SOURCE_TYPES = frozenset(
    {
        "owner_input",
        "execution_result",
        "decision_record",
        "evidence_summary",
        "document_reference",
    }
)
DISALLOWED_SOURCE_TYPES = frozenset(
    {
        "raw_browser_dump",
        "raw_printer_payload",
        "raw_mailbox_dump",
        "raw_email_body",
        "raw_transcript",
        "transcript_fragment",
        "connector_dump",
        "connector_content",
        "trace_entry",
        "workflow_state",
        "idempotency_state",
        "speculative_inference",
    }
)
DISALLOWED_RAW_KEYS = frozenset(
    {
        "raw_artifact",
        "raw_browser_dump",
        "raw_content",
        "raw_mailbox_dump",
        "raw_email_body",
        "raw_printer_payload",
        "raw_trace",
        "raw_transcript",
        "transcript_fragment",
        "trace_entries",
        "transcript_text",
    }
)


class MemoryPolicyError(RuntimeError):
    """Raised when canonical memory promotion is not allowed."""


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_payload(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return {str(key).strip(): value[key] for key in sorted(value) if str(key).strip()}
    raise MemoryPolicyError("structured_payload must be a mapping")


def _normalize_confidence(value: object) -> float:
    try:
        normalized = float(value)
    except (TypeError, ValueError) as exc:
        raise MemoryPolicyError("confidence must be a number") from exc
    if normalized < 0.0 or normalized > 1.0:
        raise MemoryPolicyError("confidence must be between 0.0 and 1.0")
    return round(normalized, 6)


def _normalize_uuid(value: object, *, field_name: str) -> str:
    normalized = _normalize_text(value)
    if not normalized:
        raise MemoryPolicyError(f"{field_name} must not be empty")
    try:
        UUID(normalized)
    except (TypeError, ValueError, AttributeError):
        if not normalized:
            raise MemoryPolicyError(f"{field_name} must not be empty")
    return normalized


@dataclass(frozen=True, slots=True)
class MemoryPromotionCandidate:
    candidate_id: str = field(default_factory=lambda: str(uuid4()))
    candidate_kind: str = ""
    memory_type: str = ""
    domain_scope: str = ""
    domain_type: str = ""
    owner_ref: str | None = None
    subject_ref: str | None = None
    content_summary: str = ""
    structured_payload: dict[str, Any] = field(default_factory=dict)
    trust_level: str = "validated"
    trust_class: str = "governed"
    source_type: str = ""
    source_ref: str = ""
    source_trace_id: str = ""
    evidence_ref: str = ""
    confidence: float = 1.0
    validation_passed: bool = True
    trace_complete: bool = True
    evidence_exists: bool = True
    created_at: str = field(default_factory=utc_timestamp)
    updated_at: str = field(default_factory=utc_timestamp)
    lifecycle_state: str = "proposed"

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "candidate_id",
            _normalize_uuid(self.candidate_id, field_name="candidate_id"),
        )
        object.__setattr__(self, "candidate_kind", _normalize_text(self.candidate_kind).lower())
        object.__setattr__(self, "memory_type", _normalize_text(self.memory_type).lower())
        object.__setattr__(
            self,
            "domain_scope",
            _normalize_text(self.domain_scope or self.domain_type).lower(),
        )
        object.__setattr__(self, "domain_type", self.domain_scope)
        object.__setattr__(self, "owner_ref", _normalize_text(self.owner_ref) or None)
        object.__setattr__(self, "subject_ref", _normalize_text(self.subject_ref) or None)
        object.__setattr__(self, "content_summary", _normalize_text(self.content_summary))
        object.__setattr__(self, "structured_payload", _normalize_payload(self.structured_payload))
        object.__setattr__(self, "trust_level", _normalize_text(self.trust_level).lower())
        object.__setattr__(self, "trust_class", _normalize_text(self.trust_class))
        object.__setattr__(self, "source_type", _normalize_text(self.source_type).lower())
        object.__setattr__(self, "source_ref", _normalize_text(self.source_ref))
        object.__setattr__(
            self,
            "source_trace_id",
            _normalize_text(self.source_trace_id) or f"trace:{_normalize_text(self.source_ref)}",
        )
        object.__setattr__(
            self,
            "evidence_ref",
            _normalize_text(self.evidence_ref) or f"evidence:{_normalize_text(self.source_ref)}",
        )
        object.__setattr__(self, "confidence", _normalize_confidence(self.confidence))
        object.__setattr__(self, "validation_passed", bool(self.validation_passed))
        object.__setattr__(self, "trace_complete", bool(self.trace_complete))
        object.__setattr__(self, "evidence_exists", bool(self.evidence_exists))
        object.__setattr__(self, "created_at", _normalize_text(self.created_at) or utc_timestamp())
        object.__setattr__(self, "updated_at", _normalize_text(self.updated_at) or self.created_at)
        object.__setattr__(self, "lifecycle_state", _normalize_text(self.lifecycle_state).lower() or "proposed")

        if self.candidate_kind not in ALLOWED_CANDIDATE_KINDS:
            raise MemoryPolicyError("candidate_kind is not supported for canonical promotion")
        if self.memory_type not in SUPPORTED_MEMORY_TYPES:
            raise MemoryPolicyError("memory_type is not supported for canonical promotion")
        if self.domain_scope not in SUPPORTED_MEMORY_DOMAINS:
            raise MemoryPolicyError("domain_scope is not supported for canonical promotion")
        if self.trust_level not in SUPPORTED_TRUST_LEVELS:
            raise MemoryPolicyError("trust_level is not supported for canonical promotion")
        if self.lifecycle_state != "proposed":
            raise MemoryPolicyError("promotion candidates must start in proposed state")
        if not self.trust_class:
            raise MemoryPolicyError("trust_class must not be empty")
        if not self.source_ref:
            raise MemoryPolicyError("source_ref must not be empty")
        if not self.source_trace_id:
            raise MemoryPolicyError("source_trace_id must not be empty")
        if not self.evidence_ref:
            raise MemoryPolicyError("evidence_ref must not be empty")
        if not self.owner_ref and not self.subject_ref:
            raise MemoryPolicyError("owner_ref or subject_ref must be provided")

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "candidate_kind": self.candidate_kind,
            "memory_type": self.memory_type,
            "type": self.memory_type,
            "domain_scope": self.domain_scope,
            "domain_type": self.domain_scope,
            "owner_ref": self.owner_ref,
            "subject_ref": self.subject_ref,
            "content_summary": self.content_summary,
            "structured_payload": dict(self.structured_payload),
            "trust_level": self.trust_level,
            "trust_class": self.trust_class,
            "source_type": self.source_type,
            "source_ref": self.source_ref,
            "source_trace_id": self.source_trace_id,
            "evidence_ref": self.evidence_ref,
            "confidence": self.confidence,
            "validation_passed": self.validation_passed,
            "trace_complete": self.trace_complete,
            "evidence_exists": self.evidence_exists,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "lifecycle_state": self.lifecycle_state,
        }


@dataclass(frozen=True, slots=True)
class MemoryPolicyDecision:
    allowed: bool
    policy_code: str
    reason: str

    def to_dict(self) -> dict[str, object]:
        return {
            "allowed": self.allowed,
            "policy_code": self.policy_code,
            "reason": self.reason,
        }


def evaluate_memory_candidate(candidate: MemoryPromotionCandidate) -> MemoryPolicyDecision:
    if candidate.source_type in DISALLOWED_SOURCE_TYPES:
        return MemoryPolicyDecision(
            allowed=False,
            policy_code="disallowed_source_type",
            reason="raw or unvalidated sources cannot become canonical memory",
        )
    if candidate.source_type not in ALLOWED_SOURCE_TYPES:
        return MemoryPolicyDecision(
            allowed=False,
            policy_code="unknown_source_type",
            reason="canonical memory promotion fails closed for unknown source types",
        )
    if any(key in DISALLOWED_RAW_KEYS for key in candidate.structured_payload):
        return MemoryPolicyDecision(
            allowed=False,
            policy_code="raw_content_blocked",
            reason="raw artifacts, transcripts, dumps, and trace payloads are not canonical memory",
        )
    if not candidate.content_summary:
        return MemoryPolicyDecision(
            allowed=False,
            policy_code="missing_summary",
            reason="canonical memory requires a controlled summary",
        )
    if not candidate.trace_complete:
        return MemoryPolicyDecision(
            allowed=False,
            policy_code="trace_incomplete",
            reason="canonical memory promotion requires a complete execution trace",
        )
    if not candidate.evidence_exists:
        return MemoryPolicyDecision(
            allowed=False,
            policy_code="missing_evidence",
            reason="canonical memory promotion requires evidence",
        )
    if not candidate.validation_passed:
        return MemoryPolicyDecision(
            allowed=False,
            policy_code="validation_failed",
            reason="canonical memory promotion requires explicit validation",
        )

    if candidate.candidate_kind == "owner_fact":
        if candidate.source_type != "owner_input" or candidate.memory_type != "fact":
            return MemoryPolicyDecision(False, "owner_fact_shape_invalid", "owner facts must come from validated owner input and promote to fact memory")
        if not bool(candidate.structured_payload.get("validated")):
            return MemoryPolicyDecision(False, "owner_fact_not_validated", "owner facts require explicit validation before canonical promotion")
        if not candidate.structured_payload.get("fact_key"):
            return MemoryPolicyDecision(False, "owner_fact_missing_key", "owner facts require a stable fact_key")
        return MemoryPolicyDecision(True, "allowed", "validated owner fact is eligible for canonical memory")

    if candidate.candidate_kind == "owner_preference":
        if candidate.source_type != "owner_input" or candidate.memory_type != "preference":
            return MemoryPolicyDecision(False, "owner_preference_shape_invalid", "owner preferences must come from validated owner input and promote to preference memory")
        if not bool(candidate.structured_payload.get("validated")):
            return MemoryPolicyDecision(False, "owner_preference_not_validated", "owner preferences require explicit validation before canonical promotion")
        if not candidate.structured_payload.get("preference_key"):
            return MemoryPolicyDecision(False, "owner_preference_missing_key", "owner preferences require a stable preference_key")
        return MemoryPolicyDecision(True, "allowed", "validated owner preference is eligible for canonical memory")

    if candidate.candidate_kind == "execution_result":
        if candidate.source_type != "execution_result":
            return MemoryPolicyDecision(False, "execution_result_source_invalid", "execution-result promotion requires execution_result source_type")
        if candidate.memory_type != "fact":
            return MemoryPolicyDecision(False, "execution_result_type_invalid", "execution-result promotion is bounded to fact memory in v1")
        if not bool(candidate.structured_payload.get("approved")):
            return MemoryPolicyDecision(False, "execution_result_not_approved", "only approved execution results can become canonical memory")
        if _normalize_text(candidate.structured_payload.get("result_status")).lower() not in {
            "approved",
            "completed",
            "success",
        }:
            return MemoryPolicyDecision(False, "execution_result_status_invalid", "execution result must have a successful approved outcome")
        if not candidate.structured_payload.get("fact_key"):
            return MemoryPolicyDecision(False, "execution_result_missing_key", "execution result facts require a stable fact_key")
        return MemoryPolicyDecision(True, "allowed", "approved execution result is eligible for canonical memory")

    if candidate.candidate_kind == "decision":
        if candidate.source_type != "decision_record" or candidate.memory_type != "decision":
            return MemoryPolicyDecision(False, "decision_shape_invalid", "structured decisions must use decision_record source_type and decision memory type")
        if not candidate.structured_payload.get("decision_ref"):
            return MemoryPolicyDecision(False, "decision_missing_ref", "structured decisions require a decision_ref")
        if not (
            candidate.structured_payload.get("decision_scope")
            or candidate.structured_payload.get("decision_key")
            or candidate.structured_payload.get("decision_ref")
        ):
            return MemoryPolicyDecision(False, "decision_missing_scope", "structured decisions require a stable scope or reference")
        return MemoryPolicyDecision(True, "allowed", "structured decision is eligible for canonical memory")

    if candidate.candidate_kind == "evidence_summary":
        if candidate.source_type != "evidence_summary" or candidate.memory_type != "fact":
            return MemoryPolicyDecision(False, "evidence_shape_invalid", "bounded evidence summaries must use evidence_summary source_type and fact memory type")
        if not bool(candidate.structured_payload.get("bounded_summary")):
            return MemoryPolicyDecision(False, "evidence_not_bounded", "evidence summaries must explicitly declare bounded_summary=true")
        if not candidate.structured_payload.get("fact_key"):
            return MemoryPolicyDecision(False, "evidence_missing_key", "bounded evidence summaries require a stable fact_key")
        return MemoryPolicyDecision(True, "allowed", "bounded execution evidence summary is eligible for canonical memory")

    if candidate.candidate_kind == "document_reference":
        if candidate.source_type != "document_reference" or candidate.memory_type != "document_ref":
            return MemoryPolicyDecision(False, "document_reference_shape_invalid", "controlled document references must use document_reference source_type and document_ref memory type")
        if not (
            candidate.structured_payload.get("document_id")
            or candidate.structured_payload.get("document_locator")
        ):
            return MemoryPolicyDecision(False, "document_reference_missing_identity", "controlled document references require document_id or document_locator")
        return MemoryPolicyDecision(True, "allowed", "controlled document reference is eligible for canonical memory")

    return MemoryPolicyDecision(
        allowed=False,
        policy_code="unsupported_candidate_kind",
        reason="candidate kind is not supported for canonical memory",
    )


def assert_memory_candidate_allowed(candidate: MemoryPromotionCandidate) -> None:
    decision = evaluate_memory_candidate(candidate)
    if not decision.allowed:
        raise MemoryPolicyError(decision.policy_code)
