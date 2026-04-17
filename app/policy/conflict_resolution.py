from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.memory.memory_object import effective_status
from app.memory.memory_registry import get_artifact_by_id
from app.state.state_store import set_state
from app.storage.storage_adapter import load_artifact, save_artifact


CONFLICT_ESCALATION_TYPE = "conflict_escalation"
PENDING_RESOLUTION_STATUS = "pending_resolution"
RESOLVED_STATUS = "resolved"
VALID_RESOLUTION_TYPES = frozenset({"owner_override", "dev_override", "merge_allowed"})


class ConflictResolutionError(RuntimeError):
    """Raised when a conflict escalation artifact cannot be resolved."""


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _validate_resolution_type(resolution_type: object) -> str:
    normalized = _normalize_text(resolution_type)
    if normalized not in VALID_RESOLUTION_TYPES:
        allowed = ", ".join(sorted(VALID_RESOLUTION_TYPES))
        raise ConflictResolutionError(
            f"resolution_type must be one of: {allowed}"
        )
    return normalized


def _validate_actor(actor: object) -> str:
    normalized = _normalize_text(actor)
    if not normalized:
        raise ConflictResolutionError("actor must not be empty.")
    return normalized


def resolve_conflict(
    artifact_id: object,
    resolution_type: object,
    actor: object,
) -> dict[str, Any]:
    normalized_artifact_id = _normalize_text(artifact_id)
    if not normalized_artifact_id:
        raise ConflictResolutionError("artifact_id must not be empty.")

    normalized_resolution_type = _validate_resolution_type(resolution_type)
    normalized_actor = _validate_actor(actor)

    registry_entry = get_artifact_by_id(normalized_artifact_id)
    if not isinstance(registry_entry, dict):
        raise ConflictResolutionError(
            f"conflict artifact not found: {normalized_artifact_id}"
        )
    if _normalize_text(registry_entry.get("type")) != CONFLICT_ESCALATION_TYPE:
        raise ConflictResolutionError(
            f"artifact is not a {CONFLICT_ESCALATION_TYPE}: {normalized_artifact_id}"
        )

    artifact = load_artifact(
        _normalize_text(registry_entry.get("domain")),
        _normalize_text(registry_entry.get("local_path")),
    )
    if _normalize_text(artifact.get("type")) != CONFLICT_ESCALATION_TYPE:
        raise ConflictResolutionError(
            f"artifact is not a {CONFLICT_ESCALATION_TYPE}: {normalized_artifact_id}"
        )

    status = effective_status(artifact)
    if status not in {PENDING_RESOLUTION_STATUS, "pending", "active"}:
        raise ConflictResolutionError(
            f"artifact status does not allow resolution: {status or 'missing'}"
        )

    resolution_payload = {
        "resolved_by": normalized_actor,
        "resolution_type": normalized_resolution_type,
        "timestamp": _utc_timestamp(),
    }

    payload = dict(artifact.get("payload") or {})
    saved_path = save_artifact(
        _normalize_text(artifact.get("domain")),
        CONFLICT_ESCALATION_TYPE,
        payload,
        overwrite=True,
        artifact_status=RESOLVED_STATUS,
        resolution=resolution_payload,
    )
    print(
        f"[RESOLUTION] resolved artifact={normalized_artifact_id} "
        f"by={normalized_actor} type={normalized_resolution_type}"
    )
    resolved_artifact = load_artifact(
        _normalize_text(artifact.get("domain")),
        saved_path,
    )
    set_state(
        "conflict",
        _normalize_text(payload.get("logical_id") or payload.get("resource_id") or normalized_artifact_id),
        "resolved",
        str(saved_path),
        domain=_normalize_text(artifact.get("domain")),
    )
    return resolved_artifact
