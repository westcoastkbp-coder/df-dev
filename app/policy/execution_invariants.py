from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from app.policy.replay_protection import check_replay
from app.state.state_store import StateStoreError, get_state


_KNOWN_DOMAINS = ("dev", "ownerbox")
_TERMINAL_ENTITY_STATES = frozenset({"resolved", "archived"})
_ACTIVE_CONFLICT_STATES = frozenset({"conflict_active", "pending_resolution", "active"})
_ALLOWED_ENTITY_TYPES = frozenset({"task", "resource", "conflict", "owner_object"})


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_mapping(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _task_domain(task_packet: dict[str, Any]) -> str:
    payload = _normalize_mapping(task_packet.get("payload"))
    memory_context = _normalize_mapping(task_packet.get("memory_context"))
    normalized = _normalize_text(
        payload.get("domain")
        or task_packet.get("domain")
        or memory_context.get("domain")
        or "dev"
    ).lower()
    if normalized in {"owner", "ownerbox"}:
        return "ownerbox"
    return "dev"


def _first_non_empty(*values: object) -> str:
    for value in values:
        normalized = _normalize_text(value)
        if normalized:
            return normalized
    return ""


def _tracked_entities(task_packet: dict[str, Any]) -> list[dict[str, str]]:
    payload = _normalize_mapping(task_packet.get("payload"))
    memory_context = _normalize_mapping(task_packet.get("memory_context"))
    tracked: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    resource_id = _first_non_empty(
        payload.get("resource_id"),
        task_packet.get("resource_id"),
    )
    if resource_id:
        tracked.append({"entity_type": "resource", "entity_id": resource_id})
        seen.add(("resource", resource_id))

    explicit_entity_type = _normalize_text(
        payload.get("entity_type")
        or task_packet.get("entity_type")
        or memory_context.get("entity_type")
    ).lower()
    explicit_entity_id = _first_non_empty(
        payload.get("entity_id"),
        task_packet.get("entity_id"),
        payload.get("logical_id"),
        task_packet.get("logical_id"),
        payload.get("idempotency_key"),
        task_packet.get("idempotency_key"),
    )
    if (
        explicit_entity_type in _ALLOWED_ENTITY_TYPES
        and explicit_entity_id
        and (explicit_entity_type, explicit_entity_id) not in seen
    ):
        tracked.append(
            {
                "entity_type": explicit_entity_type,
                "entity_id": explicit_entity_id,
            }
        )

    return tracked


def _state(entity_type: str, entity_id: str, *, domain: str) -> dict[str, Any] | None:
    try:
        return get_state(entity_type, entity_id, domain=domain)
    except StateStoreError:
        return None


def _violation(
    violation_type: str,
    *,
    entity_type: str = "",
    entity_id: str = "",
    domain: str = "",
    other_domain: str = "",
    state: str = "",
    previous_trace_id: str = "",
) -> dict[str, str]:
    payload = {"type": _normalize_text(violation_type)}
    if entity_type:
        payload["entity_type"] = _normalize_text(entity_type)
    if entity_id:
        payload["entity_id"] = _normalize_text(entity_id)
    if domain:
        payload["domain"] = _normalize_text(domain)
    if other_domain:
        payload["other_domain"] = _normalize_text(other_domain)
    if state:
        payload["state"] = _normalize_text(state)
    if previous_trace_id:
        payload["previous_trace_id"] = _normalize_text(previous_trace_id)
    return payload


def _append_violation(
    violations: list[dict[str, str]],
    violation: dict[str, str],
    *,
    task_id: str,
) -> None:
    if violation in violations:
        return
    violations.append(violation)
    print(f"[INVARIANT] violation type={violation['type']} task={task_id or 'unknown'}")


def check_invariants(task_packet: dict[str, Any] | None) -> dict[str, Any]:
    task = _normalize_mapping(task_packet)
    task_id = _normalize_text(task.get("task_id"))
    task_domain = _task_domain(task)
    violations: list[dict[str, str]] = []

    replay_decision = check_replay(task)
    if not bool(replay_decision.get("allowed", True)):
        _append_violation(
            violations,
            _violation(
                "replay_execution",
                previous_trace_id=_normalize_text(
                    replay_decision.get("previous_trace_id")
                ),
            ),
            task_id=task_id,
        )

    tracked_entities = _tracked_entities(task)
    for tracked in tracked_entities:
        entity_type = tracked["entity_type"]
        entity_id = tracked["entity_id"]
        same_domain_state = _state(entity_type, entity_id, domain=task_domain)
        other_states = [
            {
                "domain": domain,
                "state": state_payload,
            }
            for domain in _KNOWN_DOMAINS
            if domain != task_domain
            for state_payload in [_state(entity_type, entity_id, domain=domain)]
            if state_payload is not None
        ]

        if other_states:
            for other in other_states:
                _append_violation(
                    violations,
                    _violation(
                        "domain_isolation",
                        entity_type=entity_type,
                        entity_id=entity_id,
                        domain=task_domain,
                        other_domain=str(other["domain"]),
                    ),
                    task_id=task_id,
                )

        if same_domain_state is None and not other_states:
            _append_violation(
                violations,
                _violation(
                    "state_consistency",
                    entity_type=entity_type,
                    entity_id=entity_id,
                    domain=task_domain,
                ),
                task_id=task_id,
            )
            continue

        if same_domain_state is not None:
            normalized_state = _normalize_text(same_domain_state.get("state")).lower()
            if normalized_state in _TERMINAL_ENTITY_STATES:
                _append_violation(
                    violations,
                    _violation(
                        "resolved_entity",
                        entity_type=entity_type,
                        entity_id=entity_id,
                        domain=task_domain,
                        state=normalized_state,
                    ),
                    task_id=task_id,
                )

        conflict_state = _state("conflict", entity_id, domain="ownerbox")
        if conflict_state is not None:
            normalized_conflict_state = _normalize_text(
                conflict_state.get("state")
            ).lower()
            if normalized_conflict_state in _ACTIVE_CONFLICT_STATES:
                _append_violation(
                    violations,
                    _violation(
                        "conflicting_active_state",
                        entity_type="conflict",
                        entity_id=entity_id,
                        domain="ownerbox",
                        state=normalized_conflict_state,
                    ),
                    task_id=task_id,
                )

    return {
        "allowed": not violations,
        "violations": violations,
        "action": "allow" if not violations else "block",
    }
