from __future__ import annotations

from app.policy.cross_domain_conflict_gate import evaluate_cross_domain_conflict


def _task_packet(*, domain: str = "dev", resource_id: str = "crew-west") -> dict[str, object]:
    return {
        "task_id": 9,
        "instruction": "Execute the task",
        "memory_context": {"domain": domain, "type": "task"},
        "payload": {
            "domain": domain,
            "resource_id": resource_id,
        },
        "status": "created",
    }


def _artifact(
    artifact_id: str,
    *,
    domain: str,
    resource_id: str,
    status: str = "running",
    artifact_type: str = "task",
) -> dict[str, object]:
    return {
        "id": artifact_id,
        "domain": domain,
        "type": artifact_type,
        "status": status,
        "payload": {
            "domain": domain,
            "resource_id": resource_id,
        },
    }


def test_cross_domain_conflict_gate_allows_same_resource_in_same_domain() -> None:
    decision = evaluate_cross_domain_conflict(
        _task_packet(domain="dev", resource_id="crew-west"),
        [_artifact("artifact-1", domain="dev", resource_id="crew-west")],
    )

    assert decision == {
        "allowed": True,
        "reason": "no_cross_domain_conflict",
        "conflict_with": None,
        "action": "continue",
    }


def test_cross_domain_conflict_gate_blocks_same_resource_in_different_domain() -> None:
    decision = evaluate_cross_domain_conflict(
        _task_packet(domain="dev", resource_id="crew-west"),
        [_artifact("artifact-2", domain="ownerbox", resource_id="crew-west")],
    )

    assert decision == {
        "allowed": False,
        "reason": "cross_domain_conflict_detected",
        "conflict_with": "artifact-2",
        "action": "block",
    }


def test_cross_domain_conflict_gate_allows_different_resource() -> None:
    decision = evaluate_cross_domain_conflict(
        _task_packet(domain="dev", resource_id="crew-west"),
        [_artifact("artifact-3", domain="ownerbox", resource_id="crew-east")],
    )

    assert decision == {
        "allowed": True,
        "reason": "no_cross_domain_conflict",
        "conflict_with": None,
        "action": "continue",
    }


def test_cross_domain_conflict_gate_allows_completed_artifact_in_other_domain() -> None:
    decision = evaluate_cross_domain_conflict(
        _task_packet(domain="dev", resource_id="crew-west"),
        [_artifact("artifact-4", domain="ownerbox", resource_id="crew-west", status="completed")],
    )

    assert decision == {
        "allowed": True,
        "reason": "no_cross_domain_conflict",
        "conflict_with": None,
        "action": "continue",
    }
