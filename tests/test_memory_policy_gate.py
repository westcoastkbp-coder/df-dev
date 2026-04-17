from __future__ import annotations

from datetime import datetime, timezone

import app.policy.memory_policy_gate as memory_policy_gate_module
from app.policy.memory_policy_gate import evaluate_memory_policy


def _task_packet(*, tags: list[str] | None = None) -> dict[str, object]:
    memory_context: dict[str, object] = {
        "domain": "dev",
        "type": "task",
    }
    if tags is not None:
        memory_context["tags"] = tags
    return {
        "task_id": 9,
        "instruction": "Implement the task",
        "memory_context": memory_context,
    }


def _artifact(
    artifact_id: str,
    *,
    domain: str = "dev",
    artifact_type: str = "task",
    timestamp: str = "2026-04-14T11:00:00Z",
    tags: list[str] | None = None,
) -> dict[str, object]:
    return {
        "id": artifact_id,
        "domain": domain,
        "type": artifact_type,
        "timestamp": timestamp,
        "tags": list(tags or []),
    }


def test_evaluate_memory_policy_allows_when_no_memory_match(monkeypatch) -> None:
    monkeypatch.setattr(
        memory_policy_gate_module,
        "_utc_now",
        lambda: datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc),
    )

    decision = evaluate_memory_policy(_task_packet(), [])

    assert decision == {
        "allowed": True,
        "reason": "no_recent_duplicate",
        "matched_artifact_id": None,
        "action": "continue",
    }


def test_evaluate_memory_policy_allows_when_match_is_older_than_window(monkeypatch) -> None:
    monkeypatch.setattr(
        memory_policy_gate_module,
        "_utc_now",
        lambda: datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc),
    )

    decision = evaluate_memory_policy(
        _task_packet(),
        [_artifact("task-old", timestamp="2026-04-13T11:59:59Z")],
    )

    assert decision["allowed"] is True
    assert decision["reason"] == "no_recent_duplicate"
    assert decision["matched_artifact_id"] is None
    assert decision["action"] == "continue"


def test_evaluate_memory_policy_blocks_recent_same_domain_type_and_tags(monkeypatch) -> None:
    monkeypatch.setattr(
        memory_policy_gate_module,
        "_utc_now",
        lambda: datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc),
    )

    decision = evaluate_memory_policy(
        _task_packet(tags=["urgent", "finance"]),
        [
            _artifact(
                "task-recent",
                timestamp="2026-04-14T11:30:00Z",
                tags=["finance", "urgent"],
            )
        ],
    )

    assert decision == {
        "allowed": False,
        "reason": "recent_duplicate_detected",
        "matched_artifact_id": "task-recent",
        "action": "block",
    }


def test_evaluate_memory_policy_allows_for_different_domain_match(monkeypatch) -> None:
    monkeypatch.setattr(
        memory_policy_gate_module,
        "_utc_now",
        lambda: datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc),
    )

    decision = evaluate_memory_policy(
        _task_packet(),
        [_artifact("owner-task", domain="ownerbox", timestamp="2026-04-14T11:30:00Z")],
    )

    assert decision["allowed"] is True
    assert decision["reason"] == "no_recent_duplicate"


def test_evaluate_memory_policy_allows_for_different_type_match(monkeypatch) -> None:
    monkeypatch.setattr(
        memory_policy_gate_module,
        "_utc_now",
        lambda: datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc),
    )

    decision = evaluate_memory_policy(
        _task_packet(),
        [_artifact("dev-document", artifact_type="document", timestamp="2026-04-14T11:30:00Z")],
    )

    assert decision["allowed"] is True
    assert decision["reason"] == "no_recent_duplicate"


def test_evaluate_memory_policy_allows_when_required_tags_do_not_match_exactly(monkeypatch) -> None:
    monkeypatch.setattr(
        memory_policy_gate_module,
        "_utc_now",
        lambda: datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc),
    )

    decision = evaluate_memory_policy(
        _task_packet(tags=["urgent"]),
        [_artifact("task-recent", timestamp="2026-04-14T11:30:00Z", tags=["urgent", "finance"])],
    )

    assert decision["allowed"] is True
    assert decision["reason"] == "no_recent_duplicate"
