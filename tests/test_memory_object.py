from __future__ import annotations

import pytest

from app.memory.memory_object import (
    MemoryObjectError,
    make_artifact_object,
    make_conflict_object,
    make_trace_object,
)


def test_canonical_memory_object_constructors_produce_expected_defaults() -> None:
    artifact = make_artifact_object(
        id="artifact-1",
        domain="dev",
        payload={"summary": "artifact"},
        local_path="/tmp/artifact.json",
        artifact_type="task",
    ).to_dict()
    trace = make_trace_object(
        id="trace-1",
        domain="ownerbox",
        payload={"task_id": "71"},
        local_path="/tmp/trace.json",
    ).to_dict()
    conflict = make_conflict_object(
        id="conflict-1",
        domain="ownerbox",
        payload={"resource_id": "crew-west"},
        local_path="/tmp/conflict.json",
        state="pending_resolution",
    ).to_dict()

    assert artifact["memory_class"] == "artifact"
    assert artifact["status"] == "active"
    assert artifact["truth_level"] == "working"
    assert artifact["execution_role"] == "output"
    assert artifact["type"] == "task"
    assert artifact["payload"] == {"summary": "artifact"}

    assert trace["memory_class"] == "trace"
    assert trace["execution_role"] == "evidence"
    assert trace["type"] == "execution_trace"

    assert conflict["memory_class"] == "conflict"
    assert conflict["execution_role"] == "blocker"
    assert conflict["state"] == "pending_resolution"
    assert conflict["type"] == "conflict_escalation"


def test_canonical_memory_object_rejects_invalid_domain() -> None:
    with pytest.raises(MemoryObjectError, match="domain must be one of"):
        make_artifact_object(
            id="artifact-1",
            domain="qa",
            payload={},
            artifact_type="task",
        )
