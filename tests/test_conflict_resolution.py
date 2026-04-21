from __future__ import annotations

import json
from pathlib import Path

import pytest

import scripts.run_codex_task as run_codex_task_module
from app.memory import memory_registry
from app.policy.conflict_resolution import ConflictResolutionError, resolve_conflict
from app.policy.cross_domain_conflict_gate import evaluate_cross_domain_conflict
from app.state.state_store import set_state
from app.storage import storage_adapter
from scripts.run_codex_task import run_codex_task


def _write_policy(tmp_path: Path) -> Path:
    policy_path = tmp_path / "config" / "contour_policy.json"
    policy_path.parent.mkdir(parents=True, exist_ok=True)
    policy_path.write_text(
        json.dumps(
            {
                "contours": {
                    "df-dev": {
                        "working_root": str(tmp_path / "df-dev"),
                    },
                    "ownerbox": {
                        "working_root": str(tmp_path / "ownerbox"),
                    },
                }
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return policy_path


def _conflict_payload(*, resource_id: str = "crew-west") -> dict[str, object]:
    return {
        "id": f"conflict-{resource_id}",
        "logical_id": resource_id,
        "resource_id": resource_id,
        "requesting_domain": "dev",
        "other_domain": "ownerbox",
        "task_type": "task",
        "conflict_with": "artifact-ownerbox-1",
    }


def _task_packet(*, resource_id: str = "crew-west") -> dict[str, object]:
    return {
        "task_id": 99,
        "instruction": "Execute the task",
        "memory_context": {"domain": "dev", "type": "task"},
        "payload": {
            "domain": "dev",
            "resource_id": resource_id,
        },
        "status": "created",
    }


def _conflicting_artifact(*, resource_id: str = "crew-west") -> dict[str, object]:
    return {
        "id": "artifact-ownerbox-1",
        "domain": "ownerbox",
        "type": "task",
        "status": "running",
        "payload": {
            "domain": "ownerbox",
            "resource_id": resource_id,
        },
    }


def test_resolve_conflict_updates_pending_artifact(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(storage_adapter, "POLICY_FILE", _write_policy(tmp_path))
    monkeypatch.setattr(
        memory_registry,
        "REGISTRY_FILE",
        tmp_path / "df-system" / "memory_registry.json",
    )

    created_path = storage_adapter.save_artifact(
        "ownerbox",
        "conflict_escalation",
        _conflict_payload(),
        artifact_status="pending_resolution",
        resolution=None,
    )

    resolved = resolve_conflict("conflict-crew-west", "owner_override", "ownerbox")
    registry_entry = memory_registry.get_artifact_by_id("conflict-crew-west")

    assert (
        created_path
        == tmp_path
        / "ownerbox"
        / "artifacts"
        / "conflict_escalation_conflict-crew-west.json"
    )
    assert resolved["status"] == "resolved"
    assert resolved["state"] == "resolved"
    assert resolved["resolution"]["resolved_by"] == "ownerbox"
    assert resolved["resolution"]["resolution_type"] == "owner_override"
    assert registry_entry is not None
    assert registry_entry["status"] == "resolved"
    assert registry_entry["state"] == "resolved"
    assert registry_entry["resolution"]["resolution_type"] == "owner_override"


def test_resolve_conflict_rejects_already_resolved_artifact(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(storage_adapter, "POLICY_FILE", _write_policy(tmp_path))
    monkeypatch.setattr(
        memory_registry,
        "REGISTRY_FILE",
        tmp_path / "df-system" / "memory_registry.json",
    )

    storage_adapter.save_artifact(
        "ownerbox",
        "conflict_escalation",
        _conflict_payload(),
        artifact_status="pending_resolution",
        resolution=None,
    )
    resolve_conflict("conflict-crew-west", "dev_override", "dev")

    with pytest.raises(
        ConflictResolutionError,
        match="artifact status does not allow resolution: resolved",
    ):
        resolve_conflict("conflict-crew-west", "owner_override", "ownerbox")


def test_resolve_conflict_rejects_wrong_artifact_type(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(storage_adapter, "POLICY_FILE", _write_policy(tmp_path))
    monkeypatch.setattr(
        memory_registry,
        "REGISTRY_FILE",
        tmp_path / "df-system" / "memory_registry.json",
    )

    storage_adapter.save_artifact(
        "ownerbox",
        "task",
        {
            "id": "owner-task",
            "summary": "Not a conflict artifact.",
        },
    )

    with pytest.raises(
        ConflictResolutionError,
        match="artifact is not a conflict_escalation: owner-task",
    ):
        resolve_conflict("owner-task", "owner_override", "ownerbox")


def test_resolved_conflict_allows_execution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(storage_adapter, "POLICY_FILE", _write_policy(tmp_path))
    monkeypatch.setattr(
        memory_registry,
        "REGISTRY_FILE",
        tmp_path / "df-system" / "memory_registry.json",
    )

    storage_adapter.save_artifact(
        "ownerbox",
        "conflict_escalation",
        _conflict_payload(),
        artifact_status="resolved",
        resolution={
            "resolved_by": "ownerbox",
            "resolution_type": "merge_allowed",
            "timestamp": "2026-04-14T12:00:00Z",
        },
    )

    decision = evaluate_cross_domain_conflict(
        _task_packet(),
        [_conflicting_artifact()],
    )

    assert decision == {
        "allowed": True,
        "reason": "previously_resolved_conflict",
        "conflict_with": "conflict-crew-west",
        "action": "continue",
    }


def test_run_codex_task_allows_execution_after_conflict_resolution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(storage_adapter, "POLICY_FILE", _write_policy(tmp_path))
    monkeypatch.setattr(
        memory_registry,
        "REGISTRY_FILE",
        tmp_path / "df-system" / "memory_registry.json",
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "evaluate_memory_policy",
        lambda task_packet, resolved_memory: {
            "allowed": True,
            "reason": "no_recent_duplicate",
            "matched_artifact_id": None,
            "action": "continue",
        },
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "resolve_memory",
        lambda context: [_conflicting_artifact()],
    )

    task_path = tmp_path / "task-61.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 61,\n'
            '  "instruction": "Implement the conflict resolution task",\n'
            '  "constraints": "Modify only necessary parts.",\n'
            '  "success_criteria": "Execution continues after a conflict is resolved.",\n'
            '  "memory_context": {"domain": "dev", "type": "task"},\n'
            '  "payload": {"domain": "dev", "resource_id": "crew-west"}\n'
            "}\n"
        ),
        encoding="utf-8",
    )

    first_task, first_artifact_path = run_codex_task(
        task_path,
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )
    first_output = capsys.readouterr().out
    first_artifact = json.loads(first_artifact_path.read_text(encoding="utf-8"))

    conflict_entry = memory_registry.get_artifact_by_logical_key(
        memory_registry.compute_artifact_key(
            "ownerbox", "conflict_escalation", "crew-west"
        )
    )
    assert conflict_entry is not None
    assert conflict_entry["status"] == "active"
    assert conflict_entry["state"] == "pending_resolution"
    assert (
        "[CONFLICT] blocked resource=crew-west domain=dev other_domain=ownerbox"
        in first_output
    )
    assert first_artifact["result_type"] == "cross_domain_conflict_blocked"
    assert first_task["failure_reason"] == "cross_domain_conflict_detected"

    resolved_artifact = resolve_conflict(
        conflict_entry["id"],
        "merge_allowed",
        "ownerbox",
    )
    assert resolved_artifact["status"] == "resolved"
    assert resolved_artifact["state"] == "resolved"
    set_state(
        "resource",
        "crew-west",
        "reserved",
        str(tmp_path / "df-dev" / "artifacts" / "resource-crew-west.json"),
        domain="dev",
    )

    second_task, second_artifact_path = run_codex_task(
        task_path,
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )
    second_output = capsys.readouterr().out

    assert "[CONFLICT] previously_resolved -> allow" in second_output
    assert second_task["cross_domain_conflict_decision"] == {
        "allowed": True,
        "reason": "previously_resolved_conflict",
        "conflict_with": conflict_entry["id"],
        "action": "continue",
    }
    assert second_artifact_path == tmp_path / "artifacts" / "task-61.txt"
    assert (
        second_artifact_path.read_text(encoding="utf-8")
        == "Implement the conflict resolution task"
    )
