from __future__ import annotations

import json
from pathlib import Path

import pytest

import scripts.run_codex_task as run_codex_task_module
from app.memory import memory_registry
from app.policy.conflict_resolution import resolve_conflict
from app.state.state_store import (
    StateStoreError,
    get_state,
    list_active_states,
    set_state,
)
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


def _configure_storage(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(storage_adapter, "POLICY_FILE", _write_policy(tmp_path))
    monkeypatch.setattr(
        memory_registry,
        "REGISTRY_FILE",
        tmp_path / "df-system" / "memory_registry.json",
    )


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


def test_set_state_creates_new_active_state(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_storage(monkeypatch, tmp_path)

    state = set_state(
        "task",
        "DF-STATE-1",
        "completed",
        str(tmp_path / "df-dev" / "artifacts" / "traces" / "DF-STATE-1.json"),
        domain="dev",
    )

    stored_path = tmp_path / "df-dev" / "state" / "task" / "DF-STATE-1.json"
    stored_record = json.loads(stored_path.read_text(encoding="utf-8"))

    assert state["domain"] == "dev"
    assert state["entity_type"] == "task"
    assert state["entity_id"] == "DF-STATE-1"
    assert state["state"] == "completed"
    assert state["version"] == 1
    assert stored_record["memory_class"] == "state"
    assert stored_record["status"] == "active"
    assert stored_record["payload"]["entity_id"] == "DF-STATE-1"
    assert stored_record["payload"]["state"] == "completed"
    assert stored_record["payload"]["source_artifact"].endswith("DF-STATE-1.json")


def test_updating_state_replaces_previous_and_preserves_superseded_history(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_storage(monkeypatch, tmp_path)

    first = set_state(
        "task",
        "DF-STATE-2",
        "running",
        str(tmp_path / "df-dev" / "artifacts" / "traces" / "first.json"),
        domain="dev",
    )
    second = set_state(
        "task",
        "DF-STATE-2",
        "completed",
        str(tmp_path / "df-dev" / "artifacts" / "traces" / "second.json"),
        domain="dev",
    )

    active = get_state("task", "DF-STATE-2", domain="dev")
    history_dir = tmp_path / "df-dev" / "state" / "task" / "history"
    history_records = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted(history_dir.glob("DF-STATE-2-*.json"))
    ]

    assert first["state"] == "running"
    assert second["state"] == "completed"
    assert active is not None
    assert active["state"] == "completed"
    assert active["source_artifact"].endswith("second.json")
    assert len(history_records) == 1
    assert history_records[0]["status"] == "superseded"
    assert history_records[0]["payload"]["state"] == "running"
    assert history_records[0]["payload"]["source_artifact"].endswith("first.json")


def test_get_state_and_list_active_states_preserve_domain_isolation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_storage(monkeypatch, tmp_path)

    set_state(
        "resource",
        "crew-west",
        "reserved",
        str(tmp_path / "df-dev" / "artifacts" / "resource-dev.json"),
        domain="dev",
    )
    set_state(
        "resource",
        "crew-west",
        "owner_locked",
        str(tmp_path / "ownerbox" / "artifacts" / "resource-owner.json"),
        domain="ownerbox",
    )

    dev_state = get_state("resource", "crew-west", domain="dev")
    owner_state = get_state("resource", "crew-west", domain="ownerbox")

    assert dev_state is not None
    assert owner_state is not None
    assert dev_state["state"] == "reserved"
    assert owner_state["state"] == "owner_locked"
    assert [item["entity_id"] for item in list_active_states("dev")] == ["crew-west"]
    assert [item["entity_id"] for item in list_active_states("ownerbox")] == [
        "crew-west"
    ]

    with pytest.raises(StateStoreError, match="ambiguous"):
        get_state("resource", "crew-west")


def test_execution_updates_task_state(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure_storage(monkeypatch, tmp_path)

    task_path = tmp_path / "task-801.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 801,\n'
            '  "instruction": "Write the ownerbox artifact",\n'
            '  "constraints": "Modify only necessary parts.",\n'
            '  "success_criteria": "State is updated after execution.",\n'
            '  "domain": "ownerbox"\n'
            "}\n"
        ),
        encoding="utf-8",
    )

    task, _artifact_path = run_codex_task(
        task_path,
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    state = get_state("task", "801", domain="ownerbox")

    assert state is not None
    assert state["state"] == "completed"
    assert state["source_artifact"] == str(task["execution_trace_artifact_path"])


def test_conflict_creation_and_resolution_update_conflict_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_storage(monkeypatch, tmp_path)
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

    task_path = tmp_path / "task-802.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 802,\n'
            '  "instruction": "Trigger the conflict",\n'
            '  "constraints": "Modify only necessary parts.",\n'
            '  "success_criteria": "Conflict state is tracked.",\n'
            '  "memory_context": {"domain": "dev", "type": "task"},\n'
            '  "payload": {"domain": "dev", "resource_id": "crew-west"}\n'
            "}\n"
        ),
        encoding="utf-8",
    )

    _task, _artifact_path = run_codex_task(
        task_path,
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    conflict_state = get_state("conflict", "crew-west", domain="ownerbox")
    conflict_entry = memory_registry.get_artifact_by_logical_key(
        memory_registry.compute_artifact_key(
            "ownerbox", "conflict_escalation", "crew-west"
        )
    )

    assert conflict_state is not None
    assert conflict_state["state"] == "conflict_active"
    assert conflict_entry is not None
    assert conflict_state["source_artifact"] == str(conflict_entry["local_path"])

    resolve_conflict("conflict-crew-west", "owner_override", "ownerbox")
    resolved_state = get_state("conflict", "crew-west", domain="ownerbox")

    assert resolved_state is not None
    assert resolved_state["state"] == "resolved"
    assert resolved_state["source_artifact"].endswith(
        "conflict_escalation_conflict-crew-west.json"
    )


def test_replay_blocked_keeps_existing_task_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_storage(monkeypatch, tmp_path)

    task_path = tmp_path / "task-803.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 803,\n'
            '  "instruction": "Execute once",\n'
            '  "constraints": "Modify only necessary parts.",\n'
            '  "success_criteria": "Replay does not mutate task state."\n'
            "}\n"
        ),
        encoding="utf-8",
    )

    first_task, _ = run_codex_task(
        task_path,
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )
    initial_state = get_state("task", "803", domain="dev")

    second_task, _ = run_codex_task(
        task_path,
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )
    replay_state = get_state("task", "803", domain="dev")

    assert initial_state is not None
    assert replay_state is not None
    assert second_task["failure_reason"] == "already_executed"
    assert replay_state == initial_state
    assert replay_state["source_artifact"] == str(
        first_task["execution_trace_artifact_path"]
    )
