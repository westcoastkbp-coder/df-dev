from __future__ import annotations

import json
from pathlib import Path

import app.context.shared_context_store as shared_context_store_module
from app.config.hybrid_runtime import load_runtime_config
from app.context.shared_context_store import get_context, set_context
from app.execution.action_result import build_action_result
from app.execution.lead_estimate_read import resolve_estimate_decision
from app.orchestrator.execution_runner import run_execution
from app.orchestrator.task_factory import create_task, save_task
import app.orchestrator.execution_runner as execution_runner_module


def _shared_env(storage_root: Path, *, role: str) -> dict[str, str]:
    return {
        "ENV_ROLE": role,
        "DF_STORAGE_ROOT": str(storage_root),
    }


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    records: list[dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        records.append(json.loads(stripped))
    return records


def test_local_write_is_visible_to_remote_and_remote_write_is_visible_to_local(
    tmp_path: Path,
) -> None:
    shared_root = tmp_path / "shared-context"
    local_env = _shared_env(shared_root, role="local_dev")
    remote_env = _shared_env(shared_root, role="remote_runtime")

    set_context(
        "global_context",
        {"operator": "zephyrus", "authority": "local"},
        environ=local_env,
        root_dir=tmp_path,
    )
    remote_view = get_context("global_context", environ=remote_env, root_dir=tmp_path)
    assert remote_view["operator"] == "zephyrus"
    assert remote_view["authority"] == "local"

    set_context(
        "active_task:REMOTE-TASK-1",
        {"status": "EXECUTING", "runtime_owner": "cpu-server"},
        task_id="REMOTE-TASK-1",
        interaction_id="interaction-remote-1",
        environ=remote_env,
        root_dir=tmp_path,
    )
    local_view = get_context("active_task:REMOTE-TASK-1", environ=local_env, root_dir=tmp_path)
    assert local_view["task_id"] == "REMOTE-TASK-1"
    assert local_view["interaction_id"] == "interaction-remote-1"
    assert dict(local_view["value"])["runtime_owner"] == "cpu-server"


def test_task_creation_writes_active_context_and_interaction_event(
    tmp_path: Path,
    monkeypatch,
) -> None:
    shared_root = tmp_path / "shared-context"
    env = _shared_env(shared_root, role="local_dev")
    monkeypatch.setenv("ENV_ROLE", env["ENV_ROLE"])
    monkeypatch.setenv("DF_STORAGE_ROOT", env["DF_STORAGE_ROOT"])

    store_path = tmp_path / "tasks.json"
    created = create_task(
        {
            "status": "created",
            "intent": "new_lead",
            "interaction_id": "interaction-1",
            "payload": {"summary": "Need an ADU estimate"},
        },
        store_path=store_path,
    )

    context = get_context(f"active_task:{created['task_id']}", environ=env, root_dir=tmp_path)
    assert context["task_id"] == created["task_id"]
    assert context["interaction_id"] == "interaction-1"
    assert dict(context["value"])["status"] == "CREATED"

    config = load_runtime_config(root_dir=tmp_path, environ=env)
    interaction_events = _read_jsonl(config.storage_paths.interactions_file)
    assert any(
        entry["event_type"] == "task_created"
        and entry["task_id"] == created["task_id"]
        and entry["interaction_id"] == "interaction-1"
        for entry in interaction_events
    )


def test_decision_resolution_reads_and_writes_shared_context(
    tmp_path: Path,
    monkeypatch,
) -> None:
    shared_root = tmp_path / "shared-context"
    env = _shared_env(shared_root, role="local_dev")
    monkeypatch.setenv("ENV_ROLE", env["ENV_ROLE"])
    monkeypatch.setenv("DF_STORAGE_ROOT", env["DF_STORAGE_ROOT"])

    set_context(
        "global_context",
        {"office": "west", "policy_authority": "local"},
        environ=env,
        root_dir=tmp_path,
    )
    set_context(
        "active_task:DECISION-1",
        {"status": "VALIDATED", "lead_snapshot": {"project_type": "ADU"}},
        task_id="DECISION-1",
        interaction_id="interaction-2",
        environ=env,
        root_dir=tmp_path,
    )

    decision = resolve_estimate_decision(
        task_id="DECISION-1",
        payload={
            "interaction_id": "interaction-2",
            "workflow_type": "lead_estimate_decision",
            "lead_id": "LEAD-1",
            "lead_data": {
                "project_type": "ADU",
                "scope_summary": "Detached ADU",
                "contact_info": {"phone": "555-0100"},
            },
        },
    )

    assert decision["next_step"] == "create_estimate_task"
    context = get_context("active_task:DECISION-1", environ=env, root_dir=tmp_path)
    decision_context = dict(context["value"])["decision_context"]
    assert dict(decision_context["global_context"])["office"] == "west"
    assert dict(decision_context["active_task_context"])["value"]["status"] == "VALIDATED"

    config = load_runtime_config(root_dir=tmp_path, environ=env)
    decision_events = _read_jsonl(config.storage_paths.decisions_file)
    assert any(
        entry["event_type"] == "decision_resolved"
        and entry["task_id"] == "DECISION-1"
        and entry["interaction_id"] == "interaction-2"
        for entry in decision_events
    )


def test_decision_resolution_compacts_recursive_active_task_snapshot(
    tmp_path: Path,
    monkeypatch,
) -> None:
    shared_root = tmp_path / "shared-context"
    env = _shared_env(shared_root, role="local_dev")
    monkeypatch.setenv("ENV_ROLE", env["ENV_ROLE"])
    monkeypatch.setenv("DF_STORAGE_ROOT", env["DF_STORAGE_ROOT"])

    set_context(
        "active_task:DECISION-COMPACT-1",
        {
            "status": "VALIDATED",
            "decision_context": {
                "active_task_context": {
                    "task_id": "DECISION-COMPACT-1",
                    "value": {
                        "status": "VALIDATED",
                        "decision_context": {
                            "active_task_context": {
                                "task_id": "DECISION-COMPACT-1",
                                "value": {"status": "STALE"},
                            }
                        },
                    },
                }
            },
        },
        task_id="DECISION-COMPACT-1",
        interaction_id="interaction-compact-1",
        environ=env,
        root_dir=tmp_path,
    )

    resolve_estimate_decision(
        task_id="DECISION-COMPACT-1",
        payload={
            "interaction_id": "interaction-compact-1",
            "workflow_type": "lead_estimate_decision",
            "lead_id": "LEAD-COMPACT-1",
            "lead_data": {
                "project_type": "ADU",
                "scope_summary": "Detached ADU",
                "contact_info": {"phone": "555-0100"},
            },
        },
    )

    context = get_context("active_task:DECISION-COMPACT-1", environ=env, root_dir=tmp_path)
    active_snapshot = dict(dict(context["value"])["decision_context"])["active_task_context"]
    assert active_snapshot["task_id"] == "DECISION-COMPACT-1"
    assert dict(active_snapshot["value"])["status"] == "VALIDATED"
    assert "decision_context" not in dict(active_snapshot["value"])
    assert "previous_context" not in dict(active_snapshot["value"])
    assert "history" not in dict(active_snapshot["value"])


def test_oversized_active_task_context_is_ignored_and_replaced(
    tmp_path: Path,
    monkeypatch,
) -> None:
    shared_root = tmp_path / "shared-context"
    env = _shared_env(shared_root, role="local_dev")
    monkeypatch.setenv("ENV_ROLE", env["ENV_ROLE"])
    monkeypatch.setenv("DF_STORAGE_ROOT", env["DF_STORAGE_ROOT"])
    monkeypatch.setattr(shared_context_store_module, "MAX_JSON_FILE_BYTES", 64)

    config = load_runtime_config(root_dir=tmp_path, environ=env)
    oversized_path = config.storage_paths.active_threads_dir / "task-OVERSIZED-1.json"
    oversized_path.parent.mkdir(parents=True, exist_ok=True)
    oversized_path.write_text(
        json.dumps(
            {
                "schema_version": "v1",
                "key": "active_task:OVERSIZED-1",
                "updated_at": "2026-04-13T00:00:00Z",
                "task_id": "OVERSIZED-1",
                "interaction_id": "interaction-oversized-1",
                "value": {"blob": "x" * 256},
            }
        ),
        encoding="utf-8",
    )

    assert get_context("active_task:OVERSIZED-1", environ=env, root_dir=tmp_path) == {}

    set_context(
        "active_task:OVERSIZED-1",
        {"status": "CREATED"},
        task_id="OVERSIZED-1",
        interaction_id="interaction-oversized-1",
        environ=env,
        root_dir=tmp_path,
    )
    replaced = get_context("active_task:OVERSIZED-1", environ=env, root_dir=tmp_path)
    assert dict(replaced["value"])["status"] == "CREATED"


def test_execution_runner_updates_shared_context_for_completion(
    tmp_path: Path,
    monkeypatch,
) -> None:
    shared_root = tmp_path / "shared-context"
    env = _shared_env(shared_root, role="local_dev")
    monkeypatch.setenv("ENV_ROLE", env["ENV_ROLE"])
    monkeypatch.setenv("DF_STORAGE_ROOT", env["DF_STORAGE_ROOT"])

    store_path = tmp_path / "tasks.json"
    monkeypatch.setattr(execution_runner_module.task_factory_module, "TASK_SYSTEM_FILE", store_path)

    task = create_task(
        {
            "status": "created",
            "intent": "generic_task",
            "interaction_id": "interaction-3",
            "payload": {"summary": "Execute generic task"},
        },
        store_path=store_path,
    )
    task["status"] = "VALIDATED"
    task["last_updated_at"] = "2026-04-06T12:00:00Z"
    save_task(task, store_path=store_path)

    def persist(updated_task: dict[str, object]) -> None:
        save_task(updated_task, store_path=store_path)

    executed = run_execution(
        task,
        now=lambda: "2026-04-06T12:00:01Z",
        persist=persist,
        executor=lambda task_data: build_action_result(
            status="completed",
            task_id=task_data["task_id"],
            action_type=str(task_data.get("intent", "")).upper(),
            result_payload={"ok": True},
            error_code="",
            error_message="",
            source="test",
            diagnostic_message="done",
        ),
    )

    assert executed["status"] == "COMPLETED"
    context = get_context(f"active_task:{task['task_id']}", environ=env, root_dir=tmp_path)
    assert dict(context["value"])["status"] == "COMPLETED"
    assert dict(dict(context["value"])["result"])["status"] == "completed"

    config = load_runtime_config(root_dir=tmp_path, environ=env)
    interaction_events = _read_jsonl(config.storage_paths.interactions_file)
    event_types = [entry["event_type"] for entry in interaction_events if entry["task_id"] == task["task_id"]]
    assert "execution_started" in event_types
    assert "execution_completed" in event_types
