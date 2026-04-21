from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import app.orchestrator.task_factory as task_factory_module
import app.orchestrator.task_state_store as task_state_store_module
from app.execution.action_result import build_action_result
from app.orchestrator.execution_runner import run_execution


def _configure_runtime(monkeypatch, tmp_path: Path) -> Path:
    store_path = tmp_path / "data" / "task_system.json"
    monkeypatch.setattr(task_state_store_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store_module,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )
    monkeypatch.setattr(task_factory_module, "TASK_SYSTEM_FILE", store_path)
    task_factory_module.clear_task_runtime_store()
    return store_path


def _persist(task_data: dict[str, object], *, store_path: Path) -> None:
    task_factory_module.save_task(dict(task_data), store_path=store_path)


def test_single_task_preserves_lineage_under_repeated_approval_execution_and_signal(
    monkeypatch,
    tmp_path: Path,
) -> None:
    store_path = _configure_runtime(monkeypatch, tmp_path)
    signal_text = "Создать Task: проверить lineage protection"

    created = task_factory_module.create_task(
        {
            "status": "awaiting_approval",
            "text": signal_text,
            "payload": {
                "summary": signal_text,
                "fallback": "lineage_protection_check",
            },
        },
        store_path=store_path,
    )
    task_id = str(created.get("task_id", "")).strip()

    repeated_signal_ids = [task_id]
    for _ in range(5):
        repeated = task_factory_module.create_task(
            {
                "status": "awaiting_approval",
                "text": signal_text,
                "payload": {
                    "summary": signal_text,
                    "fallback": "lineage_protection_check",
                },
            },
            store_path=store_path,
        )
        repeated_signal_ids.append(str(repeated.get("task_id", "")).strip())

    approved = task_factory_module.apply_task_approval(
        task_id,
        approved=True,
        approved_by="system-test",
        store_path=store_path,
    )

    repeated_approval_errors: list[str] = []
    for _ in range(3):
        try:
            task_factory_module.apply_task_approval(
                task_id,
                approved=True,
                approved_by="system-test",
                store_path=store_path,
            )
        except ValueError as exc:
            repeated_approval_errors.append(str(exc))

    external_effects: list[str] = []

    def executor(task_data: dict[str, object]) -> dict[str, object]:
        external_effects.append(str(task_data.get("task_id", "")).strip())
        return build_action_result(
            status="completed",
            task_id=task_data.get("task_id"),
            action_type=str(task_data.get("intent", "")).strip().upper(),
            result_payload={"external_effects": len(external_effects)},
            error_code="",
            error_message="",
            source="test_task_lineage_protection",
        )

    executor.__module__ = "test_task_lineage_protection"

    execution_statuses: list[str] = []
    for _ in range(5):
        current = (
            task_factory_module.get_task(task_id, store_path=store_path) or approved
        )
        executed = run_execution(
            json.loads(json.dumps(current)),
            now=lambda: "2026-04-05T20:00:00Z",
            persist=lambda updated_task: _persist(updated_task, store_path=store_path),
            executor=executor,
        )
        execution_statuses.append(str(executed.get("status", "")).strip())

    db_path = task_state_store_module.db_path_for(store_path)
    with sqlite3.connect(str(db_path)) as connection:
        connection.row_factory = sqlite3.Row
        task_versions = connection.execute(
            """
            SELECT version_number, lineage_root_task_id, lineage_branch_key
            FROM task_versions
            WHERE task_id = ?
            ORDER BY version_number ASC
            """,
            (task_id,),
        ).fetchall()
        execution_branches = connection.execute(
            """
            SELECT branch_key, lineage_root_task_id, latest_execution_key, status
            FROM execution_branches
            WHERE task_id = ?
            ORDER BY created_at ASC
            """,
            (task_id,),
        ).fetchall()

    lineage_roots = {str(row["lineage_root_task_id"]).strip() for row in task_versions}
    branch_keys = {str(row["lineage_branch_key"]).strip() for row in task_versions}
    branch_rows = {str(row["branch_key"]).strip() for row in execution_branches}
    lineage_integrity = (
        len(set(repeated_signal_ids)) == 1
        and len(lineage_roots) == 1
        and len(branch_keys) == 1
        and len(branch_rows) == 1
        and len(external_effects) == 1
        and all(status == "COMPLETED" for status in execution_statuses)
        and len(repeated_approval_errors) == 3
    )
    branches_count = len(execution_branches)
    observed = {
        "lineage_integrity": lineage_integrity,
        "branches_count": branches_count,
        "PASS/FAIL": "PASS" if lineage_integrity and branches_count == 1 else "FAIL",
    }

    assert observed == {
        "lineage_integrity": True,
        "branches_count": 1,
        "PASS/FAIL": "PASS",
    }
