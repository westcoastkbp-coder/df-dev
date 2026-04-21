from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import app.policy.policy_gate as policy_gate_module
import app.product.runner as runner_module
import memory.storage
import runtime.system_log as system_log_module
from app.execution import paths as paths_module
from app.execution.task_schema import TASK_CONTRACT_VERSION
from app.orchestrator import execution_runner as execution_runner_module
from app.orchestrator import mock_executor as mock_executor_module
from app.orchestrator import task_factory as task_factory_module
from app.orchestrator import task_state_store as task_state_store_module
from app.orchestrator import task_memory as task_memory_module
from app.orchestrator.task_queue import InMemoryTaskQueue
from functools import partial
from app.orchestrator.task_worker import process_next_queued_task as _process_next_queued_task
from tests.system_context import WORKING_SYSTEM_CONTEXT

process_next_queued_task = partial(_process_next_queued_task, system_context=WORKING_SYSTEM_CONTEXT)


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_lead_followup_creates_file_memory_and_logs(monkeypatch, tmp_path: Path) -> None:
    output_dir = tmp_path / "runtime" / "out"
    leads_dir = output_dir / "leads"
    leads_dir.mkdir(parents=True, exist_ok=True)
    (leads_dir / "lead_001.txt").write_text(
        "\n".join(
            [
                "request: Client wants ADU project, lot 5000 sqft, asking for price",
                "type: ADU",
                "lot_size: 5000 sqft",
                "timestamp: 2026-04-04T03:46:58Z",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    logs_dir = tmp_path / "runtime" / "logs"
    queue_file = tmp_path / "runtime" / "state" / "task_queue.json"
    task_log_file = logs_dir / "tasks.log"
    system_log_file = logs_dir / "system.log"
    policy_log_file = logs_dir / "policy.log"
    task_state_db_file = tmp_path / "runtime" / "state" / "task_state.sqlite3"
    task_store_file = tmp_path / "data" / "tasks.json"

    import app.orchestrator.task_queue as task_queue_module

    monkeypatch.setattr(paths_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(paths_module, "OUTPUT_DIR", Path("runtime/out"))
    monkeypatch.setattr(paths_module, "TASKS_FILE", task_store_file)
    monkeypatch.setattr(runner_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(runner_module, "RUNTIME_OUT_DIR", Path("runtime/out"))
    monkeypatch.setattr(task_factory_module, "TASK_SYSTEM_FILE", task_store_file)
    monkeypatch.setattr(task_memory_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(task_state_store_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(task_state_store_module, "TASK_STATE_DB_FILE", Path("runtime/state/task_state.sqlite3"))
    monkeypatch.setattr(mock_executor_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(task_queue_module, "TASK_QUEUE_FILE", queue_file)
    monkeypatch.setattr(task_queue_module, "TASK_LOG_FILE", task_log_file)
    monkeypatch.setattr(system_log_module, "SYSTEM_LOG_FILE", system_log_file)
    monkeypatch.setattr(policy_gate_module, "POLICY_LOG_FILE", policy_log_file)
    monkeypatch.setattr(
        task_memory_module,
        "TASK_MEMORY_FILE",
        Path("runtime/state/task_memory.json"),
    )
    monkeypatch.setattr(
        execution_runner_module,
        "store_task_result",
        task_memory_module.store_task_result,
    )
    monkeypatch.setattr(
        mock_executor_module,
        "dispatch_action_trigger",
        runner_module.dispatch_action_trigger,
    )
    monkeypatch.setattr(memory.storage, "save_task_record", lambda task_data: None)
    task_factory_module.clear_task_runtime_store()

    queue = InMemoryTaskQueue()
    task_data: dict[str, object] = {
        "task_contract_version": TASK_CONTRACT_VERSION,
        "task_id": "DF-LEAD-FOLLOWUP-V1",
        "created_at": "2026-04-04T00:00:00Z",
        "status": "pending",
        "intent": "lead_followup",
        "payload": {
            "lead_file": r"runtime\out\leads\lead_001.txt",
            "summary": "Generate follow-up for existing lead_001",
        },
        "notes": [],
        "history": [],
        "interaction_id": "DF-LEAD-FOLLOWUP-V1",
        "job_id": "DF-LEAD-FOLLOWUP-V1",
        "trace_id": "DF-LEAD-FOLLOWUP-V1",
    }

    def fetch_task(task_id: str) -> dict[str, object] | None:
        return task_data if task_id == "DF-LEAD-FOLLOWUP-V1" else None

    def persist(updated_task: dict[str, object]) -> None:
        task_data.update(updated_task)

    assert queue.enqueue_task("DF-LEAD-FOLLOWUP-V1") is True

    executed_task = process_next_queued_task(
        queue=queue,
        now=lambda: "2026-04-04T00:00:00Z",
        fetch_task=fetch_task,
        persist=persist,
        timeout=0.0,
        telemetry_collector=lambda: {},
        network_snapshot_collector=lambda: {},
    )

    assert executed_task is not None
    assert executed_task["status"] == "COMPLETED"

    followup_file = leads_dir / "lead_001_followup.txt"
    assert followup_file.exists()
    followup_content = followup_file.read_text(encoding="utf-8")
    assert "summary: Lead for ADU project on 5000 sqft lot requesting pricing." in followup_content
    assert "recommended_next_action: Prepare preliminary pricing range and schedule qualification call." in followup_content
    assert "short_message: Thanks for reaching out about your ADU project." in followup_content

    task_log = _read_jsonl(task_log_file)
    assert [entry["event_type"] for entry in task_log[:2]] == ["queue_enqueue", "queue_dequeue"]

    policy_log = _read_jsonl(policy_log_file)
    assert any(
        entry["task_id"] == "DF-LEAD-FOLLOWUP-V1"
        and entry["event_type"] == "policy_decision"
        and entry["status"] == "allowed"
        for entry in policy_log
    )

    system_log = _read_jsonl(system_log_file)
    assert any(
        entry["event_type"] == "mode"
        and entry["task_id"] == "DF-LEAD-FOLLOWUP-V1"
        and "[MODE]" in entry["details"].get("message", "")
        for entry in system_log
    )
    assert any(
        "completed WRITE_FILE -> runtime/out/leads/lead_001_followup.txt"
        in entry["details"].get("message", "")
        for entry in system_log
    )
    assert any(
        "updated task_memory for DF-LEAD-FOLLOWUP-V1" in entry["details"].get("message", "")
        for entry in system_log
    )

    connection = sqlite3.connect(str(task_state_db_file))
    try:
        rows = connection.execute(
            "SELECT descriptor FROM Task WHERE memory_ref = ?",
            ("DF-LEAD-FOLLOWUP-V1",),
        ).fetchall()
    finally:
        connection.close()
    assert rows
    assert "DF-LEAD-FOLLOWUP-V1" in rows[0][0]
    assert "lead_followup" in rows[0][0]

