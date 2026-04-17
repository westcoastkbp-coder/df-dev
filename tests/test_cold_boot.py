from __future__ import annotations

import json
from pathlib import Path

import app.execution.cold_boot as cold_boot_module
import app.execution.lead_estimate_decision as lead_estimate_decision_module
import app.execution.paths as paths_module
import app.orchestrator.task_factory as task_factory_module
import app.orchestrator.task_memory as task_memory_module
import app.orchestrator.task_queue as task_queue_module
import app.orchestrator.task_state_store as task_state_store_module
import app.orchestrator.task_worker as task_worker_module
import app.policy.policy_gate as policy_gate_module
import runtime.system_log as system_log_module
import runtime.token_efficiency as token_efficiency_module
import runtime.token_telemetry as token_telemetry_module


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _configure_cold_boot_runtime(monkeypatch, tmp_path: Path) -> Path:
    root_dir = tmp_path
    data_dir = root_dir / "data"
    task_store_path = data_dir / "tasks.json"
    task_system_path = data_dir / "task_system.json"
    memory_file = data_dir / "memory.json"
    contacts_file = data_dir / "contacts.json"
    cold_boot_log_file = root_dir / "runtime" / "logs" / "cold_boot.jsonl"
    system_log_file = root_dir / "runtime" / "logs" / "system.log"
    task_log_file = root_dir / "runtime" / "logs" / "tasks.log"
    policy_log_file = root_dir / "runtime" / "logs" / "policy.log"
    task_queue_file = root_dir / "runtime" / "state" / "task_queue.json"
    token_usage_log_file = root_dir / "runtime" / "logs" / "token_usage.jsonl"
    token_efficiency_log_file = root_dir / "runtime" / "logs" / "token_efficiency.jsonl"
    execution_priority_log_file = root_dir / "runtime" / "logs" / "execution_priority.jsonl"

    monkeypatch.setattr(paths_module, "ROOT_DIR", root_dir)
    monkeypatch.setattr(paths_module, "DATA_DIR", data_dir)
    monkeypatch.setattr(paths_module, "TASKS_FILE", task_store_path)
    monkeypatch.setattr(paths_module, "TASK_SYSTEM_FILE", task_system_path)
    monkeypatch.setattr(paths_module, "MEMORY_FILE", memory_file)
    monkeypatch.setattr(paths_module, "CONTACTS_FILE", contacts_file)
    monkeypatch.setattr(task_factory_module, "TASK_SYSTEM_FILE", task_system_path)
    monkeypatch.setattr(lead_estimate_decision_module, "TASKS_FILE", task_store_path)

    monkeypatch.setattr(task_state_store_module, "ROOT_DIR", root_dir)
    monkeypatch.setattr(
        task_state_store_module,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )

    monkeypatch.setattr(task_memory_module, "ROOT_DIR", root_dir)
    monkeypatch.setattr(
        task_memory_module,
        "TASK_MEMORY_FILE",
        Path("runtime/state/task_memory.json"),
    )

    monkeypatch.setattr(task_queue_module, "TASK_QUEUE_FILE", task_queue_file)
    monkeypatch.setattr(task_queue_module, "TASK_LOG_FILE", task_log_file)

    monkeypatch.setattr(system_log_module, "SYSTEM_LOG_FILE", system_log_file)
    monkeypatch.setattr(system_log_module, "TASK_LOG_FILE", task_log_file)
    monkeypatch.setattr(policy_gate_module, "POLICY_LOG_FILE", policy_log_file)
    monkeypatch.setattr(token_telemetry_module, "TOKEN_USAGE_LOG_FILE", token_usage_log_file)
    monkeypatch.setattr(
        token_efficiency_module,
        "TOKEN_EFFICIENCY_LOG_FILE",
        token_efficiency_log_file,
    )
    monkeypatch.setattr(
        task_worker_module,
        "EXECUTION_PRIORITY_LOG_FILE",
        execution_priority_log_file,
    )

    return cold_boot_log_file


def test_cold_boot_runs_lead_estimate_from_zero_state(monkeypatch, tmp_path: Path) -> None:
    cold_boot_log_file = _configure_cold_boot_runtime(monkeypatch, tmp_path)

    report = cold_boot_module.run_cold_boot_validation(log_path=cold_boot_log_file)

    assert report["status"] == "PASS"
    assert report["scenario"] == "lead_estimate_decision"
    assert report["no_dependency_on_previous_runs"] is True
    assert report["no_missing_initialization"] is True
    assert report["correct_execution_path"] is True
    assert report["determinism_preserved"] is True
    assert report["baseline_snapshot"] == report["cold_boot_snapshot"]

    boot_log = _read_jsonl(cold_boot_log_file)
    assert [entry["stage"] for entry in boot_log] == [
        "reset_environment",
        "initialize_system",
        "scenario_baseline",
        "reset_environment",
        "initialize_system",
        "scenario_cold_boot",
        "verification",
    ]
    assert boot_log[-1]["details"] == {
        "no_dependency_on_previous_runs": True,
        "no_missing_initialization": True,
        "correct_execution_path": True,
        "determinism_preserved": True,
        "mismatched_field": "",
    }

    system_log = _read_jsonl(tmp_path / "runtime" / "logs" / "system.log")
    assert any(entry["event_type"] == "workflow" for entry in system_log)
    assert any(entry["event_type"] == "binding" for entry in system_log)
