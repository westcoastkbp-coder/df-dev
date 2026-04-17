from __future__ import annotations

from pathlib import Path

import app.execution.lead_estimate_decision as lead_estimate_decision_module
import app.execution.paths as paths_module
import app.orchestrator.task_factory as task_factory_module
import app.orchestrator.task_state_store as task_state_store_module
from app.execution.real_workflow_validation import (
    run_validation_pack,
    write_validation_report,
)


def _configure_validation_runtime(monkeypatch, tmp_path: Path) -> Path:
    task_store_path = tmp_path / "data" / "tasks.json"
    monkeypatch.setattr(task_state_store_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store_module,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )
    monkeypatch.setattr(paths_module, "TASKS_FILE", task_store_path)
    monkeypatch.setattr(lead_estimate_decision_module, "TASKS_FILE", task_store_path)
    task_factory_module.clear_task_runtime_store()
    return task_store_path


def test_validation_pack_executes_all_scenarios(monkeypatch, tmp_path: Path) -> None:
    task_store_path = _configure_validation_runtime(monkeypatch, tmp_path)

    report = run_validation_pack(store_path=task_store_path)

    scenario_ids = [item["scenario_id"] for item in report["scenarios"]]
    pass_fail = [item["pass_fail"] for item in report["scenarios"]]

    assert report["workflow_type"] == "lead_estimate_decision"
    assert report["scenario_count"] == 5
    assert scenario_ids == [
        "qualified_lead",
        "incomplete_scope_lead",
        "non_qualified_lead",
        "ambiguous_lead",
        "repeated_same_lead",
    ]
    assert pass_fail == ["pass", "pass", "pass", "pass", "pass"]


def test_repeated_same_scenario_gives_same_result(monkeypatch, tmp_path: Path) -> None:
    task_store_path = _configure_validation_runtime(monkeypatch, tmp_path)

    first = run_validation_pack(store_path=task_store_path)
    second = run_validation_pack(store_path=task_store_path)

    repeated_first = next(item for item in first["scenarios"] if item["scenario_id"] == "repeated_same_lead")
    repeated_second = next(item for item in second["scenarios"] if item["scenario_id"] == "repeated_same_lead")

    assert repeated_first == repeated_second


def test_validation_pack_checks_child_task_creation(monkeypatch, tmp_path: Path) -> None:
    task_store_path = _configure_validation_runtime(monkeypatch, tmp_path)

    report = run_validation_pack(store_path=task_store_path)
    indexed = {item["scenario_id"]: item for item in report["scenarios"]}

    assert indexed["qualified_lead"]["actual_action"] == "create_estimate_task"
    assert indexed["incomplete_scope_lead"]["actual_action"] == "request_missing_scope"
    assert indexed["ambiguous_lead"]["actual_action"] == "manual_review"
    assert indexed["qualified_lead"]["failure_class"] == ""
    assert indexed["incomplete_scope_lead"]["failure_class"] == ""
    assert indexed["ambiguous_lead"]["failure_class"] == ""


def test_validation_pack_checks_archive_path(monkeypatch, tmp_path: Path) -> None:
    task_store_path = _configure_validation_runtime(monkeypatch, tmp_path)

    report = run_validation_pack(store_path=task_store_path)
    archived = next(item for item in report["scenarios"] if item["scenario_id"] == "non_qualified_lead")

    assert archived["expected_action"] == "archive_lead"
    assert archived["actual_action"] == "archive_lead"
    assert archived["pass_fail"] == "pass"
    assert archived["failure_class"] == ""


def test_validation_report_output_is_deterministic(monkeypatch, tmp_path: Path) -> None:
    task_store_path = _configure_validation_runtime(monkeypatch, tmp_path)
    output_path = tmp_path / "runtime" / "out" / "validation" / "lead_validation.json"

    first_path = write_validation_report(store_path=task_store_path, output_path=output_path)
    first_output = first_path.read_text(encoding="utf-8")
    second_path = write_validation_report(store_path=task_store_path, output_path=output_path)
    second_output = second_path.read_text(encoding="utf-8")

    assert first_path == output_path
    assert second_path == output_path
    assert first_output == second_output
