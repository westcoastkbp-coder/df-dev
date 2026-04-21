from __future__ import annotations

import json
from pathlib import Path

import app.execution.lead_estimate_decision as lead_estimate_decision_module
import app.execution.paths as paths_module
import app.orchestrator.task_factory as task_factory_module
import app.orchestrator.task_state_store as task_state_store_module
from app.execution.batch_real_lead_runner import run_real_lead_batch, write_batch_report


def _configure_batch_runtime(monkeypatch, tmp_path: Path) -> Path:
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


def _batch_input() -> list[dict[str, object]]:
    return [
        {
            "lead_id": "lead-batch-001",
            "contact_info": {"phone": "555-0100"},
            "project_type": "ADU",
            "scope_summary": "Detached ADU with pricing request",
        },
        {
            "lead_id": "lead-batch-002",
            "contact_info": {"email": "client@example.com"},
            "project_type": "bathroom",
            "scope_summary": "Primary bath remodel",
        },
        {
            "lead_id": "lead-batch-003",
            "contact_info": {},
            "project_type": "adu",
            "scope_summary": "Garage conversion",
        },
        {
            "lead_id": "lead-batch-004",
            "contact_info": {"phone": "555-0103"},
            "project_type": "roof",
            "scope_summary": "Roof replacement",
        },
        {
            "lead_id": "lead-batch-005",
            "contact_info": {"phone": "555-0104"},
            "project_type": "general",
            "scope_summary": "",
        },
    ]


def test_batch_run_executes_all_inputs(monkeypatch, tmp_path: Path) -> None:
    task_store_path = _configure_batch_runtime(monkeypatch, tmp_path)
    reports_dir = tmp_path / "runtime" / "out" / "reports" / "batch"

    report = run_real_lead_batch(
        _batch_input(),
        store_path=task_store_path,
        output_dir=reports_dir,
    )

    assert report["total_runs"] == 5
    assert len(report["individual_reports"]) == 5
    for item in report["individual_reports"]:
        assert Path(item["report_path"]).exists()


def test_batch_run_is_deterministic_for_same_input(monkeypatch, tmp_path: Path) -> None:
    task_store_path = _configure_batch_runtime(monkeypatch, tmp_path)
    reports_dir = tmp_path / "runtime" / "out" / "reports" / "batch"

    first = run_real_lead_batch(
        _batch_input(),
        store_path=task_store_path,
        output_dir=reports_dir,
    )
    second = run_real_lead_batch(
        _batch_input(),
        store_path=task_store_path,
        output_dir=reports_dir,
    )

    assert first["total_runs"] == second["total_runs"]
    assert first["passed_runs"] == second["passed_runs"]
    assert first["failed_runs"] == second["failed_runs"]
    assert first["failure_distribution"] == second["failure_distribution"]
    assert first["manual_review_count"] == second["manual_review_count"]
    assert first["most_common_failure"] == second["most_common_failure"]


def test_batch_aggregation_counts_are_correct(monkeypatch, tmp_path: Path) -> None:
    task_store_path = _configure_batch_runtime(monkeypatch, tmp_path)
    reports_dir = tmp_path / "runtime" / "out" / "reports" / "batch"

    report = run_real_lead_batch(
        _batch_input(),
        store_path=task_store_path,
        output_dir=reports_dir,
    )

    assert report["passed_runs"] == 2
    assert report["failed_runs"] == 3
    assert report["failure_distribution"] == {
        "classification_error": 0,
        "missing_required_input": 3,
        "wrong_archive_path": 0,
        "wrong_child_task_type": 0,
        "traceability_gap": 0,
        "state_inconsistency": 0,
        "operator_usability_issue": 0,
    }
    assert report["manual_review_count"] == 0
    assert report["most_common_failure"] == "missing_required_input"
    assert report["notes"] == "batch_validated"


def test_batch_report_output_is_written(monkeypatch, tmp_path: Path) -> None:
    task_store_path = _configure_batch_runtime(monkeypatch, tmp_path)
    output_path = tmp_path / "runtime" / "out" / "reports" / "batch_report.json"
    reports_dir = tmp_path / "runtime" / "out" / "reports" / "batch"

    written_path = write_batch_report(
        _batch_input(),
        store_path=task_store_path,
        output_path=output_path,
        reports_dir=reports_dir,
    )
    written_report = json.loads(written_path.read_text(encoding="utf-8"))

    assert written_path == output_path
    assert written_report["total_runs"] == 5
    assert written_report["failed_runs"] == 3
