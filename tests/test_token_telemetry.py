from __future__ import annotations

import json
from pathlib import Path

import pytest
import app.execution.lead_estimate_decision as lead_estimate_decision_module
import app.execution.paths as paths_module
import app.orchestrator.task_factory as task_factory_module
import app.orchestrator.task_state_store as task_state_store_module
import runtime.token_telemetry as token_telemetry_module
import runtime.token_efficiency as token_efficiency_module
from app.execution.lead_estimate_contract import WORKFLOW_TYPE
from app.execution.task_schema import TASK_CONTRACT_VERSION
from app.orchestrator.execution_runner import run_execution
from runtime.token_telemetry import (
    finalize_run,
    get_average_token_cost,
    record_step,
    start_run,
)
from runtime.token_efficiency import (
    append_efficiency_record,
    build_efficiency_record,
    build_efficiency_report,
    classify_cost_quality,
    get_average_tei,
    get_average_tokens_per_step,
    get_average_tokens_per_task,
)


def _configure_runtime(monkeypatch, tmp_path: Path) -> Path:
    task_store_path = tmp_path / "data" / "tasks.json"
    monkeypatch.setattr(task_state_store_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store_module,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )
    monkeypatch.setattr(paths_module, "TASKS_FILE", task_store_path)
    monkeypatch.setattr(lead_estimate_decision_module, "TASKS_FILE", task_store_path)
    monkeypatch.setattr(
        token_telemetry_module,
        "TOKEN_USAGE_LOG_FILE",
        tmp_path / "runtime" / "logs" / "token_usage.jsonl",
    )
    monkeypatch.setattr(
        token_efficiency_module,
        "TOKEN_EFFICIENCY_LOG_FILE",
        tmp_path / "runtime" / "logs" / "token_efficiency.jsonl",
    )
    task_factory_module.clear_task_runtime_store()
    token_telemetry_module._RUN_STATE.clear()
    return task_store_path


def _build_workflow_task(*, store_path: Path, task_id: str) -> dict[str, object]:
    return task_factory_module.save_task(
        {
            "task_contract_version": TASK_CONTRACT_VERSION,
            "task_id": task_id,
            "created_at": "2026-04-04T00:00:00Z",
            "intent": WORKFLOW_TYPE,
            "payload": {
                "workflow_type": WORKFLOW_TYPE,
                "lead_id": "lead-token-001",
                "lead_data": {
                    "project_type": "ADU",
                    "scope_summary": "Detached ADU with pricing request",
                    "contact_info": {"phone": "555-0100"},
                    "lead_exists": True,
                },
            },
            "status": "pending",
            "notes": [],
            "history": [],
            "interaction_id": task_id,
            "job_id": task_id,
            "trace_id": task_id,
        },
        store_path=store_path,
    )


def _read_records(log_file: Path) -> list[dict[str, object]]:
    if not log_file.exists():
        return []
    return [
        json.loads(line)
        for line in log_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_logs_are_written_correctly(monkeypatch, tmp_path: Path) -> None:
    _configure_runtime(monkeypatch, tmp_path)
    log_file = token_telemetry_module.TOKEN_USAGE_LOG_FILE

    start_run("run-a", task_type="lead_estimate_decision")
    record_step("run-a", "input_normalization", 12, 3, 18, 80, 64)
    record_step("run-a", "decision_step", 5, 7, 25, 64, 72)
    record = finalize_run("run-a", task_type="lead_estimate_decision")

    assert record is not None
    assert record["run_id"] == "run-a"
    assert record["tokens_in"] == 17
    assert record["tokens_out"] == 10
    assert record["total_tokens"] == 27
    assert record["avg_payload_size_bytes"] == 70.0
    records = _read_records(log_file)
    assert len(records) == 1
    assert records[0]["steps"][0]["step_name"] == "input_normalization"
    assert records[0]["steps"][0]["payload_bytes_in"] == 80
    assert records[0]["steps"][0]["payload_bytes_out"] == 64
    efficiency_records = _read_records(
        token_efficiency_module.TOKEN_EFFICIENCY_LOG_FILE
    )
    assert len(efficiency_records) == 1
    assert efficiency_records[0]["successful_steps"] == 2
    assert efficiency_records[0]["payload_bytes_total"] == 280


def test_multiple_runs_tracked_independently(monkeypatch, tmp_path: Path) -> None:
    _configure_runtime(monkeypatch, tmp_path)
    log_file = token_telemetry_module.TOKEN_USAGE_LOG_FILE

    start_run("run-1", task_type="task-a")
    record_step("run-1", "decision_step", 1, 2, 10)
    start_run("run-2", task_type="task-b")
    record_step("run-2", "reporting", 3, 4, 20)
    finalize_run("run-1", task_type="task-a")
    finalize_run("run-2", task_type="task-b")

    records = _read_records(log_file)
    assert [record["run_id"] for record in records] == ["run-1", "run-2"]
    assert records[0]["total_tokens"] == 3
    assert records[1]["total_tokens"] == 7


def test_missing_token_data_handled_safely(monkeypatch, tmp_path: Path) -> None:
    _configure_runtime(monkeypatch, tmp_path)

    start_run("run-null", task_type="task-null")
    record_step("run-null", "reporting", None, None, None)
    record = finalize_run("run-null", task_type="task-null")

    assert record is not None
    assert record["tokens_in"] == 0
    assert record["tokens_out"] == 0
    assert record["total_tokens"] == 0
    assert record["estimated_tokens"] == 0
    assert record["token_source"] == "real"
    assert record["steps"][0]["tokens_in"] is None
    assert record["steps"][0]["tokens_out"] is None
    assert record["steps"][0]["estimated_tokens"] == 0
    assert record["steps"][0]["duration_ms"] == 0


def test_telemetry_does_not_affect_execution(monkeypatch, tmp_path: Path) -> None:
    task_store_path = _configure_runtime(monkeypatch, tmp_path)
    task_data = _build_workflow_task(
        store_path=task_store_path,
        task_id="DF-TOKEN-TELEMETRY-V1",
    )

    def persist(updated_task: dict[str, object]) -> None:
        task_data.update(updated_task)

    def broken_start_run(*args, **kwargs):
        raise RuntimeError("telemetry unavailable")

    monkeypatch.setattr(
        "runtime.pipeline.managed_execution.start_run", broken_start_run
    )

    executed_task = run_execution(
        task_data,
        now=lambda: "2026-04-04T00:00:00Z",
        persist=persist,
    )

    assert executed_task["status"] == "COMPLETED"
    assert executed_task["result"]["result_type"] == WORKFLOW_TYPE


def test_execution_run_writes_token_usage_log(monkeypatch, tmp_path: Path) -> None:
    task_store_path = _configure_runtime(monkeypatch, tmp_path)
    task_data = _build_workflow_task(
        store_path=task_store_path,
        task_id="DF-TOKEN-LOG-V1",
    )

    def persist(updated_task: dict[str, object]) -> None:
        task_data.update(updated_task)

    executed_task = run_execution(
        task_data,
        now=lambda: "2026-04-04T00:00:00Z",
        persist=persist,
    )

    assert executed_task["status"] == "COMPLETED"
    records = _read_records(token_telemetry_module.TOKEN_USAGE_LOG_FILE)
    assert records
    assert records[-1]["run_id"] == "DF-TOKEN-LOG-V1"
    assert [step["step_name"] for step in records[-1]["steps"]] == [
        "input_normalization",
        "decision_step",
        "action_binding",
        "reporting",
        "followup_reentry",
    ]


def test_average_token_cost_breakdown(monkeypatch, tmp_path: Path) -> None:
    _configure_runtime(monkeypatch, tmp_path)
    log_file = token_telemetry_module.TOKEN_USAGE_LOG_FILE

    start_run("run-avg-1", task_type="decision")
    record_step("run-avg-1", "decision_step", 10, 5, 100, 40, 20)
    finalize_run("run-avg-1", task_type="decision")

    start_run("run-avg-2", task_type="decision")
    record_step("run-avg-2", "decision_step", 20, 10, 200, 20, 20)
    finalize_run("run-avg-2", task_type="decision")

    averages = get_average_token_cost(log_file=log_file)

    assert averages["avg_tokens_per_run"] == 22.5
    assert averages["avg_payload_size_bytes_per_run"] == 25.0
    assert averages["breakdown_per_task_type"]["decision"]["runs"] == 2
    assert averages["breakdown_per_task_type"]["decision"]["avg_tokens_per_run"] == 22.5
    assert (
        averages["breakdown_per_task_type"]["decision"]["avg_payload_size_bytes"]
        == 25.0
    )


def test_execution_run_writes_payload_size_per_step(
    monkeypatch, tmp_path: Path
) -> None:
    task_store_path = _configure_runtime(monkeypatch, tmp_path)
    task_data = _build_workflow_task(
        store_path=task_store_path,
        task_id="DF-PAYLOAD-SIZE-LOG-V1",
    )

    def persist(updated_task: dict[str, object]) -> None:
        task_data.update(updated_task)

    executed_task = run_execution(
        task_data,
        now=lambda: "2026-04-04T00:00:00Z",
        persist=persist,
    )

    assert executed_task["status"] == "COMPLETED"
    records = _read_records(token_telemetry_module.TOKEN_USAGE_LOG_FILE)
    assert records
    step_sizes = {step["step_name"]: step for step in records[-1]["steps"]}
    assert step_sizes["input_normalization"]["payload_bytes_in"] is not None
    assert step_sizes["input_normalization"]["payload_bytes_out"] is not None
    assert step_sizes["decision_step"]["payload_bytes_out"] is not None
    assert records[-1]["avg_payload_size_bytes"] > 0


def test_execution_run_estimates_non_zero_tokens_from_payload(
    monkeypatch, tmp_path: Path
) -> None:
    task_store_path = _configure_runtime(monkeypatch, tmp_path)
    task_data = _build_workflow_task(
        store_path=task_store_path,
        task_id="DF-TOKEN-ESTIMATE-V1",
    )

    def persist(updated_task: dict[str, object]) -> None:
        task_data.update(updated_task)

    executed_task = run_execution(
        task_data,
        now=lambda: "2026-04-04T00:00:00Z",
        persist=persist,
    )

    assert executed_task["status"] == "COMPLETED"
    records = _read_records(token_telemetry_module.TOKEN_USAGE_LOG_FILE)
    assert records
    last_record = records[-1]
    assert last_record["run_id"] == "DF-TOKEN-ESTIMATE-V1"
    assert last_record["tokens_in"] == 0
    assert last_record["tokens_out"] == 0
    assert last_record["estimated_tokens"] > 0
    assert last_record["total_tokens"] == last_record["estimated_tokens"]
    assert last_record["token_source"] == "estimated"
    assert any((step.get("estimated_tokens") or 0) > 0 for step in last_record["steps"])


def test_tei_calculation() -> None:
    record = build_efficiency_record(
        {
            "run_id": "run-tei",
            "task_type": "lead_estimate_decision",
            "total_tokens": 20,
            "estimated_tokens": 0,
            "token_source": "real",
            "execution_time_ms": 90,
            "steps": [
                {"step_success": True, "payload_bytes_in": 10, "payload_bytes_out": 15},
                {"step_success": False, "payload_bytes_in": 20, "payload_bytes_out": 5},
                {
                    "step_success": True,
                    "payload_bytes_in": None,
                    "payload_bytes_out": 30,
                },
            ],
        }
    )

    assert record["executed_steps"] == 3
    assert record["successful_steps"] == 2
    assert record["estimated_tokens"] == 0
    assert record["token_source"] == "real"
    assert record["payload_bytes_total"] == 80
    assert record["tokens_per_step"] == (20 / 3)
    assert record["tokens_per_successful_step"] == 10.0
    assert record["ms_per_step"] == 30.0
    assert record["bytes_per_step"] == (80 / 3)
    assert record["tei"] == 0.1


def test_empty_efficiency_log_behavior(monkeypatch, tmp_path: Path) -> None:
    _configure_runtime(monkeypatch, tmp_path)
    log_file = token_efficiency_module.TOKEN_EFFICIENCY_LOG_FILE

    assert (
        get_average_tokens_per_task("lead_estimate_decision", log_file=log_file) == 0.0
    )
    assert (
        get_average_tokens_per_step("lead_estimate_decision", log_file=log_file) == 0.0
    )
    assert get_average_tei("lead_estimate_decision", log_file=log_file) == 0.0
    assert build_efficiency_report("lead_estimate_decision", log_file=log_file) == {
        "task_type": "lead_estimate_decision",
        "avg_tokens_per_task": 0.0,
        "avg_tokens_per_step": 0.0,
        "avg_tei": 0.0,
        "cost_quality": "expensive",
    }


def test_efficiency_rolling_averages_and_report(monkeypatch, tmp_path: Path) -> None:
    _configure_runtime(monkeypatch, tmp_path)
    log_file = token_efficiency_module.TOKEN_EFFICIENCY_LOG_FILE

    append_efficiency_record(
        {
            "run_id": "run-1",
            "task_type": "lead_estimate_decision",
            "total_tokens": 20,
            "execution_time_ms": 100,
            "steps": [
                {"step_success": True, "payload_bytes_in": 10, "payload_bytes_out": 10},
                {"step_success": True, "payload_bytes_in": 10, "payload_bytes_out": 10},
            ],
        },
        log_file=log_file,
    )
    append_efficiency_record(
        {
            "run_id": "run-2",
            "task_type": "lead_estimate_decision",
            "total_tokens": 40,
            "execution_time_ms": 200,
            "steps": [
                {"step_success": True, "payload_bytes_in": 20, "payload_bytes_out": 20},
                {"step_success": True, "payload_bytes_in": 20, "payload_bytes_out": 20},
            ],
        },
        log_file=log_file,
    )

    assert (
        get_average_tokens_per_task("lead_estimate_decision", log_file=log_file) == 30.0
    )
    assert (
        get_average_tokens_per_step("lead_estimate_decision", log_file=log_file) == 15.0
    )
    assert get_average_tei(
        "lead_estimate_decision", log_file=log_file
    ) == pytest.approx(0.075)
    assert build_efficiency_report("lead_estimate_decision", log_file=log_file) == {
        "task_type": "lead_estimate_decision",
        "avg_tokens_per_task": 30.0,
        "avg_tokens_per_step": 15.0,
        "avg_tei": pytest.approx(0.075),
        "cost_quality": "efficient",
    }


def test_cost_quality_classification_behavior() -> None:
    assert classify_cost_quality(avg_tokens_per_step=25.0, avg_tei=0.08) == "efficient"
    assert classify_cost_quality(avg_tokens_per_step=75.0, avg_tei=0.03) == "normal"
    assert classify_cost_quality(avg_tokens_per_step=175.0, avg_tei=0.01) == "expensive"


def test_efficiency_log_is_append_only(monkeypatch, tmp_path: Path) -> None:
    _configure_runtime(monkeypatch, tmp_path)
    log_file = token_efficiency_module.TOKEN_EFFICIENCY_LOG_FILE

    append_efficiency_record(
        {
            "run_id": "append-1",
            "task_type": "lead_estimate_decision",
            "total_tokens": 10,
            "execution_time_ms": 50,
            "steps": [
                {"step_success": True, "payload_bytes_in": 1, "payload_bytes_out": 1}
            ],
        },
        log_file=log_file,
    )
    append_efficiency_record(
        {
            "run_id": "append-2",
            "task_type": "lead_estimate_decision",
            "total_tokens": 12,
            "execution_time_ms": 60,
            "steps": [
                {"step_success": True, "payload_bytes_in": 2, "payload_bytes_out": 2}
            ],
        },
        log_file=log_file,
    )

    records = _read_records(log_file)
    assert [record["run_id"] for record in records] == ["append-1", "append-2"]
