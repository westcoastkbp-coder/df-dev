from __future__ import annotations

import json
from pathlib import Path

import runtime.token_report as token_report_module
import runtime.token_telemetry as token_telemetry_module
import runtime.token_efficiency as token_efficiency_module
from runtime.token_report import get_last_run_report, get_recent_runs_summary


def _write_jsonl(path: Path, records: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records),
        encoding="utf-8",
    )


def _snapshot(path: Path) -> tuple[bool, str]:
    if not path.exists():
        return False, ""
    return True, path.read_text(encoding="utf-8")


def test_empty_logs_are_safe(tmp_path: Path) -> None:
    usage_log = tmp_path / "runtime" / "logs" / "token_usage.jsonl"
    efficiency_log = tmp_path / "runtime" / "logs" / "token_efficiency.jsonl"

    report = get_last_run_report(
        usage_log_file=usage_log,
        efficiency_log_file=efficiency_log,
    )
    summary = get_recent_runs_summary(
        usage_log_file=usage_log,
        efficiency_log_file=efficiency_log,
    )

    assert report == {
        "run_id": "",
        "task_type": "",
        "total_tokens": 0,
        "total_steps": 0,
        "tokens_per_step": 0.0,
        "avg_tei": 0.0,
        "token_source": "real",
        "cost_quality": "normal",
    }
    assert summary == []


def test_valid_logs_produce_expected_report(tmp_path: Path) -> None:
    usage_log = tmp_path / "runtime" / "logs" / "token_usage.jsonl"
    efficiency_log = tmp_path / "runtime" / "logs" / "token_efficiency.jsonl"

    _write_jsonl(
        usage_log,
        [
            {
                "run_id": "run-1",
                "task_type": "lead_estimate_decision",
                "total_tokens": 120,
                "token_source": "real",
                "steps": [{"step_name": "a"}, {"step_name": "b"}, {"step_name": "c"}],
            },
            {
                "run_id": "run-2",
                "task_type": "new_lead",
                "total_tokens": 320,
                "token_source": "estimated",
                "steps": [{"step_name": "a"}, {"step_name": "b"}],
            },
        ],
    )
    _write_jsonl(
        efficiency_log,
        [
            {
                "run_id": "run-1",
                "tokens_per_step": 40.0,
                "tei": 0.06,
                "token_source": "real",
            },
            {
                "run_id": "run-2",
                "tokens_per_step": 160.0,
                "tei": 0.01,
                "token_source": "estimated",
            },
        ],
    )

    report = get_last_run_report(
        usage_log_file=usage_log,
        efficiency_log_file=efficiency_log,
    )
    summary = get_recent_runs_summary(
        limit=2,
        usage_log_file=usage_log,
        efficiency_log_file=efficiency_log,
    )

    assert report == {
        "run_id": "run-2",
        "task_type": "new_lead",
        "total_tokens": 320,
        "total_steps": 2,
        "tokens_per_step": 160.0,
        "avg_tei": 0.01,
        "token_source": "estimated",
        "cost_quality": "expensive",
    }
    assert summary == [
        {
            "run_id": "run-1",
            "tokens": 120,
            "tei": 0.06,
            "token_source": "real",
            "cost_quality": "efficient",
        },
        {
            "run_id": "run-2",
            "tokens": 320,
            "tei": 0.01,
            "token_source": "estimated",
            "cost_quality": "expensive",
        },
    ]


def test_report_reads_have_no_side_effects(tmp_path: Path) -> None:
    usage_log = tmp_path / "runtime" / "logs" / "token_usage.jsonl"
    efficiency_log = tmp_path / "runtime" / "logs" / "token_efficiency.jsonl"
    _write_jsonl(
        usage_log,
        [
            {
                "run_id": "run-1",
                "task_type": "task",
                "total_tokens": 25,
                "steps": [{"step_name": "only"}],
            }
        ],
    )
    _write_jsonl(
        efficiency_log,
        [{"run_id": "run-1", "tokens_per_step": 25.0, "tei": 0.04}],
    )
    before_usage = _snapshot(usage_log)
    before_efficiency = _snapshot(efficiency_log)

    report = get_last_run_report(
        usage_log_file=usage_log,
        efficiency_log_file=efficiency_log,
    )
    summary = get_recent_runs_summary(
        usage_log_file=usage_log,
        efficiency_log_file=efficiency_log,
    )

    assert report["run_id"] == "run-1"
    assert len(summary) == 1
    assert _snapshot(usage_log) == before_usage
    assert _snapshot(efficiency_log) == before_efficiency


def test_execution_remains_independent_from_reporting(
    monkeypatch, tmp_path: Path
) -> None:
    usage_log = tmp_path / "runtime" / "logs" / "token_usage.jsonl"
    efficiency_log = tmp_path / "runtime" / "logs" / "token_efficiency.jsonl"
    monkeypatch.setattr(token_report_module, "TOKEN_USAGE_LOG_FILE", usage_log)
    monkeypatch.setattr(
        token_report_module, "TOKEN_EFFICIENCY_LOG_FILE", efficiency_log
    )
    monkeypatch.setattr(token_telemetry_module, "TOKEN_USAGE_LOG_FILE", usage_log)
    monkeypatch.setattr(
        token_efficiency_module, "TOKEN_EFFICIENCY_LOG_FILE", efficiency_log
    )

    token_telemetry_module._RUN_STATE.clear()
    token_telemetry_module.start_run("run-exec", task_type="lead_estimate_decision")
    token_telemetry_module.record_step("run-exec", "decision_step", 7, 5, 12)
    record = token_telemetry_module.finalize_run(
        "run-exec", task_type="lead_estimate_decision"
    )

    assert record is not None
    assert record["run_id"] == "run-exec"
    assert usage_log.exists()
    assert efficiency_log.exists()

    report = get_last_run_report(
        usage_log_file=usage_log,
        efficiency_log_file=efficiency_log,
    )
    assert report["run_id"] == "run-exec"


def test_report_reflects_estimated_token_totals(tmp_path: Path) -> None:
    usage_log = tmp_path / "runtime" / "logs" / "token_usage.jsonl"
    efficiency_log = tmp_path / "runtime" / "logs" / "token_efficiency.jsonl"

    _write_jsonl(
        usage_log,
        [
            {
                "run_id": "run-estimated",
                "task_type": "lead_estimate_decision",
                "total_tokens": 250,
                "estimated_tokens": 250,
                "token_source": "estimated",
                "steps": [
                    {"step_name": "a", "estimated_tokens": 125},
                    {"step_name": "b", "estimated_tokens": 125},
                ],
            }
        ],
    )
    _write_jsonl(
        efficiency_log,
        [
            {
                "run_id": "run-estimated",
                "tokens_per_step": 125.0,
                "tei": 0.04,
                "token_source": "estimated",
            }
        ],
    )

    report = get_last_run_report(
        usage_log_file=usage_log,
        efficiency_log_file=efficiency_log,
    )

    assert report == {
        "run_id": "run-estimated",
        "task_type": "lead_estimate_decision",
        "total_tokens": 250,
        "total_steps": 2,
        "tokens_per_step": 125.0,
        "avg_tei": 0.04,
        "token_source": "estimated",
        "cost_quality": "normal",
    }
