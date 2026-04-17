from __future__ import annotations

import json
from pathlib import Path

import app.orchestrator.escalation as escalation_module
import app.orchestrator.system_health as system_health_module


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _task(
    *,
    task_id: str,
    status: str,
    created_at: str = "2026-04-05T00:00:00Z",
    last_updated_at: str = "2026-04-05T00:00:00Z",
    started_at: str = "",
    completed_at: str = "",
    failed_at: str = "",
    offload_latency: float | None = None,
    result: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "task_id": task_id,
        "task_contract_version": 1,
        "created_at": created_at,
        "last_updated_at": last_updated_at,
        "intent": "new_lead",
        "payload": {"summary": task_id},
        "status": status,
        "notes": [],
        "history": [],
        "started_at": started_at,
        "completed_at": completed_at,
        "failed_at": failed_at,
        "result": result or {},
        "offload_latency": offload_latency,
    }


def _write_escalations(path: Path, *timestamps: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    entries = [
        {
            "timestamp": timestamp,
            "task_id": f"ESC-{index}",
            "event_type": "escalation_required",
            "status": "escalation_required",
            "details": {"reason": "test"},
        }
        for index, timestamp in enumerate(timestamps, start=1)
    ]
    path.write_text(
        "\n".join(json.dumps(entry, ensure_ascii=True) for entry in entries) + ("\n" if entries else ""),
        encoding="utf-8",
    )


def test_normal_system_classifies_healthy(tmp_path: Path) -> None:
    escalation_log = tmp_path / "runtime" / "logs" / "escalations.jsonl"
    signal = system_health_module.evaluate_system_health(
        [
            _task(
                task_id="DF-HEALTHY-001",
                status="COMPLETED",
                last_updated_at="2026-04-05T00:00:20Z",
                started_at="2026-04-05T00:00:00Z",
                completed_at="2026-04-05T00:00:02Z",
            )
        ],
        now_timestamp="2026-04-05T00:01:00Z",
        escalation_log_file=escalation_log,
    )

    assert signal == {
        "status": "system_health",
        "state": "healthy",
        "reason": "system operating within deterministic thresholds",
        "metrics": {
            "last_successful_execution_timestamp": "2026-04-05T00:00:20Z",
            "failed_tasks_count": 0,
            "stuck_tasks_count": 0,
            "execution_latency_seconds": 2.0,
            "system_responsiveness_seconds": 40,
            "escalation_frequency": 0,
            "seconds_since_last_success": 40,
        },
    }


def test_repeated_failure_classifies_degraded(tmp_path: Path) -> None:
    escalation_log = tmp_path / "runtime" / "logs" / "escalations.jsonl"
    signal = system_health_module.evaluate_system_health(
        [
            _task(task_id="DF-FAIL-001", status="FAILED", failed_at="2026-04-05T00:00:30Z"),
            _task(task_id="DF-FAIL-002", status="FAILED", failed_at="2026-04-05T00:00:40Z"),
        ],
        now_timestamp="2026-04-05T00:01:00Z",
        escalation_log_file=escalation_log,
    )

    assert signal["status"] == "system_health"
    assert signal["state"] == "degraded"
    assert signal["reason"] == "repeated_failures"
    assert signal["metrics"]["failed_tasks_count"] == 2


def test_severe_issues_classify_critical_and_escalate(tmp_path: Path, monkeypatch) -> None:
    health_log = tmp_path / "runtime" / "logs" / "system_health.jsonl"
    escalation_log = tmp_path / "runtime" / "logs" / "escalations.jsonl"
    monkeypatch.setattr(system_health_module, "SYSTEM_HEALTH_LOG_FILE", health_log)
    monkeypatch.setattr(escalation_module, "ESCALATION_LOG_FILE", escalation_log)
    _write_escalations(
        escalation_log,
        "2026-04-05T00:08:00Z",
        "2026-04-05T00:09:00Z",
        "2026-04-05T00:10:00Z",
    )

    critical_tasks = [
        _task(task_id="DF-SUCCESS-OLD", status="COMPLETED", completed_at="2026-04-05T00:00:00Z"),
        _task(task_id="DF-FAIL-001", status="FAILED", failed_at="2026-04-05T00:09:00Z"),
        _task(task_id="DF-FAIL-002", status="FAILED", failed_at="2026-04-05T00:09:10Z"),
        _task(task_id="DF-FAIL-003", status="FAILED", failed_at="2026-04-05T00:09:20Z"),
        _task(task_id="DF-FAIL-004", status="FAILED", failed_at="2026-04-05T00:09:30Z"),
        _task(task_id="DF-FAIL-005", status="FAILED", failed_at="2026-04-05T00:09:40Z"),
        _task(task_id="DF-STUCK-001", status="EXECUTING", result={"status": "task_stuck"}),
        _task(task_id="DF-STUCK-002", status="CREATED", result={"status": "task_stuck"}),
        _task(task_id="DF-STUCK-003", status="DEFERRED", result={"status": "task_stuck"}),
    ]

    signal = system_health_module.assess_system_health(
        critical_tasks,
        now_timestamp="2026-04-05T00:10:00Z",
        phase="post_execution",
        task_data={"task_id": "DF-RUNTIME-HEALTH-GUARD-V1", "runtime_verdict": {}},
        escalation_log_file=escalation_log,
        health_log_file=health_log,
    )

    assert signal["state"] == "critical"
    assert "repeated_failures" in signal["reason"]
    assert "repeated_stuck_tasks" in signal["reason"]
    assert "escalation_frequency_high" in signal["reason"]
    assert _read_jsonl(health_log)[0]["details"]["state"] == "critical"
    escalation_entries = _read_jsonl(escalation_log)
    assert escalation_entries[-1]["details"] == {
        "status": "escalation_required",
        "task_id": "DF-RUNTIME-HEALTH-GUARD-V1",
        "reason": "system_health_critical",
        "severity": "critical",
    }


def test_identical_runs_produce_identical_classification(tmp_path: Path) -> None:
    escalation_log = tmp_path / "runtime" / "logs" / "escalations.jsonl"
    _write_escalations(escalation_log, "2026-04-05T00:09:00Z")
    tasks = [
        _task(
            task_id="DF-STABLE-001",
            status="COMPLETED",
            started_at="2026-04-05T00:00:00Z",
            completed_at="2026-04-05T00:00:04Z",
            last_updated_at="2026-04-05T00:00:04Z",
        ),
        _task(task_id="DF-STABLE-002", status="FAILED", failed_at="2026-04-05T00:09:30Z"),
        _task(task_id="DF-STABLE-003", status="FAILED", failed_at="2026-04-05T00:09:35Z"),
    ]

    first = system_health_module.evaluate_system_health(
        tasks,
        now_timestamp="2026-04-05T00:10:00Z",
        escalation_log_file=escalation_log,
    )
    second = system_health_module.evaluate_system_health(
        tasks,
        now_timestamp="2026-04-05T00:10:00Z",
        escalation_log_file=escalation_log,
    )

    assert first == second
