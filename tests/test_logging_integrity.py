from __future__ import annotations

import json
from pathlib import Path

import app.policy.policy_gate as policy_gate_module
import runtime.system_log as system_log_module
from app.policy.policy_gate import record_policy_decision
from runtime.system_log import log_event, log_task_execution


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_structured_logs_use_deterministic_shape(monkeypatch, tmp_path: Path) -> None:
    system_log_file = tmp_path / "runtime" / "logs" / "system.log"
    task_log_file = tmp_path / "runtime" / "logs" / "tasks.log"
    policy_log_file = tmp_path / "runtime" / "logs" / "policy.log"

    monkeypatch.setattr(system_log_module, "SYSTEM_LOG_FILE", system_log_file)
    monkeypatch.setattr(system_log_module, "TASK_LOG_FILE", task_log_file)
    monkeypatch.setattr(policy_gate_module, "POLICY_LOG_FILE", policy_log_file)

    log_event("system", {"message": "alpha"}, task_id="DF-LOG-STRUCTURE-V1", status="observed")
    log_task_execution(
        task_id="DF-LOG-STRUCTURE-V1",
        status="completed",
        result_type="WRITE_FILE",
    )
    record_policy_decision(
        "DF-LOG-STRUCTURE-V1",
        allowed=True,
        reason="",
    )

    records = (
        _read_jsonl(system_log_file)
        + _read_jsonl(task_log_file)
        + _read_jsonl(policy_log_file)
    )
    assert len(records) == 3
    for record in records:
        assert sorted(record.keys()) == [
            "details",
            "event_type",
            "status",
            "task_id",
            "timestamp",
        ]
        assert record["task_id"] == "DF-LOG-STRUCTURE-V1"


def test_logging_failure_does_not_raise(monkeypatch, tmp_path: Path) -> None:
    system_log_file = tmp_path / "runtime" / "logs" / "system.log"
    monkeypatch.setattr(system_log_module, "SYSTEM_LOG_FILE", system_log_file)

    original_open = Path.open

    def failing_open(self: Path, *args, **kwargs):  # type: ignore[no-untyped-def]
        if self == system_log_file:
            raise OSError("logging unavailable")
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", failing_open)

    log_event("system", "should not fail", task_id="DF-LOG-FAILSAFE-V1", status="observed")


def test_mode_trace_message_infers_task_id(monkeypatch, tmp_path: Path) -> None:
    system_log_file = tmp_path / "runtime" / "logs" / "system.log"
    monkeypatch.setattr(system_log_module, "SYSTEM_LOG_FILE", system_log_file)

    log_event(
        "mode",
        "[MODE]\ntask: DF-LOG-MODE-V1\nexecution: LOCAL\ncompute: cpu_mode",
    )

    records = _read_jsonl(system_log_file)
    assert records[0]["task_id"] == "DF-LOG-MODE-V1"
    assert records[0]["event_type"] == "mode"
