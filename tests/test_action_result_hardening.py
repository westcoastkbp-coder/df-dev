from __future__ import annotations

import json
from pathlib import Path

from app.execution.action_result import build_action_result, build_invalid_action_result_signal, validate_action_result
from app.execution.task_schema import TASK_CONTRACT_VERSION
from app.orchestrator.execution_runner import run_execution


def _build_task(task_id: str) -> dict[str, object]:
    return {
        "task_contract_version": TASK_CONTRACT_VERSION,
        "task_id": task_id,
        "created_at": "2026-04-04T00:00:00Z",
        "intent": "new_lead",
        "payload": {"summary": "Detached ADU"},
        "status": "VALIDATED",
        "notes": [],
        "history": [],
        "interaction_id": task_id,
        "job_id": task_id,
        "trace_id": task_id,
    }

def _read_violation_log(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _decision_trace() -> dict[str, str]:
    return {
        "reason": "test decision",
        "context_used": "task=test",
        "action_type": "NEW_LEAD",
        "policy_result": "allowed: policy gate passed",
        "confidence": "high",
    }


def test_valid_typed_action_result_passes() -> None:
    result = build_action_result(
        status="completed",
        task_id="DF-ACTION-VALID-V1",
        action_type="NEW_LEAD",
        result_payload={"output_path": "runtime/out/leads/lead_001.txt"},
        error_code="",
        error_message="",
        source="product_executor",
        diagnostic_message="lead captured",
    )

    assert validate_action_result(result, expected_task_id="DF-ACTION-VALID-V1") == result


def test_missing_required_field_fails() -> None:
    try:
        validate_action_result(
            {
                "status": "completed",
                "action_type": "NEW_LEAD",
                "result_payload": {},
                "error_code": "",
                "source": "product_executor",
            }
        )
    except ValueError as exc:
        assert str(exc) == "action result missing required fields: decision_trace, error_message"
    else:
        raise AssertionError("expected validation failure")


def test_extra_field_fails() -> None:
    try:
        validate_action_result(
            {
                "status": "completed",
                "action_type": "NEW_LEAD",
                "result_payload": {},
                "error_code": "",
                "error_message": "",
                "source": "product_executor",
                "decision_trace": _decision_trace(),
                "summary": "freeform",
            }
        )
    except ValueError as exc:
        assert str(exc) == "action result contains unsupported fields: summary"
    else:
        raise AssertionError("expected validation failure")


def test_raw_text_result_fails() -> None:
    try:
        validate_action_result("lead created")
    except ValueError as exc:
        assert str(exc) == "action result must be a dict"
    else:
        raise AssertionError("expected validation failure")


def test_malformed_nested_payload_fails() -> None:
    try:
        validate_action_result(
            {
                "status": "completed",
                "action_type": "NEW_LEAD",
                "result_payload": {"payload": object()},
                "error_code": "",
                "error_message": "",
                "source": "product_executor",
                "decision_trace": _decision_trace(),
            }
        )
    except ValueError as exc:
        assert str(exc) == "result_payload must contain only structured JSON-like values"
    else:
        raise AssertionError("expected validation failure")


def test_execution_stops_on_invalid_result_and_logs(monkeypatch, tmp_path: Path) -> None:
    violation_log = tmp_path / "runtime" / "logs" / "action_result_violations.jsonl"
    monkeypatch.setattr("app.execution.action_result.ACTION_RESULT_VIOLATIONS_LOG", violation_log)

    task = _build_task("DF-ACTION-STOP-V1")
    persisted: list[str] = []

    def persist(updated_task: dict[str, object]) -> None:
        persisted.append(str(updated_task.get("status", "")))

    executed = run_execution(
        task,
        now=lambda: "2026-04-04T00:00:00Z",
        persist=persist,
        executor=lambda _: "plain text is blocked",
    )

    assert executed["status"] == "FAILED"
    assert executed["result"] == build_invalid_action_result_signal(
        task_id="DF-ACTION-STOP-V1",
        action_type="NEW_LEAD",
        reason="action result must be a dict",
        source=__name__,
    )
    assert persisted == ["EXECUTING", "FAILED"]
    logged = _read_violation_log(violation_log)
    assert logged[-1]["reason"] == "action result must be a dict"


def test_invalid_result_failure_is_deterministic_across_runs(monkeypatch, tmp_path: Path) -> None:
    violation_log = tmp_path / "runtime" / "logs" / "action_result_violations.jsonl"
    monkeypatch.setattr("app.execution.action_result.ACTION_RESULT_VIOLATIONS_LOG", violation_log)

    def execute_once() -> dict[str, object]:
        task = _build_task("DF-ACTION-DETERMINISTIC-V1")
        return run_execution(
            task,
            now=lambda: "2026-04-04T00:00:00Z",
            persist=lambda _: None,
            executor=lambda _: {"status": "completed"},
        )["result"]

    first = execute_once()
    second = execute_once()

    assert first == second
    assert first["status"] == "invalid_action_result"
