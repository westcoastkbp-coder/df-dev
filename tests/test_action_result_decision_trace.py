from __future__ import annotations

import pytest

from app.execution.action_result import build_action_result, validate_action_result


def test_build_action_result_includes_required_decision_trace() -> None:
    result = build_action_result(
        status="completed",
        task_id="DF-ACTION-TRACE-V1",
        action_type="WRITE_FILE",
        result_payload={"path": r"runtime\out\trace.txt"},
        error_code="",
        error_message="",
        source="product_runner",
        diagnostic_message="write completed",
    )

    assert result["decision_trace"] == {
        "reason": "write completed",
        "context_used": result["decision_trace"]["context_used"],
        "action_type": "WRITE_FILE",
        "policy_result": "allowed: policy gate passed",
        "confidence": "high",
        "vendor": "openai",
    }


def test_validate_action_result_rejects_missing_decision_trace() -> None:
    with pytest.raises(ValueError, match="decision_trace"):
        validate_action_result(
            {
                "status": "completed",
                "action_type": "WRITE_FILE",
                "result_payload": {"path": r"runtime\out\trace.txt"},
                "error_code": "",
                "error_message": "",
                "source": "product_runner",
            }
        )
