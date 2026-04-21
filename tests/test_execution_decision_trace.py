from __future__ import annotations

from pathlib import Path

import app.execution.execution_boundary as execution_boundary_module
import app.product.runner as product_runner_module
from app.product.runner import dispatch_action_trigger


def test_dispatch_action_trigger_includes_decision_trace(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(product_runner_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        execution_boundary_module,
        "EXECUTION_BOUNDARY_VIOLATIONS_LOG",
        tmp_path / "runtime" / "logs" / "execution_boundary_violations.jsonl",
    )

    with execution_boundary_module.execution_boundary(
        {"task_id": "DF-DECISION-TRACE-V1", "intent": "new_lead"},
        policy_validated=True,
    ):
        result = dispatch_action_trigger(
            {
                "action_type": "WRITE_FILE",
                "payload": {
                    "task_id": "DF-DECISION-TRACE-V1",
                    "path": r"runtime\out\trace.txt",
                    "content": "trace me",
                },
            },
            task_state={"task_id": "DF-DECISION-TRACE-V1", "status": "EXECUTING"},
        )

    trace = result["decision_trace"]
    assert result["status"] == "completed"
    assert set(trace) == {
        "reason",
        "context_used",
        "action_type",
        "policy_result",
        "confidence",
        "vendor",
    }
    assert trace["action_type"] == "WRITE_FILE"
    assert trace["policy_result"] == "allowed: policy gate passed"
    assert trace["confidence"] == "high"
    assert trace["vendor"] == "openai"
