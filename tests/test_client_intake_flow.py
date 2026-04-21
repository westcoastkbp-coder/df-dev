from __future__ import annotations

import json
from pathlib import Path

import app.execution.paths as paths_module
import app.product.runner as runner_module
from app.execution.action_result import validate_action_result
from app.execution.execution_boundary import execution_boundary
from app.execution.input_normalizer import normalize_input
from app.execution.product_executor import execute_product_task


def test_normalize_input_recognizes_client_intake_flow() -> None:
    intent, payload = normalize_input(
        text="Lock client intake flow as the standard office process.",
    )

    assert intent == "client_intake_flow"
    assert payload["flow_name"] == "client_intake_flow"
    assert payload["steps"] == [
        "add to CRM",
        "send email",
        "create doc",
        "create meeting",
    ]
    assert payload["reusable"] is True
    assert payload["callable_as_one_action"] is True


def test_execute_product_task_locks_client_intake_flow(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(paths_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(paths_module, "OUTPUT_DIR", Path("runtime/out"))
    monkeypatch.setattr(runner_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(runner_module, "RUNTIME_OUT_DIR", Path("runtime/out"))

    task_data = {
        "task_id": "DF-CLIENT-INTAKE-FLOW-V1",
        "status": "VALIDATED",
        "intent": "client_intake_flow",
        "payload": {},
    }

    with execution_boundary(task_data, policy_validated=True):
        result = execute_product_task(task_data)

    validated = validate_action_result(
        result,
        expected_task_id="DF-CLIENT-INTAKE-FLOW-V1",
    )

    assert validated["status"] == "completed"
    assert validated["result_payload"]["action"] == "client flow locked"
    assert validated["result_payload"]["status"] == "success"
    assert validated["result_payload"]["output"] == "client_intake_flow"
    assert validated["result_payload"]["reusable"] is True
    assert validated["result_payload"]["callable_as_one_action"] is True

    flow_path = tmp_path / "runtime" / "out" / "office" / "client_intake_flow.json"
    assert flow_path.exists()

    saved_flow = json.loads(flow_path.read_text(encoding="utf-8"))
    assert saved_flow == {
        "callable_as_one_action": True,
        "flow_name": "client_intake_flow",
        "locked": True,
        "process_type": "standard_office",
        "reusable": True,
        "steps": [
            "add to CRM",
            "send email",
            "create doc",
            "create meeting",
        ],
    }
