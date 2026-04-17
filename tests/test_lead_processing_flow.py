from __future__ import annotations

import json
from pathlib import Path

import app.orchestrator.task_factory as task_factory_module
import app.orchestrator.task_state_store as task_state_store_module
from app.execution.lead_processing_flow import run_lead_processing_flow
import app.execution.lead_processing_flow as lead_processing_flow_module
import integrations.google_sheets_gateway as google_sheets_gateway_module


def _configure_runtime(monkeypatch, tmp_path: Path) -> Path:
    task_system_path = tmp_path / "data" / "task_system.json"
    monkeypatch.setattr(task_state_store_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store_module,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )
    monkeypatch.setattr(task_factory_module, "TASK_SYSTEM_FILE", task_system_path)
    monkeypatch.setattr(
        google_sheets_gateway_module,
        "MOCK_SHEETS_ROOT",
        tmp_path / "runtime" / "out" / "external_business" / "google_sheets_gateway",
    )
    task_factory_module.clear_task_runtime_store()
    return task_system_path


def test_lead_processing_flow_runs_end_to_end_for_email_branch(monkeypatch, tmp_path: Path) -> None:
    task_system_path = _configure_runtime(monkeypatch, tmp_path)
    captured_email: dict[str, object] = {}

    monkeypatch.setattr(
        lead_processing_flow_module,
        "send_email",
        lambda **payload: (
            captured_email.update(dict(payload))
            or {
                "ok": True,
                "provider": "gmail_gateway",
                "operation": "send_email",
                "status": "completed",
                "resource": {
                    "message_id": "msg-lead-001",
                    "thread_id": "thread-lead-001",
                    "to": payload["to"],
                    "subject": payload["subject"],
                },
                "error": None,
                "log": ["email sent"],
            }
        ),
    )

    result = run_lead_processing_flow(
        {
            "name": "Jamie Client",
            "phone": "555-0101",
            "request": "Please send an estimate ASAP for my ADU project.",
        },
        store_path=task_system_path,
    )

    task = result["task"]

    assert task["task_id"]
    assert task["type"] == "lead_processing"
    assert task["status"] == "COMPLETED"
    assert result["decision"]["next_step"] == "send_email"
    assert result["action_result"]["status"] == "completed"
    assert result["action_result"]["action_type"] == "SEND_EMAIL"
    assert set(result["action_result"]["decision_trace"]) == {
        "reason",
        "context_used",
        "action_type",
        "policy_result",
        "confidence",
        "vendor",
    }
    assert result["action_result"]["decision_trace"]["vendor"] == "google"
    assert captured_email["to"] == lead_processing_flow_module.DEFAULT_LEAD_NOTIFICATION_EMAIL
    assert "Jamie Client" in str(captured_email["subject"])
    assert task["result"]["decision"]["adapter"] == "gmail"
    assert task["result"]["action"]["result_payload"]["adapter"] == "gmail"
    assert any(event["event"] == "lead_decision_recorded" for event in task["history"])
    assert any(event["event"] == "lead_action_executed" for event in task["history"])
    assert task["history"][-1]["event"] == "task_completed"


def test_lead_processing_flow_runs_end_to_end_for_crm_branch(monkeypatch, tmp_path: Path) -> None:
    task_system_path = _configure_runtime(monkeypatch, tmp_path)

    result = run_lead_processing_flow(
        {
            "name": "Robin Prospect",
            "phone": "555-0102",
            "request": "Please log my details in the CRM for next week.",
        },
        store_path=task_system_path,
    )

    task = result["task"]
    sheet_rows_path = (
        tmp_path
        / "runtime"
        / "out"
        / "external_business"
        / "google_sheets_gateway"
        / "rows"
        / "digital-foreman-crm.jsonl"
    )
    rows = [
        json.loads(line)
        for line in sheet_rows_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    assert task["task_id"]
    assert task["type"] == "lead_processing"
    assert task["status"] == "COMPLETED"
    assert result["action_result"]["decision_trace"]["vendor"] == "google"
    assert result["decision"]["next_step"] == "create_crm_entry"
    assert result["action_result"]["status"] == "completed"
    assert result["action_result"]["action_type"] == "CREATE_CRM_ENTRY"
    assert rows[-1]["worksheet"] == "Incoming Leads"
    assert rows[-1]["row"]["name"] == "Robin Prospect"
    assert rows[-1]["row"]["phone"] == "555-0102"
    assert task["result"]["decision_trace"]["action_type"] == "CREATE_CRM_ENTRY"
    assert task["result"]["action"]["result_payload"]["adapter"] == "google.sheets"
    assert any(event["event"] == "lead_intake_recorded" for event in task["history"])
    assert any(event["event"] == "lead_action_executed" for event in task["history"])
    assert task["history"][-1]["event"] == "task_completed"
