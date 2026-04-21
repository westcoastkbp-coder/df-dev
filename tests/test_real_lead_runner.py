from __future__ import annotations

import json
from pathlib import Path

import app.execution.lead_estimate_decision as lead_estimate_decision_module
import app.execution.paths as paths_module
import app.execution.real_lead_runner as real_lead_runner_module
import app.orchestrator.task_factory as task_factory_module
import app.orchestrator.task_state_store as task_state_store_module
import app.policy.policy_gate as policy_gate_module
import runtime.system_log as system_log_module
from app.execution.real_lead_runner import run_real_lead, write_real_lead_run_report


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _configure_runner_runtime(monkeypatch, tmp_path: Path) -> Path:
    task_store_path = tmp_path / "data" / "tasks.json"
    monkeypatch.setattr(task_state_store_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store_module,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )
    monkeypatch.setattr(paths_module, "TASKS_FILE", task_store_path)
    monkeypatch.setattr(lead_estimate_decision_module, "TASKS_FILE", task_store_path)
    monkeypatch.setattr(system_log_module, "TASK_LOG_FILE", tmp_path / "runtime" / "logs" / "tasks.log")
    task_factory_module.clear_task_runtime_store()
    return task_store_path


def test_real_lead_runner_qualified_lead(monkeypatch, tmp_path: Path) -> None:
    task_store_path = _configure_runner_runtime(monkeypatch, tmp_path)
    policy_log_file = tmp_path / "runtime" / "logs" / "policy.log"
    system_log_file = tmp_path / "runtime" / "logs" / "system.log"
    monkeypatch.setattr(policy_gate_module, "POLICY_LOG_FILE", policy_log_file)
    monkeypatch.setattr(system_log_module, "SYSTEM_LOG_FILE", system_log_file)

    report = run_real_lead(
        {
            "lead_id": "lead-real-qualified-001",
            "contact_info": {"phone": "555-0100"},
            "project_type": "ADU",
            "scope_summary": "Detached ADU with pricing request",
        },
        store_path=task_store_path,
    )

    assert report["workflow_type"] == "lead_estimate_decision"
    assert report["next_action"] == "create_estimate_task"
    assert report["created_child_task_ids"]
    assert report["archived_state_flag"] is False
    assert report["persisted_state_ok"] is True
    assert report["trace_link_ok"] is True
    assert report["pass_fail"] == "pass"
    assert report["failure_class"] == "none"
    policy_entries = _read_jsonl(policy_log_file)
    system_entries = _read_jsonl(system_log_file)
    assert any("input contract valid" in entry["details"]["reason"] for entry in policy_entries)
    assert any("real lead input allowed" in entry["details"].get("message", "") for entry in system_entries)


def test_real_lead_runner_missing_contact_info_blocked(monkeypatch, tmp_path: Path) -> None:
    task_store_path = _configure_runner_runtime(monkeypatch, tmp_path)
    policy_log_file = tmp_path / "runtime" / "logs" / "policy.log"
    system_log_file = tmp_path / "runtime" / "logs" / "system.log"
    monkeypatch.setattr(policy_gate_module, "POLICY_LOG_FILE", policy_log_file)
    monkeypatch.setattr(system_log_module, "SYSTEM_LOG_FILE", system_log_file)

    report = run_real_lead(
        {
            "lead_id": "lead-real-missing-contact-001",
            "contact_info": {},
            "project_type": "ADU",
            "scope_summary": "Detached ADU with pricing request",
        },
        store_path=task_store_path,
    )

    assert report["workflow_type"] == "lead_estimate_decision"
    assert report["next_action"] == ""
    assert len(report["created_child_task_ids"]) == 1
    assert report["parent_task_id"] == ""
    assert report["pass_fail"] == "fail"
    assert report["failure_class"] == "missing_required_input"
    assert "contact_info" in report["notes"]
    tasks = task_factory_module.load_tasks(task_store_path)
    assert len(tasks) == 1
    followup_task = task_factory_module.get_task(report["created_child_task_ids"][0], store_path=task_store_path)
    assert followup_task is not None
    assert followup_task["intent"] == "missing_input_followup"
    assert followup_task["status"] == "VALIDATED"
    assert followup_task["payload"] == {
        "workflow_type": "missing_input_followup",
        "parent_lead_id": "lead-real-missing-contact-001",
        "missing_fields": ["contact_info"],
        "required_action": "request_input_completion",
        "status": "pending",
    }
    policy_entries = _read_jsonl(policy_log_file)
    system_entries = _read_jsonl(system_log_file)
    task_entries = _read_jsonl(tmp_path / "runtime" / "logs" / "tasks.log")
    assert any("missing required fields: contact_info" in entry["details"]["reason"] for entry in policy_entries)
    assert any("real lead input blocked" in entry["details"].get("message", "") for entry in system_entries)
    assert any(entry["event_type"] == "task_execution" and entry["details"].get("result_type") == "missing_input_followup" for entry in task_entries)


def test_real_lead_runner_missing_scope_summary_blocked(monkeypatch, tmp_path: Path) -> None:
    task_store_path = _configure_runner_runtime(monkeypatch, tmp_path)

    report = run_real_lead(
        {
            "lead_id": "lead-real-missing-scope-001",
            "contact_info": {"phone": "555-0101"},
            "project_type": "ADU",
            "scope_summary": "",
        },
        store_path=task_store_path,
    )

    assert report["pass_fail"] == "fail"
    assert report["failure_class"] == "missing_required_input"
    assert "scope_summary" in report["notes"]
    followup_task = task_factory_module.get_task(report["created_child_task_ids"][0], store_path=task_store_path)
    assert followup_task is not None
    assert followup_task["payload"]["missing_fields"] == ["scope_summary"]


def test_real_lead_runner_invalid_project_type_blocked(monkeypatch, tmp_path: Path) -> None:
    task_store_path = _configure_runner_runtime(monkeypatch, tmp_path)

    report = run_real_lead(
        {
            "lead_id": "lead-real-invalid-project-001",
            "contact_info": {"email": "client@example.com"},
            "project_type": "roof",
            "scope_summary": "Client wants project review",
        },
        store_path=task_store_path,
    )

    assert report["pass_fail"] == "fail"
    assert report["failure_class"] == "missing_required_input"
    assert report["notes"] == "invalid project_type"
    assert report["created_child_task_ids"] == []
    assert task_factory_module.load_tasks(task_store_path) == []


def test_real_lead_runner_multiple_missing_fields_followup(monkeypatch, tmp_path: Path) -> None:
    task_store_path = _configure_runner_runtime(monkeypatch, tmp_path)

    report = run_real_lead(
        {
            "lead_id": "lead-real-missing-multi-001",
            "contact_info": {},
            "project_type": "ADU",
            "scope_summary": "",
        },
        store_path=task_store_path,
    )

    followup_task = task_factory_module.get_task(report["created_child_task_ids"][0], store_path=task_store_path)
    assert followup_task is not None
    assert followup_task["payload"]["missing_fields"] == ["contact_info", "scope_summary"]


def test_real_lead_runner_normalization_is_consistent(monkeypatch, tmp_path: Path) -> None:
    _configure_runner_runtime(monkeypatch, tmp_path)

    normalized = real_lead_runner_module._normalize_input(
        {
            "lead_id": "  lead-real-normalized-001  ",
            "contact_info": {"phone": " 555-0102 ", "email": " "},
            "project_type": " ADU ",
            "scope_summary": " Detached ADU ",
            "urgency_level": " ",
            "location": "  Los Angeles ",
            "notes": "",
        }
    )

    assert normalized == {
        "lead_id": "lead-real-normalized-001",
        "contact_info": {"phone": "555-0102", "email": None},
        "project_type": "adu",
        "scope_summary": "Detached ADU",
        "urgency_level": None,
        "location": "Los Angeles",
        "notes": None,
        "qualification_flags": {},
        "lead_invalid": False,
        "unsupported_request": False,
        "lead_exists": True,
    }


def test_real_lead_runner_report_output_shape_is_deterministic(monkeypatch, tmp_path: Path) -> None:
    task_store_path = _configure_runner_runtime(monkeypatch, tmp_path)
    output_path = tmp_path / "runtime" / "out" / "reports" / "run_report.json"
    lead_input = {
        "lead_id": "lead-real-repeat-001",
        "contact_info": {"phone": "555-0102"},
        "project_type": "ADU",
        "scope_summary": "Garage conversion with pricing request",
    }

    first_path = write_real_lead_run_report(
        lead_input,
        store_path=task_store_path,
        output_path=output_path,
    )
    first_report = json.loads(first_path.read_text(encoding="utf-8"))
    second_path = write_real_lead_run_report(
        lead_input,
        store_path=task_store_path,
        output_path=output_path,
    )
    second_report = json.loads(second_path.read_text(encoding="utf-8"))

    assert first_path == output_path
    assert second_path == output_path
    assert sorted(first_report.keys()) == sorted(second_report.keys())
    assert first_report["run_id"] == second_report["run_id"]
    assert first_report["workflow_type"] == second_report["workflow_type"]
    assert sorted(first_report["decision"].keys()) == sorted(second_report["decision"].keys())
    assert len(first_report["created_child_task_ids"]) == len(second_report["created_child_task_ids"])


def test_real_lead_runner_repeated_same_lead_run_does_not_duplicate_child_task(
    monkeypatch,
    tmp_path: Path,
) -> None:
    task_store_path = _configure_runner_runtime(monkeypatch, tmp_path)
    system_log_file = tmp_path / "runtime" / "logs" / "system.log"
    monkeypatch.setattr(system_log_module, "SYSTEM_LOG_FILE", system_log_file)
    lead_input = {
        "lead_id": "lead-real-idempotent-001",
        "contact_info": {"phone": "555-0113"},
        "project_type": "ADU",
        "scope_summary": "Detached ADU with pricing request",
    }

    first_report = run_real_lead(lead_input, store_path=task_store_path)
    second_report = run_real_lead(lead_input, store_path=task_store_path)

    assert first_report["created_child_task_ids"] == second_report["created_child_task_ids"]
    tasks = task_factory_module.load_tasks(task_store_path)
    assert len(tasks) == 2


def test_repeated_missing_input_run_reuses_existing_followup_task(monkeypatch, tmp_path: Path) -> None:
    task_store_path = _configure_runner_runtime(monkeypatch, tmp_path)
    system_log_file = tmp_path / "runtime" / "logs" / "system.log"
    monkeypatch.setattr(system_log_module, "SYSTEM_LOG_FILE", system_log_file)
    lead_input = {
        "lead_id": "lead-real-followup-idempotent-001",
        "contact_info": {},
        "project_type": "ADU",
        "scope_summary": "",
    }

    first_report = run_real_lead(lead_input, store_path=task_store_path)
    second_report = run_real_lead(lead_input, store_path=task_store_path)

    assert first_report["created_child_task_ids"] == second_report["created_child_task_ids"]
    tasks = task_factory_module.load_tasks(task_store_path)
    assert len(tasks) == 1
    followup_task = task_factory_module.get_task(first_report["created_child_task_ids"][0], store_path=task_store_path)
    assert followup_task is not None
    assert followup_task["intent"] == "missing_input_followup"
    system_entries = _read_jsonl(system_log_file)
    assert any(
        entry["details"].get("message", "").find("idempotent_skip") >= 0
        and first_report["created_child_task_ids"][0] in entry["details"].get("message", "")
        for entry in system_entries
    )


def test_missing_input_followup_completion_triggers_reentry(monkeypatch, tmp_path: Path) -> None:
    task_store_path = _configure_runner_runtime(monkeypatch, tmp_path)
    policy_log_file = tmp_path / "runtime" / "logs" / "policy.log"
    system_log_file = tmp_path / "runtime" / "logs" / "system.log"
    monkeypatch.setattr(policy_gate_module, "POLICY_LOG_FILE", policy_log_file)
    monkeypatch.setattr(system_log_module, "SYSTEM_LOG_FILE", system_log_file)

    followup_task = task_factory_module.create_task(
        {
            "status": "created",
            "intent": "missing_input_followup",
            "payload": {
                "workflow_type": "missing_input_followup",
                "parent_lead_id": "lead-reentry-001",
                "missing_fields": ["contact_info"],
                "required_action": "request_input_completion",
                "updated_lead_input": {
                    "lead_id": "lead-reentry-001",
                    "contact_info": {"phone": "555-0109"},
                    "project_type": "ADU",
                    "scope_summary": "Detached ADU with pricing request",
                },
            },
        },
        store_path=task_store_path,
    )
    followup_task["status"] = "running"
    task_factory_module.save_task(followup_task, store_path=task_store_path)

    completed_followup = task_factory_module.close_task(
        str(followup_task.get("task_id", "")),
        store_path=task_store_path,
    )

    assert completed_followup["status"] == "COMPLETED"
    tasks = task_factory_module.load_tasks(task_store_path)
    reentry_tasks = [
        task
        for task in tasks
        if str(task.get("intent", "")).strip() == "lead_estimate_decision"
        and str(task.get("followup_task_id", "")).strip()
        == str(followup_task.get("task_id", "")).strip()
    ]
    assert len(reentry_tasks) == 1

    reentry_task = reentry_tasks[0]
    assert reentry_task["task_id"] != followup_task["task_id"]
    assert reentry_task["status"] == "COMPLETED"
    assert reentry_task["source_lead_id"] == "lead-reentry-001"
    assert reentry_task["followup_task_id"] == followup_task["task_id"]
    assert reentry_task["result"]["decision"]["next_step"] == "create_estimate_task"
    assert reentry_task["result"]["binding"]["binding_action"] == "create_estimate_task"

    child_task = task_factory_module.get_task(
        str(reentry_task["result"]["binding"]["child_task_id"]),
        store_path=task_store_path,
    )
    assert child_task is not None
    assert child_task["intent"] == "estimate_task"
    assert child_task["payload"]["parent_task_id"] == reentry_task["task_id"]

    task_entries = _read_jsonl(tmp_path / "runtime" / "logs" / "tasks.log")
    system_entries = _read_jsonl(system_log_file)
    assert any(
        entry["task_id"] == followup_task["task_id"]
        and entry["event_type"] == "workflow_reentry"
        and entry["status"] == "created"
        for entry in task_entries
    )
    assert any("follow-up re-entry created" in entry["details"].get("message", "") for entry in system_entries)


def test_missing_input_followup_completion_without_required_data_skips_reentry(
    monkeypatch,
    tmp_path: Path,
) -> None:
    task_store_path = _configure_runner_runtime(monkeypatch, tmp_path)
    system_log_file = tmp_path / "runtime" / "logs" / "system.log"
    monkeypatch.setattr(system_log_module, "SYSTEM_LOG_FILE", system_log_file)

    followup_task = task_factory_module.create_task(
        {
            "status": "created",
            "intent": "missing_input_followup",
            "payload": {
                "workflow_type": "missing_input_followup",
                "parent_lead_id": "lead-reentry-missing-001",
                "missing_fields": ["scope_summary"],
                "required_action": "request_input_completion",
                "updated_lead_input": {
                    "lead_id": "lead-reentry-missing-001",
                    "contact_info": {"phone": "555-0110"},
                    "project_type": "ADU",
                },
            },
        },
        store_path=task_store_path,
    )
    followup_task["status"] = "running"
    task_factory_module.save_task(followup_task, store_path=task_store_path)

    task_factory_module.close_task(
        str(followup_task.get("task_id", "")),
        store_path=task_store_path,
    )

    tasks = task_factory_module.load_tasks(task_store_path)
    reentry_tasks = [
        task for task in tasks if str(task.get("intent", "")).strip() == "lead_estimate_decision"
    ]
    assert reentry_tasks == []

    task_entries = _read_jsonl(tmp_path / "runtime" / "logs" / "tasks.log")
    assert any(
        entry["task_id"] == followup_task["task_id"]
        and entry["event_type"] == "workflow_reentry"
        and entry["status"] == "missing_required_data"
        for entry in task_entries
    )


def test_missing_input_reentry_keeps_decision_and_action_contract(monkeypatch, tmp_path: Path) -> None:
    task_store_path = _configure_runner_runtime(monkeypatch, tmp_path)

    followup_task = task_factory_module.create_task(
        {
            "status": "created",
            "intent": "missing_input_followup",
            "payload": {
                "workflow_type": "missing_input_followup",
                "parent_lead_id": "lead-reentry-contract-001",
                "missing_fields": ["scope_summary"],
                "required_action": "request_input_completion",
                "updated_lead_input": {
                    "lead_id": "lead-reentry-contract-001",
                    "contact_info": {"phone": "555-0111"},
                    "project_type": "ADU",
                    "scope_summary": "Garage conversion with pricing request",
                },
            },
        },
        store_path=task_store_path,
    )
    followup_task["status"] = "running"
    task_factory_module.save_task(followup_task, store_path=task_store_path)
    task_factory_module.close_task(
        str(followup_task.get("task_id", "")),
        store_path=task_store_path,
    )

    reentry_task = next(
        task
        for task in task_factory_module.load_tasks(task_store_path)
        if str(task.get("intent", "")).strip() == "lead_estimate_decision"
        and str(task.get("followup_task_id", "")).strip()
        == str(followup_task.get("task_id", "")).strip()
    )

    assert reentry_task["result"]["decision"] == {
        "decision": "create_estimate",
        "confidence": "high",
        "next_step": "create_estimate_task",
    }
    assert reentry_task["result"]["binding"]["binding_action"] == "create_estimate_task"
    assert reentry_task["result"]["binding"]["child_task_created"] is True


def test_missing_input_reentry_is_created_only_once(monkeypatch, tmp_path: Path) -> None:
    task_store_path = _configure_runner_runtime(monkeypatch, tmp_path)
    system_log_file = tmp_path / "runtime" / "logs" / "system.log"
    monkeypatch.setattr(system_log_module, "SYSTEM_LOG_FILE", system_log_file)

    followup_task = task_factory_module.create_task(
        {
            "status": "created",
            "intent": "missing_input_followup",
            "payload": {
                "workflow_type": "missing_input_followup",
                "parent_lead_id": "lead-reentry-once-001",
                "missing_fields": ["contact_info"],
                "required_action": "request_input_completion",
                "updated_lead_input": {
                    "lead_id": "lead-reentry-once-001",
                    "contact_info": {"phone": "555-0112"},
                    "project_type": "ADU",
                    "scope_summary": "New detached ADU",
                },
            },
        },
        store_path=task_store_path,
    )
    followup_task["status"] = "running"
    persisted_followup = task_factory_module.save_task(followup_task, store_path=task_store_path)

    completed_followup = task_factory_module.close_task(
        str(persisted_followup.get("task_id", "")),
        store_path=task_store_path,
    )
    second_result = real_lead_runner_module.reenter_completed_followup(
        completed_followup,
        store_path=task_store_path,
    )

    reentry_tasks = [
        task
        for task in task_factory_module.load_tasks(task_store_path)
        if str(task.get("intent", "")).strip() == "lead_estimate_decision"
        and str(task.get("followup_task_id", "")).strip()
        == str(followup_task.get("task_id", "")).strip()
    ]
    assert len(reentry_tasks) == 1
    assert second_result["status"] == "already_exists"

    task_entries = _read_jsonl(tmp_path / "runtime" / "logs" / "tasks.log")
    assert any(
        entry["task_id"] == followup_task["task_id"]
        and entry["event_type"] == "workflow_reentry"
        and entry["status"] == "already_exists"
        for entry in task_entries
    )


def test_missing_input_reentry_with_different_payload_creates_new_task(
    monkeypatch,
    tmp_path: Path,
) -> None:
    task_store_path = _configure_runner_runtime(monkeypatch, tmp_path)

    followup_task = task_factory_module.create_task(
        {
            "status": "created",
            "intent": "missing_input_followup",
            "payload": {
                "workflow_type": "missing_input_followup",
                "parent_lead_id": "lead-reentry-different-001",
                "missing_fields": ["scope_summary"],
                "required_action": "request_input_completion",
                "updated_lead_input": {
                    "lead_id": "lead-reentry-different-001",
                    "contact_info": {"phone": "555-0114"},
                    "project_type": "ADU",
                    "scope_summary": "Detached ADU",
                },
            },
        },
        store_path=task_store_path,
    )
    followup_task["status"] = "completed"
    persisted_followup = task_factory_module.save_task(followup_task, store_path=task_store_path)

    first_result = real_lead_runner_module.reenter_completed_followup(
        persisted_followup,
        store_path=task_store_path,
    )
    updated_followup = dict(persisted_followup)
    updated_payload = dict(updated_followup.get("payload", {}) or {})
    updated_payload["updated_lead_input"] = {
        "lead_id": "lead-reentry-different-001",
        "contact_info": {"phone": "555-0114"},
        "project_type": "ADU",
        "scope_summary": "Detached ADU with garage conversion",
    }
    updated_followup["payload"] = updated_payload

    second_result = real_lead_runner_module.reenter_completed_followup(
        updated_followup,
        store_path=task_store_path,
    )

    assert first_result["status"] == "created"
    assert second_result["status"] == "created"
    assert first_result["reentry_task_id"] != second_result["reentry_task_id"]
    reentry_tasks = [
        task
        for task in task_factory_module.load_tasks(task_store_path)
        if str(task.get("intent", "")).strip() == "lead_estimate_decision"
        and str(task.get("followup_task_id", "")).strip()
        == str(followup_task.get("task_id", "")).strip()
    ]
    assert len(reentry_tasks) == 2
