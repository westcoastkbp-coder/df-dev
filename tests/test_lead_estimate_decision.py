from __future__ import annotations

import json
from pathlib import Path

import app.policy.policy_gate as policy_gate_module
import app.execution.lead_estimate_decision as lead_estimate_decision_module
import app.execution.paths as paths_module
import app.orchestrator.task_factory as task_factory_module
from app.execution.action_result import build_action_result
from app.orchestrator import task_memory as task_memory_module
from app.orchestrator import task_state_store as task_state_store_module
import runtime.system_log as system_log_module
from app.execution.lead_estimate_decision import (
    WORKFLOW_TYPE,
    resolve_estimate_decision,
    validate_decision_contract,
    validate_input_payload,
)
from app.orchestrator.task_queue import InMemoryTaskQueue
from functools import partial
from app.orchestrator.task_worker import (
    process_next_queued_task as _process_next_queued_task,
)
from tests.system_context import WORKING_SYSTEM_CONTEXT

process_next_queued_task = partial(
    _process_next_queued_task, system_context=WORKING_SYSTEM_CONTEXT
)


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _configure_task_runtime(monkeypatch, tmp_path: Path) -> Path:
    task_store_path = tmp_path / "data" / "tasks.json"
    monkeypatch.setattr(task_memory_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_memory_module, "TASK_MEMORY_FILE", Path("runtime/state/task_memory.json")
    )
    monkeypatch.setattr(task_state_store_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store_module,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )
    monkeypatch.setattr(paths_module, "TASKS_FILE", task_store_path)
    monkeypatch.setattr(lead_estimate_decision_module, "TASKS_FILE", task_store_path)
    task_factory_module.clear_task_runtime_store()
    return task_store_path


def _binding_shape(binding: dict[str, object]) -> dict[str, object]:
    return {
        "binding_action": binding.get("binding_action"),
        "binding_status": binding.get("binding_status"),
        "child_task_created": binding.get("child_task_created"),
        "child_task_intent": binding.get("child_task_intent", ""),
        "archive_status": binding.get("archive_status", ""),
        "parent_task_id": binding.get("parent_task_id"),
        "source_lead_id": binding.get("source_lead_id"),
        "action_source": binding.get("action_source"),
        "reason_code": binding.get("reason_code"),
    }


def _build_task(
    *,
    store_path: Path,
    task_id: str,
    payload: dict[str, object],
    status: str = "pending",
) -> dict[str, object]:
    task = task_factory_module.create_task(
        {
            "task_id": task_id,
            "status": "created",
            "intent": WORKFLOW_TYPE,
            "payload": payload,
        },
        store_path=store_path,
    )
    task["intent"] = WORKFLOW_TYPE
    task["payload"] = dict(payload)
    task["status"] = status
    return task_factory_module.save_task(task, store_path=store_path)


def test_resolve_estimate_decision_with_enough_info() -> None:
    decision = resolve_estimate_decision(
        task_id="DF-LEAD-ESTIMATE-DECISION-V1",
        payload={
            "workflow_type": WORKFLOW_TYPE,
            "lead_id": "lead-001",
            "lead_data": {
                "project_type": "ADU",
                "scope_summary": "Detached ADU with pricing request",
                "contact_info": {"phone": "555-0100"},
                "lead_exists": True,
            },
        },
    )

    assert decision == {
        "decision": "create_estimate",
        "confidence": "high",
        "next_step": "create_estimate_task",
    }


def test_validate_input_payload_blocks_missing_lead_id() -> None:
    valid, reason, _ = validate_input_payload(
        {
            "workflow_type": WORKFLOW_TYPE,
            "lead_id": "",
            "lead_data": {
                "lead_exists": True,
            },
        }
    )

    assert valid is False
    assert reason == "lead_id must not be empty"


def test_validate_decision_contract_blocks_invalid_enum() -> None:
    valid, reason, _ = validate_decision_contract(
        {
            "decision": "bad_enum",
            "confidence": "high",
            "next_step": "create_estimate_task",
        }
    )

    assert valid is False
    assert reason == "invalid decision"


def test_resolve_estimate_decision_with_incomplete_scope() -> None:
    decision = resolve_estimate_decision(
        task_id="DF-LEAD-ESTIMATE-DECISION-V1",
        payload={
            "workflow_type": WORKFLOW_TYPE,
            "lead_id": "lead-002",
            "lead_data": {
                "project_type": "ADU",
                "scope_summary": "",
                "contact_info": {"phone": "555-0100"},
                "lead_exists": True,
            },
        },
    )

    assert decision == {
        "decision": "request_followup",
        "confidence": "high",
        "next_step": "request_missing_scope",
    }


def test_resolve_estimate_decision_with_unsupported_lead() -> None:
    decision = resolve_estimate_decision(
        task_id="DF-LEAD-ESTIMATE-DECISION-V1",
        payload={
            "workflow_type": WORKFLOW_TYPE,
            "lead_id": "lead-003",
            "lead_data": {
                "lead_exists": True,
                "unsupported_request": True,
                "contact_info": {"phone": "555-0100"},
            },
        },
    )

    assert decision == {
        "decision": "reject_lead",
        "confidence": "high",
        "next_step": "archive_lead",
    }


def test_resolve_estimate_decision_is_deterministic() -> None:
    payload = {
        "workflow_type": WORKFLOW_TYPE,
        "lead_id": "lead-004",
        "lead_data": {
            "project_type": "ADU",
            "scope_summary": "Garage conversion",
            "contact_info": {"phone": "555-0100"},
            "lead_exists": True,
        },
    }

    first = resolve_estimate_decision(
        task_id="DF-LEAD-ESTIMATE-DECISION-V1",
        payload=payload,
    )
    second = resolve_estimate_decision(
        task_id="DF-LEAD-ESTIMATE-DECISION-V1",
        payload=payload,
    )

    assert first == second


def test_lead_estimate_execution_logs_decision_and_policy(
    monkeypatch, tmp_path: Path
) -> None:
    logs_dir = tmp_path / "runtime" / "logs"
    queue_file = tmp_path / "runtime" / "state" / "task_queue.json"
    task_log_file = logs_dir / "tasks.log"
    system_log_file = logs_dir / "system.log"
    policy_log_file = logs_dir / "policy.log"

    import app.orchestrator.task_queue as task_queue_module

    monkeypatch.setattr(task_queue_module, "TASK_QUEUE_FILE", queue_file)
    monkeypatch.setattr(task_queue_module, "TASK_LOG_FILE", task_log_file)
    monkeypatch.setattr(system_log_module, "TASK_LOG_FILE", task_log_file)
    monkeypatch.setattr(system_log_module, "SYSTEM_LOG_FILE", system_log_file)
    monkeypatch.setattr(policy_gate_module, "POLICY_LOG_FILE", policy_log_file)
    task_store_path = _configure_task_runtime(monkeypatch, tmp_path)

    queue = InMemoryTaskQueue()
    task_data = _build_task(
        store_path=task_store_path,
        task_id="DF-LEAD-ESTIMATE-DECISION-V1",
        payload={
            "workflow_type": WORKFLOW_TYPE,
            "lead_id": "lead-001",
            "lead_data": {
                "project_type": "ADU",
                "scope_summary": "Detached ADU with pricing request",
                "contact_info": {"phone": "555-0100"},
                "lead_exists": True,
            },
        },
    )

    def fetch_task(task_id: str) -> dict[str, object] | None:
        return task_data if task_id == "DF-LEAD-ESTIMATE-DECISION-V1" else None

    def persist(updated_task: dict[str, object]) -> None:
        task_data.update(updated_task)

    assert queue.enqueue_task("DF-LEAD-ESTIMATE-DECISION-V1") is True

    executed_task = process_next_queued_task(
        queue=queue,
        now=lambda: "2026-04-04T00:00:00Z",
        fetch_task=fetch_task,
        persist=persist,
        timeout=0.0,
        telemetry_collector=lambda: {},
        network_snapshot_collector=lambda: {},
    )

    assert executed_task is not None
    assert executed_task["status"] == "COMPLETED"
    decision = dict(executed_task["result"]["decision"])
    binding = dict(executed_task["result"]["binding"])
    assert decision == {
        "decision": "create_estimate",
        "confidence": "high",
        "next_step": "create_estimate_task",
    }
    assert binding["binding_action"] == "create_estimate_task"
    assert binding["child_task_created"] is True
    child_task = task_factory_module.get_task(
        str(binding["child_task_id"]), store_path=task_store_path
    )
    assert child_task is not None
    assert child_task["intent"] == "estimate_task"
    assert child_task["payload"]["parent_task_id"] == "DF-LEAD-ESTIMATE-DECISION-V1"
    assert child_task["payload"]["source_lead_id"] == "lead-001"

    task_log = _read_jsonl(task_log_file)
    assert any(entry["task_id"] == "DF-LEAD-ESTIMATE-DECISION-V1" for entry in task_log)
    assert any(entry["event_type"] == "decision_action_binding" for entry in task_log)

    policy_log = _read_jsonl(policy_log_file)
    assert any(
        entry["task_id"] == "DF-LEAD-ESTIMATE-DECISION-V1"
        and entry["status"] == "allowed"
        and entry["details"].get("reason") == "workflow payload valid"
        for entry in policy_log
    )
    assert any(
        entry["task_id"] == "DF-LEAD-ESTIMATE-DECISION-V1"
        and entry["status"] == "allowed"
        and entry["details"].get("reason") == "decision payload valid"
        for entry in policy_log
    )

    system_log = _read_jsonl(system_log_file)
    assert any(
        entry["event_type"] == "workflow"
        and entry["details"].get("message", "").find("decision=create_estimate") >= 0
        and entry["details"].get("message", "").find("next_step=create_estimate_task")
        >= 0
        for entry in system_log
    )
    assert any(
        entry["event_type"] == "binding"
        and "binding_action=create_estimate_task" in entry["details"].get("message", "")
        for entry in system_log
    )


def test_lead_estimate_binding_shape_is_stable(monkeypatch, tmp_path: Path) -> None:
    task_store_path = _configure_task_runtime(monkeypatch, tmp_path)
    parent_task = _build_task(
        store_path=task_store_path,
        task_id="DF-LEAD-ESTIMATE-IDEMPOTENT-V1",
        payload={
            "workflow_type": WORKFLOW_TYPE,
            "lead_id": "lead-idempotent-execution-001",
            "lead_data": {
                "project_type": "ADU",
                "scope_summary": "Detached ADU with pricing request",
                "contact_info": {"phone": "555-0115"},
                "lead_exists": True,
            },
        },
    )
    decision = resolve_estimate_decision(
        task_id=parent_task["task_id"],
        payload=parent_task["payload"],
    )

    binding = lead_estimate_decision_module.bind_decision_action(
        task_data=parent_task,
        decision=decision,
        store_path=task_store_path,
    )

    assert _binding_shape(binding) == {
        "binding_action": "create_estimate_task",
        "binding_status": "child_task_created",
        "child_task_created": True,
        "child_task_intent": "estimate_task",
        "archive_status": "",
        "parent_task_id": "DF-LEAD-ESTIMATE-IDEMPOTENT-V1",
        "source_lead_id": "lead-idempotent-execution-001",
        "action_source": WORKFLOW_TYPE,
        "reason_code": "project_defined",
    }


def test_lead_estimate_execution_invalid_payload_blocked(
    monkeypatch, tmp_path: Path
) -> None:
    logs_dir = tmp_path / "runtime" / "logs"
    queue_file = tmp_path / "runtime" / "state" / "task_queue.json"
    task_log_file = logs_dir / "tasks.log"
    system_log_file = logs_dir / "system.log"
    policy_log_file = logs_dir / "policy.log"

    import app.orchestrator.task_queue as task_queue_module

    monkeypatch.setattr(task_queue_module, "TASK_QUEUE_FILE", queue_file)
    monkeypatch.setattr(task_queue_module, "TASK_LOG_FILE", task_log_file)
    monkeypatch.setattr(system_log_module, "TASK_LOG_FILE", task_log_file)
    monkeypatch.setattr(system_log_module, "SYSTEM_LOG_FILE", system_log_file)
    monkeypatch.setattr(policy_gate_module, "POLICY_LOG_FILE", policy_log_file)
    _configure_task_runtime(monkeypatch, tmp_path)

    queue = InMemoryTaskQueue()
    task_data = _build_task(
        store_path=tmp_path / "data" / "tasks.json",
        task_id="DF-LEAD-ESTIMATE-DECISION-BLOCKED-V1",
        payload={
            "workflow_type": WORKFLOW_TYPE,
            "lead_id": "",
            "lead_data": {
                "lead_exists": True,
            },
        },
    )

    def fetch_task(task_id: str) -> dict[str, object] | None:
        return task_data if task_id == "DF-LEAD-ESTIMATE-DECISION-BLOCKED-V1" else None

    def persist(updated_task: dict[str, object]) -> None:
        task_data.update(updated_task)

    assert queue.enqueue_task("DF-LEAD-ESTIMATE-DECISION-BLOCKED-V1") is True

    executed_task = process_next_queued_task(
        queue=queue,
        now=lambda: "2026-04-04T00:00:00Z",
        fetch_task=fetch_task,
        persist=persist,
        timeout=0.0,
        telemetry_collector=lambda: {},
        network_snapshot_collector=lambda: {},
    )

    assert executed_task is not None
    assert executed_task["status"] == "FAILED"
    assert "lead_id must not be empty" in str(executed_task.get("error", ""))

    policy_log = _read_jsonl(policy_log_file)
    assert any(
        entry["task_id"] == "DF-LEAD-ESTIMATE-DECISION-BLOCKED-V1"
        and entry["status"] == "blocked"
        and entry["details"].get("reason") == "lead_id must not be empty"
        for entry in policy_log
    )


def test_decision_request_missing_scope_creates_child_task(
    monkeypatch, tmp_path: Path
) -> None:
    task_store_path = _configure_task_runtime(monkeypatch, tmp_path)
    decision = resolve_estimate_decision(
        task_id="DF-LEAD-ESTIMATE-DECISION-V1",
        payload={
            "workflow_type": WORKFLOW_TYPE,
            "lead_id": "lead-010",
            "lead_data": {
                "project_type": "ADU",
                "scope_summary": "",
                "contact_info": {"phone": "555-0100"},
                "lead_exists": True,
            },
        },
    )

    parent_task = _build_task(
        store_path=task_store_path,
        task_id="DF-LEAD-ESTIMATE-DECISION-V1",
        payload={
            "workflow_type": WORKFLOW_TYPE,
            "lead_id": "lead-010",
            "lead_data": {
                "project_type": "ADU",
                "scope_summary": "",
                "contact_info": {"phone": "555-0100"},
                "lead_exists": True,
            },
        },
    )

    binding = lead_estimate_decision_module.bind_decision_action(
        task_data=parent_task,
        decision=decision,
        store_path=task_store_path,
    )

    assert binding["binding_action"] == "request_missing_scope"
    assert binding["child_task_created"] is True
    child_task = task_factory_module.get_task(
        str(binding["child_task_id"]), store_path=task_store_path
    )
    assert child_task is not None
    assert child_task["intent"] == "missing_scope_followup"
    assert child_task["payload"]["reason_code"] == "insufficient_scope"


def test_decision_archive_lead_updates_result_without_child(
    monkeypatch, tmp_path: Path
) -> None:
    task_store_path = _configure_task_runtime(monkeypatch, tmp_path)
    decision = resolve_estimate_decision(
        task_id="DF-LEAD-ESTIMATE-DECISION-V1",
        payload={
            "workflow_type": WORKFLOW_TYPE,
            "lead_id": "lead-011",
            "lead_data": {
                "lead_exists": True,
                "unsupported_request": True,
                "contact_info": {"phone": "555-0100"},
            },
        },
    )

    parent_task = _build_task(
        store_path=task_store_path,
        task_id="DF-LEAD-ESTIMATE-DECISION-V1",
        payload={
            "workflow_type": WORKFLOW_TYPE,
            "lead_id": "lead-011",
            "lead_data": {
                "lead_exists": True,
                "unsupported_request": True,
                "contact_info": {"phone": "555-0100"},
            },
        },
    )

    binding = lead_estimate_decision_module.bind_decision_action(
        task_data=parent_task,
        decision=decision,
        store_path=task_store_path,
    )

    assert binding == {
        "binding_action": "archive_lead",
        "binding_status": "archived",
        "child_task_created": False,
        "child_task_id": "",
        "archive_status": "archived",
        "parent_task_id": "DF-LEAD-ESTIMATE-DECISION-V1",
        "source_lead_id": "lead-011",
        "action_source": WORKFLOW_TYPE,
        "reason_code": "non_qualified_lead",
    }


def test_decision_manual_review_creates_child_task(monkeypatch, tmp_path: Path) -> None:
    task_store_path = _configure_task_runtime(monkeypatch, tmp_path)
    decision = resolve_estimate_decision(
        task_id="DF-LEAD-ESTIMATE-DECISION-V1",
        payload={
            "workflow_type": WORKFLOW_TYPE,
            "lead_id": "lead-012",
            "lead_data": {
                "lead_exists": False,
                "contact_info": {},
            },
        },
    )

    parent_task = _build_task(
        store_path=task_store_path,
        task_id="DF-LEAD-ESTIMATE-DECISION-V1",
        payload={
            "workflow_type": WORKFLOW_TYPE,
            "lead_id": "lead-012",
            "lead_data": {
                "lead_exists": False,
                "contact_info": {},
            },
        },
    )

    binding = lead_estimate_decision_module.bind_decision_action(
        task_data=parent_task,
        decision=decision,
        store_path=task_store_path,
    )

    assert binding["binding_action"] == "manual_review"
    assert binding["child_task_created"] is True
    child_task = task_factory_module.get_task(
        str(binding["child_task_id"]), store_path=task_store_path
    )
    assert child_task is not None
    assert child_task["intent"] == "manual_review_task"
    assert child_task["payload"]["action_source"] == WORKFLOW_TYPE


def test_invalid_decision_payload_blocked_before_binding(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_task_runtime(monkeypatch, tmp_path)
    try:
        lead_estimate_decision_module.bind_decision_action(
            task_data={"task_id": "DF-LEAD-ESTIMATE-DECISION-V1"},
            decision={
                "decision": "create_estimate",
                "confidence": "high",
                "next_step": "bad_action",
            },
        )
    except ValueError as exc:
        assert str(exc) == "invalid next_step"
    else:
        raise AssertionError("invalid decision payload should be blocked")


def test_decision_binding_result_has_minimal_decision_and_expected_action(
    monkeypatch,
    tmp_path: Path,
) -> None:
    task_store_path = _configure_task_runtime(monkeypatch, tmp_path)
    decision = resolve_estimate_decision(
        task_id="DF-LEAD-ESTIMATE-DECISION-V1",
        payload={
            "workflow_type": WORKFLOW_TYPE,
            "lead_id": "lead-013",
            "lead_data": {
                "project_type": "ADU",
                "scope_summary": "Detached ADU",
                "contact_info": {"phone": "555-0100"},
                "lead_exists": True,
            },
        },
    )

    parent_task = _build_task(
        store_path=task_store_path,
        task_id="DF-LEAD-ESTIMATE-DECISION-V1",
        payload={
            "workflow_type": WORKFLOW_TYPE,
            "lead_id": "lead-013",
            "lead_data": {
                "project_type": "ADU",
                "scope_summary": "Detached ADU",
                "contact_info": {"phone": "555-0100"},
                "lead_exists": True,
            },
        },
    )

    binding = lead_estimate_decision_module.bind_decision_action(
        task_data=parent_task,
        decision=decision,
        store_path=task_store_path,
    )

    assert decision == {
        "decision": "create_estimate",
        "confidence": "high",
        "next_step": "create_estimate_task",
    }
    assert binding["binding_action"] == "create_estimate_task"
    assert binding["reason_code"] == "project_defined"


def test_invalid_decision_contract_blocks_execution_binding(
    monkeypatch, tmp_path: Path
) -> None:
    logs_dir = tmp_path / "runtime" / "logs"
    queue_file = tmp_path / "runtime" / "state" / "task_queue.json"
    task_log_file = logs_dir / "tasks.log"
    system_log_file = logs_dir / "system.log"
    policy_log_file = logs_dir / "policy.log"

    import app.orchestrator.task_queue as task_queue_module

    _configure_task_runtime(monkeypatch, tmp_path)
    monkeypatch.setattr(task_queue_module, "TASK_QUEUE_FILE", queue_file)
    monkeypatch.setattr(task_queue_module, "TASK_LOG_FILE", task_log_file)
    monkeypatch.setattr(system_log_module, "TASK_LOG_FILE", task_log_file)
    monkeypatch.setattr(system_log_module, "SYSTEM_LOG_FILE", system_log_file)
    monkeypatch.setattr(policy_gate_module, "POLICY_LOG_FILE", policy_log_file)

    queue = InMemoryTaskQueue()
    task_data = _build_task(
        store_path=tmp_path / "data" / "tasks.json",
        task_id="DF-LEAD-ESTIMATE-DECISION-CONTRACT-BLOCK-V1",
        payload={
            "workflow_type": WORKFLOW_TYPE,
            "lead_id": "lead-020",
            "lead_data": {
                "project_type": "ADU",
                "scope_summary": "Detached ADU with pricing request",
                "contact_info": {"phone": "555-0100"},
                "lead_exists": True,
            },
        },
    )

    def fetch_task(task_id: str) -> dict[str, object] | None:
        return (
            task_data
            if task_id == "DF-LEAD-ESTIMATE-DECISION-CONTRACT-BLOCK-V1"
            else None
        )

    def persist(updated_task: dict[str, object]) -> None:
        task_data.update(updated_task)

    def invalid_executor(_: dict[str, object]) -> dict[str, object]:
        return build_action_result(
            status="completed",
            task_id="DF-LEAD-ESTIMATE-DECISION-CONTRACT-BLOCK-V1",
            action_type=WORKFLOW_TYPE.upper(),
            result_payload={
                "decision": {
                    "decision": "create_estimate",
                    "confidence": "high",
                    "next_step": "bad_action",
                },
            },
            error_code="",
            error_message="",
            source="test_executor",
            diagnostic_message="invalid contract",
        )

    assert queue.enqueue_task("DF-LEAD-ESTIMATE-DECISION-CONTRACT-BLOCK-V1") is True

    executed_task = process_next_queued_task(
        queue=queue,
        now=lambda: "2026-04-04T00:00:00Z",
        fetch_task=fetch_task,
        persist=persist,
        timeout=0.0,
        executor=invalid_executor,
        telemetry_collector=lambda: {},
        network_snapshot_collector=lambda: {},
    )

    assert executed_task is not None
    assert executed_task["status"] == "FAILED"
    assert "invalid next_step" in str(executed_task.get("error", ""))
