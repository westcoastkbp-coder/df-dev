from __future__ import annotations

import json
from pathlib import Path

from app.execution.action_contract import build_action_result_contract
from app.execution.action_dispatcher import dispatch_action
from app.memory import memory_registry
from app.memory.memory_registry import compute_artifact_key
from app.orchestrator import task_memory, task_state_store
from app.ownerbox.domain import (
    create_owner_action_scope,
    create_owner_domain,
    create_owner_memory_scope,
    create_owner_trust_profile,
)
from app.ownerbox.workflow import (
    create_owner_workflow,
    create_owner_workflow_step,
    instantiate_workflow_steps,
)
from app.ownerbox.workflow_orchestrator import OwnerWorkflowOrchestrator
from app.ownerbox.owner_orchestrator import OwnerOrchestrator
import app.execution.action_dispatcher as action_dispatcher_module
import app.execution.idempotency_store as idempotency_store_module
import app.ownerbox.owner_orchestrator as owner_orchestrator_module
import app.ownerbox.workflow_state_store as workflow_state_store_module
import app.ownerbox.workflow_orchestrator as workflow_orchestrator_module
import runtime.system_log as system_log_module


def _configure_dispatch_runtime(monkeypatch, tmp_path: Path) -> Path:
    system_log_file = tmp_path / "runtime" / "logs" / "system.log"
    task_log_file = tmp_path / "runtime" / "logs" / "tasks.log"
    task_memory_file = tmp_path / "runtime" / "state" / "task_memory.json"
    registry_file = tmp_path / "df-system" / "memory_registry.json"

    monkeypatch.setattr(system_log_module, "SYSTEM_LOG_FILE", system_log_file)
    monkeypatch.setattr(system_log_module, "TASK_LOG_FILE", task_log_file)
    monkeypatch.setattr(action_dispatcher_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(owner_orchestrator_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(workflow_orchestrator_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(task_memory, "TASK_MEMORY_FILE", task_memory_file)
    monkeypatch.setattr(task_state_store, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store,
        "TASK_STATE_DB_FILE",
        Path("runtime/task_state.sqlite3"),
    )
    monkeypatch.setattr(memory_registry, "REGISTRY_FILE", registry_file)
    monkeypatch.setattr(idempotency_store_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        idempotency_store_module,
        "IDEMPOTENCY_DB_FILE",
        Path("runtime/state/idempotency.sqlite3"),
    )
    monkeypatch.setattr(workflow_state_store_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        workflow_state_store_module,
        "WORKFLOW_STATE_DB_FILE",
        Path("runtime/state/ownerbox_workflow_state.sqlite3"),
    )
    action_dispatcher_module._ACTION_OUTCOME_CACHE.clear()
    return system_log_file


def _trace_entries(system_log_file: Path) -> list[dict[str, object]]:
    if not system_log_file.exists():
        return []
    entries: list[dict[str, object]] = []
    for line in system_log_file.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if payload.get("event_type") != "trace":
            continue
        entries.append(dict(payload.get("details", {})))
    return entries


def _owner_boundary_bundle() -> tuple[object, object, object, object]:
    memory_scope = create_owner_memory_scope(
        scope_id="owner-memory-scope-v1",
        allowed_refs=["artifact", "state"],
    )
    action_scope = create_owner_action_scope(scope_id="owner-action-scope-v1")
    trust_profile = create_owner_trust_profile(
        trust_profile_id="owner-trust-v1",
        owner_id="owner-001",
        confirmation_policy_ref="owner-confirmation-v1",
    )
    owner_domain = create_owner_domain(
        domain_id="ownerbox-main",
        owner_id="owner-001",
        trust_level="high",
        memory_scope_ref=memory_scope.scope_id,
        action_scope_ref=action_scope.scope_id,
        policy_scope_ref="owner-policy-scope-v1",
    )
    return owner_domain, memory_scope, action_scope, trust_profile


def _workflow_dispatcher(openai_calls: list[str], browser_calls: list[str], email_calls: list[str]):
    def openai_executor(action_contract: object) -> dict[str, object]:
        action_id = str(dict(action_contract)["action_id"])
        openai_calls.append(action_id)
        return build_action_result_contract(
            action_id=action_id,
            status="success",
            result_type="text_generation",
            payload={"text": "Generated bounded content."},
        )

    def browser_executor(action_contract: object) -> dict[str, object]:
        action_id = str(dict(action_contract)["action_id"])
        browser_calls.append(action_id)
        return build_action_result_contract(
            action_id=action_id,
            status="success",
            result_type="browser_submit",
            payload={
                "summary": "Submitted bounded browser step.",
                "references": {"selector": "#lead-form"},
                "metadata": {"operation": "submit_form"},
            },
        )

    def email_executor(action_contract: object) -> dict[str, object]:
        action_id = str(dict(action_contract)["action_id"])
        email_calls.append(action_id)
        operation = str(dict(dict(action_contract)["parameters"])["operation"])
        result_type = "email_draft" if operation == "create_draft" else "email_send"
        return build_action_result_contract(
            action_id=action_id,
            status="success",
            result_type=result_type,
            payload={
                "summary": f"Executed bounded email step {operation}.",
                "references": {"operation": operation},
                "metadata": {"operation": operation},
            },
        )

    def dispatcher(action_contract: object, **kwargs: object) -> dict[str, object]:
        return dispatch_action(
            action_contract,
            openai_executor=openai_executor,
            browser_executor=browser_executor,
            email_executor=email_executor,
            **kwargs,
        )

    return dispatcher


def test_workflow_object_creation_works() -> None:
    workflow = create_owner_workflow(
        workflow_id="owner-workflow-001",
        owner_id="owner-001",
        workflow_type="browser_then_email",
        title="Owner Browser Email Flow",
        step_ids=["owner-workflow-001.step1", "owner-workflow-001.step2"],
        current_step_id="owner-workflow-001.step1",
        created_at="2026-04-15T12:00:00Z",
        updated_at="2026-04-15T12:00:00Z",
    )

    assert workflow.to_dict() == {
        "workflow_id": "owner-workflow-001",
        "owner_id": "owner-001",
        "workflow_type": "browser_then_email",
        "title": "Owner Browser Email Flow",
        "step_ids": ["owner-workflow-001.step1", "owner-workflow-001.step2"],
        "status": "pending",
        "current_step_id": "owner-workflow-001.step1",
        "completed_step_count": 0,
        "last_action_id": None,
        "last_approval_id": None,
        "final_result_summary": None,
        "created_at": "2026-04-15T12:00:00Z",
        "updated_at": "2026-04-15T12:00:00Z",
    }


def test_workflow_step_creation_works() -> None:
    step = create_owner_workflow_step(
        step_id="owner-workflow-001.step1",
        workflow_id="owner-workflow-001",
        owner_id="owner-001",
        sequence_index=0,
        title="Browser Step",
        request_text="Open the bounded workflow form",
        action_type="browser_action",
        target_type="adapter",
        target_ref="browser",
        action_parameters={
            "operation": "open_page",
            "url": "https://example.com/form",
            "timeout_seconds": 10,
        },
        created_at="2026-04-15T12:00:00Z",
        updated_at="2026-04-15T12:00:00Z",
    )

    assert step.to_dict()["status"] == "pending"
    assert step.to_dict()["action_type"] == "BROWSER_ACTION"
    assert step.to_dict()["action_parameters"]["operation"] == "open_page"
    assert step.to_dict()["attempt_count"] == 0
    assert step.to_dict()["retry_status"] == "not_needed"
    assert step.to_dict()["max_retries"] == 2
    assert step.to_dict()["timeout_seconds"] == 10


def test_workflow_types_instantiate_deterministic_step_patterns() -> None:
    browser_email_steps = instantiate_workflow_steps(
        workflow_id="owner-workflow-001",
        owner_id="owner-001",
        workflow_type="browser_then_email",
        workflow_payload={
            "browser_action": {
                "request_text": "Submit the owner-controlled form",
                "action_parameters": {
                    "operation": "submit_form",
                    "url": "https://example.com/form",
                    "selector": "#lead-form",
                    "timeout_seconds": 10,
                },
            },
            "email_action": {
                "request_text": "Send the bounded owner email",
                "action_parameters": {
                    "operation": "send_email",
                    "to": ["owner@example.com"],
                    "subject": "Owner update",
                    "body": "Bounded update",
                    "attachments": [],
                },
            },
        },
    )
    draft_send_steps = instantiate_workflow_steps(
        workflow_id="owner-workflow-002",
        owner_id="owner-001",
        workflow_type="email_draft_then_send",
        workflow_payload={
            "draft_action": {
                "request_text": "Draft the bounded email",
                "action_parameters": {
                    "operation": "create_draft",
                    "to": ["owner@example.com"],
                    "subject": "Draft subject",
                    "body": "Draft body",
                    "attachments": [],
                },
            },
            "send_action": {
                "request_text": "Send the bounded email",
                "action_parameters": {
                    "operation": "send_email",
                    "to": ["owner@example.com"],
                    "subject": "Draft subject",
                    "body": "Draft body",
                    "attachments": [],
                },
            },
        },
    )

    assert [step.action_type for step in browser_email_steps] == [
        "BROWSER_ACTION",
        "EMAIL_ACTION",
    ]
    assert [step.action_parameters["operation"] for step in draft_send_steps] == [
        "create_draft",
        "send_email",
    ]
    assert browser_email_steps[0].max_retries == 2
    assert browser_email_steps[0].timeout_seconds == 10


def test_workflow_orchestrator_creates_expected_step_sequence(monkeypatch, tmp_path: Path) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    openai_calls: list[str] = []
    browser_calls: list[str] = []
    email_calls: list[str] = []
    dispatcher = _workflow_dispatcher(openai_calls, browser_calls, email_calls)
    orchestrator = OwnerWorkflowOrchestrator(
        owner_orchestrator=OwnerOrchestrator(dispatcher=dispatcher)
    )

    result = orchestrator.create_workflow(
        owner_id="owner-001",
        workflow_type="openai_then_email",
        workflow_payload={
            "openai_request": {
                "request_text": "Generate bounded owner copy",
                "action_parameters": {
                    "model": "gpt-5-mini",
                    "prompt": "Write one bounded sentence.",
                    "max_tokens": 32,
                    "temperature": 0.2,
                },
            },
            "email_action": {
                "request_text": "Send the bounded owner email",
                "action_parameters": {
                    "operation": "send_email",
                    "to": ["owner@example.com"],
                    "subject": "Owner update",
                    "body": "Bounded owner update",
                    "attachments": [],
                },
            },
        },
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-US",
    )

    assert len(result.steps) == 2
    assert openai_calls
    assert email_calls == []
    assert result.steps[0].status == "completed"
    assert result.steps[1].status == "awaiting_approval"
    assert result.workflow.status == "blocked"


def test_step_requiring_approval_does_not_execute_before_approval(monkeypatch, tmp_path: Path) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    openai_calls: list[str] = []
    browser_calls: list[str] = []
    email_calls: list[str] = []
    dispatcher = _workflow_dispatcher(openai_calls, browser_calls, email_calls)
    orchestrator = OwnerWorkflowOrchestrator(
        owner_orchestrator=OwnerOrchestrator(dispatcher=dispatcher)
    )

    result = orchestrator.create_workflow(
        owner_id="owner-001",
        workflow_type="browser_then_email",
        workflow_payload={
            "browser_action": {
                "request_text": "Submit the bounded browser form",
                "action_parameters": {
                    "operation": "submit_form",
                    "url": "https://example.com/form",
                    "selector": "#lead-form",
                    "timeout_seconds": 10,
                },
            },
            "email_action": {
                "request_text": "Send the bounded owner email",
                "action_parameters": {
                    "operation": "send_email",
                    "to": ["owner@example.com"],
                    "subject": "Owner update",
                    "body": "Bounded owner update",
                    "attachments": [],
                },
            },
        },
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-US",
    )

    assert browser_calls == []
    assert email_calls == []
    assert result.approval is not None
    assert result.current_step is not None
    assert result.current_step.status == "awaiting_approval"
    assert result.workflow.status == "blocked"


def test_approved_step_executes_once(monkeypatch, tmp_path: Path) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    openai_calls: list[str] = []
    browser_calls: list[str] = []
    email_calls: list[str] = []
    dispatcher = _workflow_dispatcher(openai_calls, browser_calls, email_calls)
    orchestrator = OwnerWorkflowOrchestrator(
        owner_orchestrator=OwnerOrchestrator(dispatcher=dispatcher)
    )

    result = orchestrator.create_workflow(
        owner_id="owner-001",
        workflow_type="browser_then_email",
        workflow_payload={
            "browser_action": {
                "request_text": "Submit the bounded browser form",
                "action_parameters": {
                    "operation": "submit_form",
                    "url": "https://example.com/form",
                    "selector": "#lead-form",
                    "timeout_seconds": 10,
                },
            },
            "email_action": {
                "request_text": "Send the bounded owner email",
                "action_parameters": {
                    "operation": "send_email",
                    "to": ["owner@example.com"],
                    "subject": "Owner update",
                    "body": "Bounded owner update",
                    "attachments": [],
                },
            },
        },
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-US",
    )

    approval_id = result.approval.approval_id
    first = orchestrator.approve_step(approval_id)
    second = orchestrator.approve_step(approval_id)

    assert len(browser_calls) == 1
    assert first.steps[0].status == "completed"
    assert second.steps[0].status == "completed"
    assert first.workflow.status == "blocked"
    assert first.steps[1].status == "awaiting_approval"


def test_workflow_action_trace_includes_workflow_and_step_metadata(
    monkeypatch,
    tmp_path: Path,
) -> None:
    system_log_file = _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()

    def browser_executor(action_contract: object) -> dict[str, object]:
        return build_action_result_contract(
            action_id=str(dict(action_contract)["action_id"]),
            status="success",
            result_type="browser_submit",
            payload={
                "summary": "Submitted bounded browser step.",
                "references": {"selector": "#lead-form"},
                "metadata": {"operation": "submit_form", "backend_used": "playwright"},
            },
        )

    def email_executor(action_contract: object) -> dict[str, object]:
        operation = str(dict(dict(action_contract)["parameters"])["operation"])
        return build_action_result_contract(
            action_id=str(dict(action_contract)["action_id"]),
            status="success",
            result_type="email_send",
            payload={
                "summary": f"Executed bounded email step {operation}.",
                "references": {"operation": operation},
                "metadata": {"operation": operation, "backend_used": "gmail"},
            },
        )

    def dispatcher(action_contract: object, **kwargs: object) -> dict[str, object]:
        return dispatch_action(
            action_contract,
            browser_executor=browser_executor,
            email_executor=email_executor,
            **kwargs,
        )

    orchestrator = OwnerWorkflowOrchestrator(
        owner_orchestrator=OwnerOrchestrator(dispatcher=dispatcher)
    )
    created = orchestrator.create_workflow(
        owner_id="owner-001",
        workflow_type="browser_then_email",
        workflow_payload={
            "browser_action": {
                "request_text": "Submit the bounded browser form",
                "action_parameters": {
                    "operation": "submit_form",
                    "url": "https://example.com/form",
                    "selector": "#lead-form",
                    "timeout_seconds": 10,
                },
            },
            "email_action": {
                "request_text": "Send the bounded owner email",
                "action_parameters": {
                    "operation": "send_email",
                    "to": ["owner@example.com"],
                    "subject": "Owner update",
                    "body": "Bounded owner update",
                    "attachments": [],
                },
            },
        },
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-US",
    )
    approved = orchestrator.approve_step(created.approval.approval_id)

    traces = _trace_entries(system_log_file)
    action_trace = next(
        trace
        for trace in reversed(traces)
        if trace.get("action_id") == approved.steps[0].action_id
        and trace.get("type") == "execution_trace"
    )

    assert action_trace["workflow_id"] == approved.workflow.workflow_id
    assert action_trace["step_id"] == approved.steps[0].step_id
    assert action_trace["approval_id"] == created.approval.approval_id
    assert action_trace["backend_used"] == "playwright"


def test_rejected_step_blocks_workflow_correctly(monkeypatch, tmp_path: Path) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    openai_calls: list[str] = []
    browser_calls: list[str] = []
    email_calls: list[str] = []
    dispatcher = _workflow_dispatcher(openai_calls, browser_calls, email_calls)
    orchestrator = OwnerWorkflowOrchestrator(
        owner_orchestrator=OwnerOrchestrator(dispatcher=dispatcher)
    )

    result = orchestrator.create_workflow(
        owner_id="owner-001",
        workflow_type="browser_then_email",
        workflow_payload={
            "browser_action": {
                "request_text": "Submit the bounded browser form",
                "action_parameters": {
                    "operation": "submit_form",
                    "url": "https://example.com/form",
                    "selector": "#lead-form",
                    "timeout_seconds": 10,
                },
            },
            "email_action": {
                "request_text": "Send the bounded owner email",
                "action_parameters": {
                    "operation": "send_email",
                    "to": ["owner@example.com"],
                    "subject": "Owner update",
                    "body": "Bounded owner update",
                    "attachments": [],
                },
            },
        },
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-US",
    )

    rejected = orchestrator.reject_step(result.approval.approval_id)

    assert browser_calls == []
    assert email_calls == []
    assert rejected.workflow.status == "rejected"
    assert rejected.steps[0].status == "rejected"
    assert rejected.steps[0].outcome == "approval_rejected"
    assert rejected.steps[1].status == "pending"


def test_workflow_status_updates_correctly_through_lifecycle(monkeypatch, tmp_path: Path) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    openai_calls: list[str] = []
    browser_calls: list[str] = []
    email_calls: list[str] = []
    dispatcher = _workflow_dispatcher(openai_calls, browser_calls, email_calls)
    orchestrator = OwnerWorkflowOrchestrator(
        owner_orchestrator=OwnerOrchestrator(dispatcher=dispatcher)
    )

    created = orchestrator.create_workflow(
        owner_id="owner-001",
        workflow_type="openai_then_email",
        workflow_payload={
            "openai_request": {
                "request_text": "Generate bounded owner copy",
                "action_parameters": {
                    "model": "gpt-5-mini",
                    "prompt": "Write one bounded sentence.",
                    "max_tokens": 32,
                    "temperature": 0.2,
                },
            },
            "email_action": {
                "request_text": "Send the bounded owner email",
                "action_parameters": {
                    "operation": "send_email",
                    "to": ["owner@example.com"],
                    "subject": "Owner update",
                    "body": "Bounded owner update",
                    "attachments": [],
                },
            },
        },
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-US",
    )

    completed = orchestrator.approve_step(created.approval.approval_id)

    assert created.workflow.status == "blocked"
    assert completed.workflow.status == "completed"
    assert [step.status for step in completed.steps] == ["completed", "completed"]
    assert len(openai_calls) == 1
    assert len(email_calls) == 1


def test_workflow_state_persists_across_reload_and_preserves_current_step_index(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    openai_calls: list[str] = []
    browser_calls: list[str] = []
    email_calls: list[str] = []
    dispatcher = _workflow_dispatcher(openai_calls, browser_calls, email_calls)

    created = OwnerWorkflowOrchestrator(
        owner_orchestrator=OwnerOrchestrator(dispatcher=dispatcher)
    ).create_workflow(
        owner_id="owner-001",
        workflow_type="openai_then_email",
        workflow_payload={
            "openai_request": {
                "request_text": "Generate bounded owner copy",
                "action_parameters": {
                    "model": "gpt-5-mini",
                    "prompt": "Write one bounded sentence.",
                    "max_tokens": 32,
                    "temperature": 0.2,
                },
            },
            "email_action": {
                "request_text": "Send the bounded owner email",
                "action_parameters": {
                    "operation": "send_email",
                    "to": ["owner@example.com"],
                    "subject": "Owner update",
                    "body": "Bounded owner update",
                    "attachments": [],
                },
            },
        },
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-US",
    )

    restored = OwnerWorkflowOrchestrator(
        owner_orchestrator=OwnerOrchestrator(dispatcher=dispatcher)
    ).resume_workflow(created.workflow.workflow_id)

    assert restored.workflow.workflow_id == created.workflow.workflow_id
    assert restored.workflow.status == "blocked"
    assert restored.steps[0].status == "completed"
    assert restored.steps[1].status == "awaiting_approval"
    assert restored.current_step is not None
    assert restored.current_step.sequence_index == 1


def test_blocked_approval_step_can_continue_after_restart(monkeypatch, tmp_path: Path) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    openai_calls: list[str] = []
    browser_calls: list[str] = []
    email_calls: list[str] = []
    dispatcher = _workflow_dispatcher(openai_calls, browser_calls, email_calls)

    created = OwnerWorkflowOrchestrator(
        owner_orchestrator=OwnerOrchestrator(dispatcher=dispatcher)
    ).create_workflow(
        owner_id="owner-001",
        workflow_type="browser_then_email",
        workflow_payload={
            "browser_action": {
                "request_text": "Submit the bounded browser form",
                "action_parameters": {
                    "operation": "submit_form",
                    "url": "https://example.com/form",
                    "selector": "#lead-form",
                    "timeout_seconds": 10,
                },
            },
            "email_action": {
                "request_text": "Send the bounded owner email",
                "action_parameters": {
                    "operation": "send_email",
                    "to": ["owner@example.com"],
                    "subject": "Owner update",
                    "body": "Bounded owner update",
                    "attachments": [],
                },
            },
        },
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-US",
    )

    restored = OwnerWorkflowOrchestrator(
        owner_orchestrator=OwnerOrchestrator(dispatcher=dispatcher)
    )
    approved = restored.approve_step(created.approval.approval_id)

    assert len(browser_calls) == 1
    assert approved.steps[0].status == "completed"
    assert approved.steps[1].status == "awaiting_approval"
    assert approved.workflow.status == "blocked"


def test_failed_workflow_state_restores_after_reload(monkeypatch, tmp_path: Path) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    openai_calls: list[str] = []

    def openai_executor(action_contract: object) -> dict[str, object]:
        action_id = str(dict(action_contract)["action_id"])
        openai_calls.append(action_id)
        return build_action_result_contract(
            action_id=action_id,
            status="failed",
            result_type="text_generation",
            payload={"metadata": {"attempt_count": 1}},
            error_code="provider_error",
            error_message="provider failed",
        )

    def dispatcher(action_contract: object, **kwargs: object) -> dict[str, object]:
        return dispatch_action(action_contract, openai_executor=openai_executor, **kwargs)

    failed = OwnerWorkflowOrchestrator(
        owner_orchestrator=OwnerOrchestrator(dispatcher=dispatcher)
    ).create_workflow(
        owner_id="owner-001",
        workflow_type="openai_then_email",
        workflow_payload={
            "openai_request": {
                "request_text": "Generate bounded owner copy",
                "action_parameters": {
                    "model": "gpt-5-mini",
                    "prompt": "Write one bounded sentence.",
                    "max_tokens": 32,
                    "temperature": 0.2,
                },
            },
            "email_action": {
                "request_text": "Send the bounded owner email",
                "action_parameters": {
                    "operation": "send_email",
                    "to": ["owner@example.com"],
                    "subject": "Owner update",
                    "body": "Bounded owner update",
                    "attachments": [],
                },
            },
        },
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-US",
    )

    restored = OwnerWorkflowOrchestrator(
        owner_orchestrator=OwnerOrchestrator(dispatcher=dispatcher)
    ).resume_workflow(failed.workflow.workflow_id)

    assert len(openai_calls) == 1
    assert restored.workflow.status == "failed"
    assert restored.steps[0].status == "failed"
    assert restored.steps[1].status == "pending"


def test_rejected_workflow_state_restores_after_reload(monkeypatch, tmp_path: Path) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    openai_calls: list[str] = []
    browser_calls: list[str] = []
    email_calls: list[str] = []
    dispatcher = _workflow_dispatcher(openai_calls, browser_calls, email_calls)

    created = OwnerWorkflowOrchestrator(
        owner_orchestrator=OwnerOrchestrator(dispatcher=dispatcher)
    ).create_workflow(
        owner_id="owner-001",
        workflow_type="browser_then_email",
        workflow_payload={
            "browser_action": {
                "request_text": "Submit the bounded browser form",
                "action_parameters": {
                    "operation": "submit_form",
                    "url": "https://example.com/form",
                    "selector": "#lead-form",
                    "timeout_seconds": 10,
                },
            },
            "email_action": {
                "request_text": "Send the bounded owner email",
                "action_parameters": {
                    "operation": "send_email",
                    "to": ["owner@example.com"],
                    "subject": "Owner update",
                    "body": "Bounded owner update",
                    "attachments": [],
                },
            },
        },
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-US",
    )
    rejected = OwnerWorkflowOrchestrator(
        owner_orchestrator=OwnerOrchestrator(dispatcher=dispatcher)
    ).reject_step(created.approval.approval_id)
    restored = OwnerWorkflowOrchestrator(
        owner_orchestrator=OwnerOrchestrator(dispatcher=dispatcher)
    ).resume_workflow(rejected.workflow.workflow_id)

    assert restored.workflow.status == "rejected"
    assert restored.steps[0].status == "rejected"
    assert restored.steps[1].status == "pending"


def test_durable_workflow_state_stays_outside_memory_semantics(monkeypatch, tmp_path: Path) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    dispatcher = _workflow_dispatcher([], [], [])
    created = OwnerWorkflowOrchestrator(
        owner_orchestrator=OwnerOrchestrator(dispatcher=dispatcher)
    ).create_workflow(
        owner_id="owner-001",
        workflow_type="browser_then_email",
        workflow_payload={
            "browser_action": {
                "request_text": "Submit the bounded browser form",
                "action_parameters": {
                    "operation": "submit_form",
                    "url": "https://example.com/form",
                    "selector": "#lead-form",
                    "timeout_seconds": 10,
                },
            },
            "email_action": {
                "request_text": "Send the bounded owner email",
                "action_parameters": {
                    "operation": "send_email",
                    "to": ["owner@example.com"],
                    "subject": "Owner update",
                    "body": "Bounded owner update",
                    "attachments": [],
                },
            },
        },
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-US",
    )

    durable_db = tmp_path / "runtime" / "state" / "ownerbox_workflow_state.sqlite3"
    assert durable_db.exists()
    assert (
        memory_registry.get_artifact_by_logical_key(
            compute_artifact_key("ownerbox", "owner_workflow_state", created.workflow.workflow_id)
        )
        is None
    )


def test_workflow_persistence_failure_is_normalized(monkeypatch, tmp_path: Path) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()

    class FailingWorkflowStateStore:
        def list_states(self) -> tuple[object, ...]:
            return ()

        def save_state(self, snapshot: object) -> object:
            workflow_id = str(dict(snapshot)["workflow"]["workflow_id"])
            raise workflow_state_store_module.WorkflowStateStoreError(
                code="persistence_error",
                workflow_id=workflow_id,
                operation="save_state",
                reason="workflow state store unavailable",
            )

    result = OwnerWorkflowOrchestrator(
        owner_orchestrator=OwnerOrchestrator(dispatcher=_workflow_dispatcher([], [], [])),
        workflow_state_store=FailingWorkflowStateStore(),
    ).create_workflow(
        owner_id="owner-001",
        workflow_type="browser_then_email",
        workflow_payload={
            "browser_action": {
                "request_text": "Submit the bounded browser form",
                "action_parameters": {
                    "operation": "submit_form",
                    "url": "https://example.com/form",
                    "selector": "#lead-form",
                    "timeout_seconds": 10,
                },
            },
            "email_action": {
                "request_text": "Send the bounded owner email",
                "action_parameters": {
                    "operation": "send_email",
                    "to": ["owner@example.com"],
                    "subject": "Owner update",
                    "body": "Bounded owner update",
                    "attachments": [],
                },
            },
        },
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-US",
    )

    assert result.workflow.status == "failed"
    assert result.current_step is not None
    assert result.current_step.status == "failed"
    assert result.current_step.last_error == {
        "error_code": "persistence_error",
        "error_message": "workflow state store unavailable",
    }


def test_retryable_timeout_retries_safe_step_and_records_retry_metadata(
    monkeypatch,
    tmp_path: Path,
) -> None:
    system_log_file = _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    openai_calls: list[str] = []

    def openai_executor(action_contract: object) -> dict[str, object]:
        action_id = str(dict(action_contract)["action_id"])
        openai_calls.append(action_id)
        if len(openai_calls) == 1:
            return build_action_result_contract(
                action_id=action_id,
                status="failed",
                result_type="text_generation",
                payload={"metadata": {"attempt_count": 1}},
                error_code="timeout",
                error_message="OpenAI request timed out",
            )
        return build_action_result_contract(
            action_id=action_id,
            status="success",
            result_type="text_generation",
            payload={"text": "Generated bounded content."},
        )

    def dispatcher(action_contract: object, **kwargs: object) -> dict[str, object]:
        return dispatch_action(action_contract, openai_executor=openai_executor, **kwargs)

    orchestrator = OwnerWorkflowOrchestrator(
        owner_orchestrator=OwnerOrchestrator(dispatcher=dispatcher)
    )
    result = orchestrator.create_workflow(
        owner_id="owner-001",
        workflow_type="openai_then_email",
        workflow_payload={
            "openai_request": {
                "request_text": "Generate bounded owner copy",
                "max_retries": 2,
                "timeout_seconds": 5,
                "action_parameters": {
                    "model": "gpt-5-mini",
                    "prompt": "Write one bounded sentence.",
                    "max_tokens": 32,
                    "temperature": 0.2,
                },
            },
            "email_action": {
                "request_text": "Send the bounded owner email",
                "action_parameters": {
                    "operation": "send_email",
                    "to": ["owner@example.com"],
                    "subject": "Owner update",
                    "body": "Bounded owner update",
                    "attachments": [],
                },
            },
        },
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-US",
    )

    assert len(openai_calls) == 2
    assert result.steps[0].status == "completed"
    assert result.steps[0].attempt_count == 2
    assert result.steps[0].retry_status == "succeeded_after_retry"
    assert result.steps[0].outcome == "success"
    assert result.workflow.status == "blocked"

    traces = _trace_entries(system_log_file)
    workflow_trace = next(
        trace for trace in reversed(traces) if trace.get("type") == "owner_workflow_trace"
    )
    assert workflow_trace["attempt_count"] == 0
    assert workflow_trace["workflow_status"] == "blocked"
    action_traces = [
        trace
        for trace in traces
        if trace.get("type") == "execution_trace" and trace.get("action_id") == result.steps[0].action_id
    ]
    assert len(action_traces) == 2
    assert action_traces[-1]["attempt_count"] == 1


def test_non_retryable_failure_stops_workflow_without_executing_next_step(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    openai_calls: list[str] = []
    email_calls: list[str] = []

    def openai_executor(action_contract: object) -> dict[str, object]:
        action_id = str(dict(action_contract)["action_id"])
        openai_calls.append(action_id)
        return build_action_result_contract(
            action_id=action_id,
            status="failed",
            result_type="text_generation",
            payload={"metadata": {"attempt_count": 1}},
            error_code="provider_error",
            error_message="provider failed",
        )

    def email_executor(action_contract: object) -> dict[str, object]:
        email_calls.append(str(dict(action_contract)["action_id"]))
        raise AssertionError("workflow must fail before the email step runs")

    def dispatcher(action_contract: object, **kwargs: object) -> dict[str, object]:
        return dispatch_action(
            action_contract,
            openai_executor=openai_executor,
            email_executor=email_executor,
            **kwargs,
        )

    orchestrator = OwnerWorkflowOrchestrator(
        owner_orchestrator=OwnerOrchestrator(dispatcher=dispatcher)
    )
    result = orchestrator.create_workflow(
        owner_id="owner-001",
        workflow_type="openai_then_email",
        workflow_payload={
            "openai_request": {
                "request_text": "Generate bounded owner copy",
                "max_retries": 2,
                "action_parameters": {
                    "model": "gpt-5-mini",
                    "prompt": "Write one bounded sentence.",
                    "max_tokens": 32,
                    "temperature": 0.2,
                },
            },
            "email_action": {
                "request_text": "Send the bounded owner email",
                "action_parameters": {
                    "operation": "send_email",
                    "to": ["owner@example.com"],
                    "subject": "Owner update",
                    "body": "Bounded owner update",
                    "attachments": [],
                },
            },
        },
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-US",
    )

    assert len(openai_calls) == 1
    assert email_calls == []
    assert result.workflow.status == "failed"
    assert result.steps[0].status == "failed"
    assert result.steps[0].attempt_count == 1
    assert result.steps[0].retry_status == "not_retryable"
    assert result.steps[0].last_error == {
        "error_code": "provider_error",
        "error_message": "provider failed",
    }
    assert result.steps[1].status == "pending"


def test_terminal_workflow_state_persists_evidence_only_after_completion(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    openai_calls: list[str] = []
    browser_calls: list[str] = []
    email_calls: list[str] = []
    dispatcher = _workflow_dispatcher(openai_calls, browser_calls, email_calls)
    orchestrator = OwnerWorkflowOrchestrator(
        owner_orchestrator=OwnerOrchestrator(dispatcher=dispatcher)
    )

    created = orchestrator.create_workflow(
        owner_id="owner-001",
        workflow_type="openai_then_email",
        workflow_payload={
            "openai_request": {
                "request_text": "Generate bounded owner copy",
                "action_parameters": {
                    "model": "gpt-5-mini",
                    "prompt": "Write one bounded sentence.",
                    "max_tokens": 32,
                    "temperature": 0.2,
                },
            },
            "email_action": {
                "request_text": "Send the bounded owner email",
                "action_parameters": {
                    "operation": "send_email",
                    "to": ["owner@example.com"],
                    "subject": "Owner update",
                    "body": "Bounded owner update",
                    "attachments": [],
                },
            },
        },
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-US",
    )
    evidence_key = compute_artifact_key(
        "ownerbox", "owner_workflow_evidence", created.workflow.workflow_id
    )
    assert memory_registry.get_artifact_by_logical_key(evidence_key) is None

    completed = orchestrator.approve_step(created.approval.approval_id)

    assert completed.workflow.status == "completed"
    assert memory_registry.get_artifact_by_logical_key(evidence_key) is not None


def test_trace_compatible_workflow_metadata_exists(monkeypatch, tmp_path: Path) -> None:
    system_log_file = _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    openai_calls: list[str] = []
    browser_calls: list[str] = []
    email_calls: list[str] = []
    dispatcher = _workflow_dispatcher(openai_calls, browser_calls, email_calls)
    orchestrator = OwnerWorkflowOrchestrator(
        owner_orchestrator=OwnerOrchestrator(dispatcher=dispatcher)
    )

    result = orchestrator.create_workflow(
        owner_id="owner-001",
        workflow_type="browser_then_email",
        workflow_payload={
            "browser_action": {
                "request_text": "Submit the bounded browser form",
                "action_parameters": {
                    "operation": "submit_form",
                    "url": "https://example.com/form",
                    "selector": "#lead-form",
                    "timeout_seconds": 10,
                },
            },
            "email_action": {
                "request_text": "Send the bounded owner email",
                "action_parameters": {
                    "operation": "send_email",
                    "to": ["owner@example.com"],
                    "subject": "Owner update",
                    "body": "Bounded owner update",
                    "attachments": [],
                },
            },
        },
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-US",
    )

    traces = _trace_entries(system_log_file)
    workflow_trace = next(
        trace for trace in reversed(traces) if trace.get("type") == "owner_workflow_trace"
    )
    trace_key = compute_artifact_key(
        "ownerbox", "owner_workflow_trace", result.workflow.workflow_id
    )
    evidence_key = compute_artifact_key(
        "ownerbox", "owner_workflow_evidence", result.workflow.workflow_id
    )

    assert workflow_trace["workflow_id"] == result.workflow.workflow_id
    assert workflow_trace["owner_id"] == "owner-001"
    assert workflow_trace["workflow_type"] == "browser_then_email"
    assert workflow_trace["approval_id"] == result.approval.approval_id
    assert workflow_trace["step_status"] == "awaiting_approval"
    assert workflow_trace["workflow_status"] == "blocked"
    assert workflow_trace["attempt_count"] == 0
    assert memory_registry.get_artifact_by_logical_key(trace_key) is not None
    assert memory_registry.get_artifact_by_logical_key(evidence_key) is None


def test_owner_response_includes_workflow_visibility_fields(monkeypatch, tmp_path: Path) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    openai_calls: list[str] = []
    browser_calls: list[str] = []
    email_calls: list[str] = []
    dispatcher = _workflow_dispatcher(openai_calls, browser_calls, email_calls)
    orchestrator = OwnerWorkflowOrchestrator(
        owner_orchestrator=OwnerOrchestrator(dispatcher=dispatcher)
    )

    result = orchestrator.create_workflow(
        owner_id="owner-001",
        workflow_type="browser_then_email",
        workflow_payload={
            "browser_action": {
                "request_text": "Submit the bounded browser form",
                "action_parameters": {
                    "operation": "submit_form",
                    "url": "https://example.com/form",
                    "selector": "#lead-form",
                    "timeout_seconds": 10,
                },
            },
            "email_action": {
                "request_text": "Send the bounded owner email",
                "action_parameters": {
                    "operation": "send_email",
                    "to": ["owner@example.com"],
                    "subject": "Owner update",
                    "body": "Bounded owner update",
                    "attachments": [],
                },
            },
        },
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-US",
    )

    metadata = result.response_plan.metadata["workflow"]

    assert metadata["workflow_id"] == result.workflow.workflow_id
    assert metadata["workflow_type"] == "browser_then_email"
    assert metadata["workflow_status"] == "blocked"
    assert metadata["current_step_id"] == result.current_step.step_id
    assert metadata["total_steps"] == 2
    assert metadata["completed_steps"] == 0
    assert metadata["trace_artifact_path"]
    assert metadata["evidence_artifact_path"] == ""
    assert metadata["steps"][0]["approval_id"] == result.approval.approval_id
