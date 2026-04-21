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
from app.ownerbox.owner_orchestrator import OwnerOrchestrator
import app.execution.action_dispatcher as action_dispatcher_module
import app.ownerbox.owner_orchestrator as owner_orchestrator_module
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
    monkeypatch.setattr(task_memory, "TASK_MEMORY_FILE", task_memory_file)
    monkeypatch.setattr(task_state_store, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store,
        "TASK_STATE_DB_FILE",
        Path("runtime/task_state.sqlite3"),
    )
    monkeypatch.setattr(memory_registry, "REGISTRY_FILE", registry_file)
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


def test_low_risk_action_executes_immediately(monkeypatch, tmp_path: Path) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    captured: dict[str, object] = {}

    def openai_executor(action_contract: object) -> dict[str, object]:
        captured["action_contract"] = dict(action_contract)
        return build_action_result_contract(
            action_id=str(dict(action_contract)["action_id"]),
            status="success",
            result_type="text_generation",
            payload={"text": "Immediate owner response."},
        )

    def dispatcher(action_contract: object, **kwargs: object) -> dict[str, object]:
        captured["dispatch_kwargs"] = dict(kwargs)
        return dispatch_action(action_contract, openai_executor=openai_executor, **kwargs)

    result = OwnerOrchestrator(dispatcher=dispatcher).process_request(
        request_text="Summarize the current owner queue",
        owner_id="owner-001",
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-US",
    )

    assert result.approval is None
    assert result.action_result is not None
    assert dict(captured["dispatch_kwargs"])["memory_domain"] == "ownerbox"
    assert result.response_plan.trust_class == "low"
    assert result.response_plan.requires_confirmation is False


def test_high_risk_action_creates_approval_and_does_not_execute(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    dispatch_calls = 0

    def dispatcher(action_contract: object, **kwargs: object) -> dict[str, object]:
        nonlocal dispatch_calls
        dispatch_calls += 1
        raise AssertionError("high-risk action must not dispatch before approval")

    result = OwnerOrchestrator(dispatcher=dispatcher).process_request(
        request_text="Prepare owner-controlled file update",
        owner_id="owner-001",
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-US",
        action_type="write_file",
        target_type="filesystem",
        target_ref="runtime/out/ownerbox/approved.txt",
        action_parameters={"path": "runtime/out/ownerbox/approved.txt", "content": "approved"},
    )

    assert dispatch_calls == 0
    assert result.approval is not None
    assert result.approval.status == "pending"
    assert result.action_result is None
    assert result.queue_entry is not None
    assert result.queue_entry.action_status == "awaiting_confirmation"
    assert result.response_plan.response_type == "confirmation_request"
    assert result.response_plan.requires_confirmation is True
    assert result.response_plan.approval_id == result.approval.approval_id
    assert result.response_plan.trust_class == "medium"
    assert result.response_plan.preview_text is not None


def test_approve_executes_and_duplicate_approve_does_not_double_execute(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    dispatch_calls: list[str] = []

    def dispatcher(action_contract: object, **kwargs: object) -> dict[str, object]:
        dispatch_calls.append(str(dict(action_contract)["action_id"]))
        return build_action_result_contract(
            action_id=str(dict(action_contract)["action_id"]),
            status="success",
            result_type="file_write",
            payload={"note": "write approved"},
        )

    orchestrator = OwnerOrchestrator(dispatcher=dispatcher)
    interaction = orchestrator.process_request(
        request_text="Prepare owner-controlled file update",
        owner_id="owner-001",
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-US",
        action_type="write_file",
        target_type="filesystem",
        target_ref="runtime/out/ownerbox/approved.txt",
        action_parameters={"path": "runtime/out/ownerbox/approved.txt", "content": "approved"},
    )

    assert interaction.approval is not None
    first = orchestrator.approve_action(interaction.approval.approval_id)
    second = orchestrator.approve_action(interaction.approval.approval_id)

    assert len(dispatch_calls) == 1
    assert first.approval is not None
    assert first.approval.status == "approved"
    assert first.action_result is not None
    assert first.queue_entry is not None
    assert first.queue_entry.action_status == "success"
    assert second.approval is not None
    assert second.approval.status == "approved"
    assert second.action_result is not None


def test_reject_blocks_without_execution(monkeypatch, tmp_path: Path) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    dispatch_calls = 0

    def dispatcher(action_contract: object, **kwargs: object) -> dict[str, object]:
        nonlocal dispatch_calls
        dispatch_calls += 1
        raise AssertionError("rejected action must not dispatch")

    orchestrator = OwnerOrchestrator(dispatcher=dispatcher)
    interaction = orchestrator.process_request(
        request_text="Prepare owner-controlled file update",
        owner_id="owner-001",
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-US",
        action_type="write_file",
        target_type="filesystem",
        target_ref="runtime/out/ownerbox/rejected.txt",
        action_parameters={"path": "runtime/out/ownerbox/rejected.txt", "content": "rejected"},
    )

    assert interaction.approval is not None
    resolution = orchestrator.reject_action(interaction.approval.approval_id)

    assert dispatch_calls == 0
    assert resolution.approval is not None
    assert resolution.approval.status == "rejected"
    assert resolution.action_result is None
    assert resolution.queue_entry is not None
    assert resolution.queue_entry.action_status == "blocked"
    assert resolution.response_plan.summary_text == "Owner approval rejected. Action was not executed."


def test_trace_contains_approval_lifecycle(monkeypatch, tmp_path: Path) -> None:
    system_log_file = _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()

    def dispatcher(action_contract: object, **kwargs: object) -> dict[str, object]:
        return build_action_result_contract(
            action_id=str(dict(action_contract)["action_id"]),
            status="success",
            result_type="file_write",
            payload={"note": "write approved"},
        )

    orchestrator = OwnerOrchestrator(dispatcher=dispatcher)
    interaction = orchestrator.process_request(
        request_text="Prepare owner-controlled file update",
        owner_id="owner-001",
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-US",
        action_type="write_file",
        target_type="filesystem",
        target_ref="runtime/out/ownerbox/trace.txt",
        action_parameters={"path": "runtime/out/ownerbox/trace.txt", "content": "trace"},
    )

    assert interaction.approval is not None
    approval_id = interaction.approval.approval_id
    resolution = orchestrator.approve_action(approval_id)
    traces = _trace_entries(system_log_file)
    approval_events = [trace for trace in traces if trace.get("approval_id") == approval_id]
    created_key = compute_artifact_key(
        "ownerbox",
        "owner_approval_trace",
        f"{approval_id}:approval_created",
    )
    resolved_key = compute_artifact_key(
        "ownerbox",
        "owner_approval_trace",
        f"{approval_id}:approval_resolved",
    )

    assert [trace["event_name"] for trace in approval_events] == [
        "approval_created",
        "approval_resolved",
    ]
    assert approval_events[0]["approval_status"] == "pending"
    assert approval_events[1]["approval_status"] == "approved"
    assert approval_events[1]["action_id"] == resolution.approval.action_id
    assert memory_registry.get_artifact_by_logical_key(created_key) is not None
    assert memory_registry.get_artifact_by_logical_key(resolved_key) is not None


def test_unknown_action_is_blocked_safely() -> None:
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()

    def dispatcher(action_contract: object, **kwargs: object) -> dict[str, object]:
        raise AssertionError("unknown action must not dispatch")

    result = OwnerOrchestrator(dispatcher=dispatcher).process_request(
        request_text="Do an unsupported external thing",
        owner_id="owner-001",
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-US",
        action_type="custom_external_action",
        target_type="gateway",
        target_ref="custom",
    )

    assert result.request.status == "blocked"
    assert result.approval is None
    assert result.action_result is None
    assert result.response_plan.status == "blocked"
    assert "unknown action_type" in result.response_plan.summary_text


def test_browser_action_requires_approval_path(monkeypatch, tmp_path: Path) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    dispatch_calls = 0

    def dispatcher(action_contract: object, **kwargs: object) -> dict[str, object]:
        nonlocal dispatch_calls
        dispatch_calls += 1
        raise AssertionError("browser action must not dispatch before approval")

    result = OwnerOrchestrator(dispatcher=dispatcher).process_request(
        request_text="Open the owner workflow form",
        owner_id="owner-001",
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-US",
        action_type="browser_action",
        target_type="adapter",
        target_ref="browser",
        action_parameters={
            "operation": "submit_form",
            "url": "https://example.com/linkedin",
            "selector": "#lead-form",
            "timeout_seconds": 10,
        },
    )

    assert dispatch_calls == 0
    assert result.approval is not None
    assert result.approval.status == "pending"
    assert result.response_plan.trust_class == "high"
    assert result.response_plan.requires_confirmation is True
    assert result.response_plan.preview_text == "System wants to submit a form on https://example.com/linkedin."


def test_send_email_requires_approval_and_executes_once(monkeypatch, tmp_path: Path) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    dispatch_calls: list[str] = []

    def dispatcher(action_contract: object, **kwargs: object) -> dict[str, object]:
        dispatch_calls.append(str(dict(action_contract)["action_id"]))
        assert dict(kwargs)["dispatch_context"]["approval_id"]
        return build_action_result_contract(
            action_id=str(dict(action_contract)["action_id"]),
            status="success",
            result_type="email_send",
            payload={"summary": "Sent owner email", "metadata": {"operation": "send_email"}},
        )

    orchestrator = OwnerOrchestrator(dispatcher=dispatcher)
    interaction = orchestrator.process_request(
        request_text="Send the owner update email",
        owner_id="owner-001",
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-US",
        action_type="email_action",
        target_type="adapter",
        target_ref="email",
        action_parameters={
            "operation": "send_email",
            "to": ["owner@example.com"],
            "subject": "Owner update",
            "body": "Bounded body",
            "attachments": [],
        },
    )

    assert interaction.approval is not None
    assert interaction.response_plan.trust_class == "high"
    first = orchestrator.approve_action(interaction.approval.approval_id)
    second = orchestrator.approve_action(interaction.approval.approval_id)

    assert len(dispatch_calls) == 1
    assert first.action_result is not None
    assert first.action_result["payload"]["summary"] == "Sent owner email"
    assert second.action_result is not None
