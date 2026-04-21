from __future__ import annotations

import json
from pathlib import Path

from app.execution.action_contract import build_action_result_contract
from app.execution.action_dispatcher import dispatch_action
from app.memory import memory_registry
from app.orchestrator import task_memory, task_state_store
from app.ownerbox.domain import (
    create_owner_action_scope,
    create_owner_domain,
    create_owner_memory_scope,
    create_owner_trust_profile,
)
from app.ownerbox.owner_action_queue import (
    OwnerActionQueue,
    create_owner_action_queue_entry,
)
from app.ownerbox.owner_orchestrator import OwnerOrchestrator
from app.ownerbox.owner_request import create_owner_request
from app.ownerbox.owner_response_plan import create_owner_response_plan
from app.ownerbox.owner_session import create_owner_session
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


def test_owner_session_can_be_created() -> None:
    session = create_owner_session(
        owner_id="owner-001",
        active_language="en-us",
        context_ref="ownerbox/main",
        owner_session_id="owner-session-001",
        started_at="2026-04-14T12:00:00Z",
    )

    assert session.to_dict() == {
        "owner_session_id": "owner-session-001",
        "owner_id": "owner-001",
        "domain_type": "ownerbox",
        "active_language": "en-US",
        "context_ref": "ownerbox/main",
        "request_count": 0,
        "started_at": "2026-04-14T12:00:00Z",
        "last_request_at": "2026-04-14T12:00:00Z",
        "status": "active",
    }


def test_owner_request_can_be_created_and_linked_to_session() -> None:
    session = create_owner_session(
        owner_id="owner-001",
        active_language="en-US",
        owner_session_id="owner-session-001",
        started_at="2026-04-14T12:00:00Z",
    )

    request = create_owner_request(
        owner_session_id=session.owner_session_id,
        owner_id=session.owner_id,
        request_text="Show pending queue status",
        detected_language="en-us",
        request_id="owner-request-001",
        created_at="2026-04-14T12:01:00Z",
    )

    assert request.owner_session_id == session.owner_session_id
    assert request.request_type == "status_check"
    assert request.priority_class == "medium"
    assert request.normalized_payload == {
        "priority_class": "medium",
        "request_type": "status_check",
        "summary": "Show pending queue status",
    }


def test_owner_response_plan_can_be_created_deterministically() -> None:
    response_plan = create_owner_response_plan(
        response_plan_id="owner-response-plan-001",
        owner_session_id="owner-session-001",
        owner_id="owner-001",
        response_type="summary_text",
        target_language="en-us",
        summary_text="Owner queue is clear.",
        action_refs=["owner-action-001"],
        requires_confirmation=False,
        requires_high_trust=False,
        metadata={"owner_session_id": "owner-session-001"},
        created_at="2026-04-14T12:02:00Z",
        status="planned",
    )

    assert response_plan.to_dict() == {
        "response_plan_id": "owner-response-plan-001",
        "owner_session_id": "owner-session-001",
        "owner_id": "owner-001",
        "response_type": "summary_text",
        "target_language": "en-US",
        "summary_text": "Owner queue is clear.",
        "action_refs": ["owner-action-001"],
        "requires_confirmation": False,
        "requires_high_trust": False,
        "approval_id": None,
        "trust_class": None,
        "preview_text": None,
        "metadata": {"owner_session_id": "owner-session-001"},
        "created_at": "2026-04-14T12:02:00Z",
        "status": "planned",
    }


def test_owner_action_queue_entry_can_be_created() -> None:
    queue = OwnerActionQueue()
    entry = queue.enqueue(
        create_owner_action_queue_entry(
            queue_entry_id="owner-queue-001",
            owner_id="owner-001",
            action_id="owner-action-001",
            action_type="openai_request",
            action_status="success",
            requires_confirmation=False,
            requires_high_trust=False,
            priority_class="high",
            created_at="2026-04-14T12:03:00Z",
            updated_at="2026-04-14T12:03:05Z",
        )
    )

    assert entry.to_dict() == {
        "queue_entry_id": "owner-queue-001",
        "owner_id": "owner-001",
        "action_id": "owner-action-001",
        "action_type": "OPENAI_REQUEST",
        "action_status": "success",
        "requires_confirmation": False,
        "requires_high_trust": False,
        "priority_class": "high",
        "created_at": "2026-04-14T12:03:00Z",
        "updated_at": "2026-04-14T12:03:05Z",
    }
    assert queue.list_entries(owner_id="owner-001")[0] == entry


def test_owner_orchestrator_routes_through_boundary_and_dispatcher(
    monkeypatch, tmp_path: Path
) -> None:
    system_log_file = _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    captured: dict[str, object] = {}

    def openai_executor(action_contract: object) -> dict[str, object]:
        assert isinstance(action_contract, dict)
        captured["action_contract"] = dict(action_contract)
        return build_action_result_contract(
            action_id=str(action_contract["action_id"]),
            status="success",
            result_type="text_generation",
            payload={
                "text": "Owner-facing status summary.",
                "metadata": {
                    "input_tokens": 11,
                    "output_tokens": 5,
                    "total_tokens": 16,
                },
            },
        )

    def dispatcher(action_contract: object, **kwargs: object) -> dict[str, object]:
        captured["dispatch_kwargs"] = dict(kwargs)
        return dispatch_action(
            action_contract, openai_executor=openai_executor, **kwargs
        )

    result = OwnerOrchestrator(dispatcher=dispatcher).process_request(
        request_text="Show current owner queue status",
        owner_id="owner-001",
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-us",
        memory_records=[
            {
                "id": "owner-artifact-001",
                "domain": "ownerbox",
                "memory_class": "artifact",
                "status": "active",
                "truth_level": "working",
                "execution_role": "output",
                "created_at": "2026-04-14T11:55:00Z",
                "updated_at": "2026-04-14T11:55:00Z",
                "tags": ["owner"],
                "refs": ["owner:owner-001", "domain:ownerbox-main", "artifact:queue"],
                "payload": {"summary": "owner-safe memory"},
                "type": "owner_note",
            },
            {
                "id": "dev-artifact-001",
                "domain": "dev",
                "memory_class": "artifact",
                "status": "active",
                "truth_level": "working",
                "execution_role": "output",
                "created_at": "2026-04-14T11:56:00Z",
                "updated_at": "2026-04-14T11:56:00Z",
                "tags": ["dev"],
                "refs": ["owner:owner-001", "domain:ownerbox-main", "artifact:leak"],
                "payload": {"summary": "dev-secret leak"},
                "type": "dev_note",
            },
        ],
    )

    prompt = str(dict(captured["action_contract"])["parameters"]["prompt"])
    dispatch_kwargs = dict(captured["dispatch_kwargs"])
    traces = _trace_entries(system_log_file)

    assert dispatch_kwargs["memory_domain"] == "ownerbox"
    assert dict(dispatch_kwargs["domain_binding"])["domain_type"] == "ownerbox"
    assert (
        result.owner_context["boundary_application"]["blocked_cross_domain_count"] == 1
    )
    assert result.owner_context["boundary_application"]["resolved_memory_count"] == 1
    assert "owner-safe memory" in prompt
    assert "dev-secret leak" not in prompt
    assert result.request.status == "completed"
    assert result.approval is None
    assert result.action_contract is not None
    assert result.action_result is not None
    assert (
        result.action_result["payload"]["metadata"]["domain_metadata"]["owner_id"]
        == "owner-001"
    )
    assert result.response_plan.trust_class == "low"
    assert result.response_plan.approval_id is None
    assert result.response_plan.preview_text is not None
    assert result.response_plan.metadata["trace_metadata"]["domain_type"] == "ownerbox"
    assert result.queue_entry is not None
    assert result.queue_entry.owner_id == "owner-001"
    assert result.trace_metadata["owner_session_id"] == result.session.owner_session_id
    assert result.trace_metadata["request_id"] == result.request.request_id
    assert (
        result.trace_metadata["response_plan_id"]
        == result.response_plan.response_plan_id
    )
    assert traces[-1]["domain_metadata"]["owner_id"] == "owner-001"
    assert traces[-1]["result_status"] == "success"


def test_malformed_owner_input_is_handled_safely() -> None:
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()

    def dispatcher(action_contract: object, **kwargs: object) -> dict[str, object]:
        raise AssertionError("empty owner input must not dispatch")

    result = OwnerOrchestrator(dispatcher=dispatcher).process_request(
        request_text="   ",
        owner_id="owner-001",
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-us",
    )

    assert result.request.status == "blocked"
    assert result.request.request_type == "input_error"
    assert result.action_contract is None
    assert result.action_result is None
    assert result.queue_entry is None
    assert result.response_plan.response_type == "input_error"
    assert result.trace_metadata["action_id"] is None
    assert result.trace_metadata["result_status"] == "blocked"
