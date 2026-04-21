from __future__ import annotations

import json
from pathlib import Path

from app.execution.action_contract import build_action_result_contract
from app.execution.action_dispatcher import dispatch_action
from app.memory import memory_registry
from app.orchestrator import task_memory, task_state_store
from app.ownerbox.context_boundary import OwnerRequestContextRef, assemble_owner_context
from app.ownerbox.domain import (
    build_ownerbox_domain_binding,
    create_owner_action_scope,
    create_owner_domain,
    create_owner_memory_scope,
    create_owner_trust_profile,
)
from app.trace.execution_trace import create_execution_trace
from app.voice.voice_orchestrator import VoiceOrchestrator
import app.execution.action_dispatcher as action_dispatcher_module
import runtime.system_log as system_log_module


def _configure_dispatch_runtime(monkeypatch, tmp_path: Path) -> Path:
    system_log_file = tmp_path / "runtime" / "logs" / "system.log"
    task_log_file = tmp_path / "runtime" / "logs" / "tasks.log"
    task_memory_file = tmp_path / "runtime" / "state" / "task_memory.json"
    registry_file = tmp_path / "df-system" / "memory_registry.json"

    monkeypatch.setattr(system_log_module, "SYSTEM_LOG_FILE", system_log_file)
    monkeypatch.setattr(system_log_module, "TASK_LOG_FILE", task_log_file)
    monkeypatch.setattr(action_dispatcher_module, "ROOT_DIR", tmp_path)
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


def test_ownerbox_domain_structures_are_explicit_and_bounded() -> None:
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()

    assert owner_domain.to_dict() == {
        "domain_id": "ownerbox-main",
        "domain_type": "ownerbox",
        "owner_id": "owner-001",
        "trust_level": "high",
        "memory_scope_ref": "owner-memory-scope-v1",
        "action_scope_ref": "owner-action-scope-v1",
        "policy_scope_ref": "owner-policy-scope-v1",
        "created_at": owner_domain.created_at,
        "status": "active",
    }
    assert memory_scope.to_dict()["blocked_memory_classes"] == ["conflict"]
    assert "SEND_EMAIL" in action_scope.blocked_action_types
    assert action_scope.allowed_action_types == (
        "OPENAI_REQUEST",
        "READ_FILE",
        "WRITE_FILE",
        "BROWSER_ACTION",
        "EMAIL_ACTION",
        "PRINT_DOCUMENT",
    )
    assert action_scope.requires_confirmation_for == ("WRITE_FILE", "PRINT_DOCUMENT")
    assert trust_profile.to_dict() == {
        "trust_profile_id": "owner-trust-v1",
        "owner_id": "owner-001",
        "trust_class": "high_trust_owner",
        "confirmation_policy_ref": "owner-confirmation-v1",
        "approval_mode": "structured_confirmation",
        "device_binding_ref": None,
        "status": "active",
    }


def test_owner_context_boundary_filters_cross_domain_and_transcript_memory() -> None:
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    request_ref = OwnerRequestContextRef(
        request_ref="owner-request-001",
        owner_id="owner-001",
        session_ref="voice-session-001",
        trace_id="trace-owner-001",
        turn_ref="voice-turn-001",
        memory_context={"memory_class": "artifact"},
    )

    package = assemble_owner_context(
        request_ref=request_ref,
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        memory_records=[
            {
                "id": "owner-artifact-older",
                "domain": "ownerbox",
                "memory_class": "artifact",
                "status": "active",
                "truth_level": "working",
                "execution_role": "output",
                "created_at": "2026-04-14T10:00:00Z",
                "updated_at": "2026-04-14T10:00:00Z",
                "tags": ["owner"],
                "refs": [
                    "owner:owner-001",
                    "domain:ownerbox-main",
                    "artifact:estimate",
                ],
                "payload": {"summary": "older"},
                "type": "owner_note",
            },
            {
                "id": "owner-artifact-latest",
                "domain": "ownerbox",
                "memory_class": "artifact",
                "status": "active",
                "truth_level": "working",
                "execution_role": "output",
                "created_at": "2026-04-14T11:00:00Z",
                "updated_at": "2026-04-14T11:00:00Z",
                "tags": ["owner"],
                "refs": ["owner:owner-001", "domain:ownerbox-main", "artifact:plan"],
                "payload": {"summary": "latest"},
                "type": "owner_note",
            },
            {
                "id": "cross-domain-dev",
                "domain": "dev",
                "memory_class": "artifact",
                "status": "active",
                "truth_level": "working",
                "execution_role": "output",
                "created_at": "2026-04-14T11:05:00Z",
                "updated_at": "2026-04-14T11:05:00Z",
                "tags": [],
                "refs": ["owner:owner-001", "domain:ownerbox-main", "artifact:leak"],
                "payload": {"summary": "should not cross"},
                "type": "owner_note",
            },
            {
                "id": "owner-transcript",
                "domain": "ownerbox",
                "memory_class": "artifact",
                "status": "active",
                "truth_level": "working",
                "execution_role": "output",
                "created_at": "2026-04-14T11:10:00Z",
                "updated_at": "2026-04-14T11:10:00Z",
                "tags": [],
                "refs": [
                    "owner:owner-001",
                    "domain:ownerbox-main",
                    "transcript:call-001",
                ],
                "payload": {"summary": "raw transcript"},
                "type": "call_transcript",
            },
            {
                "id": "missing-owner-ref",
                "domain": "ownerbox",
                "memory_class": "artifact",
                "status": "active",
                "truth_level": "working",
                "execution_role": "output",
                "created_at": "2026-04-14T11:15:00Z",
                "updated_at": "2026-04-14T11:15:00Z",
                "tags": [],
                "refs": [],
                "payload": {"summary": "unbound"},
                "type": "owner_note",
            },
        ],
    )

    assert [entry["id"] for entry in package["resolved_memory"]] == [
        "owner-artifact-latest",
        "owner-artifact-older",
    ]
    assert package["boundary_application"] == {
        "domain_type": "ownerbox",
        "owner_id": "owner-001",
        "memory_scope_ref": "owner-memory-scope-v1",
        "action_scope_ref": "owner-action-scope-v1",
        "policy_scope_ref": "owner-policy-scope-v1",
        "scope_id": "owner-memory-scope-v1",
        "allowed_action_types": [
            "OPENAI_REQUEST",
            "READ_FILE",
            "WRITE_FILE",
            "BROWSER_ACTION",
            "EMAIL_ACTION",
            "PRINT_DOCUMENT",
        ],
        "blocked_cross_domain_count": 1,
        "blocked_transcript_count": 1,
        "filtered_out_count": 3,
        "resolved_memory_count": 2,
    }
    assert package["trace_metadata"]["domain_type"] == "ownerbox"
    assert package["trace_metadata"]["owner_id"] == "owner-001"
    assert package["trace_metadata"]["scope_refs"] == [
        "owner-memory-scope-v1",
        "owner-action-scope-v1",
        "owner-policy-scope-v1",
    ]
    assert package["trace_metadata"]["turn_ref"] == "voice-turn-001"


def test_execution_trace_exposes_ownerbox_domain_metadata() -> None:
    owner_domain, _memory_scope, _action_scope, trust_profile = _owner_boundary_bundle()
    domain_binding = build_ownerbox_domain_binding(
        owner_domain,
        trust_profile=trust_profile,
        request_ref="owner-request-001",
        session_ref="voice-session-001",
        trace_id="trace-owner-001",
    )

    trace = create_execution_trace(
        {
            "task_id": "owner-task-001",
            "domain_binding": domain_binding,
        },
        {
            "final_decision": {
                "action": "execute",
                "artifact_id": "owner-task-001.txt",
            },
            "execution_status": "executed",
        },
    )

    assert trace["domain"] == "ownerbox"
    assert trace["domain_metadata"] == {
        "domain_id": "ownerbox-main",
        "domain_type": "ownerbox",
        "owner_id": "owner-001",
        "trust_level": "high",
        "memory_scope_ref": "owner-memory-scope-v1",
        "action_scope_ref": "owner-action-scope-v1",
        "policy_scope_ref": "owner-policy-scope-v1",
        "trust_profile_id": "owner-trust-v1",
        "trust_class": "high_trust_owner",
        "request_ref": "owner-request-001",
        "session_ref": "voice-session-001",
        "trace_id": "trace-owner-001",
        "scope_refs": [
            "owner-memory-scope-v1",
            "owner-action-scope-v1",
            "owner-policy-scope-v1",
        ],
    }


def test_action_dispatcher_preserves_ownerbox_binding_in_trace_and_registry(
    monkeypatch,
    tmp_path: Path,
) -> None:
    system_log_file = _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, _memory_scope, _action_scope, trust_profile = _owner_boundary_bundle()
    domain_binding = build_ownerbox_domain_binding(
        owner_domain,
        trust_profile=trust_profile,
        request_ref="owner-request-001",
        session_ref="voice-session-001",
        trace_id="trace-owner-001",
    )

    result = dispatch_action(
        {
            "action_id": "act-ownerbox-001",
            "action_type": "openai_request",
            "target_type": "adapter",
            "target_ref": "openai",
            "parameters": {
                "model": "gpt-5-mini",
                "prompt": "Return one bounded sentence.",
                "max_tokens": 32,
                "temperature": 0.2,
            },
            "execution_mode": "dry_run",
            "confirmation_policy": "not_required",
            "idempotency_key": "ownerbox:act-ownerbox-001",
            "requested_by": "ownerbox_boundary_test",
            "timestamp": "2026-04-14T12:00:00Z",
            "schema_version": "v1",
        },
        memory_domain="ownerbox",
        domain_binding=domain_binding,
    )

    metadata = result["payload"]["metadata"]
    assert metadata["domain_metadata"]["owner_id"] == "owner-001"
    assert metadata["domain_metadata"]["memory_scope_ref"] == "owner-memory-scope-v1"

    trace_entries = _trace_entries(system_log_file)
    assert trace_entries[-1]["memory_domain"] == "ownerbox"
    assert trace_entries[-1]["domain_metadata"]["owner_id"] == "owner-001"

    trace_path = Path(str(metadata["trace_artifact_path"]))
    trace_record = json.loads(trace_path.read_text(encoding="utf-8"))
    assert trace_record["domain"] == "ownerbox"
    assert set(trace_record["refs"]) >= {
        "action:act-ownerbox-001",
        "domain:ownerbox-main",
        "owner:owner-001",
        "scope:owner-memory-scope-v1",
        "scope:owner-action-scope-v1",
        "scope:owner-policy-scope-v1",
        "trust_profile:owner-trust-v1",
    }
    assert trace_record["payload"]["domain_metadata"]["owner_id"] == "owner-001"

    registry_entry = memory_registry.get_artifact_by_logical_key(
        "ownerbox:execution_trace:act-ownerbox-001"
    )
    assert registry_entry is not None
    assert registry_entry["domain"] == "ownerbox"


def test_voice_orchestrator_preserves_ownerbox_binding_through_session_and_dispatch() -> (
    None
):
    owner_domain, _memory_scope, _action_scope, trust_profile = _owner_boundary_bundle()
    domain_binding = build_ownerbox_domain_binding(
        owner_domain,
        trust_profile=trust_profile,
        request_ref="owner-request-voice-001",
        session_ref="voice-session-owner-001",
        trace_id="trace-owner-voice-001",
    )
    captured: dict[str, object] = {}

    def dispatcher(action_contract: object, **kwargs: object) -> dict[str, object]:
        captured["action_contract"] = action_contract
        captured.update(kwargs)
        return build_action_result_contract(
            action_id="voice-action-fixed-001",
            status="success",
            result_type="text_generation",
            payload={
                "text": "Owner context is bounded.",
                "metadata": {
                    "dispatcher_latency_ms": 7,
                },
            },
        )

    result = VoiceOrchestrator(dispatcher=dispatcher).process_turn(
        input_text="Summarize owner priorities.",
        caller_id="caller-owner-001",
        channel_type="phone",
        active_language="en-US",
        detected_language="en-US",
        domain_binding=domain_binding,
    )

    assert captured["memory_domain"] == "ownerbox"
    assert captured["domain_binding"]["owner_id"] == "owner-001"
    assert result.session.domain_binding["domain_type"] == "ownerbox"
    assert result.session.domain_binding["owner_id"] == "owner-001"
    assert result.turn.trace_metadata is not None
    assert (
        result.turn.trace_metadata.domain_binding["memory_scope_ref"]
        == "owner-memory-scope-v1"
    )
