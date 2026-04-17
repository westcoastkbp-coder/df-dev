from __future__ import annotations

import json
from pathlib import Path

import pytest

import scripts.run_codex_task as run_codex_task_module
from app.memory import memory_registry
from app.policy.execution_invariants import check_invariants
from app.state.state_store import set_state
from app.storage import storage_adapter
from app.trace.execution_trace import create_execution_trace
from scripts.run_codex_task import run_codex_task


def _write_policy(tmp_path: Path) -> Path:
    policy_path = tmp_path / "config" / "contour_policy.json"
    policy_path.parent.mkdir(parents=True, exist_ok=True)
    policy_path.write_text(
        json.dumps(
            {
                "contours": {
                    "df-dev": {
                        "working_root": str(tmp_path / "df-dev"),
                    },
                    "ownerbox": {
                        "working_root": str(tmp_path / "ownerbox"),
                    },
                }
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return policy_path


def _configure_storage(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(storage_adapter, "POLICY_FILE", _write_policy(tmp_path))
    monkeypatch.setattr(
        memory_registry,
        "REGISTRY_FILE",
        tmp_path / "df-system" / "memory_registry.json",
    )


def _state_source(tmp_path: Path, domain: str, name: str) -> str:
    domain_root = "ownerbox" if domain == "ownerbox" else "df-dev"
    return str(tmp_path / domain_root / "artifacts" / f"{name}.json")


def _task_packet(
    *,
    task_id: str,
    resource_id: str | None = None,
    domain: str = "dev",
    use_tool_call: bool = False,
) -> dict[str, object]:
    payload: dict[str, object] = {"domain": domain}
    if resource_id is not None:
        payload["resource_id"] = resource_id
    packet: dict[str, object] = {
        "task_id": task_id,
        "instruction": f"Execute task {task_id}",
        "constraints": "Modify only necessary parts.",
        "success_criteria": "Execution either blocks safely or writes the artifact.",
        "memory_context": {"domain": domain, "type": "task"},
        "payload": payload,
        "system_context": {},
    }
    if use_tool_call:
        packet["tool_call"] = {
            "tool_name": "google_docs.create_document",
            "input": {
                "title": f"Task {task_id}",
                "content": "This should only execute when invariants allow it.",
            },
        }
    return packet


def _store_trace(tmp_path: Path, *, task_id: str, status: str, relative_name: str) -> None:
    trace_payload = create_execution_trace(
        {
            "task_id": task_id,
            "payload": {"domain": "dev"},
        },
        {
            "resolver": {"resolved_memory": []},
            "memory_policy": {"allowed": True, "reason": "no_recent_duplicate"},
            "conflict_gate": {"allowed": True, "reason": "no_cross_domain_conflict"},
            "replay_protection": {"allowed": True, "reason": "not_previously_executed"},
            "execution_invariants": {"allowed": True, "violations": [], "action": "allow"},
            "final_decision": {"action": "execute" if status == "executed" else "block"},
            "execution_status": status,
        },
    )
    storage_adapter.save_artifact(
        "dev",
        "execution_trace",
        trace_payload,
        overwrite=True,
        relative_path=Path("traces") / relative_name,
    )


def _assert_violation(decision: dict[str, object], violation_type: str) -> None:
    assert decision["allowed"] is False
    assert decision["action"] == "block"
    assert any(
        isinstance(item, dict) and item.get("type") == violation_type
        for item in decision["violations"]
    )


def test_check_invariants_blocks_replay_violation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_storage(monkeypatch, tmp_path)
    _store_trace(
        tmp_path,
        task_id="DF-INVARIANT-REPLAY-V1",
        status="executed",
        relative_name="DF-INVARIANT-REPLAY-V1.json",
    )

    decision = check_invariants(_task_packet(task_id="DF-INVARIANT-REPLAY-V1"))

    _assert_violation(decision, "replay_execution")


def test_check_invariants_blocks_conflicting_active_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_storage(monkeypatch, tmp_path)
    set_state(
        "resource",
        "crew-west",
        "reserved",
        _state_source(tmp_path, "dev", "resource-dev"),
        domain="dev",
    )
    set_state(
        "conflict",
        "crew-west",
        "conflict_active",
        _state_source(tmp_path, "ownerbox", "conflict-ownerbox"),
        domain="ownerbox",
    )

    decision = check_invariants(
        _task_packet(task_id="DF-INVARIANT-CONFLICT-V1", resource_id="crew-west")
    )

    _assert_violation(decision, "conflicting_active_state")


def test_check_invariants_blocks_resolved_entity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_storage(monkeypatch, tmp_path)
    set_state(
        "resource",
        "crew-west",
        "resolved",
        _state_source(tmp_path, "dev", "resource-dev-resolved"),
        domain="dev",
    )

    decision = check_invariants(
        _task_packet(task_id="DF-INVARIANT-RESOLVED-V1", resource_id="crew-west")
    )

    _assert_violation(decision, "resolved_entity")


def test_check_invariants_blocks_cross_domain_violation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_storage(monkeypatch, tmp_path)
    set_state(
        "resource",
        "crew-west",
        "owner_locked",
        _state_source(tmp_path, "ownerbox", "resource-owner"),
        domain="ownerbox",
    )

    decision = check_invariants(
        _task_packet(task_id="DF-INVARIANT-DOMAIN-V1", resource_id="crew-west")
    )

    _assert_violation(decision, "domain_isolation")


def test_check_invariants_blocks_when_tracked_state_is_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_storage(monkeypatch, tmp_path)

    decision = check_invariants(
        _task_packet(task_id="DF-INVARIANT-MISSING-STATE-V1", resource_id="crew-west")
    )

    _assert_violation(decision, "state_consistency")


def test_check_invariants_allows_valid_execution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_storage(monkeypatch, tmp_path)
    set_state(
        "resource",
        "crew-west",
        "reserved",
        _state_source(tmp_path, "dev", "resource-dev-valid"),
        domain="dev",
    )

    decision = check_invariants(
        _task_packet(task_id="DF-INVARIANT-ALLOW-V1", resource_id="crew-west")
    )

    assert decision == {
        "allowed": True,
        "violations": [],
        "action": "allow",
    }


def test_run_codex_task_blocks_when_invariant_conflict_state_detected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure_storage(monkeypatch, tmp_path)
    set_state(
        "resource",
        "crew-west",
        "reserved",
        _state_source(tmp_path, "dev", "resource-dev"),
        domain="dev",
    )
    set_state(
        "conflict",
        "crew-west",
        "conflict_active",
        _state_source(tmp_path, "ownerbox", "conflict-ownerbox"),
        domain="ownerbox",
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "_contextual_tool_call",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("execution should not continue after invariant conflict block")
        ),
    )

    task, artifact_path = run_codex_task(
        _task_packet(
            task_id="901",
            resource_id="crew-west",
            use_tool_call=True,
        ),
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    output = capsys.readouterr().out
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))

    assert "[INVARIANT] violation type=conflicting_active_state task=901" in output
    assert task["execution_invariants_decision"]["allowed"] is False
    assert task["failure_reason"] == "conflicting_active_state"
    assert artifact["result_type"] == "execution_invariants_blocked"
    assert artifact["reason"] == "conflicting_active_state"


def test_run_codex_task_blocks_when_invariant_resolved_entity_detected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_storage(monkeypatch, tmp_path)
    set_state(
        "resource",
        "crew-west",
        "resolved",
        _state_source(tmp_path, "dev", "resource-dev-resolved"),
        domain="dev",
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "_contextual_tool_call",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("execution should not continue after resolved-entity block")
        ),
    )

    task, artifact_path = run_codex_task(
        _task_packet(
            task_id="902",
            resource_id="crew-west",
            use_tool_call=True,
        ),
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))

    assert task["failure_reason"] == "resolved_entity"
    assert artifact["result_type"] == "execution_invariants_blocked"
    assert artifact["reason"] == "resolved_entity"


def test_run_codex_task_blocks_when_invariant_cross_domain_detected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_storage(monkeypatch, tmp_path)
    set_state(
        "resource",
        "crew-west",
        "owner_locked",
        _state_source(tmp_path, "ownerbox", "resource-owner"),
        domain="ownerbox",
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "_contextual_tool_call",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("execution should not continue after domain-isolation block")
        ),
    )

    task, artifact_path = run_codex_task(
        _task_packet(
            task_id="903",
            resource_id="crew-west",
            use_tool_call=True,
        ),
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))

    assert task["failure_reason"] == "domain_isolation"
    assert artifact["result_type"] == "execution_invariants_blocked"
    assert artifact["reason"] == "domain_isolation"


def test_run_codex_task_blocks_when_invariant_state_is_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_storage(monkeypatch, tmp_path)
    monkeypatch.setattr(
        run_codex_task_module,
        "_contextual_tool_call",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("execution should not continue after state-consistency block")
        ),
    )

    task, artifact_path = run_codex_task(
        _task_packet(
            task_id="904",
            resource_id="crew-west",
            use_tool_call=True,
        ),
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))

    assert task["failure_reason"] == "state_consistency"
    assert artifact["result_type"] == "execution_invariants_blocked"
    assert artifact["reason"] == "state_consistency"


def test_run_codex_task_blocks_replay_violation_in_execution_flow(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_storage(monkeypatch, tmp_path)

    first_task, first_artifact_path = run_codex_task(
        _task_packet(task_id="905"),
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )
    second_task, second_artifact_path = run_codex_task(
        _task_packet(task_id="905"),
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    assert first_artifact_path == second_artifact_path
    assert first_task["task_id"] == "905" or first_task["task_id"] == 905
    assert second_task["replay_protection_decision"]["allowed"] is False
    assert second_task["failure_reason"] == "already_executed"


def test_run_codex_task_allows_valid_invariant_checked_execution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_storage(monkeypatch, tmp_path)
    set_state(
        "resource",
        "crew-west",
        "reserved",
        _state_source(tmp_path, "dev", "resource-dev-valid"),
        domain="dev",
    )

    task, artifact_path = run_codex_task(
        _task_packet(task_id="906", resource_id="crew-west"),
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    assert task["execution_invariants_decision"] == {
        "allowed": True,
        "violations": [],
        "action": "allow",
    }
    assert artifact_path.read_text(encoding="utf-8") == "Execute task 906"
