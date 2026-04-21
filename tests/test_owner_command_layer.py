from __future__ import annotations

import json
import importlib
from pathlib import Path

from app.product.owner_command import owner_command
from app.orchestrator import task_factory, task_state_store


def _configure_state_backend(monkeypatch, tmp_path: Path) -> Path:
    monkeypatch.setattr(task_state_store, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )
    monkeypatch.setattr(
        task_factory,
        "TASK_SYSTEM_FILE",
        tmp_path / "data" / "task_system.json",
    )
    import runtime.system_log as system_log_module
    owner_command_module = importlib.import_module("app.product.owner_command")

    monkeypatch.setattr(
        system_log_module,
        "SYSTEM_LOG_FILE",
        tmp_path / "runtime" / "logs" / "system.log",
    )
    monkeypatch.setattr(
        owner_command_module,
        "OWNER_COMMAND_LOG_FILE",
        tmp_path / "runtime" / "logs" / "owner_commands.log",
    )
    task_factory.clear_task_runtime_store()
    return tmp_path / "data" / "task_system.json"


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _build_task(
    store_path: Path,
    *,
    task_id: str,
    status: str = "created",
    approval_status: str = "approved",
    priority: str = "high",
    execution_mode: str = "auto",
) -> dict[str, object]:
    return task_factory.create_task(
        {
            "task_id": task_id,
            "status": status,
            "intent": "business_signal_response",
            "task_type": "lead",
            "execution_mode": execution_mode,
            "approval_status": approval_status,
            "payload": {
                "summary": f"owner-command:{task_id}",
                "priority": priority,
                "decision": {
                    "decision_id": f"{task_id.lower()}_decision",
                    "decision_type": "low_review_count_decision",
                    "priority": priority,
                    "confidence": 1.0,
                    "reason": "reduced_visibility",
                    "recommended_actions": ["request_more_reviews"],
                    "execution_mode": execution_mode,
                },
                "recommended_action": "request_more_reviews",
            },
        },
        store_path=store_path,
    )


def test_owner_command_override_decision_updates_task_payload_and_logs(
    monkeypatch, tmp_path: Path
) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    created = _build_task(store_path, task_id="DF-OWNER-OVERRIDE-V1")

    result = owner_command(
        {
            "command_type": "override_decision",
            "who": "owner",
            "why": "manual correction after call review",
            "task_id": created["task_id"],
            "changes": {
                "reason": "owner_verified_priority_shift",
                "recommended_action": "investigate_visibility_drop",
                "confidence": 0.45,
            },
        },
        store_path=store_path,
    )
    restored = task_factory.get_task(created["task_id"], store_path)
    assert restored is not None

    decision = dict(dict(restored["payload"]).get("decision", {}))
    assert result["status"] == "accepted"
    assert decision["reason"] == "owner_verified_priority_shift"
    assert decision["recommended_actions"] == ["investigate_visibility_drop"]
    assert decision["confidence"] == 0.45
    assert dict(restored["payload"])["recommended_action"] == "investigate_visibility_drop"

    system_log = _read_jsonl(tmp_path / "runtime" / "logs" / "system.log")
    owner_event = next(item for item in system_log if item["event_type"] == "owner_command")
    assert owner_event["details"]["user"] == "owner"
    assert owner_event["details"]["target"] == "df-owner-override-v1_decision"
    assert owner_event["details"]["change_applied"]["reason"] == "manual correction after call review"

    command_log = _read_jsonl(tmp_path / "runtime" / "logs" / "owner_commands.log")
    command_entry = next(item for item in command_log if item["event_type"] == "command_log")
    assert command_entry["details"]["user"] == "owner"
    assert command_entry["details"]["command_type"] == "override_decision"
    assert command_entry["details"]["target"] == "df-owner-override-v1_decision"
    assert command_entry["details"]["change_applied"]["changes"][0]["reason"] == "owner_verified_priority_shift"
    assert command_entry["timestamp"]


def test_owner_command_change_priority_updates_task_and_decision(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    created = _build_task(store_path, task_id="DF-OWNER-PRIORITY-V1", priority="medium")

    result = owner_command(
        {
            "command_type": "change_priority",
            "who": "owner",
            "why": "customer escalation",
            "task_id": created["task_id"],
            "priority": "urgent",
        },
        store_path=store_path,
    )
    restored = task_factory.get_task(created["task_id"], store_path)
    assert restored is not None

    assert result["changed"]["priority"] == "urgent"
    assert dict(restored["payload"])["priority"] == "urgent"
    assert dict(dict(restored["payload"]).get("decision", {}))["priority"] == "urgent"


def test_owner_command_approve_all_pending_uses_approval_gate(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    first = _build_task(
        store_path,
        task_id="DF-OWNER-APPROVE-1",
        status="awaiting_approval",
        approval_status="pending",
        execution_mode="confirmation",
    )
    second = _build_task(
        store_path,
        task_id="DF-OWNER-APPROVE-2",
        status="awaiting_approval",
        approval_status="pending",
        execution_mode="confirmation",
    )

    result = owner_command(
        {
            "command_type": "approve_all_pending",
            "who": "owner",
            "why": "bulk review complete",
        },
        store_path=store_path,
    )

    first_restored = task_factory.get_task(first["task_id"], store_path)
    second_restored = task_factory.get_task(second["task_id"], store_path)
    assert first_restored is not None
    assert second_restored is not None
    assert result["approved_count"] == 2
    assert first_restored["status"] == "VALIDATED"
    assert second_restored["status"] == "VALIDATED"
    assert first_restored["approval_status"] == "approved"
    assert second_restored["approval_status"] == "approved"


def test_owner_command_reject_task_fails_non_terminal_task(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    created = _build_task(store_path, task_id="DF-OWNER-REJECT-V1")

    result = owner_command(
        {
            "command_type": "reject_task",
            "who": "owner",
            "why": "bad fit for customer request",
            "task_id": created["task_id"],
        },
        store_path=store_path,
    )
    restored = task_factory.get_task(created["task_id"], store_path)
    assert restored is not None

    assert result["changed"]["status"] == "FAILED"
    assert restored["status"] == "FAILED"
    assert restored["approval_status"] == "rejected"
    assert restored["rejected_by"] == "owner"


def test_owner_command_force_recompute_marks_task_for_controlled_recompute(
    monkeypatch, tmp_path: Path
) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    created = _build_task(store_path, task_id="DF-OWNER-RECOMPUTE-V1")

    result = owner_command(
        {
            "command_type": "force_recompute",
            "who": "owner",
            "why": "fresh input arrived from client",
            "task_id": created["task_id"],
        },
        store_path=store_path,
    )
    restored = task_factory.get_task(created["task_id"], store_path)
    assert restored is not None

    payload = dict(restored["payload"])
    decision = dict(payload.get("decision", {}))
    assert result["changed"]["force_recompute"] is True
    assert dict(payload["force_recompute"])["requested"] is True
    assert decision["recompute_requested"] is True
    assert decision["recompute_reason"] == "fresh input arrived from client"
