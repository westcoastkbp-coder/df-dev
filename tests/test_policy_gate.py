from __future__ import annotations

import json
from pathlib import Path

import app.policy.policy_gate as policy_gate_module
from app.policy.policy_gate import evaluate_external_action_policy, evaluate_policy
from app.product.runner import dispatch_action_trigger


def test_evaluate_policy_allows_valid_action() -> None:
    result = evaluate_policy(
        {
            "action_type": "WRITE_FILE",
            "payload": {
                "task_id": "DF-POLICY-GATE-V1",
                "path": r"runtime\out\policy-gate.log",
                "content": "ok",
            },
        },
        {
            "task_id": "DF-POLICY-GATE-V1",
            "status": "running",
        },
    )

    assert result.execution_allowed is True
    assert result.reason == ""
    assert result.policy_trace["known_action_type"] is True
    assert result.policy_trace["task_state_allows_execution"] is True


def test_evaluate_policy_blocks_invalid_action() -> None:
    result = evaluate_policy(
        {
            "action_type": "DELETE_FILE",
            "payload": {
                "task_id": "DF-POLICY-GATE-V1",
            },
        },
        {
            "task_id": "DF-POLICY-GATE-V1",
            "status": "running",
        },
    )

    assert result.execution_allowed is False
    assert "unknown action_type" in result.reason


def test_evaluate_policy_same_input_same_result() -> None:
    descriptor = {
        "action_type": "READ_FILE",
        "payload": {
            "task_id": "DF-POLICY-GATE-V1",
            "path": r"runtime\out\policy-gate.log",
        },
    }
    task_state = {
        "task_id": "DF-POLICY-GATE-V1",
        "status": "running",
    }

    first = evaluate_policy(descriptor, task_state)
    second = evaluate_policy(descriptor, task_state)

    assert first == second


def test_dispatch_action_trigger_has_no_policy_bypass() -> None:
    result = dispatch_action_trigger(
        {
            "action_type": "WRITE_FILE",
            "payload": {
                "task_id": "DF-POLICY-GATE-V1",
                "path": r"runtime\out\policy-gate.log",
                "content": "blocked",
            },
        }
    )

    assert result["status"] == "execution_boundary_violation"
    assert result["result_payload"]["result_type"] == "execution_boundary_violation"
    assert result["reason"] == "direct_action_call_blocked"


def test_evaluate_policy_writes_policy_log(monkeypatch, tmp_path: Path) -> None:
    policy_log = tmp_path / "runtime" / "logs" / "policy.log"
    monkeypatch.setattr(policy_gate_module, "POLICY_LOG_FILE", policy_log)

    allowed = evaluate_policy(
        {
            "action_type": "WRITE_FILE",
            "payload": {
                "task_id": "DF-POLICY-ALLOW",
                "path": r"runtime\out\policy-allow.log",
                "content": "ok",
            },
        },
        {
            "task_id": "DF-POLICY-ALLOW",
            "status": "running",
        },
    )
    blocked = evaluate_policy(
        {
            "action_type": "DELETE_FILE",
            "payload": {
                "task_id": "DF-POLICY-BLOCK",
            },
        },
        {
            "task_id": "DF-POLICY-BLOCK",
            "status": "running",
        },
    )

    assert allowed.execution_allowed is True
    assert blocked.execution_allowed is False

    log_output = [
        json.loads(line)
        for line in policy_log.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert log_output[0] == {
        "timestamp": log_output[0]["timestamp"],
        "task_id": "DF-POLICY-ALLOW",
        "event_type": "policy_decision",
        "status": "allowed",
        "details": {"reason": "-"},
    }
    assert log_output[1] == {
        "timestamp": log_output[1]["timestamp"],
        "task_id": "DF-POLICY-BLOCK",
        "event_type": "policy_decision",
        "status": "blocked",
        "details": {"reason": "unknown action_type: DELETE_FILE"},
    }


def test_evaluate_external_action_policy_blocks_critical_action_without_confirmation() -> None:
    result = evaluate_external_action_policy(
        "SEND_EMAIL",
        {
            "destination": "gmail_gateway",
            "to": "ops@example.com",
            "subject": "Status update",
            "body": "Hello",
        },
        {
            "task_id": "DF-POLICY-CRITICAL-V1",
            "status": "running",
        },
    )

    assert result.execution_allowed is False
    assert result.reason == "critical action requires confirmation: SEND_EMAIL"
    assert result.policy_trace["policy_action_type"] == "critical"
    assert result.policy_trace["confirmation_received"] is False


def test_evaluate_external_action_policy_allows_critical_action_with_confirmation() -> None:
    result = evaluate_external_action_policy(
        "SEND_EMAIL",
        {
            "destination": "gmail_gateway",
            "to": "ops@example.com",
            "subject": "Status update",
            "body": "Hello",
            "confirmed": True,
        },
        {
            "task_id": "DF-POLICY-CRITICAL-V2",
            "status": "running",
        },
    )

    assert result.execution_allowed is True
    assert result.reason == ""
    assert result.policy_trace["policy_action_type"] == "critical"
    assert result.policy_trace["confirmation_received"] is True
