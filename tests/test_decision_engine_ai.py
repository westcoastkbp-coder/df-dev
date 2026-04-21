from __future__ import annotations

import pytest

import app.execution.decision_engine as decision_engine_module
from app.execution.orchestrator_client import OrchestratorUnavailableError


def _task() -> dict[str, object]:
    return {
        "task_id": "DF-AI-DECISION-V1",
        "intent": "write_file",
        "payload": {
            "path": r"runtime\out\ai-decision.txt",
            "content": "hello",
        },
        "status": "running",
    }


def _context() -> dict[str, object]:
    return {
        "decision_mode": "ai",
        "task_state": {
            "task_id": "DF-AI-DECISION-V1",
            "status": "running",
        },
        "command_name": "write_file",
        "context_summary": {
            "decision_mode": "ai",
            "active_mode": "execution",
        },
    }


def test_ai_decision_returns_valid_action_and_trace(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_call(*_args, **_kwargs) -> dict[str, object]:
        return {
            "action": "write_file",
            "action_type": "write",
            "target": r"runtime\out\ai-decision.txt",
            "parameters": {
                "path": r"runtime\out\ai-decision.txt",
                "content": "hello",
            },
            "requires_confirmation": False,
            "reason": "AI selected write_file because the task is an approved file write.",
        }

    monkeypatch.setattr(
        decision_engine_module.orchestrator_client_module,
        "call_orchestrator",
        fake_call,
    )

    plan = decision_engine_module.decide(_task(), _context())
    trace = decision_engine_module.decision_trace_for_plan(
        plan,
        task=_task(),
        context=_context(),
    )

    assert plan["decision_source"] == "ai"
    assert plan["action"] == "write_file"
    assert plan["action_type"] == "write"
    assert plan["target"] == r"runtime\out\ai-decision.txt"
    assert plan["vendor"] == "openai"
    assert plan["policy_result"] == "allowed: policy gate passed"
    assert trace["reason"] == "AI selected write_file because the task is an approved file write."
    assert "source=ai" in trace["context_used"]
    assert trace["vendor"] == "openai"


def test_invalid_ai_output_fails_without_rule_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        decision_engine_module.orchestrator_client_module,
        "call_orchestrator",
        lambda *_args, **_kwargs: {
            "action": "write_file",
            "action_type": "unsafe",
            "target": r"runtime\out\ai-decision.txt",
            "parameters": {},
            "requires_confirmation": False,
            "reason": "bad plan",
        },
    )

    with pytest.raises(ValueError, match="invalid ai action_type"):
        decision_engine_module.decide(_task(), _context())


def test_ai_mode_falls_back_to_rule_based_when_orchestrator_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable(*_args, **_kwargs) -> dict[str, object]:
        raise OrchestratorUnavailableError("OPENAI_API_KEY is not configured")

    monkeypatch.setattr(
        decision_engine_module.orchestrator_client_module,
        "call_orchestrator",
        unavailable,
    )

    plan = decision_engine_module.decide(_task(), _context())

    assert plan["decision_source"] == "rule"
    assert plan["action"] == "write_file"
    assert "rule-based fallback" in plan["reason"]
