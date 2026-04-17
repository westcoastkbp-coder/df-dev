from __future__ import annotations

import copy
import json
from pathlib import Path
import sys

import pytest

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import control.codex_loop as codex_loop
from control.decision_contract import build_escalation_decision


def _base_context() -> dict[str, object]:
    return {
        "system": "Digital Foreman",
        "status": "kernel_debug",
        "broken_modules": ["execution_replay"],
        "broken": {"execution_replay": "failing"},
        "modules_state": {},
        "next_required": "",
    }


def _assert_trace_present(payload: dict[str, object]) -> None:
    trace = payload["decision_trace"]
    assert trace["source"] == "memory_analysis"
    assert trace["confidence"] == "high"


def test_escalation_includes_decision_trace() -> None:
    decision = build_escalation_decision("execution_replay", "repeated_failures")

    assert decision["status"] == "escalated"
    assert decision["decision_trace"] == {
        "source": "memory_analysis",
        "trigger": "repeated_failures",
        "confidence": "high",
    }


def test_audit_includes_decision_trace(monkeypatch, capsys) -> None:
    state = {
        "context": _base_context(),
        "audit": [],
    }

    monkeypatch.setattr(codex_loop, "load_context", lambda: state["context"])
    monkeypatch.setattr(
        codex_loop,
        "save_context",
        lambda payload: state.__setitem__("context", copy.deepcopy(payload)),
    )
    monkeypatch.setattr(
        codex_loop,
        "log_execution",
        lambda entry: state["audit"].append(copy.deepcopy(entry)),
    )
    monkeypatch.setattr(
        codex_loop,
        "detect_unstable_module",
        lambda module: {
            "unstable": True,
            "reason": "repeated_failures",
        },
    )

    def fail_run_test(test_path):
        raise AssertionError("run_test should not execute for escalated modules")

    monkeypatch.setattr(codex_loop, "run_test", fail_run_test)

    codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())

    assert output["decision_trace"] == {
        "source": "memory_analysis",
        "trigger": "repeated_failures",
        "confidence": "high",
    }
    assert state["audit"] == [
        {
            "module": "execution_replay",
            "status": "ESCALATED",
            "local_test": "NOT_RUN",
            "review": "NOT_RUN",
            "git": {},
            "decision_trace": {
                "type": "escalation",
                "reason": "repeated_failures",
            },
        }
    ]


def test_missing_decision_trace_fails_contract() -> None:
    decision = build_escalation_decision("execution_replay", "repeated_failures")
    decision_without_trace = dict(decision)
    decision_without_trace.pop("decision_trace")

    with pytest.raises(KeyError):
        _assert_trace_present(decision_without_trace)
