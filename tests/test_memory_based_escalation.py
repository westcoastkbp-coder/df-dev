from __future__ import annotations

import copy
import json
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import control.analysis as analysis
import control.codex_loop as codex_loop


def _base_context() -> dict[str, object]:
    return {
        "system": "Digital Foreman",
        "status": "kernel_debug",
        "broken_modules": ["execution_replay"],
        "broken": {"execution_replay": "failing"},
        "modules_state": {},
        "next_required": "",
    }


def _run_escalation_case(monkeypatch, capsys, audit_entries):
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
        analysis,
        "read_audit_log",
        lambda limit=10: copy.deepcopy(audit_entries[-limit:]),
    )

    def fail_run_test(test_path):
        raise AssertionError("run_test should not execute for escalated modules")

    monkeypatch.setattr(codex_loop, "run_test", fail_run_test)

    result = codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())
    return result, output, state


def test_repeated_fail_entries_escalate_before_execution(monkeypatch, capsys) -> None:
    _, output, state = _run_escalation_case(
        monkeypatch,
        capsys,
        [
            {"module": "execution_replay", "status": "FAIL"},
            {"module": "execution_replay", "status": "FAIL"},
            {"module": "execution_replay", "status": "FAIL"},
        ],
    )

    assert output == {
        "status": "escalated",
        "module": "execution_replay",
        "reason": "repeated_failures",
        "action": "manual_intervention_required",
        "decision_trace": {
            "source": "memory_analysis",
            "trigger": "repeated_failures",
            "confidence": "high",
        },
    }
    assert state["context"]["next_required"] == "manual intervention required for execution_replay"
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


def test_repeated_blocked_entries_escalate_before_execution(monkeypatch, capsys) -> None:
    _, output, state = _run_escalation_case(
        monkeypatch,
        capsys,
        [
            {"module": "execution_replay", "status": "BLOCKED"},
            {"module": "execution_replay", "status": "BLOCKED"},
            {"module": "execution_replay", "status": "BLOCKED"},
        ],
    )

    assert output == {
        "status": "escalated",
        "module": "execution_replay",
        "reason": "repeated_blocked_reviews",
        "action": "manual_intervention_required",
        "decision_trace": {
            "source": "memory_analysis",
            "trigger": "repeated_blocked_reviews",
            "confidence": "high",
        },
    }
    assert state["context"]["next_required"] == "manual intervention required for execution_replay"
    assert state["audit"][0]["status"] == "ESCALATED"


def test_clean_history_does_not_escalate(monkeypatch, capsys) -> None:
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
        analysis,
        "read_audit_log",
        lambda limit=10: [
            {"module": "execution_replay", "status": "FAIL"},
            {"module": "execution_replay", "status": "WORKING"},
        ],
    )
    monkeypatch.setattr(codex_loop, "run_test", lambda test_path: ("pass", "ok"))
    monkeypatch.setattr(
        codex_loop,
        "run_external_review",
        lambda task_id, summary, files: {
            "status": "review_complete",
            "decision": "APPROVED",
            "packet": {"task_id": task_id},
            "claude": {"status": "ok"},
            "gemini": {"verdict": "VERIFIED"},
        },
    )
    monkeypatch.setattr(codex_loop, "get_git_commit", lambda: "abc123")
    monkeypatch.setattr(codex_loop, "get_git_branch", lambda: "main")

    result = codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())

    assert result is None
    assert output == {"status": "pass", "fixed": "execution_replay"}
    assert state["audit"][-1]["status"] == "WORKING"
    assert all(entry["status"] != "ESCALATED" for entry in state["audit"])


def test_already_working_module_returns_before_execution(monkeypatch, capsys) -> None:
    state = {
        "context": {
            **_base_context(),
            "modules_state": {
                "execution_replay": {
                    "status": "WORKING",
                }
            },
        }
    }

    monkeypatch.setattr(codex_loop, "load_context", lambda: state["context"])

    def fail_detect_unstable(module):
        raise AssertionError("detect_unstable_module should not run for working modules")

    def fail_run_test(test_path):
        raise AssertionError("run_test should not execute for working modules")

    monkeypatch.setattr(codex_loop, "detect_unstable_module", fail_detect_unstable)
    monkeypatch.setattr(codex_loop, "run_test", fail_run_test)

    result = codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())

    assert result == {
        "status": "already_working",
        "module": "execution_replay",
    }
    assert output == result
