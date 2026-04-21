from __future__ import annotations

import copy
import json
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import control.codex_loop as codex_loop
from control.error_classifier import classify_error


def _base_context() -> dict[str, object]:
    return {
        "system": "Digital Foreman",
        "status": "kernel_debug",
        "broken_modules": ["execution_replay"],
        "broken": {"execution_replay": "failing"},
        "modules_state": {},
        "next_required": "",
    }


def test_fail_path_classifies_execution_error(monkeypatch, capsys) -> None:
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
        lambda module: {"unstable": False, "reason": ""},
    )
    monkeypatch.setattr(codex_loop, "run_test", lambda test_path: ("fail", "broken"))
    class FailedExecution:
        returncode = 1

    monkeypatch.setattr(
        codex_loop,
        "execute_fix_task",
        lambda prompt: FailedExecution(),
    )

    codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())

    assert output == {
        "strategy": "code_fix",
        "instruction": "Fix failing test in execution_replay",
        "error_type": "execution_error",
    }
    assert state["context"]["last_error"] == {
        "module": "execution_replay",
        "type": "execution_error",
    }
    assert state["audit"][-1]["error_type"] == "execution_error"


def test_blocked_path_classifies_verification_error(monkeypatch, capsys) -> None:
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
        lambda module: {"unstable": False, "reason": ""},
    )
    monkeypatch.setattr(codex_loop, "run_test", lambda test_path: ("pass", "ok"))
    monkeypatch.setattr(
        codex_loop,
        "run_external_review",
        lambda task_id, summary, files: {
            "status": "review_complete",
            "decision": "BLOCKED",
            "packet": {"task_id": task_id},
            "claude": {"status": "ok"},
            "gemini": {"verdict": "NOT_VERIFIED"},
        },
    )
    monkeypatch.setattr(codex_loop, "get_git_commit", lambda: "abc123")
    monkeypatch.setattr(codex_loop, "get_git_branch", lambda: "main")

    codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())

    assert output["status"] == "blocked_by_review"
    assert state["context"]["last_error"] == {
        "module": "execution_replay",
        "type": "verification_error",
    }
    assert state["audit"][0]["error_type"] == "verification_error"


def test_timeout_path_classifies_timeout_error(monkeypatch, capsys) -> None:
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
        lambda module: {"unstable": False, "reason": ""},
    )
    monkeypatch.setattr(codex_loop, "run_test", lambda test_path: ("timeout", ""))

    codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())

    assert output == {
        "goal": "fix infinite loop or blocking execution",
        "priority": "critical",
    }
    assert state["context"]["last_error"] == {
        "module": "execution_replay",
        "type": "timeout_error",
    }
    assert state["audit"][0]["error_type"] == "timeout_error"


def test_unknown_case_classifies_unknown_error() -> None:
    assert classify_error("PASS", "APPROVED") == "unknown_error"
