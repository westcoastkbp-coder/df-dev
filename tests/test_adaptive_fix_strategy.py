from __future__ import annotations

import copy
import json
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

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


def test_execution_error_uses_code_fix_strategy(monkeypatch, capsys) -> None:
    state = {
        "context": _base_context(),
        "audit": [],
    }

    class FailedExecution:
        returncode = 1

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
    monkeypatch.setattr(
        codex_loop, "execute_fix_task", lambda prompt: FailedExecution()
    )

    codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())

    assert output == {
        "strategy": "code_fix",
        "instruction": "Fix failing test in execution_replay",
        "error_type": "execution_error",
    }
    assert state["context"]["last_strategy"] == "code_fix"
    assert state["audit"][0]["strategy"] == "code_fix"


def test_verification_error_uses_logic_review_strategy(monkeypatch, capsys) -> None:
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
    assert state["context"]["last_strategy"] == "logic_review"
    assert state["audit"][0]["strategy"] == "logic_review"


def test_timeout_error_uses_performance_fix_strategy(monkeypatch, capsys) -> None:
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
    assert state["context"]["last_strategy"] == "performance_fix"
    assert state["audit"][0]["strategy"] == "performance_fix"


def test_unknown_error_uses_generic_fix_strategy() -> None:
    assert codex_loop.build_fix_task("execution_replay", "unknown_error") == {
        "strategy": "generic_fix",
        "instruction": "Investigate failure in execution_replay",
        "error_type": "unknown_error",
    }


def test_strategy_is_stored_in_context(monkeypatch, capsys) -> None:
    state = {
        "context": _base_context(),
        "audit": [],
    }

    class FailedExecution:
        returncode = 1

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
    monkeypatch.setattr(
        codex_loop, "execute_fix_task", lambda prompt: FailedExecution()
    )

    codex_loop.main()
    capsys.readouterr()

    assert state["context"]["last_strategy"] == "code_fix"
