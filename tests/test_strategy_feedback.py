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
        "strategy_history": [],
    }


def test_success_records_strategy_feedback(monkeypatch, capsys) -> None:
    state = {
        "context": _base_context(),
        "audit": [],
    }

    class SuccessfulExecution:
        returncode = 0

    class SuccessfulValidation:
        returncode = 0

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
    monkeypatch.setattr(codex_loop, "execute_fix_task", lambda prompt: SuccessfulExecution())
    monkeypatch.setattr(codex_loop, "run_in_dev_env", lambda *args, **kwargs: SuccessfulValidation())
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

    codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())

    assert output == {"status": "pass", "fixed": "execution_replay"}
    assert state["context"]["strategy_feedback"] == {
        "strategy": "code_fix",
        "result": "success",
    }


def test_failure_records_strategy_feedback(monkeypatch, capsys) -> None:
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
    monkeypatch.setattr(codex_loop, "execute_fix_task", lambda prompt: FailedExecution())

    codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())

    assert output == {
        "strategy": "code_fix",
        "instruction": "Fix failing test in execution_replay",
        "error_type": "execution_error",
    }
    assert state["context"]["strategy_feedback"] == {
        "strategy": "code_fix",
        "result": "failure",
    }


def test_strategy_history_accumulates(monkeypatch, capsys) -> None:
    state = {
        "context": {
            **_base_context(),
            "strategy_history": [
                {
                    "strategy": "generic_fix",
                    "result": "failure",
                }
            ],
        },
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
    monkeypatch.setattr(codex_loop, "execute_fix_task", lambda prompt: FailedExecution())

    codex_loop.main()
    capsys.readouterr()

    assert state["context"]["strategy_history"] == [
        {
            "strategy": "generic_fix",
            "result": "failure",
        },
        {
            "strategy": "code_fix",
            "result": "failure",
        },
    ]


def test_audit_contains_strategy_feedback(monkeypatch, capsys) -> None:
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
    monkeypatch.setattr(codex_loop, "execute_fix_task", lambda prompt: FailedExecution())

    codex_loop.main()
    capsys.readouterr()

    assert state["audit"][-1]["strategy_feedback"] == {
        "strategy": "code_fix",
        "result": "failure",
    }
