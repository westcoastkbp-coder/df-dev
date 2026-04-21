from __future__ import annotations

import copy
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import control.audit_log as audit_log
import control.codex_loop as codex_loop


def test_log_execution_writes_jsonl_row(monkeypatch, tmp_path):
    target = tmp_path / "audit_log.jsonl"
    monkeypatch.setattr(audit_log, "LOG_PATH", target)

    audit_log.log_execution({"module": "execution_replay", "status": "WORKING"})

    rows = audit_log.read_audit_log()
    assert len(rows) == 1
    assert rows[0]["module"] == "execution_replay"
    assert rows[0]["status"] == "WORKING"
    assert "time" in rows[0]


def test_read_audit_log_returns_rows_in_order(monkeypatch, tmp_path):
    target = tmp_path / "audit_log.jsonl"
    monkeypatch.setattr(audit_log, "LOG_PATH", target)

    audit_log.log_execution({"module": "one", "status": "FAIL"})
    audit_log.log_execution({"module": "two", "status": "WORKING"})

    rows = audit_log.read_audit_log()
    assert [row["module"] for row in rows] == ["one", "two"]


def test_codex_loop_writes_working_entry_on_approved_path(monkeypatch, capsys):
    state = {
        "context": {
            "system": "Digital Foreman",
            "status": "kernel_debug",
            "broken_modules": ["execution_replay"],
            "broken": {"execution_replay": "failing"},
            "modules_state": {},
        },
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
            "decision": "APPROVED",
            "packet": {"task_id": task_id},
            "claude": {"status": "ok"},
            "gemini": {"verdict": "VERIFIED"},
        },
    )
    monkeypatch.setattr(codex_loop, "get_git_commit", lambda: "abc123")
    monkeypatch.setattr(codex_loop, "get_git_branch", lambda: "main")

    codex_loop.main()
    capsys.readouterr()

    assert state["audit"][-1]["status"] == "WORKING"
    assert state["audit"][-1]["review"] == "APPROVED"


def test_codex_loop_writes_blocked_entry_on_blocked_path(monkeypatch, capsys):
    state = {
        "context": {
            "system": "Digital Foreman",
            "status": "kernel_debug",
            "broken_modules": ["execution_replay"],
            "broken": {"execution_replay": "failing"},
            "modules_state": {},
        },
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
    capsys.readouterr()

    assert state["audit"][-1]["status"] == "BLOCKED"
    assert state["audit"][-1]["review"] == "BLOCKED"
