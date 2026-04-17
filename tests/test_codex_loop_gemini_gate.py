from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import control.codex_loop as codex_loop


def test_codex_loop_blocks_when_review_is_blocked(monkeypatch, capsys):
    saved = {}

    monkeypatch.setattr(
        codex_loop,
        "load_context",
        lambda: {
            "status": "NOT_WORKING",
            "broken_modules": ["execution_replay"],
            "broken": {"execution_replay": "failing"},
        },
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
    monkeypatch.setattr(codex_loop, "save_context", lambda payload: saved.setdefault("payload", payload))

    codex_loop.main()

    output = json.loads(capsys.readouterr().out.strip())
    assert output["status"] == "blocked_by_review"
    assert output["module"] == "execution_replay"
    assert saved["payload"]["status"] == "NOT_WORKING"
    assert "execution_replay" in saved["payload"]["broken"]
    assert saved["payload"]["last_codex_loop"]["status"] == "blocked_by_review"


def test_codex_loop_continues_when_review_is_approved(monkeypatch, capsys):
    saved = {}

    monkeypatch.setattr(
        codex_loop,
        "load_context",
        lambda: {
            "status": "NOT_WORKING",
            "broken_modules": ["execution_replay"],
            "broken": {"execution_replay": "failing"},
        },
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
    monkeypatch.setattr(codex_loop, "save_context", lambda payload: saved.setdefault("payload", payload))

    codex_loop.main()

    output = json.loads(capsys.readouterr().out.strip())
    assert output == {"status": "pass", "fixed": "execution_replay"}
    assert saved["payload"]["status"] == "WORKING"
    assert saved["payload"]["last_codex_loop"]["local_test"] == "pass"
    assert saved["payload"]["last_codex_loop"]["review"]["decision"] == "APPROVED"
