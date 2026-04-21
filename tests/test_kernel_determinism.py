from __future__ import annotations

import copy
import json
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import control.codex_loop as codex_loop


def _base_context() -> dict:
    return {
        "system": "Digital Foreman",
        "status": "kernel_debug",
        "modules": {
            "execution": "partial",
            "review_gate": "working",
            "gemini": "connected",
            "claude": "connected",
            "codex": "active",
        },
        "broken_modules": ["execution_replay"],
        "broken": {"execution_replay": "failing"},
        "known_issues": [],
        "history": [],
        "last_update": "",
        "modules_state": {},
    }


def _run_loop(monkeypatch, capsys, *, test_result, review_result=None):
    state = {"context": _base_context()}

    monkeypatch.setattr(codex_loop, "load_context", lambda: state["context"])

    def fake_save_context(payload):
        state["context"] = copy.deepcopy(payload)

    monkeypatch.setattr(codex_loop, "save_context", fake_save_context)
    monkeypatch.setattr(
        codex_loop,
        "detect_unstable_module",
        lambda module: {"unstable": False, "reason": ""},
    )
    monkeypatch.setattr(codex_loop, "run_test", lambda test_path: test_result)
    monkeypatch.setattr(codex_loop, "execute_fix_task", lambda prompt: {"status": "timeout", "reason": "not_used"})

    if review_result is not None:
        monkeypatch.setattr(
            codex_loop,
            "run_external_review",
            lambda task_id, summary, files: copy.deepcopy(review_result),
        )

    codex_loop.main()

    output = json.loads(capsys.readouterr().out.strip())
    return output, copy.deepcopy(state["context"])


def test_same_input_same_result(monkeypatch, capsys):
    review_result = {
        "status": "review_complete",
        "decision": "APPROVED",
        "packet": {"task_id": "execution_replay"},
        "claude": {"status": "ok"},
        "gemini": {"verdict": "VERIFIED"},
    }

    output_one, context_one = _run_loop(
        monkeypatch,
        capsys,
        test_result=("pass", "ok"),
        review_result=review_result,
    )
    output_two, context_two = _run_loop(
        monkeypatch,
        capsys,
        test_result=("pass", "ok"),
        review_result=review_result,
    )

    assert output_one == output_two
    assert (
        context_one["modules_state"]["execution_replay"]
        == context_two["modules_state"]["execution_replay"]
    )


def test_blocked_review_never_produces_working(monkeypatch, capsys):
    output, context = _run_loop(
        monkeypatch,
        capsys,
        test_result=("pass", "ok"),
        review_result={
            "status": "review_complete",
            "decision": "BLOCKED",
            "packet": {"task_id": "execution_replay"},
            "claude": {"status": "ok"},
            "gemini": {"verdict": "NOT_VERIFIED"},
        },
    )

    assert output["status"] == "blocked_by_review"
    assert context["modules_state"]["execution_replay"]["status"] != "WORKING"


def test_approved_review_always_persists_working(monkeypatch, capsys):
    output, context = _run_loop(
        monkeypatch,
        capsys,
        test_result=("pass", "ok"),
        review_result={
            "status": "review_complete",
            "decision": "APPROVED",
            "packet": {"task_id": "execution_replay"},
            "claude": {"status": "ok"},
            "gemini": {"verdict": "VERIFIED"},
        },
    )

    assert output == {"status": "pass", "fixed": "execution_replay"}
    assert context["modules_state"]["execution_replay"]["status"] == "WORKING"
    assert context["modules_state"]["execution_replay"]["last_test"] == "PASS"
    assert context["modules_state"]["execution_replay"]["review"] == "APPROVED"


def test_fail_path_never_writes_false_success(monkeypatch, capsys):
    output, context = _run_loop(
        monkeypatch,
        capsys,
        test_result=("fail", "broken"),
    )

    assert output == {"status": "timeout", "reason": "not_used"}
    assert context.get("modules_state", {}).get("execution_replay", {}).get("status") != "WORKING"
    assert context["next_required"] == "Resolve execution_replay via test_execution_replay.py"
