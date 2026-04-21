from __future__ import annotations

import copy
import json
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import control.codex_loop as codex_loop

REAL_COMMIT_LOOP_RESULT = codex_loop.commit_loop_result


def _base_context() -> dict[str, object]:
    return {
        "system": "Digital Foreman",
        "status": "NOT_WORKING",
        "broken_modules": ["execution_replay"],
        "broken": {"execution_replay": "failing"},
        "modules_state": {},
        "next_required": "",
        "strategy_history": [],
    }


def test_commit_loop_result_stages_and_commits_after_approved_success(
    monkeypatch,
) -> None:
    calls: list[tuple[list[str], dict[str, object]]] = []

    def fake_run(command, **kwargs):
        calls.append((list(command), dict(kwargs)))
        return None

    monkeypatch.setattr(codex_loop, "run_in_dev_env", fake_run)

    REAL_COMMIT_LOOP_RESULT("execution_replay", "WORKING", "APPROVED")

    assert calls == [
        (
            ["git", "add", "."],
            {"cwd": codex_loop.ROOT, "check": True},
        ),
        (
            [
                "git",
                "commit",
                "-m",
                "dev: update session state",
            ],
            {"cwd": codex_loop.ROOT, "check": True},
        ),
        (
            ["git", "push"],
            {"cwd": codex_loop.ROOT, "check": True},
        ),
    ]


def test_commit_loop_result_creates_empty_commit_after_blocked_review(
    monkeypatch,
) -> None:
    calls: list[tuple[list[str], dict[str, object]]] = []

    def fake_run(command, **kwargs):
        calls.append((list(command), dict(kwargs)))
        return None

    monkeypatch.setattr(codex_loop, "run_in_dev_env", fake_run)

    REAL_COMMIT_LOOP_RESULT("execution_replay", "BLOCKED", "BLOCKED")

    assert calls == [
        (
            ["git", "add", "."],
            {"cwd": codex_loop.ROOT, "check": True},
        ),
        (
            [
                "git",
                "commit",
                "--allow-empty",
                "-m",
                "dev: update session state",
            ],
            {"cwd": codex_loop.ROOT, "check": True},
        ),
        (
            ["git", "push"],
            {"cwd": codex_loop.ROOT, "check": True},
        ),
    ]


def test_commit_loop_result_raises_when_push_fails(monkeypatch) -> None:
    calls: list[tuple[list[str], dict[str, object]]] = []

    def fake_run(command, **kwargs):
        command_list = list(command)
        calls.append((command_list, dict(kwargs)))
        if command_list == ["git", "push"]:
            raise RuntimeError("push failed")
        return None

    monkeypatch.setattr(codex_loop, "run_in_dev_env", fake_run)

    try:
        REAL_COMMIT_LOOP_RESULT("execution_replay", "WORKING", "APPROVED")
    except RuntimeError as exc:
        assert str(exc) == "GITHUB_SYNC_FAILED"
    else:
        raise AssertionError("push failure should raise GITHUB_SYNC_FAILED")

    assert calls == [
        (
            ["git", "add", "."],
            {"cwd": codex_loop.ROOT, "check": True},
        ),
        (
            [
                "git",
                "commit",
                "-m",
                "dev: update session state",
            ],
            {"cwd": codex_loop.ROOT, "check": True},
        ),
        (
            ["git", "push"],
            {"cwd": codex_loop.ROOT, "check": True},
        ),
    ]


def test_codex_loop_success_path_invokes_commit_discipline(monkeypatch, capsys) -> None:
    state = {"context": _base_context(), "audit": []}
    commits: list[tuple[str, str, str]] = []

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
    monkeypatch.setattr(codex_loop, "get_git_branch", lambda: "codex/test-branch")
    monkeypatch.setattr(
        codex_loop,
        "commit_loop_result",
        lambda module_name, status, review: commits.append(
            (module_name, status, review)
        ),
    )

    codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())

    assert output == {"status": "pass", "fixed": "execution_replay"}
    assert commits == [("execution_replay", "WORKING", "APPROVED")]


def test_codex_loop_blocked_path_invokes_commit_discipline(monkeypatch, capsys) -> None:
    state = {"context": _base_context(), "audit": []}
    commits: list[tuple[str, str, str]] = []

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
        "read_audit_log",
        lambda limit=50: [],
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
    monkeypatch.setattr(codex_loop, "get_git_branch", lambda: "codex/test-branch")
    monkeypatch.setattr(
        codex_loop,
        "commit_loop_result",
        lambda module_name, status, review: commits.append(
            (module_name, status, review)
        ),
    )

    codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())

    assert output["status"] == "blocked_by_review"
    assert commits == [("execution_replay", "BLOCKED", "BLOCKED")]
