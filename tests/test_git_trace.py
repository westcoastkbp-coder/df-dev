from __future__ import annotations

from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import control.git_trace as git_trace
import control.codex_loop as codex_loop


def test_git_trace_returns_values_when_git_available(monkeypatch):
    def fake_run_in_dev_env(args, **kwargs):
        if args == ["git", "rev-parse", "HEAD"]:
            return SimpleNamespace(stdout="abc123\n")
        return SimpleNamespace(stdout="main\n")

    monkeypatch.setattr(git_trace, "run_in_dev_env", fake_run_in_dev_env)

    result = {
        "commit": git_trace.get_git_commit(),
        "branch": git_trace.get_git_branch(),
    }

    assert set(result) == {"commit", "branch"}
    assert result["commit"] == "abc123"
    assert result["branch"] == "main"


def test_git_trace_fallback_when_git_unavailable(monkeypatch):
    def raise_error(*args, **kwargs):
        raise RuntimeError("git unavailable")

    monkeypatch.setattr(git_trace, "run_in_dev_env", raise_error)

    result = {
        "commit": git_trace.get_git_commit(),
        "branch": git_trace.get_git_branch(),
    }

    assert set(result) == {"commit", "branch"}
    assert result["commit"] == "no_git"
    assert result["branch"] == "no_branch"


def test_ensure_not_main_branch_allows_feature_branch(monkeypatch):
    monkeypatch.setattr(git_trace, "run_in_dev_env", lambda *args, **kwargs: SimpleNamespace(stdout="codex/feature-branch\n"))

    git_trace.ensure_not_main_branch()


def test_ensure_not_main_branch_blocks_main(monkeypatch):
    monkeypatch.setattr(git_trace, "run_in_dev_env", lambda *args, **kwargs: SimpleNamespace(stdout="main\n"))

    with pytest.raises(RuntimeError, match="DIRECT_MODIFICATION_OF_MAIN_BRANCH_FORBIDDEN"):
        git_trace.ensure_not_main_branch()


def test_ensure_not_main_branch_requires_git_context(monkeypatch):
    def raise_error(*args, **kwargs):
        raise RuntimeError("git unavailable")

    monkeypatch.setattr(git_trace, "run_in_dev_env", raise_error)

    with pytest.raises(RuntimeError, match="GIT_CONTEXT_REQUIRED"):
        git_trace.ensure_not_main_branch()


def test_codex_loop_checks_branch_before_loading_context(monkeypatch):
    monkeypatch.setattr(
        codex_loop,
        "ensure_not_main_branch",
        lambda: (_ for _ in ()).throw(
            RuntimeError("DIRECT_MODIFICATION_OF_MAIN_BRANCH_FORBIDDEN")
        ),
    )
    monkeypatch.setattr(
        codex_loop,
        "load_context",
        lambda: (_ for _ in ()).throw(AssertionError("load_context should not run")),
    )

    with pytest.raises(RuntimeError, match="DIRECT_MODIFICATION_OF_MAIN_BRANCH_FORBIDDEN"):
        codex_loop.main()
