from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import control.codex_loop as codex_loop
from control.dev_session import build_codex_execution_prompt as real_build_codex_execution_prompt


def test_execute_fix_task_injects_dev_session_context(monkeypatch, tmp_path: Path) -> None:
    (tmp_path / "tasks" / "github").mkdir(parents=True, exist_ok=True)
    (tmp_path / "tasks" / "github" / "issue-9.json").write_text(
        json.dumps(
            {
                "issue_number": 9,
                "title": "Require dev_session before Codex actions",
                "body": "Read docs/dev_session.md first.",
                "labels": ["STATUS: PROCESSING"],
                "raw_url": "https://github.com/example/repo/issues/9",
                "source_status": "open",
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    (tmp_path / "docs").mkdir(parents=True, exist_ok=True)
    (tmp_path / "docs" / "dev_session.md").write_text(
        "# Dev Session\n\nseed\n\n<!-- DEV_SESSION_STATE:START -->\n"
        + json.dumps(
            {
                "current_task": "Seeded session",
                "last_failing_test": "tests/test_example.py::test_failure",
                "next_step": "Read this file first.",
                "rules": [],
                "source_of_truth": {
                    "dev_session_path": "/docs/dev_session.md",
                    "github_context": {
                        "issue_number": 9,
                        "packet_path": "tasks/github/issue-9.json",
                        "status": "available",
                        "title": "Require dev_session before Codex actions",
                        "body": "Read docs/dev_session.md first.",
                        "labels": ["STATUS: PROCESSING"],
                        "raw_url": "https://github.com/example/repo/issues/9",
                        "source_status": "open",
                    },
                },
                "status": "in_progress",
                "step_log": [],
                "updated_at": "2026-04-13T15:30:14Z",
                "what_is_fixed": [],
                "what_remains": [],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n<!-- DEV_SESSION_STATE:END -->\n",
        encoding="utf-8",
    )

    captured: dict[str, object] = {}

    def fake_run(command, **kwargs):
        captured["command"] = list(command)
        captured["kwargs"] = dict(kwargs)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(codex_loop, "ROOT", tmp_path)
    monkeypatch.setattr(codex_loop, "get_git_branch", lambda: "issue-9")
    monkeypatch.setattr(
        codex_loop,
        "build_codex_execution_prompt",
        lambda prompt, **kwargs: real_build_codex_execution_prompt(prompt, **kwargs),
    )
    monkeypatch.setattr(codex_loop, "run_in_dev_env", fake_run)

    codex_loop.execute_fix_task("Fix the failing test with a minimal patch.")

    assert captured["command"][0:2] == ["codex", "exec"]
    assert "/docs/dev_session.md" in captured["command"][2]
    assert "tasks/github/issue-9.json" in captured["command"][2]
    assert "Fix the failing test with a minimal patch." in captured["command"][2]
