from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import control.task_guard as task_guard_module
from control.task_guard import should_execute_task


def test_should_execute_task_allows_fresh_task(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        task_guard_module,
        "fetch_github_issue",
        lambda issue_number, repo_name=None, token=None: SimpleNamespace(labels=[]),
    )

    result = should_execute_task(
        task_id=9,
        artifact_path=tmp_path / "artifacts" / "task-9.txt",
    )

    assert result == {
        "should_execute": True,
        "reason": "no final execution record found",
    }


def test_should_execute_task_skips_completed_issue(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        task_guard_module,
        "fetch_github_issue",
        lambda issue_number, repo_name=None, token=None: SimpleNamespace(
            labels=[SimpleNamespace(name="STATUS: DONE")]
        ),
    )

    result = should_execute_task(
        task_id=9,
        artifact_path=tmp_path / "artifacts" / "task-9.txt",
    )

    assert result == {
        "should_execute": False,
        "reason": "issue #9 already has STATUS: DONE",
    }


def test_should_execute_task_force_execution_overrides_completed_issue(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(
        task_guard_module,
        "fetch_github_issue",
        lambda issue_number, repo_name=None, token=None: SimpleNamespace(
            labels=[SimpleNamespace(name="STATUS: DONE")]
        ),
    )

    result = should_execute_task(
        task_id=9,
        artifact_path=tmp_path / "artifacts" / "task-9.txt",
        allow_existing_artifact=True,
    )

    assert result == {
        "should_execute": True,
        "reason": "force execution override active",
        "force_execution_used": True,
        "force_execution_reason": "done_label_override",
    }


def test_should_execute_task_skips_existing_final_artifact(tmp_path) -> None:
    artifact_path = tmp_path / "artifacts" / "task-9.txt"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text("existing artifact", encoding="utf-8")

    result = should_execute_task(
        task_id=9,
        artifact_path=artifact_path,
    )

    assert result == {
        "should_execute": False,
        "reason": f"final artifact already exists at {artifact_path}",
    }


def test_should_execute_task_does_not_consult_git_commit_markers(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        task_guard_module,
        "has_execution_commit_marker",
        lambda task_id, repo_root=None: (_ for _ in ()).throw(AssertionError("git should not gate runtime")),
    )
    monkeypatch.setattr(
        task_guard_module,
        "fetch_github_issue",
        lambda issue_number, repo_name=None, token=None: SimpleNamespace(labels=[]),
    )

    result = should_execute_task(
        task_id=9,
        artifact_path=tmp_path / "artifacts" / "task-9.txt",
    )

    assert result == {
        "should_execute": True,
        "reason": "no final execution record found",
    }


def test_has_execution_commit_marker_reads_existing_commit(monkeypatch, tmp_path) -> None:
    def fake_run(
        args: list[str],
        cwd: Path,
        capture_output: bool,
        text: bool,
        check: bool,
    ) -> SimpleNamespace:
        assert args == [
            "git",
            "log",
            "--all",
            "--grep",
            "DF task 9: execution result",
            "--format=%H",
            "-n",
            "1",
        ]
        return SimpleNamespace(returncode=0, stdout="abc123def456\n", stderr="")

    monkeypatch.setattr(task_guard_module, "run_in_dev_env", fake_run)

    assert task_guard_module.has_execution_commit_marker(9, repo_root=tmp_path) is True
