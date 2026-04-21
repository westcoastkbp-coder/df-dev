from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from control.dev_runtime import run_in_dev_env
from control.github_issue_intake import fetch_github_issue

REPO_ROOT = Path(__file__).resolve().parents[1]
DONE_LABEL = "STATUS: DONE"


def _label_names(labels: Iterable[Any]) -> list[str]:
    return [str(getattr(label, "name", label)) for label in labels]


def has_final_artifact(artifact_path: Path | str) -> bool:
    return Path(artifact_path).exists()


def issue_has_done_label(issue: Any) -> bool:
    return DONE_LABEL in _label_names(getattr(issue, "labels", ()))


def has_execution_commit_marker(
    task_id: int,
    repo_root: Path | str | None = None,
) -> bool:
    root_dir = Path(repo_root) if repo_root is not None else REPO_ROOT
    result = run_in_dev_env(
        [
            "git",
            "log",
            "--all",
            "--grep",
            f"DF task {task_id}: execution result",
            "--format=%H",
            "-n",
            "1",
        ],
        cwd=root_dir,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        message = result.stderr.strip() or result.stdout.strip() or "git log failed"
        raise RuntimeError(message)
    return bool(result.stdout.strip())


def should_execute_task(
    task_id: int,
    artifact_path: Path | str,
    repo_name: str | None = None,
    token: str | None = None,
    repo_root: Path | str | None = None,
    allow_existing_artifact: bool = False,
) -> dict[str, object]:
    path = Path(artifact_path)
    artifact_exists = has_final_artifact(path)

    if artifact_exists and not allow_existing_artifact:
        return {
            "should_execute": False,
            "reason": f"final artifact already exists at {path}",
        }

    issue = fetch_github_issue(
        issue_number=task_id,
        repo_name=repo_name,
        token=token,
    )
    has_done_label = issue_has_done_label(issue)
    if has_done_label and not allow_existing_artifact:
        return {
            "should_execute": False,
            "reason": f"issue #{task_id} already has {DONE_LABEL}",
        }

    if allow_existing_artifact and (artifact_exists or has_done_label):
        force_execution_reason = ""
        if artifact_exists and has_done_label:
            force_execution_reason = "artifact_and_done_label_override"
        elif artifact_exists:
            force_execution_reason = "artifact_override"
        else:
            force_execution_reason = "done_label_override"
        return {
            "should_execute": True,
            "reason": "force execution override active",
            "force_execution_used": True,
            "force_execution_reason": force_execution_reason,
        }

    return {
        "should_execute": True,
        "reason": "no final execution record found",
    }
