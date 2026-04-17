from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from control.github_issue_intake import fetch_github_issue

DONE_LABEL = "STATUS: DONE"
FAILED_LABEL = "STATUS: FAILED"
STATUS_LABELS_TO_REMOVE = {
    "STATUS: NEW",
    "STATUS: PROCESSING",
    "PROCESSING",
    DONE_LABEL,
    FAILED_LABEL,
}


def _normalize_artifact_path(artifact_path: Path | str) -> str:
    return str(artifact_path).replace("\\", "/")


def build_execution_comment(
    artifact_path: Path | str,
    commit_hash: str | None = None,
) -> str:
    lines = [
        "Execution completed.",
        f"Artifact: {_normalize_artifact_path(artifact_path)}",
    ]
    normalized_commit_hash = str(commit_hash or "").strip()
    if normalized_commit_hash:
        lines.append(f"Commit: {normalized_commit_hash}")
    return "\n".join(lines)


def build_validation_failed_comment(reason: str) -> str:
    return f"Validation failed: {reason}"


def _build_status_label_names(
    labels: Iterable[Any],
    status_label: str,
) -> list[str]:
    next_labels: list[str] = []

    for label in labels:
        label_name = str(getattr(label, "name", label))
        if label_name in STATUS_LABELS_TO_REMOVE:
            continue
        next_labels.append(label_name)

    next_labels.append(status_label)
    return next_labels


def build_done_label_names(labels: Iterable[Any]) -> list[str]:
    return _build_status_label_names(labels, DONE_LABEL)


def build_failed_label_names(labels: Iterable[Any]) -> list[str]:
    return _build_status_label_names(labels, FAILED_LABEL)


def _update_issue_status(
    issue: Any,
    comment_body: str,
    status_labels: list[str],
) -> tuple[Any, int | None]:
    comment = issue.create_comment(comment_body)
    issue.set_labels(*status_labels)
    return issue, getattr(comment, "id", None)


def update_issue_execution_status(
    issue_number: int,
    commit_hash: str | None = None,
    artifact_path: Path | str = "",
    repo_name: str | None = None,
    token: str | None = None,
) -> tuple[Any, int | None]:
    if not str(artifact_path).strip():
        raise ValueError("artifact_path is required")

    issue = fetch_github_issue(
        issue_number=issue_number,
        repo_name=repo_name,
        token=token,
    )
    return _update_issue_status(
        issue=issue,
        comment_body=build_execution_comment(
            artifact_path=artifact_path,
            commit_hash=commit_hash,
        ),
        status_labels=build_done_label_names(getattr(issue, "labels", ())),
    )


def mark_issue_validation_failed(
    issue_number: int,
    reason: str,
    repo_name: str | None = None,
    token: str | None = None,
) -> tuple[Any, int | None]:
    issue = fetch_github_issue(
        issue_number=issue_number,
        repo_name=repo_name,
        token=token,
    )
    return _update_issue_status(
        issue=issue,
        comment_body=build_validation_failed_comment(reason),
        status_labels=build_failed_label_names(getattr(issue, "labels", ())),
    )
