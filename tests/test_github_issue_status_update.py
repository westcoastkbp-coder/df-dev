from __future__ import annotations

from types import SimpleNamespace

from control.github_issue_status_update import (
    build_failed_label_names,
    build_validation_failed_comment,
    build_done_label_names,
    build_execution_comment,
    mark_issue_validation_failed,
    update_issue_execution_status,
)


def test_build_execution_comment_is_traceable() -> None:
    comment = build_execution_comment(
        artifact_path="artifacts/task-9.txt",
        commit_hash="abc123def456",
    )

    assert comment == (
        "Execution completed.\n"
        "Artifact: artifacts/task-9.txt\n"
        "Commit: abc123def456"
    )


def test_build_execution_comment_omits_commit_when_unavailable() -> None:
    comment = build_execution_comment(
        artifact_path="artifacts/task-9.txt",
        commit_hash=None,
    )

    assert comment == (
        "Execution completed.\n"
        "Artifact: artifacts/task-9.txt"
    )


def test_build_done_label_names_replaces_in_progress_status() -> None:
    labels = [
        SimpleNamespace(name="TYPE: TASK"),
        SimpleNamespace(name="STATUS: NEW"),
        SimpleNamespace(name="STATUS: PROCESSING"),
        SimpleNamespace(name="priority: high"),
    ]

    label_names = build_done_label_names(labels)

    assert label_names == [
        "TYPE: TASK",
        "priority: high",
        "STATUS: DONE",
    ]


def test_build_failed_label_names_replaces_in_progress_status() -> None:
    labels = [
        SimpleNamespace(name="TYPE: TASK"),
        SimpleNamespace(name="STATUS: NEW"),
        SimpleNamespace(name="STATUS: PROCESSING"),
        SimpleNamespace(name="priority: high"),
    ]

    label_names = build_failed_label_names(labels)

    assert label_names == [
        "TYPE: TASK",
        "priority: high",
        "STATUS: FAILED",
    ]


def test_build_validation_failed_comment_is_traceable() -> None:
    assert (
        build_validation_failed_comment("artifact file is empty")
        == "Validation failed: artifact file is empty"
    )


def test_update_issue_execution_status_comments_and_sets_done_labels(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeIssue:
        def __init__(self) -> None:
            self.labels = [
                SimpleNamespace(name="TYPE: TASK"),
                SimpleNamespace(name="STATUS: PROCESSING"),
            ]

        def create_comment(self, body: str) -> SimpleNamespace:
            captured["comment_body"] = body
            return SimpleNamespace(id=321)

        def set_labels(self, *labels: str) -> None:
            captured["labels"] = list(labels)

    def fake_fetch_github_issue(
        issue_number: int,
        repo_name: str | None = None,
        token: str | None = None,
    ) -> FakeIssue:
        captured["issue_number"] = issue_number
        captured["repo_name"] = repo_name
        captured["token"] = token
        return FakeIssue()

    monkeypatch.setattr(
        "control.github_issue_status_update.fetch_github_issue",
        fake_fetch_github_issue,
    )

    issue, comment_id = update_issue_execution_status(
        issue_number=9,
        commit_hash="abc123def456",
        artifact_path="D:/digital_foreman/artifacts/task-9.txt",
        repo_name="example/repo",
        token="secret-token",
    )

    assert comment_id == 321
    assert captured == {
        "issue_number": 9,
        "repo_name": "example/repo",
        "token": "secret-token",
        "comment_body": (
            "Execution completed.\n"
            "Artifact: D:/digital_foreman/artifacts/task-9.txt\n"
            "Commit: abc123def456"
        ),
        "labels": ["TYPE: TASK", "STATUS: DONE"],
    }
    assert isinstance(issue, FakeIssue)


def test_update_issue_execution_status_allows_missing_commit_hash(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeIssue:
        def __init__(self) -> None:
            self.labels = [SimpleNamespace(name="STATUS: PROCESSING")]

        def create_comment(self, body: str) -> SimpleNamespace:
            captured["comment_body"] = body
            return SimpleNamespace(id=987)

        def set_labels(self, *labels: str) -> None:
            captured["labels"] = list(labels)

    monkeypatch.setattr(
        "control.github_issue_status_update.fetch_github_issue",
        lambda issue_number, repo_name=None, token=None: FakeIssue(),
    )

    _, comment_id = update_issue_execution_status(
        issue_number=9,
        commit_hash=None,
        artifact_path="D:/digital_foreman/tasks/subtasks/task-9-subtasks.json",
    )

    assert comment_id == 987
    assert captured == {
        "comment_body": (
            "Execution completed.\n"
            "Artifact: D:/digital_foreman/tasks/subtasks/task-9-subtasks.json"
        ),
        "labels": ["STATUS: DONE"],
    }


def test_mark_issue_validation_failed_comments_and_sets_failed_labels(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeIssue:
        def __init__(self) -> None:
            self.labels = [
                SimpleNamespace(name="TYPE: TASK"),
                SimpleNamespace(name="STATUS: PROCESSING"),
            ]

        def create_comment(self, body: str) -> SimpleNamespace:
            captured["comment_body"] = body
            return SimpleNamespace(id=654)

        def set_labels(self, *labels: str) -> None:
            captured["labels"] = list(labels)

    def fake_fetch_github_issue(
        issue_number: int,
        repo_name: str | None = None,
        token: str | None = None,
    ) -> FakeIssue:
        captured["issue_number"] = issue_number
        captured["repo_name"] = repo_name
        captured["token"] = token
        return FakeIssue()

    monkeypatch.setattr(
        "control.github_issue_status_update.fetch_github_issue",
        fake_fetch_github_issue,
    )

    issue, comment_id = mark_issue_validation_failed(
        issue_number=9,
        reason="artifact file is empty",
        repo_name="example/repo",
        token="secret-token",
    )

    assert comment_id == 654
    assert captured == {
        "issue_number": 9,
        "repo_name": "example/repo",
        "token": "secret-token",
        "comment_body": "Validation failed: artifact file is empty",
        "labels": ["TYPE: TASK", "STATUS: FAILED"],
    }
    assert isinstance(issue, FakeIssue)
