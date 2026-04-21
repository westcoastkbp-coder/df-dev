from __future__ import annotations

import json

from control.dev_session import (
    build_codex_execution_prompt,
    ensure_dev_session,
    extract_last_failing_test,
    read_dev_session,
)


def test_ensure_dev_session_creates_markdown_from_github_packet(tmp_path) -> None:
    packet_path = tmp_path / "tasks" / "github" / "issue-9.json"
    packet_path.parent.mkdir(parents=True, exist_ok=True)
    packet_path.write_text(
        json.dumps(
            {
                "issue_number": 9,
                "title": "Require GitHub-backed session context",
                "body": "Stop relying on model memory.",
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
    (tmp_path / "tmp_pytest.out").write_text(
        "FAILED tests/test_client_intake_flow.py::test_execute_product_task_locks_client_intake_flow\n",
        encoding="utf-8",
    )

    state = ensure_dev_session(
        repo_root=tmp_path,
        issue_number=9,
        current_task="DF-ENFORCE-GITHUB-CONTEXT-V1",
        last_failing_test=extract_last_failing_test(repo_root=tmp_path),
        fixed_items=["Created docs/dev_session.md."],
        remaining_items=["Wire the file into Codex execution."],
        status="in_progress",
        next_step="Inject the session file into the Codex prompt.",
    )

    session_path = tmp_path / "docs" / "dev_session.md"
    session_text = session_path.read_text(encoding="utf-8")

    assert session_path.is_file()
    assert state["source_of_truth"]["github_context"]["packet_path"] == "tasks/github/issue-9.json"
    assert "Require GitHub-backed session context" in session_text
    assert "tests/test_client_intake_flow.py::test_execute_product_task_locks_client_intake_flow" in session_text


def test_build_codex_execution_prompt_embeds_session_and_github_context(tmp_path) -> None:
    packet_path = tmp_path / "tasks" / "github" / "issue-9.json"
    packet_path.parent.mkdir(parents=True, exist_ok=True)
    packet_path.write_text(
        json.dumps(
            {
                "issue_number": 9,
                "title": "Use GitHub as the source of truth",
                "body": "Read the packet first.",
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
    ensure_dev_session(
        repo_root=tmp_path,
        issue_number=9,
        current_task="Fix the failing test from the session file.",
        last_failing_test="tests/test_example.py::test_failure",
        status="in_progress",
        next_step="Open /docs/dev_session.md before editing.",
    )

    prompt = build_codex_execution_prompt(
        "Fix the failing test with a minimal patch.",
        repo_root=tmp_path,
        issue_number=9,
    )

    assert "/docs/dev_session.md" in prompt
    assert "tasks/github/issue-9.json" in prompt
    assert "Use GitHub as the source of truth" in prompt
    assert "Fix the failing test with a minimal patch." in prompt


def test_read_dev_session_round_trips_structured_state(tmp_path) -> None:
    ensure_dev_session(
        repo_root=tmp_path,
        issue_number=None,
        current_task="Track the active development session.",
        last_failing_test="tests/test_example.py::test_failure",
        fixed_items=["Created a structured markdown session file."],
        remaining_items=["Keep the session state current."],
        status="in_progress",
        next_step="Update the file after each step.",
    )

    state = read_dev_session(repo_root=tmp_path)

    assert state["current_task"] == "Track the active development session."
    assert state["last_failing_test"] == "tests/test_example.py::test_failure"
    assert state["what_is_fixed"] == ["Created a structured markdown session file."]
