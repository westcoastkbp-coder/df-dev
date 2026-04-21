from __future__ import annotations

import copy
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
DEV_SESSION_RELATIVE_PATH = Path("docs") / "dev_session.md"
GITHUB_PACKET_DIR = Path("tasks") / "github"
DEV_SESSION_COMMIT_MESSAGE = "dev: update session state"
DEV_SESSION_STATE_START = "<!-- DEV_SESSION_STATE:START -->"
DEV_SESSION_STATE_END = "<!-- DEV_SESSION_STATE:END -->"
FAILED_TEST_PATTERN = re.compile(r"^FAILED\s+(\S+)")
ISSUE_BRANCH_PATTERN = re.compile(r"(?:^|/)issue-(\d+)$")
DEFAULT_PYTEST_OUTPUTS = (
    "tmp_pytest.out",
    "tmp_pytest_vv.out",
    "tmp_targeted.out",
)
DEFAULT_RULES = (
    "Before each action, read /docs/dev_session.md.",
    "Continue only from /docs/dev_session.md and the linked GitHub packet.",
    "After each step, update /docs/dev_session.md with what changed, the new status, and the next step.",
    f'After each step, run: git add . ; git commit -m "{DEV_SESSION_COMMIT_MESSAGE}" ; git push.',
    "Do not perform work without reading /docs/dev_session.md first.",
)


def _root_dir(repo_root: Path | str | None = None) -> Path:
    return Path(repo_root) if repo_root is not None else REPO_ROOT


def _session_path(repo_root: Path | str | None = None) -> Path:
    return _root_dir(repo_root) / DEV_SESSION_RELATIVE_PATH


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _relative_posix(path: Path, root_dir: Path) -> str:
    return path.resolve().relative_to(root_dir.resolve()).as_posix()


def parse_issue_number_from_branch(branch_name: object) -> int | None:
    match = ISSUE_BRANCH_PATTERN.search(_normalize_text(branch_name))
    if match is None:
        return None
    return int(match.group(1))


def github_packet_path(
    issue_number: int,
    *,
    repo_root: Path | str | None = None,
) -> Path:
    return _root_dir(repo_root) / GITHUB_PACKET_DIR / f"issue-{int(issue_number)}.json"


def load_github_packet(
    issue_number: int,
    *,
    repo_root: Path | str | None = None,
) -> dict[str, Any]:
    path = github_packet_path(issue_number, repo_root=repo_root)
    if not path.is_file():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _github_context_payload(
    issue_number: int | None,
    *,
    repo_root: Path | str | None = None,
) -> dict[str, Any]:
    if issue_number is None:
        return {
            "issue_number": None,
            "packet_path": "",
            "status": "unavailable",
            "title": "",
            "body": "",
            "labels": [],
            "raw_url": "",
            "source_status": "",
        }

    packet_path = github_packet_path(issue_number, repo_root=repo_root)
    packet = load_github_packet(issue_number, repo_root=repo_root)
    root_dir = _root_dir(repo_root)
    relative_packet_path = _relative_posix(packet_path, root_dir)
    if not packet:
        return {
            "issue_number": int(issue_number),
            "packet_path": relative_packet_path,
            "status": "missing",
            "title": "",
            "body": "",
            "labels": [],
            "raw_url": "",
            "source_status": "",
        }

    return {
        "issue_number": int(packet.get("issue_number") or issue_number),
        "packet_path": relative_packet_path,
        "status": "available",
        "title": _normalize_text(packet.get("title")),
        "body": _normalize_text(packet.get("body")),
        "labels": [
            _normalize_text(label)
            for label in packet.get("labels", [])
            if _normalize_text(label)
        ],
        "raw_url": _normalize_text(packet.get("raw_url")),
        "source_status": _normalize_text(packet.get("source_status")),
    }


def extract_last_failing_test(
    *,
    repo_root: Path | str | None = None,
    candidate_files: Sequence[str] | None = None,
) -> str:
    root_dir = _root_dir(repo_root)
    for relative_name in candidate_files or DEFAULT_PYTEST_OUTPUTS:
        path = root_dir / relative_name
        if not path.is_file():
            continue
        for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            match = FAILED_TEST_PATTERN.match(raw_line.strip())
            if match is not None:
                return match.group(1)
    return ""


def _merge_unique_strings(
    existing: Sequence[object] | None,
    additions: Sequence[object] | None,
) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for candidate in list(existing or ()) + list(additions or ()):
        normalized = _normalize_text(candidate)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        merged.append(normalized)
    return merged


def _default_state() -> dict[str, Any]:
    return {
        "updated_at": "",
        "source_of_truth": {
            "dev_session_path": "/docs/dev_session.md",
            "github_context": _github_context_payload(None),
        },
        "rules": list(DEFAULT_RULES),
        "current_task": "",
        "last_failing_test": "",
        "what_is_fixed": [],
        "what_remains": [],
        "status": "pending",
        "next_step": "",
        "step_log": [],
    }


def _extract_state_block(markdown_text: str) -> str:
    start = markdown_text.find(DEV_SESSION_STATE_START)
    end = markdown_text.find(DEV_SESSION_STATE_END)
    if start == -1 or end == -1 or end <= start:
        return ""
    return markdown_text[start + len(DEV_SESSION_STATE_START) : end].strip()


def read_dev_session(
    *,
    repo_root: Path | str | None = None,
) -> dict[str, Any]:
    path = _session_path(repo_root)
    if not path.is_file():
        return {}
    raw_state = _extract_state_block(path.read_text(encoding="utf-8"))
    if not raw_state:
        return {}
    try:
        loaded = json.loads(raw_state)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    if not isinstance(loaded, dict):
        return {}
    return loaded


def _render_bullets(items: Sequence[str]) -> str:
    if not items:
        return "- none recorded"
    return "\n".join(f"- {item}" for item in items)


def _render_step_log(entries: Sequence[dict[str, Any]]) -> str:
    if not entries:
        return "- no steps recorded"

    lines: list[str] = []
    for entry in entries:
        timestamp = _normalize_text(entry.get("time")) or _utc_now()
        did = _normalize_text(entry.get("did")) or "updated session state"
        status = _normalize_text(entry.get("status")) or "pending"
        next_step = _normalize_text(entry.get("next_step"))
        line = f"- {timestamp} | {status} | {did}"
        if next_step:
            line = f"{line} | next: {next_step}"
        lines.append(line)
    return "\n".join(lines)


def _render_markdown(state: dict[str, Any]) -> str:
    source_of_truth = dict(state.get("source_of_truth", {}))
    github_context = dict(source_of_truth.get("github_context", {}))
    serialized_state = json.dumps(state, indent=2, sort_keys=True)

    return (
        "# Dev Session\n\n"
        "This file is the mandatory local context for Codex development work.\n"
        "GitHub is the required upstream source of truth for issue context.\n\n"
        "## Rules\n"
        f"{_render_bullets([_normalize_text(rule) for rule in state.get('rules', [])])}\n\n"
        "## GitHub Context\n"
        f"- packet_path: {_normalize_text(github_context.get('packet_path')) or '(not linked)'}\n"
        f"- issue_number: {_normalize_text(github_context.get('issue_number')) or '(not linked)'}\n"
        f"- issue_title: {_normalize_text(github_context.get('title')) or '(not linked)'}\n"
        f"- issue_status: {_normalize_text(github_context.get('source_status')) or _normalize_text(github_context.get('status')) or '(unknown)'}\n"
        f"- issue_labels: {', '.join(github_context.get('labels', [])) or '(none)'}\n"
        f"- issue_url: {_normalize_text(github_context.get('raw_url')) or '(not linked)'}\n\n"
        "## Current State\n"
        f"- current_task: {_normalize_text(state.get('current_task')) or '(not set)'}\n"
        f"- last_failing_test: {_normalize_text(state.get('last_failing_test')) or '(not recorded)'}\n"
        f"- status: {_normalize_text(state.get('status')) or '(not set)'}\n"
        f"- next_step: {_normalize_text(state.get('next_step')) or '(not set)'}\n\n"
        "## What Is Fixed\n"
        f"{_render_bullets(state.get('what_is_fixed', []))}\n\n"
        "## What Remains\n"
        f"{_render_bullets(state.get('what_remains', []))}\n\n"
        "## Step Log\n"
        f"{_render_step_log(state.get('step_log', []))}\n\n"
        f"{DEV_SESSION_STATE_START}\n"
        f"{serialized_state}\n"
        f"{DEV_SESSION_STATE_END}\n"
    )


def write_dev_session(
    state: dict[str, Any],
    *,
    repo_root: Path | str | None = None,
) -> Path:
    path = _session_path(repo_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    state_copy = copy.deepcopy(state)
    state_copy["updated_at"] = _utc_now()
    path.write_text(_render_markdown(state_copy), encoding="utf-8")
    return path


def ensure_dev_session(
    *,
    repo_root: Path | str | None = None,
    issue_number: int | None = None,
    current_task: str | None = None,
    last_failing_test: str | None = None,
    fixed_items: Sequence[str] | None = None,
    remaining_items: Sequence[str] | None = None,
    status: str | None = None,
    next_step: str | None = None,
) -> dict[str, Any]:
    state = read_dev_session(repo_root=repo_root) or _default_state()
    source_of_truth = dict(state.get("source_of_truth", {}))
    source_of_truth["dev_session_path"] = "/docs/dev_session.md"
    github_context = _github_context_payload(issue_number, repo_root=repo_root)
    if not github_context.get("issue_number"):
        github_context = (
            dict(source_of_truth.get("github_context", {})) or github_context
        )
    source_of_truth["github_context"] = github_context
    state["source_of_truth"] = source_of_truth
    state["rules"] = list(DEFAULT_RULES)
    if current_task is not None:
        state["current_task"] = _normalize_text(current_task)
    elif not _normalize_text(state.get("current_task")) and github_context.get("title"):
        state["current_task"] = (
            f"Issue #{github_context['issue_number']}: {github_context['title']}"
        )
    if last_failing_test is not None:
        state["last_failing_test"] = _normalize_text(last_failing_test)
    if status is not None:
        state["status"] = _normalize_text(status)
    if next_step is not None:
        state["next_step"] = _normalize_text(next_step)
    state["what_is_fixed"] = _merge_unique_strings(
        state.get("what_is_fixed", []),
        fixed_items,
    )
    state["what_remains"] = _merge_unique_strings(
        state.get("what_remains", []),
        remaining_items,
    )
    if not isinstance(state.get("step_log"), list):
        state["step_log"] = []
    write_dev_session(state, repo_root=repo_root)
    return state


def update_dev_session(
    *,
    repo_root: Path | str | None = None,
    issue_number: int | None = None,
    current_task: str | None = None,
    last_failing_test: str | None = None,
    fixed_items: Sequence[str] | None = None,
    remaining_items: Sequence[str] | None = None,
    status: str | None = None,
    next_step: str | None = None,
    did: str | None = None,
) -> dict[str, Any]:
    state = ensure_dev_session(
        repo_root=repo_root,
        issue_number=issue_number,
        current_task=current_task,
        last_failing_test=last_failing_test,
        fixed_items=fixed_items,
        remaining_items=remaining_items,
        status=status,
        next_step=next_step,
    )
    if did is not None:
        step_log = list(state.get("step_log", []))
        step_log.append(
            {
                "time": _utc_now(),
                "did": _normalize_text(did),
                "status": _normalize_text(status)
                or _normalize_text(state.get("status")),
                "next_step": _normalize_text(next_step)
                or _normalize_text(state.get("next_step")),
            }
        )
        state["step_log"] = step_log
    write_dev_session(state, repo_root=repo_root)
    return state


def build_codex_execution_prompt(
    base_prompt: str,
    *,
    repo_root: Path | str | None = None,
    issue_number: int | None = None,
) -> str:
    state = ensure_dev_session(repo_root=repo_root, issue_number=issue_number)
    session_path = _session_path(repo_root)
    session_text = session_path.read_text(encoding="utf-8")
    github_context = dict(state.get("source_of_truth", {}).get("github_context", {}))
    issue_number_text = (
        _normalize_text(github_context.get("issue_number")) or "unlinked"
    )
    issue_title = _normalize_text(github_context.get("title")) or "missing"
    issue_status = _normalize_text(
        github_context.get("source_status")
    ) or _normalize_text(github_context.get("status"))
    issue_labels = ", ".join(github_context.get("labels", [])) or "(none)"
    issue_url = _normalize_text(github_context.get("raw_url")) or "(not linked)"
    github_body = _normalize_text(github_context.get("body")) or "(no body available)"
    packet_path = _normalize_text(github_context.get("packet_path")) or "(missing)"

    return (
        "MANDATORY CONTEXT RULES:\n"
        "- Before any action, read /docs/dev_session.md.\n"
        "- Continue only from /docs/dev_session.md and the GitHub packet below.\n"
        "- GitHub is the mandatory source of truth for task context.\n"
        "- After each step, update /docs/dev_session.md with what changed, the new status, and the next step.\n"
        f'- After each step, run: git add . ; git commit -m "{DEV_SESSION_COMMIT_MESSAGE}" ; git push.\n'
        "- Do not perform work without reading /docs/dev_session.md first.\n\n"
        "GITHUB CONTEXT:\n"
        f"- packet_path: {packet_path}\n"
        f"- issue_number: {issue_number_text}\n"
        f"- issue_title: {issue_title}\n"
        f"- issue_status: {issue_status or '(unknown)'}\n"
        f"- issue_labels: {issue_labels}\n"
        f"- issue_url: {issue_url}\n"
        f"- issue_body: {github_body}\n\n"
        "DEV SESSION SNAPSHOT:\n"
        f"{session_text}\n\n"
        "TASK REQUEST:\n"
        f"{base_prompt}"
    )
