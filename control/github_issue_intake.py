from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from control.env_loader import load_env

DEFAULT_REPO_NAME = "westcoastkbp-coder/jarvis-digital-foreman"
DEFAULT_PACKET_DIR = Path("tasks") / "github"
DEFAULT_PACKET_STATUS = "fetched"


def _isoformat_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return (
        value.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _utc_now() -> str:
    return _isoformat_utc(datetime.now(timezone.utc))


def _env_value(name: str) -> str:
    value = os.getenv(name)
    if value:
        return value.strip()

    try:
        value = load_env().get(name, "")
    except FileNotFoundError:
        value = ""

    return str(value).strip()


def resolve_github_token(explicit_token: str | None = None) -> str:
    token = str(explicit_token or "").strip() or _env_value("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GITHUB_TOKEN is required to read GitHub issues.")
    return token


def resolve_repo_name(explicit_repo_name: str | None = None) -> str:
    repo_name = (
        str(explicit_repo_name or "").strip()
        or _env_value("GITHUB_REPOSITORY")
        or DEFAULT_REPO_NAME
    )
    if not repo_name:
        raise RuntimeError("GitHub repository name is required.")
    return repo_name


def fetch_github_issue(
    issue_number: int,
    repo_name: str | None = None,
    token: str | None = None,
) -> Any:
    try:
        from github import Github
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "PyGithub is required to read GitHub issues. Install it with "
            "`python -m pip install PyGithub`."
        ) from exc

    client = Github(resolve_github_token(token))
    repo = client.get_repo(resolve_repo_name(repo_name))
    return repo.get_issue(number=issue_number)


def normalize_issue_to_task_packet(
    issue: Any,
    fetched_at: str | None = None,
) -> dict[str, Any]:
    packet: dict[str, Any] = {
        "issue_id": int(issue.id),
        "issue_number": int(issue.number),
        "source": "github",
        "title": str(issue.title or ""),
        "body": str(issue.body or ""),
        "labels": [str(label.name) for label in getattr(issue, "labels", ())],
        "status": DEFAULT_PACKET_STATUS,
        "fetched_at": fetched_at or _utc_now(),
        "raw_url": getattr(issue, "html_url", None) or getattr(issue, "url", None),
    }

    created_at = getattr(issue, "created_at", None)
    if isinstance(created_at, datetime):
        packet["created_at"] = _isoformat_utc(created_at)

    source_status = getattr(issue, "state", None)
    if source_status:
        packet["source_status"] = str(source_status)

    return packet


def packet_output_path(
    issue_number: int,
    output_dir: Path | str | None = None,
) -> Path:
    base_dir = Path(output_dir) if output_dir is not None else DEFAULT_PACKET_DIR
    return base_dir / f"issue-{issue_number}.json"


def write_task_packet(
    packet: dict[str, Any],
    output_dir: Path | str | None = None,
) -> Path:
    path = packet_output_path(int(packet["issue_number"]), output_dir=output_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(packet, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def fetch_and_store_issue_task_packet(
    issue_number: int,
    repo_name: str | None = None,
    output_dir: Path | str | None = None,
    token: str | None = None,
) -> tuple[dict[str, Any], Path]:
    issue = fetch_github_issue(
        issue_number=issue_number,
        repo_name=repo_name,
        token=token,
    )
    packet = normalize_issue_to_task_packet(issue)
    path = write_task_packet(packet, output_dir=output_dir)
    return packet, path
