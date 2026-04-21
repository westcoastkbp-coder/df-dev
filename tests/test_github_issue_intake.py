from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace

from control.github_issue_intake import (
    normalize_issue_to_task_packet,
    write_task_packet,
)


def test_normalize_issue_to_task_packet_includes_required_fields() -> None:
    issue = SimpleNamespace(
        id=987654321,
        number=42,
        title="Normalize GitHub issue intake",
        body=None,
        labels=[SimpleNamespace(name="bug"), SimpleNamespace(name="codex")],
        state="open",
        created_at=datetime(2026, 4, 10, 18, 15, tzinfo=timezone.utc),
        html_url="https://github.com/example/repo/issues/42",
        url="https://api.github.com/repos/example/repo/issues/42",
    )

    packet = normalize_issue_to_task_packet(
        issue,
        fetched_at="2026-04-10T19:00:00Z",
    )

    assert packet == {
        "issue_id": 987654321,
        "issue_number": 42,
        "source": "github",
        "title": "Normalize GitHub issue intake",
        "body": "",
        "labels": ["bug", "codex"],
        "status": "fetched",
        "source_status": "open",
        "created_at": "2026-04-10T18:15:00Z",
        "fetched_at": "2026-04-10T19:00:00Z",
        "raw_url": "https://github.com/example/repo/issues/42",
    }


def test_write_task_packet_uses_predictable_issue_path(tmp_path) -> None:
    packet = {
        "issue_id": 987654321,
        "issue_number": 42,
        "source": "github",
        "title": "Normalize GitHub issue intake",
        "body": "Issue body",
        "labels": ["bug"],
        "status": "fetched",
        "fetched_at": "2026-04-10T19:00:00Z",
        "raw_url": "https://github.com/example/repo/issues/42",
    }

    path = write_task_packet(packet, output_dir=tmp_path / "tasks" / "github")

    assert path == tmp_path / "tasks" / "github" / "issue-42.json"
    assert json.loads(path.read_text(encoding="utf-8")) == packet
