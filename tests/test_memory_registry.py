from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.memory import memory_registry
from app.storage import storage_adapter


def _write_policy(tmp_path: Path) -> Path:
    policy_path = tmp_path / "config" / "contour_policy.json"
    policy_path.parent.mkdir(parents=True, exist_ok=True)
    policy_path.write_text(
        json.dumps(
            {
                "contours": {
                    "df-dev": {
                        "working_root": str(tmp_path / "df-dev"),
                    },
                    "ownerbox": {
                        "working_root": str(tmp_path / "ownerbox"),
                    },
                }
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return policy_path


def test_register_artifact_persists_json_index(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    registry_path = tmp_path / "df-system" / "memory_registry.json"
    monkeypatch.setattr(memory_registry, "REGISTRY_FILE", registry_path)

    entry = memory_registry.register_artifact(
        "artifact-1",
        "ownerbox",
        "document",
        tmp_path / "ownerbox" / "artifacts" / "document_artifact-1.json",
        remote_path="DF/owner/document_artifact-1.json",
        timestamp="2026-04-14T12:00:00Z",
        tags=["finance", "insurance"],
    )

    stored_payload = json.loads(registry_path.read_text(encoding="utf-8"))

    assert entry["id"] == "artifact-1"
    assert entry["domain"] == "ownerbox"
    assert entry["memory_class"] == "artifact"
    assert entry["status"] == "active"
    assert entry["truth_level"] == "working"
    assert entry["execution_role"] == "output"
    assert entry["type"] == "document"
    assert entry["logical_key"] == "ownerbox:document:artifact-1"
    assert entry["local_path"] == str(
        tmp_path / "ownerbox" / "artifacts" / "document_artifact-1.json"
    )
    assert entry["remote_path"] == "DF/owner/document_artifact-1.json"
    assert entry["created_at"] == "2026-04-14T12:00:00Z"
    assert entry["updated_at"] == "2026-04-14T12:00:00Z"
    assert entry["timestamp"] == "2026-04-14T12:00:00Z"
    assert entry["tags"] == ["finance", "insurance"]
    assert entry["refs"] == []
    assert entry["payload"] is None
    assert stored_payload == {"artifacts": [entry]}


def test_save_artifact_registers_entry_and_supports_get_by_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(storage_adapter, "POLICY_FILE", _write_policy(tmp_path))
    monkeypatch.setattr(
        memory_registry,
        "REGISTRY_FILE",
        tmp_path / "df-system" / "memory_registry.json",
    )

    saved_path = storage_adapter.save_artifact(
        "ownerbox",
        "task",
        {
            "id": "renew-insurance",
            "summary": "Renew car insurance next week.",
        },
    )

    entry = memory_registry.get_artifact_by_id("renew-insurance")

    assert (
        saved_path == tmp_path / "ownerbox" / "artifacts" / "task_renew-insurance.json"
    )
    assert entry is not None
    assert entry["id"] == "renew-insurance"
    assert entry["domain"] == "ownerbox"
    assert entry["memory_class"] == "artifact"
    assert entry["status"] == "active"
    assert entry["type"] == "task"
    assert entry["logical_key"] == "ownerbox:task:renew-insurance"
    assert entry["local_path"] == str(saved_path)
    assert entry["remote_path"] is None
    assert entry["tags"] == []
    assert entry["refs"] == []
    assert entry["payload"] == {
        "id": "renew-insurance",
        "summary": "Renew car insurance next week.",
    }


def test_registry_lists_can_filter_by_domain_and_type(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(storage_adapter, "POLICY_FILE", _write_policy(tmp_path))
    monkeypatch.setattr(
        memory_registry,
        "REGISTRY_FILE",
        tmp_path / "df-system" / "memory_registry.json",
    )

    storage_adapter.save_artifact(
        "ownerbox",
        "task",
        {
            "id": "owner-task",
            "summary": "Owner task artifact.",
        },
    )
    storage_adapter.save_artifact(
        "dev",
        "document",
        {
            "id": "dev-doc",
            "summary": "Dev document artifact.",
        },
    )
    storage_adapter.save_artifact(
        "dev",
        "task",
        {
            "id": "dev-task",
            "summary": "Dev task artifact.",
        },
    )

    ownerbox_entries = memory_registry.list_by_domain("ownerbox")
    dev_entries = memory_registry.list_by_domain("dev")
    task_entries = memory_registry.list_by_type("task")
    document_entries = memory_registry.list_by_type("document")

    assert [entry["id"] for entry in ownerbox_entries] == ["owner-task"]
    assert [entry["id"] for entry in dev_entries] == ["dev-doc", "dev-task"]
    assert [entry["id"] for entry in task_entries] == ["owner-task", "dev-task"]
    assert [entry["id"] for entry in document_entries] == ["dev-doc"]
