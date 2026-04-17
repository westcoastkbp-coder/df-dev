from __future__ import annotations

from pathlib import Path

from app.memory import memory_registry, memory_resolver


def test_resolve_memory_returns_latest_matching_artifacts_first(
    monkeypatch,
    tmp_path: Path,
) -> None:
    registry_path = tmp_path / "df-system" / "memory_registry.json"
    monkeypatch.setattr(memory_registry, "REGISTRY_FILE", registry_path)

    memory_registry.register_artifact(
        "task-older",
        "dev",
        "task",
        tmp_path / "df-dev" / "artifacts" / "task_task-older.json",
        timestamp="2026-04-14T09:00:00Z",
    )
    memory_registry.register_artifact(
        "task-latest",
        "dev",
        "task",
        tmp_path / "df-dev" / "artifacts" / "task_task-latest.json",
        timestamp="2026-04-14T11:00:00Z",
    )

    resolved = memory_resolver.resolve_memory({"domain": "dev", "type": "task"})

    assert [entry["id"] for entry in resolved] == ["task-latest", "task-older"]


def test_resolve_memory_enforces_domain_and_type_filters(
    monkeypatch,
    tmp_path: Path,
) -> None:
    registry_path = tmp_path / "df-system" / "memory_registry.json"
    monkeypatch.setattr(memory_registry, "REGISTRY_FILE", registry_path)

    memory_registry.register_artifact(
        "owner-task",
        "ownerbox",
        "task",
        tmp_path / "ownerbox" / "artifacts" / "task_owner-task.json",
        timestamp="2026-04-14T10:00:00Z",
    )
    memory_registry.register_artifact(
        "dev-document",
        "dev",
        "document",
        tmp_path / "df-dev" / "artifacts" / "document_dev-document.json",
        timestamp="2026-04-14T10:30:00Z",
    )
    memory_registry.register_artifact(
        "dev-task",
        "dev",
        "task",
        tmp_path / "df-dev" / "artifacts" / "task_dev-task.json",
        timestamp="2026-04-14T11:00:00Z",
    )

    resolved = memory_resolver.resolve_memory({"domain": "dev", "type": "task"})

    assert [entry["id"] for entry in resolved] == ["dev-task"]


def test_resolve_memory_can_filter_by_tags(monkeypatch, tmp_path: Path) -> None:
    registry_path = tmp_path / "df-system" / "memory_registry.json"
    monkeypatch.setattr(memory_registry, "REGISTRY_FILE", registry_path)

    memory_registry.register_artifact(
        "tagged-task",
        "dev",
        "task",
        tmp_path / "df-dev" / "artifacts" / "task_tagged-task.json",
        timestamp="2026-04-14T11:00:00Z",
        tags=["finance", "urgent"],
    )
    memory_registry.register_artifact(
        "other-task",
        "dev",
        "task",
        tmp_path / "df-dev" / "artifacts" / "task_other-task.json",
        timestamp="2026-04-14T11:30:00Z",
        tags=["finance"],
    )

    resolved = memory_resolver.resolve_memory(
        {"domain": "dev", "type": "task", "tags": ["finance", "urgent"]}
    )

    assert [entry["id"] for entry in resolved] == ["tagged-task"]


def test_resolve_memory_can_filter_by_memory_class(monkeypatch, tmp_path: Path) -> None:
    registry_path = tmp_path / "df-system" / "memory_registry.json"
    monkeypatch.setattr(memory_registry, "REGISTRY_FILE", registry_path)

    memory_registry.register_artifact(
        "trace-1",
        "dev",
        "execution_trace",
        tmp_path / "df-dev" / "artifacts" / "traces" / "trace-1.json",
        timestamp="2026-04-14T10:00:00Z",
    )
    memory_registry.register_artifact(
        "task-1",
        "dev",
        "task",
        tmp_path / "df-dev" / "artifacts" / "task_task-1.json",
        timestamp="2026-04-14T11:00:00Z",
    )

    resolved = memory_resolver.resolve_memory({"domain": "dev", "memory_class": "trace"})

    assert [entry["id"] for entry in resolved] == ["trace-1"]
    assert resolved[0]["type"] == "execution_trace"
    assert resolved[0]["memory_class"] == "trace"
