from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.memory import memory_registry
from app.storage import storage_adapter
from scripts.run_codex_task import run_codex_task


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


def _write_storage_backend(tmp_path: Path, *, webdav_enabled: bool) -> Path:
    backend_path = tmp_path / "config" / "storage_backend.json"
    backend_path.parent.mkdir(parents=True, exist_ok=True)
    backend_path.write_text(
        json.dumps(
            {
                "backend": "local",
                "default": "local",
                "webdav_enabled": webdav_enabled,
                "webdav_url": "http://localhost:8080/remote.php/dav/files/admin",
                "opencloud_enabled": True,
                "opencloud": {
                    "base_url": "https://cloud.example.com/remote.php/dav/spaces/space-id",
                    "username_env": "OPENCLOUD_USERNAME",
                    "app_token_env": "OPENCLOUD_APP_TOKEN",
                    "ownerbox_remote_root": "ownerbox",
                    "dev_remote_root": "exports/dev",
                    "timeout_seconds": 5,
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return backend_path


def test_save_artifact_is_idempotent_by_logical_key(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(storage_adapter, "POLICY_FILE", _write_policy(tmp_path))
    monkeypatch.setattr(storage_adapter, "STORAGE_BACKEND_FILE", _write_storage_backend(tmp_path, webdav_enabled=False))
    monkeypatch.setattr(memory_registry, "REGISTRY_FILE", tmp_path / "df-system" / "memory_registry.json")

    first_path = storage_adapter.save_artifact(
        "dev",
        "task",
        {
            "logical_id": "task-42",
            "summary": "First payload",
        },
    )
    second_path = storage_adapter.save_artifact(
        "dev",
        "task",
        {
            "logical_id": "task-42",
            "summary": "Second payload should not overwrite the first artifact",
        },
    )
    output = capsys.readouterr().out
    stored_payload = json.loads(first_path.read_text(encoding="utf-8"))
    registry_payload = json.loads(memory_registry.REGISTRY_FILE.read_text(encoding="utf-8"))

    assert first_path == second_path
    assert first_path.exists()
    assert stored_payload["payload"]["summary"] == "First payload"
    assert len(list(first_path.parent.glob("task_*.json"))) == 1
    assert len(registry_payload["artifacts"]) == 1
    assert registry_payload["artifacts"][0]["logical_key"] == "dev:task:task-42"
    assert "[STORAGE] local saved" in output
    assert "[STORAGE] idempotent_hit artifact=task-42" in output


def test_save_artifact_does_not_repeat_webdav_upload_on_idempotent_hit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(storage_adapter, "POLICY_FILE", _write_policy(tmp_path))
    monkeypatch.setattr(storage_adapter, "STORAGE_BACKEND_FILE", _write_storage_backend(tmp_path, webdav_enabled=True))
    monkeypatch.setattr(memory_registry, "REGISTRY_FILE", tmp_path / "df-system" / "memory_registry.json")
    monkeypatch.setenv("WEBDEV_USER", "admin")
    monkeypatch.setenv("WEBDEV_PASSWORD", "test-password")

    upload_calls: list[tuple[str, str]] = []

    def fake_upload(local_path: Path | str, remote_path: str) -> str:
        upload_calls.append((str(local_path), remote_path))
        return remote_path

    monkeypatch.setattr(storage_adapter, "upload_to_webdav", fake_upload)

    first_path = storage_adapter.save_artifact(
        "ownerbox",
        "task",
        {
            "logical_id": "owner-sync",
            "summary": "Upload once",
        },
    )
    second_path = storage_adapter.save_artifact(
        "ownerbox",
        "task",
        {
            "logical_id": "owner-sync",
            "summary": "Do not upload again",
        },
    )
    output = capsys.readouterr().out

    assert first_path == second_path
    assert len(upload_calls) == 1
    assert upload_calls[0][1] == "DF/owner/task_owner-sync.json"
    assert "[STORAGE] webdav uploaded DF/owner/task_owner-sync.json" in output
    assert "[STORAGE] idempotent_hit artifact=owner-sync" in output


def test_run_codex_task_repeated_execution_reuses_same_artifact_path(tmp_path: Path) -> None:
    task_path = tmp_path / "task-51.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 51,\n'
            '  "instruction": "Implement the idempotent task",\n'
            '  "constraints": "Modify only necessary parts.",\n'
            '  "success_criteria": "Artifact is written once per logical task."\n'
            "}\n"
        ),
        encoding="utf-8",
    )

    first_task, first_artifact_path = run_codex_task(
        task_path,
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )
    second_task, second_artifact_path = run_codex_task(
        task_path,
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    artifact_paths = list((tmp_path / "artifacts").glob("task-*.txt"))

    assert first_task["task_id"] == second_task["task_id"] == 51
    assert first_artifact_path == second_artifact_path == tmp_path / "artifacts" / "task-51.txt"
    assert len(artifact_paths) == 1
