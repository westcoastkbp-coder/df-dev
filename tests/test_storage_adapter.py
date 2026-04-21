from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
import requests

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


def _write_storage_backend(tmp_path: Path) -> Path:
    backend_path = tmp_path / "config" / "storage_backend.json"
    backend_path.parent.mkdir(parents=True, exist_ok=True)
    backend_path.write_text(
        json.dumps(
            {
                "backend": "local",
                "default": "local",
                "webdav_enabled": False,
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


def test_ownerbox_artifact_can_be_saved_loaded_and_archived(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(storage_adapter, "POLICY_FILE", _write_policy(tmp_path))

    saved_path = storage_adapter.save_artifact(
        "ownerbox",
        "task",
        {
            "id": "renew-insurance",
            "summary": "Renew car insurance next week.",
        },
    )
    loaded = storage_adapter.load_artifact("ownerbox", saved_path)
    archived_path = storage_adapter.archive_artifact("ownerbox", saved_path)

    assert (
        saved_path == tmp_path / "ownerbox" / "artifacts" / "task_renew-insurance.json"
    )
    assert loaded["id"] == "renew-insurance"
    assert loaded["domain"] == "ownerbox"
    assert loaded["type"] == "task"
    assert archived_path == (
        tmp_path / "ownerbox" / "artifacts" / "archive" / "task_renew-insurance.json"
    )
    assert archived_path.exists()
    assert not saved_path.exists()


def test_dev_artifact_can_be_saved_and_loaded(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(storage_adapter, "POLICY_FILE", _write_policy(tmp_path))

    saved_path = storage_adapter.save_artifact(
        "dev",
        "report",
        {
            "id": "baseline-check",
            "summary": "Dev contour baseline report.",
        },
    )
    loaded = storage_adapter.load_artifact("dev", saved_path)

    assert (
        saved_path == tmp_path / "df-dev" / "artifacts" / "report_baseline-check.json"
    )
    assert loaded["domain"] == "dev"
    assert loaded["payload"]["summary"] == "Dev contour baseline report."


def test_cross_domain_access_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(storage_adapter, "POLICY_FILE", _write_policy(tmp_path))

    ownerbox_path = storage_adapter.save_artifact(
        "ownerbox",
        "task",
        {
            "id": "owner-only",
            "summary": "Owner-only artifact.",
        },
    )

    with pytest.raises(
        storage_adapter.BoundaryViolationError, match="outside the allowed namespace"
    ):
        storage_adapter.load_artifact("dev", ownerbox_path)


def test_invalid_domain_and_missing_artifact_raise_clear_exceptions(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(storage_adapter, "POLICY_FILE", _write_policy(tmp_path))

    with pytest.raises(storage_adapter.InvalidDomainError, match="Invalid domain"):
        storage_adapter.resolve_path("qa", "task")

    missing_path = tmp_path / "ownerbox" / "artifacts" / "task_missing.json"
    with pytest.raises(
        storage_adapter.ArtifactNotFoundError, match="Artifact not found"
    ):
        storage_adapter.load_artifact("ownerbox", missing_path)


def test_ownerbox_artifact_can_sync_and_fetch_from_opencloud(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(storage_adapter, "POLICY_FILE", _write_policy(tmp_path))
    monkeypatch.setattr(
        storage_adapter,
        "STORAGE_BACKEND_FILE",
        _write_storage_backend(tmp_path),
    )
    monkeypatch.setenv("OPENCLOUD_USERNAME", "owner-user")
    monkeypatch.setenv("OPENCLOUD_APP_TOKEN", "owner-token")

    remote_store: dict[str, bytes] = {}

    def fake_webdav_request(
        method: str,
        remote_path: str,
        config: dict[str, object],
        *,
        content: bytes | None = None,
    ) -> httpx.Response:
        if method == "MKCOL":
            return httpx.Response(201)
        if method == "PUT":
            remote_store[remote_path] = content or b""
            return httpx.Response(201)
        if method == "GET":
            return httpx.Response(200, content=remote_store[remote_path])
        raise AssertionError(f"unexpected method: {method}")

    monkeypatch.setattr(storage_adapter, "_webdav_request", fake_webdav_request)

    saved_path = storage_adapter.save_artifact(
        "ownerbox",
        "task",
        {
            "id": "sync-me",
            "summary": "Ownerbox cloud sync artifact.",
        },
    )
    remote_path = storage_adapter.sync_to_opencloud("ownerbox", saved_path)
    saved_path.unlink()
    fetched_path = storage_adapter.fetch_from_opencloud("ownerbox", remote_path)
    loaded = storage_adapter.load_artifact("ownerbox", fetched_path)

    assert remote_path == "ownerbox/task_sync-me.json"
    assert fetched_path == tmp_path / "ownerbox" / "artifacts" / "task_sync-me.json"
    assert loaded["domain"] == "ownerbox"
    assert loaded["payload"]["summary"] == "Ownerbox cloud sync artifact."


def test_dev_opencloud_is_export_only(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(storage_adapter, "POLICY_FILE", _write_policy(tmp_path))
    monkeypatch.setattr(
        storage_adapter,
        "STORAGE_BACKEND_FILE",
        _write_storage_backend(tmp_path),
    )

    with pytest.raises(storage_adapter.BoundaryViolationError, match="export-only"):
        storage_adapter.fetch_from_opencloud(
            "dev", "exports/dev/report_baseline-check.json"
        )


def test_save_artifact_uploads_to_webdav_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(storage_adapter, "POLICY_FILE", _write_policy(tmp_path))
    backend_path = _write_storage_backend(tmp_path)
    backend_config = json.loads(backend_path.read_text(encoding="utf-8"))
    backend_config["webdav_enabled"] = True
    backend_path.write_text(
        json.dumps(backend_config, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(storage_adapter, "STORAGE_BACKEND_FILE", backend_path)
    monkeypatch.setenv("WEBDEV_USER", "admin")
    monkeypatch.setenv("WEBDEV_PASSWORD", "test-password")

    captured: dict[str, str] = {}

    def fake_upload(local_path: Path | str, remote_path: str) -> str:
        captured["local_path"] = str(local_path)
        captured["remote_path"] = remote_path
        return remote_path

    monkeypatch.setattr(storage_adapter, "upload_to_webdav", fake_upload)

    saved_path = storage_adapter.save_artifact(
        "ownerbox",
        "task",
        {
            "id": "auto-sync",
            "summary": "Auto WebDAV sync.",
        },
    )

    assert saved_path.exists()
    assert captured["local_path"] == str(saved_path)
    assert captured["remote_path"] == "DF/owner/task_auto-sync.json"


def test_save_artifact_keeps_local_when_webdav_upload_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(storage_adapter, "POLICY_FILE", _write_policy(tmp_path))
    backend_path = _write_storage_backend(tmp_path)
    backend_config = json.loads(backend_path.read_text(encoding="utf-8"))
    backend_config["webdav_enabled"] = True
    backend_path.write_text(
        json.dumps(backend_config, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(storage_adapter, "STORAGE_BACKEND_FILE", backend_path)
    monkeypatch.setenv("WEBDEV_USER", "admin")
    monkeypatch.setenv("WEBDEV_PASSWORD", "test-password")

    def failing_upload(local_path: Path | str, remote_path: str) -> str:
        raise storage_adapter.WebDAVNetworkError("boom")

    monkeypatch.setattr(storage_adapter, "upload_to_webdav", failing_upload)

    saved_path = storage_adapter.save_artifact(
        "dev",
        "report",
        {
            "id": "local-only",
            "summary": "Keep local even when WebDAV fails.",
        },
    )
    output = capsys.readouterr().out

    assert saved_path.exists()
    assert "[STORAGE] local saved" in output
    assert "[STORAGE] webdav warning" in output


def test_save_artifact_warns_and_keeps_local_when_webdav_env_is_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(storage_adapter, "POLICY_FILE", _write_policy(tmp_path))
    backend_path = _write_storage_backend(tmp_path)
    backend_config = json.loads(backend_path.read_text(encoding="utf-8"))
    backend_config["webdav_enabled"] = True
    backend_path.write_text(
        json.dumps(backend_config, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(storage_adapter, "STORAGE_BACKEND_FILE", backend_path)
    monkeypatch.delenv("WEBDEV_USER", raising=False)
    monkeypatch.delenv("WEBDEV_PASSWORD", raising=False)

    saved_path = storage_adapter.save_artifact(
        "ownerbox",
        "task",
        {
            "id": "env-missing",
            "summary": "Missing env should not break local save.",
        },
    )
    output = capsys.readouterr().out

    assert saved_path.exists()
    assert "[STORAGE] local saved" in output
    assert "WEBDEV_USER and WEBDEV_PASSWORD" in output


def test_upload_to_webdav_uses_basic_auth(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    backend_path = _write_storage_backend(tmp_path)
    backend_config = json.loads(backend_path.read_text(encoding="utf-8"))
    backend_config["webdav_enabled"] = True
    backend_path.write_text(
        json.dumps(backend_config, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(storage_adapter, "STORAGE_BACKEND_FILE", backend_path)
    monkeypatch.setenv("WEBDEV_USER", "admin")
    monkeypatch.setenv("WEBDEV_PASSWORD", "test-password")

    local_file = tmp_path / "artifact.json"
    local_file.write_text('{"ok":true}\n', encoding="utf-8")
    captured: dict[str, object] = {}

    class DummyResponse:
        status_code = 201

    def fake_put(url: str, data, auth, timeout: int):
        captured["url"] = url
        captured["auth"] = auth
        captured["timeout"] = timeout
        captured["body"] = data.read()
        return DummyResponse()

    monkeypatch.setattr(requests, "put", fake_put)

    remote_path = storage_adapter.upload_to_webdav(local_file, "DF/dev/artifact.json")

    assert remote_path == "DF/dev/artifact.json"
    assert (
        captured["url"]
        == "http://localhost:8080/remote.php/dav/files/admin/DF/dev/artifact.json"
    )
    assert captured["auth"] == ("admin", "test-password")
    assert captured["body"] == b'{"ok":true}\n'
