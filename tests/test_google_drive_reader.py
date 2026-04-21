from __future__ import annotations

from integrations import google_drive_reader


def test_read_google_drive_file_returns_none_without_credentials(monkeypatch) -> None:
    monkeypatch.delenv("GOOGLE_CLIENT_ID", raising=False)
    monkeypatch.delenv("GOOGLE_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("GOOGLE_REFRESH_TOKEN", raising=False)

    assert (
        google_drive_reader.read_google_drive_file({"drive_file_id": "file-001"})
        is None
    )


def test_read_google_drive_file_truncates_content(monkeypatch) -> None:
    content = "A" * (google_drive_reader.MAX_CONTENT_CHARS + 25)

    monkeypatch.setenv("GOOGLE_CLIENT_ID", "client-id")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv("GOOGLE_REFRESH_TOKEN", "refresh-token")
    monkeypatch.setattr(google_drive_reader, "_access_token", lambda: "access-token")
    monkeypatch.setattr(
        google_drive_reader,
        "_fetch_file_metadata",
        lambda file_id, access_token: {
            "id": file_id,
            "name": "Project Notes",
            "mimeType": "text/plain",
            "size": str(len(content)),
        },
    )
    monkeypatch.setattr(
        google_drive_reader,
        "_fetch_file_content",
        lambda file_id, mime_type, access_token: content.encode("utf-8"),
    )

    result = google_drive_reader.read_google_drive_file({"drive_file_id": "file-002"})

    assert result == {
        "file_id": "file-002",
        "name": "Project Notes",
        "content": content[: google_drive_reader.MAX_CONTENT_CHARS],
        "size": len(content),
    }
