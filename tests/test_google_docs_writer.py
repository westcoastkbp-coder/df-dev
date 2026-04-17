from __future__ import annotations

from integrations import google_docs_writer


def test_create_google_doc_returns_document_metadata(monkeypatch) -> None:
    captured_write: dict[str, str] = {}
    long_content = "A" * (google_docs_writer.MAX_CONTENT_CHARS + 25)

    monkeypatch.setenv("GOOGLE_CLIENT_ID", "client-id")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv("GOOGLE_REFRESH_TOKEN", "refresh-token")
    monkeypatch.setattr(google_docs_writer, "_access_token", lambda: "access-token")
    monkeypatch.setattr(
        google_docs_writer,
        "_create_document",
        lambda title, access_token: {
            "doc_id": "doc-12345",
            "name": title,
        },
    )
    monkeypatch.setattr(
        google_docs_writer,
        "_write_document_content",
        lambda doc_id, content, access_token: captured_write.update(
            {
                "doc_id": doc_id,
                "content": content,
                "access_token": access_token,
            }
        ),
    )

    result = google_docs_writer.create_google_doc(
        {
            "title": "Project Kickoff",
            "content": long_content,
        }
    )

    assert result == {
        "doc_id": "doc-12345",
        "name": "Project Kickoff",
        "url": "https://docs.google.com/document/d/doc-12345",
    }
    assert captured_write == {
        "doc_id": "doc-12345",
        "content": long_content[: google_docs_writer.MAX_CONTENT_CHARS],
        "access_token": "access-token",
    }
