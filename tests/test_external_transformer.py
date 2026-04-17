from __future__ import annotations

from control.external_transformer import build_drive_to_google_doc_content


def test_build_drive_to_google_doc_content_formats_files_deterministically() -> None:
    expected_content = (
        "Document Title: Client Summary\n"
        "Transform Mode: plain_summary\n"
        "Source File Count: 2\n"
        "Generated At: 2026-04-10T18:00:00Z\n\n"
        "Source File 1: Client Notes\n"
        "Source File ID: drive-file-001\n"
        "Extracted Content:\n"
        "Line one\n"
        "Line two\n\n"
        "Source File 2: Action Items\n"
        "Source File ID: drive-file-002\n"
        "Extracted Content:\n"
        "Follow up with client\n"
    )
    transformed = build_drive_to_google_doc_content(
        {
            "external_files": [
                {
                    "file_id": "drive-file-001",
                    "name": "Client Notes",
                    "content": "Line one\nLine two",
                },
                {
                    "file_id": "drive-file-002",
                    "name": "Action Items",
                    "content": "Follow up with client",
                },
            ]
        },
        output_doc_title="Client Summary",
        transform_mode="plain_summary",
        generated_at="2026-04-10T18:00:00Z",
    )

    assert transformed == {
        "content": expected_content,
        "content_summary": expected_content[:200],
        "generated_at": "2026-04-10T18:00:00Z",
        "source_file_ids": ["drive-file-001", "drive-file-002"],
        "source_file_names": ["Client Notes", "Action Items"],
        "transform_mode": "plain_summary",
    }
