from __future__ import annotations

import json
from types import SimpleNamespace

import scripts.update_personal_context as update_personal_context_script
from modules.personal.context_store import (
    apply_personal_context_update,
    default_personal_context,
    update_personal_context_file,
)


def test_apply_personal_context_update_merges_owner_vehicle_and_reminders() -> None:
    existing = default_personal_context()
    existing["vehicles"] = [
        {
            "name": "Foreman Truck",
            "model": "Tacoma",
            "year": "2021",
            "vin": "VIN-001",
            "maintenance": [{"id": "oil", "status": "done"}],
        }
    ]

    updated = apply_personal_context_update(
        existing,
        {
            "owner": {"name": "Avery", "notes": "Primary owner"},
            "vehicles": [
                {
                    "vin": "VIN-001",
                    "maintenance": [{"id": "tires", "status": "scheduled"}],
                }
            ],
            "reminders": [{"id": "dmv-renewal", "title": "Renew registration"}],
        },
    )

    assert updated["owner"] == {
        "name": "Avery",
        "notes": "Primary owner",
    }
    assert updated["vehicles"] == [
        {
            "name": "Foreman Truck",
            "model": "Tacoma",
            "year": "2021",
            "vin": "VIN-001",
            "maintenance": [
                {"id": "oil", "status": "done"},
                {"id": "tires", "status": "scheduled"},
            ],
        }
    ]
    assert updated["reminders"] == [
        {"id": "dmv-renewal", "title": "Renew registration"},
    ]


def test_update_personal_context_file_writes_default_structure_when_missing(tmp_path) -> None:
    context_path = tmp_path / "personal" / "personal_context.json"

    updated_context, saved_path = update_personal_context_file(
        {
            "immigration": [{"id": "i94", "title": "Check extension date"}],
        },
        context_path=context_path,
    )

    assert saved_path == context_path
    assert updated_context["owner"] == {"name": "", "notes": ""}
    assert updated_context["immigration"] == [{"id": "i94", "title": "Check extension date"}]
    assert json.loads(context_path.read_text(encoding="utf-8")) == updated_context


def test_update_personal_context_script_reads_task_payload(monkeypatch, tmp_path, capsys) -> None:
    task_path = tmp_path / "task-77.json"
    context_path = tmp_path / "personal" / "personal_context.json"
    task_path.write_text(
        json.dumps(
            {
                "task_id": 77,
                "task_type": "personal_context_update",
                "personal_context_update": {
                    "owner": {"name": "Avery"},
                    "dmv": [{"id": "registration", "title": "Renew registration"}],
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        update_personal_context_script,
        "parse_args",
        lambda: SimpleNamespace(update_path=str(task_path), context_file=str(context_path)),
    )

    exit_code = update_personal_context_script.main()

    output = capsys.readouterr().out
    written = json.loads(context_path.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert f"PERSONAL_CONTEXT_UPDATED: {context_path}" in output
    assert written["owner"] == {"name": "Avery", "notes": ""}
    assert written["dmv"] == [{"id": "registration", "title": "Renew registration"}]
