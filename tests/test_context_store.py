from __future__ import annotations

import json
from pathlib import Path

from control import context_store


def test_load_context_seeds_default_files(tmp_path: Path) -> None:
    context_dir = tmp_path / "context"

    payload = context_store.load_context(context_dir)

    assert payload == {
        "system_context": {
            "active_mode": "dev",
            "current_focus": "",
            "last_update": "",
        },
        "owner_context": {
            "identity": {},
            "immigration": {},
            "permits": {},
            "notes": "",
        },
        "business_context": {
            "projects": [],
            "clients": [],
            "status": "",
        },
    }
    assert (context_dir / "system_context.json").is_file()
    assert (context_dir / "owner_context.json").is_file()
    assert (context_dir / "business_context.json").is_file()


def test_set_active_mode_updates_system_context(tmp_path: Path) -> None:
    context_dir = tmp_path / "context"
    context_store.load_context(context_dir)

    updated = context_store.set_active_mode(
        "owner",
        current_focus="permits",
        context_dir=context_dir,
    )

    assert updated["active_mode"] == "owner"
    assert updated["current_focus"] == "permits"
    assert updated["last_update"]
    saved_payload = json.loads((context_dir / "system_context.json").read_text(encoding="utf-8"))
    assert saved_payload == updated


def test_get_active_context_returns_active_scope(tmp_path: Path) -> None:
    context_dir = tmp_path / "context"
    context_store.load_context(context_dir)
    context_store.update_context(
        "owner_context",
        {
            "identity": {"name": "Owner"},
            "notes": "priority path",
        },
        context_dir=context_dir,
    )
    context_store.set_active_mode("owner", context_dir=context_dir)

    active_context = context_store.get_active_context(context_dir)

    assert active_context["active_mode"] == "owner"
    assert active_context["system_context"]["active_mode"] == "owner"
    assert active_context["active_context"] == {
        "identity": {"name": "Owner"},
        "immigration": {},
        "permits": {},
        "notes": "priority path",
    }
