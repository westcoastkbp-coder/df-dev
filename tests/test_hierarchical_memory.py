from __future__ import annotations

from pathlib import Path

from app.orchestrator import orchestrator
import memory.memory_store as memory_store_module


def test_build_assistant_context_exposes_full_memory_and_scoped_files(
    monkeypatch,
    tmp_path: Path,
) -> None:
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)

    memory_store_module.write_memory(
        "owner_memory",
        {
            "owner_name": "Anton Vorontsov",
            "strategic_focus": "build and commercialize correctly",
        },
    )
    memory_store_module.write_memory(
        "project_state",
        {
            "current_stage": "use phase",
            "core_status": "stable",
        },
    )

    assistant_context = memory_store_module.build_assistant_context()

    assert assistant_context["access_level"] == "assistant_context"
    assert (
        assistant_context["shared_memory"]["owner_memory"]["owner_name"]
        == "Anton Vorontsov"
    )
    assert (
        assistant_context["shared_memory"]["project_state"]["core_status"] == "stable"
    )
    assert set(assistant_context["scoped_memory"]) == {
        "google_context",
        "web_context",
        "dev_context",
    }


def test_build_agent_context_limits_memory_to_role_scope(
    monkeypatch,
    tmp_path: Path,
) -> None:
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)

    memory_store_module.write_memory(
        "owner_memory",
        {
            "owner_name": "Anton Vorontsov",
            "strategic_focus": "sensitive owner strategy",
            "public_positioning": "Owner/operator of Digital Foreman",
        },
    )
    memory_store_module.write_memory(
        "project_state",
        {
            "current_stage": "business validation",
            "core_status": "stable",
            "system_mode": "whole_system_coherence",
        },
    )

    agent_context = memory_store_module.build_agent_context("web_operator")

    assert agent_context["access_level"] == "agent_context"
    assert "assistant_context" not in agent_context
    assert "owner_context" not in agent_context["shared_memory_summary"]
    assert "owner_memory" not in agent_context
    assert set(agent_context["scoped_memory"]) == {"web_context"}
    assert agent_context["shared_memory_summary"]["core_status"] == "stable"


def test_build_agent_task_payload_routes_scoped_memory_by_role(
    monkeypatch,
    tmp_path: Path,
) -> None:
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    monkeypatch.setattr(
        orchestrator,
        "build_assistant_context",
        memory_store_module.build_assistant_context,
    )
    monkeypatch.setattr(
        orchestrator, "build_agent_context", memory_store_module.build_agent_context
    )

    payload = orchestrator.build_agent_task_payload(
        {
            "task_id": "DF-1",
            "goal": "Implement a feature",
        },
        "coder_agent",
    )

    assert payload["memory_access_level"] == "agent_context"
    assert "assistant_context" not in payload
    assert set(payload["agent_context"]["scoped_memory"]) == {"dev_context"}


def test_build_agent_task_payload_allows_explicit_full_memory_override(
    monkeypatch,
    tmp_path: Path,
) -> None:
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    monkeypatch.setattr(
        orchestrator,
        "build_assistant_context",
        memory_store_module.build_assistant_context,
    )
    monkeypatch.setattr(
        orchestrator, "build_agent_context", memory_store_module.build_agent_context
    )

    payload = orchestrator.build_agent_task_payload(
        {
            "task_id": "DF-2",
            "goal": "Implement a feature",
            "full_memory_roles": ["coder_agent"],
        },
        "coder_agent",
    )

    assert payload["memory_access_level"] == "assistant_context"
    assert payload["assistant_context"]["access_level"] == "assistant_context"
    assert set(payload["agent_context"]["scoped_memory"]) == {"dev_context"}


def test_apply_memory_context_includes_scoped_agent_memory(
    monkeypatch,
    tmp_path: Path,
) -> None:
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    monkeypatch.setattr(
        orchestrator, "build_agent_context", memory_store_module.build_agent_context
    )
    monkeypatch.setattr(
        orchestrator,
        "build_assistant_context",
        memory_store_module.build_assistant_context,
    )
    monkeypatch.setattr(orchestrator, "load_recent_memory_records", lambda limit=3: [])

    task_data = {
        "task_id": "DF-3",
        "resolved_context": "Fill the form safely.",
        "context": "Fill the form safely.",
    }

    updated = orchestrator.apply_memory_context(task_data, role="web_operator")

    assert updated["memory_access_level"] == "agent_context"
    assert "Scoped agent memory:" in updated["context"]
    assert "scope: web_context" in updated["context"]
