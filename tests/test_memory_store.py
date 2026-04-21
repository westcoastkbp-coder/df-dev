from __future__ import annotations

import json
from pathlib import Path

import pytest

import memory.memory_store as memory_store_module


def test_write_memory_appends_decision_entry(monkeypatch, tmp_path: Path) -> None:
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)

    entry = memory_store_module.write_memory(
        "decisions",
        {
            "decision": "Use unified JSON memory",
            "reason": "Keep persistence simple",
        },
    )

    stored_payload = json.loads((memory_dir / "decisions.json").read_text(encoding="utf-8"))

    assert entry["decision"] == "Use unified JSON memory"
    assert entry["reason"] == "Keep persistence simple"
    assert entry["timestamp"]
    assert stored_payload["decisions"][-1]["decision"] == "Use unified JSON memory"


def test_get_project_state_returns_default_structure(monkeypatch, tmp_path: Path) -> None:
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)

    assert memory_store_module.get_project_state() == {
        "active_block": "",
        "core_status": "",
        "current_stage": "",
        "focus": "",
        "next_step": "",
        "next_steps": [],
        "operating_phase": "",
        "system_mode": "",
    }


def test_build_memory_summary_returns_structured_payload(monkeypatch, tmp_path: Path) -> None:
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)

    memory_store_module.write_memory(
        "decisions",
        {
            "decision": "Keep JSON memory only",
            "reason": "Avoid drift",
        },
    )
    memory_store_module.write_memory(
        "architecture",
        {
            "system_rules": ["Do not use DB memory"],
        },
    )
    memory_store_module.write_memory(
        "owner_memory",
        {
            "owner_name": "Anton Vorontsov",
            "priorities": ["EB1", "Digital Foreman"],
            "strategic_focus": "use + validate + preserve architecture + commercialize correctly",
        },
    )
    memory_store_module.write_memory(
        "project_state",
        {
            "current_stage": "core complete",
            "active_block": "none",
            "core_status": "complete",
            "operating_phase": "use_phase",
            "system_mode": "whole_system_coherence",
            "focus": "real usage, coherence, validation, commercialization path",
            "next_step": "owner operations",
            "next_steps": ["owner operations"],
        },
    )

    summary = memory_store_module.build_memory_summary()

    assert summary == {
        "active_block": "none",
        "architecture_rules": ["Do not use DB memory"],
        "core_status": "complete",
        "current_stage": "core complete",
        "focus": "real usage, coherence, validation, commercialization path",
        "last_decisions": ["Keep JSON memory only"],
        "next_step": "owner operations",
        "operating_phase": "use_phase",
        "owner_context": {
            "owner_name": "Anton Vorontsov",
            "strategic_focus": "use + validate + preserve architecture + commercialize correctly",
        },
        "owner_priorities": ["EB1", "Digital Foreman"],
        "system_mode": "whole_system_coherence",
    }


def test_build_memory_snapshot_includes_raw_memory_and_summary(monkeypatch, tmp_path: Path) -> None:
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)

    memory_store_module.write_memory(
        "project_state",
        {
            "current_stage": "core complete",
            "core_status": "complete",
            "operating_phase": "use_phase",
        },
    )

    snapshot = memory_store_module.build_memory_snapshot()

    assert snapshot["project_state"]["current_stage"] == "core complete"
    assert snapshot["memory_summary"]["core_status"] == "complete"
    assert snapshot["memory_summary"]["operating_phase"] == "use_phase"
    assert "owner_memory" in snapshot


def test_build_memory_summary_tolerates_missing_owner_memory_file(monkeypatch, tmp_path: Path) -> None:
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)

    memory_store_module.write_memory(
        "project_state",
        {
            "current_stage": "core complete",
            "core_status": "complete",
        },
    )
    owner_memory_path = memory_dir / "owner_memory.json"
    if owner_memory_path.exists():
        owner_memory_path.unlink()

    summary = memory_store_module.build_memory_summary()

    assert summary["current_stage"] == "core complete"
    assert summary["core_status"] == "complete"
    assert summary["owner_priorities"] == []
    assert "owner_context" not in summary


def test_write_execution_system_context_persists_valid_schema(monkeypatch, tmp_path: Path) -> None:
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)

    stored_context = memory_store_module.write_context(
        memory_store_module.DEFAULT_EXECUTION_SYSTEM_CONTEXT,
    )

    assert set(stored_context) == {"system_state", "active_tasks", "last_actions", "metadata"}
    assert stored_context["metadata"]["version"] == memory_store_module.EXECUTION_CONTEXT_VERSION
    assert stored_context["metadata"]["updated_at"]
    assert len(stored_context["metadata"]["checksum"]) == 64
    assert memory_store_module.read_context() == stored_context


def test_read_execution_system_context_fails_for_corrupted_payload(monkeypatch, tmp_path: Path) -> None:
    memory_dir = tmp_path / "memory"
    memory_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    (memory_dir / "system_context.json").write_text("{broken json", encoding="utf-8")

    with pytest.raises(RuntimeError, match="CONTEXT_INVALID"):
        memory_store_module.read_context()


def test_read_execution_system_context_fails_for_checksum_mismatch(monkeypatch, tmp_path: Path) -> None:
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    stored_context = memory_store_module.write_context(
        memory_store_module.DEFAULT_EXECUTION_SYSTEM_CONTEXT,
    )
    tampered_context = json.loads(json.dumps(stored_context))
    tampered_context["last_actions"] = ["manually edited"]
    context_path = memory_dir / "system_context.json"
    context_path.write_text(json.dumps(tampered_context, indent=2) + "\n", encoding="utf-8")

    with pytest.warns(RuntimeWarning, match="checksum mismatch"):
        with pytest.raises(RuntimeError, match="CONTEXT_INVALID"):
            memory_store_module.read_context()


def test_read_execution_system_context_trims_oversized_payload(monkeypatch, tmp_path: Path) -> None:
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)

    large_context = json.loads(json.dumps(memory_store_module.DEFAULT_EXECUTION_SYSTEM_CONTEXT))
    large_context["active_tasks"] = [f"task-{index:04d}-" + ("x" * 120) for index in range(400)]
    large_context["last_actions"] = [f"action-{index:04d}-" + ("y" * 120) for index in range(400)]

    stored_context = memory_store_module.write_context(large_context)
    context_path = memory_dir / "system_context.json"

    assert context_path.stat().st_size <= memory_store_module.EXECUTION_CONTEXT_MAX_BYTES
    assert (
        len(stored_context["active_tasks"]) < len(large_context["active_tasks"])
        or len(stored_context["last_actions"]) < len(large_context["last_actions"])
    )
    assert stored_context["last_actions"] == large_context["last_actions"][-len(stored_context["last_actions"]) :]
    assert memory_store_module.read_context()["metadata"]["checksum"] == stored_context["metadata"]["checksum"]


def test_update_context_rolls_back_when_candidate_is_invalid(monkeypatch, tmp_path: Path) -> None:
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    original_context = memory_store_module.write_context(
        memory_store_module.DEFAULT_EXECUTION_SYSTEM_CONTEXT,
    )

    with pytest.raises(RuntimeError, match="CONTEXT_INVALID"):
        memory_store_module.update_context(
            lambda payload: {
                "system_state": payload["system_state"],
                "active_tasks": payload["active_tasks"],
                "last_actions": payload["last_actions"],
            }
        )

    assert memory_store_module.read_context() == original_context
