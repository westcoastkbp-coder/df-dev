from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.learning.memory_ranker import rank_memory
from app.memory import memory_registry, memory_resolver
import app.policy.memory_policy_gate as memory_policy_gate_module
from app.policy.memory_policy_gate import evaluate_memory_policy


def _write_config(tmp_path: Path, *, enabled: bool = True, top_k: int = 10) -> Path:
    config_path = tmp_path / "config" / "memory_ranking.json"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        json.dumps({"enabled": enabled, "top_k": top_k}, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
    return config_path


def _artifact(
    artifact_id: str,
    *,
    domain: str = "dev",
    artifact_type: str = "task",
    memory_class: str = "artifact",
    timestamp: str = "2026-04-14T11:00:00Z",
    tags: list[str] | None = None,
) -> dict[str, object]:
    return {
        "id": artifact_id,
        "domain": domain,
        "type": artifact_type,
        "memory_class": memory_class,
        "timestamp": timestamp,
        "updated_at": timestamp,
        "tags": list(tags or []),
    }


def test_rank_memory_orders_conflict_then_recent_matches() -> None:
    ranked = rank_memory(
        {"domain": "dev", "type": "task", "tags": ["finance", "urgent"]},
        [
            _artifact(
                "task-recent",
                timestamp="2026-04-14T11:30:00Z",
                tags=["finance", "urgent"],
            ),
            _artifact(
                "task-old",
                timestamp="2026-04-14T09:30:00Z",
                tags=["finance", "urgent"],
            ),
            _artifact(
                "conflict-1",
                artifact_type="conflict_escalation",
                memory_class="conflict",
                timestamp="2026-04-14T08:00:00Z",
            ),
        ],
    )

    assert [entry["memory_id"] for entry in ranked] == [
        "conflict-1",
        "task-recent",
        "task-old",
    ]
    assert ranked[0]["score"] >= ranked[1]["score"] >= ranked[2]["score"]
    assert [entry["rank"] for entry in ranked] == [1, 2, 3]


def test_rank_memory_applies_domain_boost_when_other_signals_are_equal() -> None:
    ranked = rank_memory(
        {"domain": "dev", "type": "task"},
        [
            _artifact("same-domain", domain="dev", timestamp="2026-04-14T11:00:00Z"),
            _artifact(
                "other-domain", domain="ownerbox", timestamp="2026-04-14T11:00:00Z"
            ),
        ],
    )

    assert [entry["memory_id"] for entry in ranked] == ["same-domain", "other-domain"]
    assert ranked[0]["score"] > ranked[1]["score"]


def test_resolve_memory_enforces_top_k_and_returns_ranked_order(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        memory_registry,
        "REGISTRY_FILE",
        tmp_path / "df-system" / "memory_registry.json",
    )
    monkeypatch.setattr(
        memory_resolver, "MEMORY_RANKING_CONFIG_FILE", _write_config(tmp_path, top_k=2)
    )

    memory_registry.register_artifact(
        "task-oldest",
        "dev",
        "task",
        tmp_path / "df-dev" / "artifacts" / "task_oldest.json",
        timestamp="2026-04-14T09:00:00Z",
        tags=["finance", "urgent"],
    )
    memory_registry.register_artifact(
        "task-middle",
        "dev",
        "task",
        tmp_path / "df-dev" / "artifacts" / "task_middle.json",
        timestamp="2026-04-14T10:00:00Z",
        tags=["finance", "urgent"],
    )
    memory_registry.register_artifact(
        "task-latest",
        "dev",
        "task",
        tmp_path / "df-dev" / "artifacts" / "task_latest.json",
        timestamp="2026-04-14T11:00:00Z",
        tags=["finance", "urgent"],
    )

    resolved = memory_resolver.resolve_memory(
        {"domain": "dev", "type": "task", "tags": ["finance", "urgent"]}
    )

    output = capsys.readouterr().out
    assert [entry["id"] for entry in resolved] == ["task-latest", "task-middle"]
    assert "[RANKER] scored 3 objects" in output
    assert "[RANKER] top selected ids=['task-latest', 'task-middle']" in output


def test_resolver_ranking_keeps_policy_gate_behavior_correct(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        memory_registry,
        "REGISTRY_FILE",
        tmp_path / "df-system" / "memory_registry.json",
    )
    monkeypatch.setattr(
        memory_resolver, "MEMORY_RANKING_CONFIG_FILE", _write_config(tmp_path, top_k=2)
    )
    monkeypatch.setattr(
        memory_policy_gate_module,
        "_utc_now",
        lambda: datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc),
    )

    memory_registry.register_artifact(
        "task-extra-recent",
        "dev",
        "task",
        tmp_path / "df-dev" / "artifacts" / "task_extra_recent.json",
        timestamp="2026-04-14T11:50:00Z",
        tags=["finance", "urgent", "ops"],
    )
    memory_registry.register_artifact(
        "task-exact-recent",
        "dev",
        "task",
        tmp_path / "df-dev" / "artifacts" / "task_exact_recent.json",
        timestamp="2026-04-14T11:40:00Z",
        tags=["finance", "urgent"],
    )
    memory_registry.register_artifact(
        "task-exact-old",
        "dev",
        "task",
        tmp_path / "df-dev" / "artifacts" / "task_exact_old.json",
        timestamp="2026-04-14T09:00:00Z",
        tags=["finance", "urgent"],
    )

    resolved = memory_resolver.resolve_memory(
        {"domain": "dev", "type": "task", "tags": ["finance", "urgent"]}
    )
    decision = evaluate_memory_policy(
        {
            "task_id": 91,
            "memory_context": {
                "domain": "dev",
                "type": "task",
                "tags": ["finance", "urgent"],
            },
        },
        resolved,
    )

    assert [entry["id"] for entry in resolved] == [
        "task-exact-recent",
        "task-extra-recent",
    ]
    assert decision == {
        "allowed": False,
        "reason": "recent_duplicate_detected",
        "matched_artifact_id": "task-exact-recent",
        "action": "block",
    }
