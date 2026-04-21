from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.learning import memory_ranker as memory_ranker_module
from app.learning.memory_ranker import rank_memory
from app.learning.model_output_contract import (
    ModelOutputContractError,
    normalize_model_output,
)
from app.learning.model_ranker_adapter import score_with_model
from app.memory import memory_registry, memory_resolver
import app.policy.memory_policy_gate as memory_policy_gate_module
from app.policy.memory_policy_gate import evaluate_memory_policy


def _write_config(
    tmp_path: Path,
    *,
    enabled: bool = True,
    use_model: bool = False,
    top_k: int = 10,
) -> Path:
    config_path = tmp_path / "config" / "memory_ranking.json"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        json.dumps(
            {"enabled": enabled, "top_k": top_k, "use_model": use_model},
            indent=2,
            sort_keys=True,
        )
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


def test_score_with_model_returns_versioned_contract() -> None:
    scored = score_with_model(
        {"domain": "dev", "type": "task", "tags": ["finance", "urgent"]},
        [
            _artifact("task-1", tags=["finance", "urgent"]),
            _artifact("task-2", tags=["finance"]),
        ],
    )

    assert scored["schema_version"] == "v1"
    assert scored["output_type"] == "ranking"
    assert isinstance(scored["model_id"], str) and scored["model_id"]
    assert [entry["entity_id"] for entry in scored["items"]] == ["task-1", "task-2"]
    assert all(0.0 <= float(entry["score"]) <= 1.0 for entry in scored["items"])
    assert all(0.0 <= float(entry["confidence"]) <= 1.0 for entry in scored["items"])


def test_normalize_model_output_rejects_invalid_payload() -> None:
    with pytest.raises(ModelOutputContractError, match="missing required fields"):
        normalize_model_output(
            {
                "schema_version": "v1",
                "model_id": "memory_ranker_v1",
                "items": [],
            }
        )


def test_normalize_model_output_rejects_out_of_range_values() -> None:
    with pytest.raises(
        ModelOutputContractError, match=r"score must be within \[0, 1\]"
    ):
        normalize_model_output(
            {
                "schema_version": "v1",
                "model_id": "memory_ranker_v1",
                "output_type": "ranking",
                "items": [
                    {
                        "entity_id": "task-1",
                        "score": 1.2,
                        "confidence": 0.9,
                    }
                ],
            }
        )


def test_normalize_model_output_orders_and_deduplicates_items() -> None:
    normalized = normalize_model_output(
        {
            "schema_version": "v1",
            "model_id": "memory_ranker_v1",
            "output_type": "ranking",
            "items": [
                {"entity_id": "task-2", "score": 0.4, "confidence": 0.3},
                {"entity_id": "task-1", "score": 0.9, "confidence": 0.8},
                {"entity_id": "task-2", "score": 0.7, "confidence": 0.6},
            ],
        }
    )

    assert normalized["items"] == [
        {"entity_id": "task-1", "score": 0.9, "confidence": 0.8},
        {"entity_id": "task-2", "score": 0.7, "confidence": 0.6},
    ]


def test_rank_memory_uses_heuristic_only_when_model_disabled(
    capsys: pytest.CaptureFixture[str],
) -> None:
    ranked = rank_memory(
        {"domain": "dev", "type": "task", "tags": ["finance", "urgent"]},
        [
            _artifact(
                "task-recent",
                timestamp="2026-04-14T11:30:00Z",
                tags=["finance", "urgent"],
            ),
            _artifact(
                "conflict-1",
                artifact_type="conflict_escalation",
                memory_class="conflict",
                timestamp="2026-04-14T08:00:00Z",
            ),
        ],
        model_enabled=False,
    )

    output = capsys.readouterr().out
    assert [entry["memory_id"] for entry in ranked] == ["conflict-1", "task-recent"]
    assert "[RANKER] heuristic_only" in output
    assert "[RANKER] model_enabled" not in output


def test_rank_memory_combines_model_and_heuristic_scores(
    capsys: pytest.CaptureFixture[str],
) -> None:
    context = {"domain": "dev", "type": "task", "tags": ["finance", "urgent"]}
    artifacts = [
        _artifact(
            "task-best", timestamp="2026-04-14T11:30:00Z", tags=["finance", "urgent"]
        ),
        _artifact("task-partial", timestamp="2026-04-14T11:00:00Z", tags=["finance"]),
    ]

    heuristic_ranked = rank_memory(context, artifacts, model_enabled=False)
    model_scores = {
        entry["entity_id"]: float(entry["score"])
        for entry in score_with_model(context, artifacts)["items"]
    }
    combined_ranked = rank_memory(context, artifacts, model_enabled=True)

    output = capsys.readouterr().out
    heuristic_scores = {
        entry["memory_id"]: float(entry["score"]) for entry in heuristic_ranked
    }
    combined_scores = {
        entry["memory_id"]: float(entry["score"]) for entry in combined_ranked
    }

    for memory_id, heuristic_score in heuristic_scores.items():
        assert combined_scores[memory_id] == round(
            (0.6 * model_scores[memory_id]) + (0.4 * heuristic_score),
            6,
        )

    assert "[RANKER] model_enabled" in output
    assert "[RANKER] combined scoring applied" in output


def test_rank_memory_model_can_change_ordering() -> None:
    context = {"domain": "dev", "type": "task", "tags": ["finance", "urgent"]}
    artifacts = [
        _artifact(
            "task-recent",
            timestamp="2026-04-14T11:30:00Z",
            tags=["finance", "urgent"],
        ),
        _artifact(
            "conflict-1",
            artifact_type="conflict_escalation",
            memory_class="conflict",
            timestamp="2026-04-14T08:00:00Z",
        ),
    ]

    heuristic_ranked = rank_memory(context, artifacts, model_enabled=False)
    combined_ranked = rank_memory(context, artifacts, model_enabled=True)

    assert [entry["memory_id"] for entry in heuristic_ranked] == [
        "conflict-1",
        "task-recent",
    ]
    assert [entry["memory_id"] for entry in combined_ranked] == [
        "task-recent",
        "conflict-1",
    ]


def test_rank_memory_falls_back_to_heuristic_when_model_output_is_invalid(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    context = {"domain": "dev", "type": "task", "tags": ["finance", "urgent"]}
    artifacts = [
        _artifact(
            "task-recent", timestamp="2026-04-14T11:30:00Z", tags=["finance", "urgent"]
        ),
        _artifact(
            "task-old", timestamp="2026-04-14T09:30:00Z", tags=["finance", "urgent"]
        ),
    ]

    def _invalid_model_output(
        task_packet: dict[str, object] | None,
        memory_objects: list[dict[str, object]] | None,
        *,
        model_id: str | None = None,
    ) -> dict[str, object]:
        return {
            "schema_version": "v1",
            "model_id": "memory_ranker_v1",
            "output_type": "ranking",
            "items": [
                {
                    "entity_id": "task-recent",
                    "score": 1.5,
                    "confidence": 0.5,
                },
                {
                    "entity_id": "task-old",
                    "score": 0.1,
                    "confidence": 0.5,
                },
            ],
        }

    monkeypatch.setattr(memory_ranker_module, "score_with_model", _invalid_model_output)

    heuristic_ranked = rank_memory(context, artifacts, model_enabled=False)
    fallback_ranked = rank_memory(context, artifacts, model_enabled=True)

    output = capsys.readouterr().out
    assert fallback_ranked == heuristic_ranked
    assert "[RANKER] model_enabled" in output
    assert "[RANKER] heuristic_only" in output
    assert "[RANKER] combined scoring applied" not in output


def test_rank_memory_falls_back_to_heuristic_when_schema_version_mismatches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = {"domain": "dev", "type": "task", "tags": ["finance", "urgent"]}
    artifacts = [
        _artifact(
            "task-recent", timestamp="2026-04-14T11:30:00Z", tags=["finance", "urgent"]
        ),
        _artifact(
            "task-old", timestamp="2026-04-14T09:30:00Z", tags=["finance", "urgent"]
        ),
    ]

    def _unsupported_schema_output(
        task_packet: dict[str, object] | None,
        memory_objects: list[dict[str, object]] | None,
        *,
        model_id: str | None = None,
    ) -> dict[str, object]:
        return {
            "schema_version": "v2",
            "model_id": "memory_ranker_v1",
            "output_type": "ranking",
            "items": [
                {"entity_id": "task-recent", "score": 0.9, "confidence": 0.9},
                {"entity_id": "task-old", "score": 0.1, "confidence": 0.1},
            ],
        }

    monkeypatch.setattr(
        memory_ranker_module, "score_with_model", _unsupported_schema_output
    )

    assert rank_memory(context, artifacts, model_enabled=True) == rank_memory(
        context,
        artifacts,
        model_enabled=False,
    )


def test_resolver_model_toggle_keeps_policy_gate_behavior(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        memory_registry,
        "REGISTRY_FILE",
        tmp_path / "df-system" / "memory_registry.json",
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

    expected_decision = {
        "allowed": False,
        "reason": "recent_duplicate_detected",
        "matched_artifact_id": "task-exact-recent",
        "action": "block",
    }
    context = {"domain": "dev", "type": "task", "tags": ["finance", "urgent"]}
    task_packet = {"task_id": 91, "memory_context": context}

    monkeypatch.setattr(
        memory_resolver, "MEMORY_RANKING_CONFIG_FILE", _write_config(tmp_path, top_k=2)
    )
    resolved_heuristic = memory_resolver.resolve_memory(context)
    decision_heuristic = evaluate_memory_policy(task_packet, resolved_heuristic)

    monkeypatch.setattr(
        memory_resolver,
        "MEMORY_RANKING_CONFIG_FILE",
        _write_config(tmp_path, use_model=True, top_k=2),
    )
    resolved_model = memory_resolver.resolve_memory(context)
    decision_model = evaluate_memory_policy(task_packet, resolved_model)

    assert [entry["id"] for entry in resolved_heuristic] == [
        "task-exact-recent",
        "task-extra-recent",
    ]
    assert [entry["id"] for entry in resolved_model] == [
        "task-exact-recent",
        "task-extra-recent",
    ]
    assert decision_heuristic == expected_decision
    assert decision_model == expected_decision
