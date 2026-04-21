from __future__ import annotations

import json
from pathlib import Path

import pytest

import app.learning.model_update_config as model_update_config_module
import app.learning.model_evaluator as model_evaluator_module
import app.learning.model_loader as model_loader_module
from app.learning.model_evaluator import evaluate_models
from app.memory import memory_registry, memory_resolver
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


def _write_model(models_root: Path, model_id: str, *, weights: dict[str, float]) -> None:
    model_path = models_root / f"{model_id}.json"
    model_path.parent.mkdir(parents=True, exist_ok=True)
    model_path.write_text(
        json.dumps(
            {
                "model_id": model_id,
                "model_type": "memory_ranker",
                "version": 1,
                "created_at": "2026-04-14T12:00:00Z",
                "features": [
                    "recency",
                    "tag_overlap",
                    "domain_match",
                    "memory_class",
                    "conflict_flag",
                    "state_flag",
                ],
                "weights": weights,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _write_model_update_config(
    path: Path,
    *,
    active_model: str = "memory_ranker_v1",
    candidate_model: str = "memory_ranker_v2",
    evaluation_mode: bool = True,
    evaluation_sample_rate: float = 1.0,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "active_model": active_model,
                "candidate_model": candidate_model,
                "evaluation_mode": evaluation_mode,
                "evaluation_sample_rate": evaluation_sample_rate,
                "memory_ranking": {
                    "last_dataset_size": 0,
                    "min_new_records": 50,
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _write_memory_ranking_config(
    path: Path,
    *,
    enabled: bool = True,
    use_model: bool = True,
    model_id: str = "memory_ranker_v1",
    top_k: int = 2,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "enabled": enabled,
                "use_model": use_model,
                "model_id": model_id,
                "top_k": top_k,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


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


def _configure_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    models_root = tmp_path / "DF" / "shared" / "models"
    evals_root = tmp_path / "DF" / "shared" / "evals"
    model_update_config = tmp_path / "config" / "model_update.json"
    memory_ranking_config = tmp_path / "config" / "memory_ranking.json"
    monkeypatch.setattr(model_loader_module, "MODELS_ROOT", models_root)
    monkeypatch.setattr(model_evaluator_module, "EVALS_ROOT", evals_root)
    monkeypatch.setattr(model_update_config_module, "MODEL_UPDATE_CONFIG_FILE", model_update_config)
    monkeypatch.setattr(memory_resolver, "MEMORY_RANKING_CONFIG_FILE", memory_ranking_config)
    monkeypatch.setattr(storage_adapter, "POLICY_FILE", _write_policy(tmp_path))
    monkeypatch.setattr(
        memory_registry,
        "REGISTRY_FILE",
        tmp_path / "df-system" / "memory_registry.json",
    )
    return models_root, evals_root, model_update_config, memory_ranking_config


def test_evaluate_models_runs_both_models_and_stores_artifact(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    models_root, evals_root, model_update_config, _memory_ranking_config = _configure_environment(
        monkeypatch, tmp_path
    )
    _write_model(
        models_root,
        "memory_ranker_v1",
        weights={
            "recency": 0.0,
            "tag_overlap": 0.0,
            "domain_match": 0.0,
            "memory_class": 0.0,
            "conflict_flag": 1.0,
            "state_flag": 0.0,
        },
    )
    _write_model(
        models_root,
        "memory_ranker_v2",
        weights={
            "recency": 0.7,
            "tag_overlap": 0.2,
            "domain_match": 0.1,
            "memory_class": 0.0,
            "conflict_flag": 0.0,
            "state_flag": 0.0,
        },
    )
    _write_model_update_config(model_update_config)
    monkeypatch.setattr(model_evaluator_module, "_utc_timestamp", lambda: "2026-04-14T12:00:00.123456Z")

    metrics = evaluate_models(
        {"domain": "dev", "type": "task", "tags": ["finance", "urgent"]},
        [
            _artifact("task-recent", timestamp="2026-04-14T11:30:00Z", tags=["finance", "urgent"]),
            _artifact(
                "conflict-1",
                artifact_type="conflict_escalation",
                memory_class="conflict",
                timestamp="2026-04-14T08:00:00Z",
            ),
        ],
    )

    assert metrics is not None
    assert metrics["active_model"] == "memory_ranker_v1"
    assert metrics["candidate_model"] == "memory_ranker_v2"
    assert 0.0 <= float(metrics["agreement_score"]) <= 1.0
    assert isinstance(metrics["top1_match"], bool)
    assert 0.0 <= float(metrics["top_k_overlap"]) <= 1.0
    artifact_path = Path(str(metrics["artifact_path"]))
    assert artifact_path.exists()
    assert artifact_path.parent == evals_root
    stored = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert stored["active_model"] == "memory_ranker_v1"
    assert stored["candidate_model"] == "memory_ranker_v2"
    assert [entry["memory_id"] for entry in stored["active_rankings"]] == ["conflict-1", "task-recent"]
    assert [entry["memory_id"] for entry in stored["candidate_rankings"]] == ["task-recent", "conflict-1"]


def test_evaluate_models_logs_summary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    models_root, _evals_root, model_update_config, _memory_ranking_config = _configure_environment(
        monkeypatch, tmp_path
    )
    _write_model(
        models_root,
        "memory_ranker_v1",
        weights={
            "recency": 0.0,
            "tag_overlap": 0.0,
            "domain_match": 0.0,
            "memory_class": 0.0,
            "conflict_flag": 1.0,
            "state_flag": 0.0,
        },
    )
    _write_model(
        models_root,
        "memory_ranker_v2",
        weights={
            "recency": 1.0,
            "tag_overlap": 0.0,
            "domain_match": 0.0,
            "memory_class": 0.0,
            "conflict_flag": 0.0,
            "state_flag": 0.0,
        },
    )
    _write_model_update_config(model_update_config)

    metrics = evaluate_models(
        {"domain": "dev", "type": "task"},
        [
            _artifact("task-recent", timestamp="2026-04-14T11:30:00Z"),
            _artifact(
                "conflict-1",
                artifact_type="conflict_escalation",
                memory_class="conflict",
                timestamp="2026-04-14T08:00:00Z",
            ),
        ],
    )

    output = capsys.readouterr().out
    assert metrics is not None
    assert "[MODEL_EVAL] active=memory_ranker_v1 candidate=memory_ranker_v2" in output


def test_resolver_evaluation_mode_does_not_change_ranking_output(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    models_root, evals_root, model_update_config, memory_ranking_config = _configure_environment(
        monkeypatch, tmp_path
    )
    _write_model(
        models_root,
        "memory_ranker_v1",
        weights={
            "recency": 0.7,
            "tag_overlap": 0.2,
            "domain_match": 0.1,
            "memory_class": 0.0,
            "conflict_flag": 0.0,
            "state_flag": 0.0,
        },
    )
    _write_model(
        models_root,
        "memory_ranker_v2",
        weights={
            "recency": 0.0,
            "tag_overlap": 0.0,
            "domain_match": 0.0,
            "memory_class": 0.0,
            "conflict_flag": 1.0,
            "state_flag": 0.0,
        },
    )
    _write_memory_ranking_config(memory_ranking_config, model_id="memory_ranker_v1", top_k=2)
    monkeypatch.setattr(model_evaluator_module, "_utc_timestamp", lambda: "2026-04-14T12:00:00.123456Z")
    monkeypatch.setattr(model_evaluator_module, "EVALS_ROOT", evals_root)

    monkeypatch.setattr(
        memory_registry,
        "REGISTRY_FILE",
        tmp_path / "df-system" / "memory_registry.json",
    )
    memory_registry.register_artifact(
        "task-recent",
        "dev",
        "task",
        tmp_path / "df-dev" / "artifacts" / "task_recent.json",
        timestamp="2026-04-14T11:30:00Z",
        tags=["finance", "urgent"],
    )
    memory_registry.register_artifact(
        "conflict-1",
        "dev",
        "conflict_escalation",
        tmp_path / "df-dev" / "artifacts" / "conflict_1.json",
        timestamp="2026-04-14T08:00:00Z",
    )

    _write_model_update_config(model_update_config, evaluation_mode=False, evaluation_sample_rate=1.0)
    without_eval = memory_resolver.resolve_memory({"domain": "dev", "type": "task", "tags": ["finance", "urgent"]})
    _ = capsys.readouterr()

    _write_model_update_config(model_update_config, evaluation_mode=True, evaluation_sample_rate=1.0)
    with_eval = memory_resolver.resolve_memory({"domain": "dev", "type": "task", "tags": ["finance", "urgent"]})
    output = capsys.readouterr().out

    assert [entry["id"] for entry in without_eval] == [entry["id"] for entry in with_eval]
    assert [entry["id"] for entry in with_eval] == ["task-recent"]
    assert "[MODEL_EVAL] active=memory_ranker_v1 candidate=memory_ranker_v2" in output
    assert len(list(evals_root.glob("*.json"))) == 1
