from __future__ import annotations

import json
from pathlib import Path

import pytest

import app.learning.model_loader as model_loader_module
from app.learning.model_loader import ModelLoaderError, load_model
from app.learning.memory_ranker import rank_memory
from app.learning.model_ranker_adapter import score_with_model


def _write_model(
    tmp_path: Path,
    *,
    model_id: str,
    weights: dict[str, float] | None = None,
    features: list[str] | None = None,
) -> Path:
    model_path = tmp_path / "DF" / "shared" / "models" / f"{model_id}.json"
    model_path.parent.mkdir(parents=True, exist_ok=True)
    model_path.write_text(
        json.dumps(
            {
                "model_id": model_id,
                "model_type": "memory_ranker",
                "version": 1,
                "created_at": "2026-04-14T12:00:00Z",
                "features": features
                or [
                    "recency",
                    "tag_overlap",
                    "domain_match",
                    "memory_class",
                    "conflict_flag",
                    "state_flag",
                ],
                "weights": weights
                or {
                    "recency": 0.4,
                    "tag_overlap": 0.25,
                    "domain_match": 0.2,
                    "memory_class": 0.1,
                    "conflict_flag": 0.03,
                    "state_flag": 0.02,
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return model_path


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


def test_load_model_returns_valid_model(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _write_model(tmp_path, model_id="valid-model")
    monkeypatch.setattr(
        model_loader_module, "MODELS_ROOT", tmp_path / "DF" / "shared" / "models"
    )

    model = load_model("valid-model")

    assert model == {
        "model_id": "valid-model",
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
        "weights": {
            "recency": 0.4,
            "tag_overlap": 0.25,
            "domain_match": 0.2,
            "memory_class": 0.1,
            "conflict_flag": 0.03,
            "state_flag": 0.02,
        },
    }


def test_load_model_rejects_invalid_model(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _write_model(
        tmp_path,
        model_id="invalid-model",
        weights={
            "recency": 0.4,
            "tag_overlap": 0.25,
            "domain_match": 0.2,
            "memory_class": 0.1,
            "conflict_flag": 0.03,
        },
    )
    monkeypatch.setattr(
        model_loader_module, "MODELS_ROOT", tmp_path / "DF" / "shared" / "models"
    )

    with pytest.raises(ModelLoaderError, match="state_flag"):
        load_model("invalid-model")


def test_score_with_model_falls_back_when_model_is_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        model_loader_module, "MODELS_ROOT", tmp_path / "DF" / "shared" / "models"
    )

    scored = score_with_model(
        {"domain": "dev", "type": "task", "tags": ["finance", "urgent"]},
        [_artifact("task-1", tags=["finance", "urgent"])],
        model_id="missing-model",
    )

    output = capsys.readouterr().out
    assert scored == {}
    assert "[MODEL] fallback -> heuristic" in output


def test_score_with_model_changes_with_loaded_weights(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _write_model(
        tmp_path,
        model_id="recency-heavy",
        weights={
            "recency": 1.0,
            "tag_overlap": 0.0,
            "domain_match": 0.0,
            "memory_class": 0.0,
            "conflict_flag": 0.0,
            "state_flag": 0.0,
        },
    )
    _write_model(
        tmp_path,
        model_id="conflict-heavy",
        weights={
            "recency": 0.0,
            "tag_overlap": 0.0,
            "domain_match": 0.0,
            "memory_class": 0.0,
            "conflict_flag": 1.0,
            "state_flag": 0.0,
        },
    )
    monkeypatch.setattr(
        model_loader_module, "MODELS_ROOT", tmp_path / "DF" / "shared" / "models"
    )
    context = {"domain": "dev", "type": "task", "tags": ["finance", "urgent"]}
    artifacts = [
        _artifact(
            "task-recent", timestamp="2026-04-14T11:30:00Z", tags=["finance", "urgent"]
        ),
        _artifact(
            "conflict-1",
            artifact_type="conflict_escalation",
            memory_class="conflict",
            timestamp="2026-04-14T08:00:00Z",
        ),
    ]

    recency_scores = {
        entry["entity_id"]: float(entry["score"])
        for entry in score_with_model(context, artifacts, model_id="recency-heavy")[
            "items"
        ]
    }
    conflict_scores = {
        entry["entity_id"]: float(entry["score"])
        for entry in score_with_model(context, artifacts, model_id="conflict-heavy")[
            "items"
        ]
    }

    assert recency_scores["task-recent"] > recency_scores["conflict-1"]
    assert conflict_scores["conflict-1"] > conflict_scores["task-recent"]


def test_rank_memory_with_loaded_model_can_change_ordering(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_model(
        tmp_path,
        model_id="recency-heavy",
        weights={
            "recency": 0.7,
            "tag_overlap": 0.2,
            "domain_match": 0.1,
            "memory_class": 0.0,
            "conflict_flag": 0.0,
            "state_flag": 0.0,
        },
    )
    monkeypatch.setattr(
        model_loader_module, "MODELS_ROOT", tmp_path / "DF" / "shared" / "models"
    )
    context = {"domain": "dev", "type": "task", "tags": ["finance", "urgent"]}
    artifacts = [
        _artifact(
            "task-recent", timestamp="2026-04-14T11:30:00Z", tags=["finance", "urgent"]
        ),
        _artifact(
            "conflict-1",
            artifact_type="conflict_escalation",
            memory_class="conflict",
            timestamp="2026-04-14T08:00:00Z",
        ),
    ]

    heuristic_ranked = rank_memory(context, artifacts, model_enabled=False)
    model_ranked = rank_memory(
        context, artifacts, model_enabled=True, model_id="recency-heavy"
    )

    output = capsys.readouterr().out
    assert [entry["memory_id"] for entry in heuristic_ranked] == [
        "conflict-1",
        "task-recent",
    ]
    assert [entry["memory_id"] for entry in model_ranked] == [
        "task-recent",
        "conflict-1",
    ]
    assert "[MODEL] loaded id=recency-heavy" in output
    assert "[RANKER] combined scoring applied" in output
