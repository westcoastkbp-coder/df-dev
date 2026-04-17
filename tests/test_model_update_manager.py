from __future__ import annotations

import json
from pathlib import Path

import pytest

import app.learning.model_update_config as model_update_config_module
import app.learning.model_loader as model_loader_module
import app.learning.model_update_manager as model_update_manager_module
from app.learning.model_update_manager import check_update_needed, trigger_model_update
from app.memory import memory_registry
from app.state.state_store import get_state
from app.storage import storage_adapter
import app.training.dataset_builder as dataset_builder_module


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


def _write_jsonl(path: Path, *records: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")


def _write_model_update_config(
    path: Path,
    *,
    min_new_records: int = 50,
    last_dataset_size: int = 0,
    active_model: str = "memory_ranker_v1",
    candidate_model: str = "",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "active_model": active_model,
                "candidate_model": candidate_model,
                "memory_ranking": {
                    "last_dataset_size": last_dataset_size,
                    "min_new_records": min_new_records,
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _write_model_file(models_root: Path, model_id: str) -> None:
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
                "weights": {
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


def _training_record(index: int) -> dict[str, object]:
    return {
        "record_id": f"mem-rank-{index}",
        "domain": "dev",
        "task_type": "memory_rank_event",
        "timestamp": "2026-04-14T12:00:00Z",
        "payload": {
            "selected_memory_id": f"artifact-{index}",
            "relevance": "high",
        },
    }


def _configure_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Path, Path, Path]:
    shared_root = tmp_path / "shared"
    models_root = tmp_path / "DF" / "shared" / "models"
    config_path = tmp_path / "config" / "model_update.json"

    monkeypatch.setattr(dataset_builder_module, "SHARED_ROOT", shared_root)
    monkeypatch.setattr(model_update_config_module, "MODEL_UPDATE_CONFIG_FILE", config_path)
    monkeypatch.setattr(model_loader_module, "MODELS_ROOT", models_root)
    monkeypatch.setattr(storage_adapter, "POLICY_FILE", _write_policy(tmp_path))
    monkeypatch.setattr(
        memory_registry,
        "REGISTRY_FILE",
        tmp_path / "df-system" / "memory_registry.json",
    )
    return shared_root, models_root, config_path


def test_check_update_needed_returns_true_when_threshold_is_exceeded(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    shared_root, models_root, config_path = _configure_environment(monkeypatch, tmp_path)
    _write_model_update_config(config_path, min_new_records=50, last_dataset_size=0)
    _write_model_file(models_root, "memory_ranker_v1")
    _write_jsonl(
        shared_root / "training" / "memory_ranking" / "collector.jsonl",
        *[_training_record(index) for index in range(51)],
    )

    assert check_update_needed("memory_ranking") is True


def test_check_update_needed_returns_false_at_threshold_boundary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    shared_root, models_root, config_path = _configure_environment(monkeypatch, tmp_path)
    _write_model_update_config(config_path, min_new_records=50, last_dataset_size=10)
    _write_model_file(models_root, "memory_ranker_v1")
    _write_jsonl(
        shared_root / "training" / "memory_ranking" / "collector.jsonl",
        *[_training_record(index) for index in range(60)],
    )

    assert check_update_needed("memory_ranking") is False


def test_trigger_model_update_creates_training_job_and_logs_event(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    shared_root, models_root, config_path = _configure_environment(monkeypatch, tmp_path)
    _write_model_update_config(config_path, min_new_records=2, last_dataset_size=0)
    _write_model_file(models_root, "memory_ranker_v1")
    _write_jsonl(
        shared_root / "training" / "memory_ranking" / "collector.jsonl",
        *[_training_record(index) for index in range(3)],
    )

    result = trigger_model_update("memory_ranking")

    output = capsys.readouterr().out
    assert result is not None
    job = result["job"]
    assert job["job_type"] == "training"
    assert job["status"] == "queued"
    assert job["payload"]["dataset_ref"] == result["dataset"]["dataset_id"]
    assert job["payload"]["model_ref"] == "memory_ranker_v2"
    assert job["payload"]["output_ref"] == "DF/shared/models/memory_ranker_v2.json"
    assert job["payload"]["params"]["expected_model_id"] == "memory_ranker_v2"
    assert job["payload"]["params"]["active_model"] == "memory_ranker_v1"
    assert get_state("compute_job", job["job_id"], domain="dev")["state"] == "queued"
    assert f"[MODEL_UPDATE] triggered dataset={result['dataset']['dataset_id']}" in output
    assert "[MODEL_UPDATE] new model candidate=memory_ranker_v2" in output


def test_trigger_model_update_increments_model_id_from_existing_candidate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    shared_root, models_root, config_path = _configure_environment(monkeypatch, tmp_path)
    _write_model_update_config(
        config_path,
        min_new_records=1,
        last_dataset_size=0,
        active_model="memory_ranker_v1",
        candidate_model="memory_ranker_v2",
    )
    _write_model_file(models_root, "memory_ranker_v1")
    _write_model_file(models_root, "memory_ranker_v2")
    _write_jsonl(
        shared_root / "training" / "memory_ranking" / "collector.jsonl",
        _training_record(1),
        _training_record(2),
    )

    result = trigger_model_update("memory_ranking")

    assert result is not None
    assert result["candidate_model"] == "memory_ranker_v3"


def test_trigger_model_update_stores_candidate_model_and_last_dataset_size(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    shared_root, models_root, config_path = _configure_environment(monkeypatch, tmp_path)
    _write_model_update_config(config_path, min_new_records=2, last_dataset_size=0)
    _write_model_file(models_root, "memory_ranker_v1")
    _write_jsonl(
        shared_root / "training" / "memory_ranking" / "collector.jsonl",
        *[_training_record(index) for index in range(4)],
    )

    result = trigger_model_update("memory_ranking")

    stored_config = json.loads(config_path.read_text(encoding="utf-8"))
    assert result is not None
    assert stored_config["active_model"] == "memory_ranker_v1"
    assert stored_config["candidate_model"] == "memory_ranker_v2"
    assert stored_config["memory_ranking"] == {
        "last_dataset_size": 4,
        "min_new_records": 2,
    }
