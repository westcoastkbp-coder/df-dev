from __future__ import annotations

import json
from pathlib import Path

import pytest

import app.learning.model_promoter as model_promoter_module
import app.learning.model_update_config as model_update_config_module
import app.learning.promotion_audit as promotion_audit_module
from app.learning.model_promoter import evaluate_promotion, promote_model
from app.learning.model_update_config import load_model_update_config, write_model_update_config


def _write_model_update_config(
    path: Path,
    *,
    active_model: str = "memory_ranker_v1",
    candidate_model: str = "memory_ranker_v2",
    promotion_rules: dict[str, object] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "active_model": active_model,
                "candidate_model": candidate_model,
                "evaluation_mode": True,
                "evaluation_sample_rate": 1.0,
                "promotion_rules": promotion_rules
                or {
                    "min_agreement": 0.7,
                    "min_top1_match": 0.6,
                    "min_overlap": 0.7,
                    "min_samples": 20,
                },
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


def _write_eval_artifacts(
    evals_root: Path,
    *,
    count: int,
    agreement_score: float,
    top_k_overlap: float,
    top1_true_count: int,
    active_model: str = "memory_ranker_v1",
    candidate_model: str = "memory_ranker_v2",
) -> list[str]:
    evals_root.mkdir(parents=True, exist_ok=True)
    refs: list[str] = []
    for index in range(count):
        path = evals_root / f"eval-{index:02d}.json"
        payload = {
            "active_model": active_model,
            "candidate_model": candidate_model,
            "agreement_score": agreement_score,
            "top1_match": index < top1_true_count,
            "top_k_overlap": top_k_overlap,
            "evaluated_at": f"2026-04-14T12:00:{index:02d}Z",
        }
        path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        refs.append(path.as_posix())
    return refs


def _configure_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    config_path = tmp_path / "config" / "model_update.json"
    evals_root = tmp_path / "DF" / "shared" / "evals"
    audits_root = tmp_path / "DF" / "shared" / "promotion_audits"
    models_root = tmp_path / "DF" / "shared" / "models"
    monkeypatch.setattr(model_update_config_module, "MODEL_UPDATE_CONFIG_FILE", config_path)
    monkeypatch.setattr(model_promoter_module, "EVALS_ROOT", evals_root)
    monkeypatch.setattr(promotion_audit_module, "PROMOTION_AUDITS_ROOT", audits_root)
    monkeypatch.setattr(promotion_audit_module, "MODELS_ROOT", models_root)
    return config_path, evals_root, audits_root, models_root


def test_successful_promotion_commits_config_and_audit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path, evals_root, audits_root, models_root = _configure_environment(monkeypatch, tmp_path)
    _write_model_update_config(config_path)
    evaluation_refs = _write_eval_artifacts(
        evals_root,
        count=20,
        agreement_score=0.8,
        top_k_overlap=0.75,
        top1_true_count=15,
    )
    models_root.mkdir(parents=True, exist_ok=True)
    (models_root / "memory_ranker_v2.json").write_text('{"model_id":"memory_ranker_v2"}\n', encoding="utf-8")

    decision = evaluate_promotion()
    promotion = promote_model()

    stored_config = json.loads(config_path.read_text(encoding="utf-8"))
    audit_records = promotion_audit_module.read_promotion_audit_records()
    committed_records = promotion_audit_module.read_committed_promotion_audits()
    output = capsys.readouterr().out
    assert decision == {
        "promote": True,
        "reason": "thresholds_met",
        "metrics": {
            "active_model": "memory_ranker_v1",
            "candidate_model": "memory_ranker_v2",
            "agreement_score": 0.8,
            "top1_match": 0.75,
            "top_k_overlap": 0.75,
            "sample_count": 20,
        },
    }
    assert promotion == decision
    assert stored_config["active_model"] == "memory_ranker_v2"
    assert stored_config["candidate_model"] == ""
    assert "[MODEL_PROMOTION] accepted → memory_ranker_v2" in output

    assert len(audit_records) == 1
    assert committed_records == audit_records
    record = audit_records[0]
    assert set(record) == {
        "promotion_id",
        "timestamp",
        "status",
        "active_model",
        "candidate_model",
        "decision",
        "reason",
        "metrics",
        "thresholds",
        "evaluation_refs",
        "config_version_before",
        "config_version_after",
        "artifact_refs",
    }
    assert record["status"] == "committed"
    assert record["decision"] == "accepted"
    assert record["reason"] == "thresholds_met"
    assert record["metrics"] == {
        "agreement_score": 0.8,
        "top1_match": 0.75,
        "top_k_overlap": 0.75,
        "sample_count": 20,
    }
    assert record["thresholds"] == {
        "min_agreement_score": 0.7,
        "min_top1_match": 0.6,
        "min_top_k_overlap": 0.7,
        "min_samples": 20,
    }
    assert record["evaluation_refs"] == evaluation_refs
    assert record["artifact_refs"] == [(models_root / "memory_ranker_v2.json").as_posix()]
    assert record["config_version_before"] != record["config_version_after"]


def test_failed_config_update_marks_audit_failed_and_keeps_active_model(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, evals_root, _audits_root, _models_root = _configure_environment(monkeypatch, tmp_path)
    _write_model_update_config(config_path)
    _write_eval_artifacts(
        evals_root,
        count=20,
        agreement_score=0.8,
        top_k_overlap=0.75,
        top1_true_count=15,
    )

    def fail_config_write(config: dict[str, object]) -> None:
        raise OSError("config write blocked")

    monkeypatch.setattr(model_promoter_module, "write_model_update_config", fail_config_write)

    with pytest.raises(OSError, match="config write blocked"):
        promote_model()

    stored_config = json.loads(config_path.read_text(encoding="utf-8"))
    audit_records = promotion_audit_module.read_promotion_audit_records()
    assert stored_config["active_model"] == "memory_ranker_v1"
    assert stored_config["candidate_model"] == "memory_ranker_v2"
    assert len(audit_records) == 1
    assert audit_records[0]["status"] == "failed"
    assert audit_records[0]["decision"] == "accepted"
    assert audit_records[0]["config_version_before"] == audit_records[0]["config_version_after"]


def test_failed_commit_audit_write_rolls_back_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, evals_root, _audits_root, _models_root = _configure_environment(monkeypatch, tmp_path)
    _write_model_update_config(config_path)
    _write_eval_artifacts(
        evals_root,
        count=20,
        agreement_score=0.8,
        top_k_overlap=0.75,
        top1_true_count=15,
    )

    original_replace = promotion_audit_module.replace_promotion_audit_record

    def fail_committed_then_allow_failed(record: dict[str, object]) -> Path:
        if record.get("status") == "committed":
            raise OSError("commit audit blocked")
        return original_replace(record)

    monkeypatch.setattr(model_promoter_module, "replace_promotion_audit_record", fail_committed_then_allow_failed)

    with pytest.raises(OSError, match="commit audit blocked"):
        promote_model()

    stored_config = json.loads(config_path.read_text(encoding="utf-8"))
    audit_records = promotion_audit_module.read_promotion_audit_records()
    assert stored_config["active_model"] == "memory_ranker_v1"
    assert stored_config["candidate_model"] == "memory_ranker_v2"
    assert len(audit_records) == 1
    assert audit_records[0]["status"] == "failed"
    assert audit_records[0]["decision"] == "accepted"
    assert audit_records[0]["config_version_before"] == audit_records[0]["config_version_after"]


def test_pending_audit_state_is_not_treated_as_committed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _config_path, _evals_root, _audits_root, _models_root = _configure_environment(monkeypatch, tmp_path)
    pending_record = promotion_audit_module.build_promotion_audit_record(
        active_model="memory_ranker_v1",
        candidate_model="memory_ranker_v2",
        accepted=True,
        reason="thresholds_met",
        metrics={
            "agreement_score": 0.8,
            "top1_match": 0.75,
            "top_k_overlap": 0.75,
            "sample_count": 20,
        },
        thresholds={
            "min_agreement_score": 0.7,
            "min_top1_match": 0.6,
            "min_top_k_overlap": 0.7,
            "min_samples": 20,
        },
        evaluation_refs=[],
        config_version_before="sha256:before",
        config_version_after="sha256:before",
        artifact_refs=[],
        status="pending",
    )
    promotion_audit_module.persist_promotion_audit_record(pending_record)

    assert promotion_audit_module.read_pending_promotion_audits() == [pending_record]
    assert promotion_audit_module.read_committed_promotion_audits() == []


def test_multiple_runs_preserve_append_only_history(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, evals_root, _audits_root, _models_root = _configure_environment(monkeypatch, tmp_path)
    _write_model_update_config(config_path)
    _write_eval_artifacts(
        evals_root,
        count=20,
        agreement_score=0.65,
        top_k_overlap=0.9,
        top1_true_count=18,
    )

    first = promote_model()

    for path in evals_root.glob("*.json"):
        path.unlink()
    _write_eval_artifacts(
        evals_root,
        count=20,
        agreement_score=0.9,
        top_k_overlap=0.85,
        top1_true_count=18,
    )

    second = promote_model()

    stored_config = json.loads(config_path.read_text(encoding="utf-8"))
    audit_records = promotion_audit_module.read_promotion_audit_records()
    assert first["promote"] is False
    assert second["promote"] is True
    assert stored_config["active_model"] == "memory_ranker_v2"
    assert stored_config["candidate_model"] == ""
    assert len(audit_records) == 2
    assert [record["status"] for record in audit_records] == ["committed", "committed"]
    assert [record["decision"] for record in audit_records] == ["rejected", "accepted"]
    assert audit_records[0]["promotion_id"] != audit_records[1]["promotion_id"]


def test_evaluate_promotion_rejects_when_sample_count_is_too_low(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, evals_root, _audits_root, _models_root = _configure_environment(monkeypatch, tmp_path)
    _write_model_update_config(config_path)
    _write_eval_artifacts(
        evals_root,
        count=19,
        agreement_score=0.9,
        top_k_overlap=0.9,
        top1_true_count=19,
    )

    decision = evaluate_promotion()

    assert decision == {
        "promote": False,
        "reason": "low_samples",
        "metrics": {
            "active_model": "memory_ranker_v1",
            "candidate_model": "memory_ranker_v2",
            "agreement_score": None,
            "top1_match": None,
            "top_k_overlap": None,
            "sample_count": 19,
        },
    }


def test_model_update_config_defaults_and_persists_promotion_rules(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, _evals_root, _audits_root, _models_root = _configure_environment(monkeypatch, tmp_path)

    config = load_model_update_config()
    assert config["promotion_rules"] == {
        "min_agreement": 0.7,
        "min_top1_match": 0.6,
        "min_overlap": 0.7,
        "min_samples": 20,
    }

    write_model_update_config(config)

    stored = json.loads(config_path.read_text(encoding="utf-8"))
    assert stored["promotion_rules"] == {
        "min_agreement": 0.7,
        "min_top1_match": 0.6,
        "min_overlap": 0.7,
        "min_samples": 20,
    }
