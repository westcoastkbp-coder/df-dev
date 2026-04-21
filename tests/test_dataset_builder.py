from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.memory import memory_registry
from app.state.state_store import get_state
from app.storage import storage_adapter
import app.training.dataset_builder as dataset_builder_module
from app.training.dataset_builder import build_dataset, create_training_job, dataset_contract_path


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


def _configure_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    shared_root = tmp_path / "shared"
    monkeypatch.setattr(dataset_builder_module, "SHARED_ROOT", shared_root)
    monkeypatch.setattr(storage_adapter, "POLICY_FILE", _write_policy(tmp_path))
    monkeypatch.setattr(
        memory_registry,
        "REGISTRY_FILE",
        tmp_path / "df-system" / "memory_registry.json",
    )
    return shared_root


def _write_jsonl(path: Path, *records: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            if isinstance(record, str):
                handle.write(record.rstrip("\n") + "\n")
                continue
            handle.write(json.dumps(record) + "\n")


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_build_dataset_normalizes_records_filters_invalid_and_saves_to_shared_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    shared_root = _configure_environment(monkeypatch, tmp_path)
    _write_jsonl(
        shared_root / "training" / "execution" / "collector-a.jsonl",
        {
            "record_id": "rec-1",
            "domain": "dev",
            "task_type": "execution_task",
            "instruction": "route command",
            "timestamp": "2026-04-14T12:00:00Z",
        },
        {
            "id": "rec-2",
            "memory_context": {"domain": "ownerbox", "type": "policy_task"},
            "payload": {"decision": "allow"},
        },
        "not-json",
        {"task_type": "missing-domain"},
    )

    dataset = build_dataset("execution")

    output = capsys.readouterr().out
    dataset_path = shared_root / "datasets" / "execution" / f"{dataset['dataset_id']}.json"
    stored = _read_json(dataset_path)

    assert dataset["dataset_type"] == "execution"
    assert dataset["version"] == 1
    assert dataset["stats"] == {
        "num_records": 2,
        "domains": ["dev", "ownerbox"],
        "task_types": ["execution_task", "policy_task"],
    }
    assert dataset["records"] == [
        {
            "record_id": "rec-1",
            "domain": "dev",
            "task_type": "execution_task",
            "source_file": "collector-a.jsonl",
            "source_line": 1,
            "payload": {
                "record_id": "rec-1",
                "domain": "dev",
                "task_type": "execution_task",
                "instruction": "route command",
                "timestamp": "2026-04-14T12:00:00Z",
            },
            "collected_at": "2026-04-14T12:00:00Z",
        },
        {
            "record_id": "rec-2",
            "domain": "ownerbox",
            "task_type": "policy_task",
            "source_file": "collector-a.jsonl",
            "source_line": 2,
            "payload": {
                "id": "rec-2",
                "memory_context": {"domain": "ownerbox", "type": "policy_task"},
                "payload": {"decision": "allow"},
            },
        },
    ]
    assert dataset_path.exists()
    assert stored == dataset
    assert dataset_contract_path("execution", dataset["dataset_id"]).startswith(
        "DF/shared/datasets/execution/"
    )
    assert f"[DATASET] built id={dataset['dataset_id']} records=2" in output


def test_dataset_versioning_preserves_previous_builds(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    shared_root = _configure_environment(monkeypatch, tmp_path)
    _write_jsonl(
        shared_root / "training" / "routing" / "collector.jsonl",
        {
            "record_id": "route-1",
            "domain": "dev",
            "task_type": "router_decision",
            "payload": {"route": "local"},
        },
    )

    first = build_dataset("routing")
    second = build_dataset("routing")

    first_path = shared_root / "datasets" / "routing" / f"{first['dataset_id']}.json"
    second_path = shared_root / "datasets" / "routing" / f"{second['dataset_id']}.json"

    assert first["dataset_id"] != second["dataset_id"]
    assert first["version"] == second["version"] == 1
    assert first_path.exists()
    assert second_path.exists()
    assert len(list((shared_root / "datasets" / "routing").glob("*.json"))) == 2


def test_create_training_job_links_dataset_to_compute_job(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    shared_root = _configure_environment(monkeypatch, tmp_path)
    _write_jsonl(
        shared_root / "training" / "policy" / "collector.jsonl",
        {
            "record_id": "policy-1",
            "domain": "dev",
            "task_type": "policy_gate",
            "payload": {"decision": "allow"},
            "task_id": "TASK-300",
        },
    )

    dataset = build_dataset("policy")
    job = create_training_job(dataset["dataset_id"], "router-head")

    output = capsys.readouterr().out
    state = get_state("compute_job", job["job_id"], domain="dev")
    job_entry = memory_registry.get_artifact_by_logical_key(
        memory_registry.compute_artifact_key("dev", "compute_job", job["job_id"])
    )
    job_path = Path(str(job_entry["local_path"]))
    job_record = _read_json(job_path)

    assert job["job_type"] == "training"
    assert job["status"] == "queued"
    assert job["payload"]["dataset_ref"] == dataset["dataset_id"]
    assert job["payload"]["model_ref"] == "router-head"
    assert job["payload"]["params"] == {
        "model_type": "router-head",
        "dataset_type": "policy",
        "dataset_version": 1,
        "dataset_contract_path": dataset_contract_path("policy", dataset["dataset_id"]),
        "dataset_local_path": str(
            shared_root / "datasets" / "policy" / f"{dataset['dataset_id']}.json"
        ),
    }
    assert job_record["payload"]["payload"]["dataset_ref"] == dataset["dataset_id"]
    assert state is not None
    assert state["state"] == "queued"
    assert f"[DATASET] training_job_created id={job['job_id']}" in output
