from __future__ import annotations

import json
from pathlib import Path

import pytest

import app.state.state_store as state_store_module
from app.compute.compute_dispatcher import (
    complete_compute_job,
    create_compute_job,
    fail_compute_job,
    get_compute_job,
    start_compute_job,
)
from app.compute.compute_job import compute_job_contract_path
from app.memory import memory_registry
from app.state.state_store import get_state, list_active_states
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


def _configure_storage(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(storage_adapter, "POLICY_FILE", _write_policy(tmp_path))
    monkeypatch.setattr(
        memory_registry,
        "REGISTRY_FILE",
        tmp_path / "df-system" / "memory_registry.json",
    )


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_create_compute_job_persists_queued_job_and_trace(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _configure_storage(monkeypatch, tmp_path)

    job = create_compute_job(
        job_id="job-dev-queued",
        job_type="eval",
        mode="local_gpu",
        requested_by="planner",
        domain="dev",
        source_task_id="TASK-101",
        payload={
            "dataset_ref": "dataset://dev/eval-set",
            "model_ref": "model://baseline",
            "output_ref": "artifact://dev/eval-output",
            "params": {"batch_size": 16},
        },
    )

    output = capsys.readouterr().out
    job_path = tmp_path / "df-dev" / "compute" / "jobs" / "job-dev-queued.json"
    trace_path = tmp_path / "df-dev" / "compute" / "traces" / "job-dev-queued" / "queued.json"
    state = get_state("compute_job", "job-dev-queued", domain="dev")
    record = _read_json(job_path)
    trace_record = _read_json(trace_path)
    registry_entry = memory_registry.get_artifact_by_logical_key(
        memory_registry.compute_artifact_key("dev", "compute_job", "job-dev-queued")
    )

    assert job["job_id"] == "job-dev-queued"
    assert job["status"] == "queued"
    assert job["result"] is None
    assert job_path.exists()
    assert trace_path.exists()
    assert record["type"] == "compute_job"
    assert record["payload"]["status"] == "queued"
    assert trace_record["type"] == "compute_job_trace"
    assert trace_record["memory_class"] == "trace"
    assert trace_record["payload"]["job_id"] == "job-dev-queued"
    assert trace_record["payload"]["source_task_id"] == "TASK-101"
    assert trace_record["payload"]["storage_path"] == "DF/dev/compute/jobs/job-dev-queued.json"
    assert trace_record["refs"] == [str(job_path), "task:TASK-101"]
    assert state is not None
    assert state["state"] == "queued"
    assert state["source_artifact"] == str(trace_path)
    assert registry_entry is not None
    assert registry_entry["local_path"] == str(job_path)
    assert get_compute_job("job-dev-queued") == job
    assert compute_job_contract_path("dev", "job-dev-queued") == "DF/dev/compute/jobs/job-dev-queued.json"
    assert "[COMPUTE] queued job=job-dev-queued mode=local_gpu" in output


def test_start_and_complete_compute_job_preserve_state_history_and_result_artifact(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _configure_storage(monkeypatch, tmp_path)
    state_timestamps = iter(
        [
            "2026-04-14T10:00:00Z",
            "2026-04-14T10:00:01Z",
            "2026-04-14T10:00:02Z",
        ]
    )
    monkeypatch.setattr(state_store_module, "_utc_timestamp", lambda: next(state_timestamps))

    create_compute_job(
        job_id="job-owner-complete",
        job_type="embedding_batch",
        mode="remote_gpu",
        requested_by="scheduler",
        domain="ownerbox",
        source_task_id="TASK-202",
        payload={
            "dataset_ref": "dataset://ownerbox/embeddings",
            "model_ref": "model://embedder-v1",
            "output_ref": "artifact://ownerbox/embeddings/out",
            "params": {"chunk_size": 128},
        },
    )
    running_job = start_compute_job("job-owner-complete")
    completed_job = complete_compute_job(
        "job-owner-complete",
        {
            "output_ref": "artifact://ownerbox/embeddings/final",
            "metrics": {"rows_processed": 2048, "duration_seconds": 33.1},
        },
    )

    output = capsys.readouterr().out
    job_path = tmp_path / "ownerbox" / "compute" / "jobs" / "job-owner-complete.json"
    result_path = tmp_path / "ownerbox" / "compute" / "results" / "job-owner-complete.json"
    completed_trace_path = (
        tmp_path / "ownerbox" / "compute" / "traces" / "job-owner-complete" / "completed.json"
    )
    state = get_state("compute_job", "job-owner-complete", domain="ownerbox")
    history_dir = tmp_path / "ownerbox" / "state" / "compute_job" / "history"
    history_states = [
        _read_json(path)["payload"]["state"]
        for path in sorted(history_dir.glob("job-owner-complete-*.json"))
    ]
    result_record = _read_json(result_path)
    completed_trace_record = _read_json(completed_trace_path)
    result_entry = memory_registry.get_artifact_by_logical_key(
        memory_registry.compute_artifact_key("ownerbox", "compute_result", "job-owner-complete")
    )

    assert running_job["status"] == "running"
    assert completed_job["status"] == "completed"
    assert completed_job["result"] == {
        "type": "compute_result",
        "job_id": "job-owner-complete",
        "status": "completed",
        "output_ref": "artifact://ownerbox/embeddings/final",
        "metrics": {"rows_processed": 2048, "duration_seconds": 33.1},
        "error": None,
    }
    assert job_path.exists()
    assert result_path.exists()
    assert completed_trace_path.exists()
    assert result_record["payload"] == {
        "id": "job-owner-complete",
        "logical_id": "job-owner-complete",
        "type": "compute_result",
        "job_id": "job-owner-complete",
        "status": "completed",
        "output_ref": "artifact://ownerbox/embeddings/final",
        "metrics": {"rows_processed": 2048, "duration_seconds": 33.1},
        "error": None,
    }
    assert completed_trace_record["payload"]["result_ref"] == str(result_path)
    assert completed_trace_record["refs"] == [
        str(job_path),
        "task:TASK-202",
        str(result_path),
    ]
    assert state is not None
    assert state["state"] == "completed"
    assert state["source_artifact"] == str(completed_trace_path)
    assert history_states == ["queued", "running"]
    assert result_entry is not None
    assert result_entry["local_path"] == str(result_path)
    assert get_compute_job("job-owner-complete") == completed_job
    assert "[COMPUTE] running job=job-owner-complete" in output
    assert "[COMPUTE] completed job=job-owner-complete" in output


def test_fail_compute_job_creates_failed_result_artifact_and_failed_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _configure_storage(monkeypatch, tmp_path)

    create_compute_job(
        job_id="job-dev-fail",
        job_type="rerank_batch",
        mode="local_gpu",
        requested_by="orchestrator",
        domain="dev",
        payload={
            "dataset_ref": "dataset://dev/rerank",
            "model_ref": "model://reranker-v1",
            "output_ref": "artifact://dev/rerank/out",
            "params": {"top_k": 20},
        },
    )
    start_compute_job("job-dev-fail")
    failed_job = fail_compute_job(
        "job-dev-fail",
        {
            "code": "gpu_timeout",
            "message": "worker heartbeat expired",
            "metrics": {"attempt": 1},
        },
    )

    output = capsys.readouterr().out
    result_path = tmp_path / "df-dev" / "compute" / "results" / "job-dev-fail.json"
    failed_trace_path = tmp_path / "df-dev" / "compute" / "traces" / "job-dev-fail" / "failed.json"
    state = get_state("compute_job", "job-dev-fail", domain="dev")
    result_record = _read_json(result_path)
    failed_trace_record = _read_json(failed_trace_path)

    assert failed_job["status"] == "failed"
    assert failed_job["result"] == {
        "type": "compute_result",
        "job_id": "job-dev-fail",
        "status": "failed",
        "output_ref": "artifact://dev/rerank/out",
        "metrics": {"attempt": 1},
        "error": {
            "code": "gpu_timeout",
            "message": "worker heartbeat expired",
            "metrics": {"attempt": 1},
        },
    }
    assert result_record["payload"]["status"] == "failed"
    assert result_record["payload"]["error"] == {
        "code": "gpu_timeout",
        "message": "worker heartbeat expired",
        "metrics": {"attempt": 1},
    }
    assert failed_trace_record["payload"]["result"]["status"] == "failed"
    assert state is not None
    assert state["state"] == "failed"
    assert state["source_artifact"] == str(failed_trace_path)
    assert get_compute_job("job-dev-fail") == failed_job
    assert "[COMPUTE] failed job=job-dev-fail" in output


def test_compute_job_domain_isolation_is_preserved(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_storage(monkeypatch, tmp_path)

    dev_job = create_compute_job(
        job_id="job-dev-isolated",
        job_type="eval",
        mode="local_gpu",
        requested_by="planner",
        domain="dev",
        payload={
            "dataset_ref": "dataset://dev/isolation",
            "model_ref": "model://baseline",
            "output_ref": "artifact://dev/isolation/out",
            "params": {},
        },
    )
    owner_job = create_compute_job(
        job_id="job-owner-isolated",
        job_type="eval",
        mode="remote_gpu",
        requested_by="planner",
        domain="ownerbox",
        payload={
            "dataset_ref": "dataset://ownerbox/isolation",
            "model_ref": "model://baseline",
            "output_ref": "artifact://ownerbox/isolation/out",
            "params": {},
        },
    )

    dev_entry = memory_registry.get_artifact_by_logical_key(
        memory_registry.compute_artifact_key("dev", "compute_job", dev_job["job_id"])
    )
    owner_entry = memory_registry.get_artifact_by_logical_key(
        memory_registry.compute_artifact_key("ownerbox", "compute_job", owner_job["job_id"])
    )

    assert dev_entry is not None
    assert owner_entry is not None
    assert dev_entry["local_path"] == str(
        tmp_path / "df-dev" / "compute" / "jobs" / "job-dev-isolated.json"
    )
    assert owner_entry["local_path"] == str(
        tmp_path / "ownerbox" / "compute" / "jobs" / "job-owner-isolated.json"
    )
    assert get_state("compute_job", "job-dev-isolated", domain="dev") is not None
    assert get_state("compute_job", "job-owner-isolated", domain="ownerbox") is not None
    assert [state["entity_id"] for state in list_active_states("dev")] == ["job-dev-isolated"]
    assert [state["entity_id"] for state in list_active_states("ownerbox")] == ["job-owner-isolated"]
    assert get_compute_job("job-dev-isolated") == dev_job
    assert get_compute_job("job-owner-isolated") == owner_job
