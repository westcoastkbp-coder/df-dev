from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.compute.compute_job import (
    ALLOWED_DOMAINS,
    ComputeJob,
    build_compute_result_artifact,
    compute_job_contract_path,
    compute_job_from_mapping,
    compute_job_relative_path,
    compute_result_relative_path,
    compute_trace_relative_path,
    local_compute_root,
    new_compute_job,
    normalize_compute_job_id,
    transitioned_compute_job,
)
from app.memory.memory_registry import compute_artifact_key, get_artifact_by_logical_key
from app.state.state_store import set_state
from app.storage.storage_adapter import save_artifact


JOB_ARTIFACT_TYPE = "compute_job"
RESULT_ARTIFACT_TYPE = "compute_result"
TRACE_ARTIFACT_TYPE = "compute_job_trace"
_TASK_REF_PREFIX = "task:"


class ComputeDispatcherError(RuntimeError):
    """Raised when the compute dispatcher cannot persist or transition a job."""


class ComputeJobTransitionError(ComputeDispatcherError):
    """Raised when a compute job transition is invalid for the current state."""


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _safe_job_id(job_id: object) -> str:
    return normalize_compute_job_id(job_id)


def _compute_domain_root(domain: str) -> Path:
    return local_compute_root(domain)


def _ensure_domain_record_path(domain: str, path: Path | str) -> Path:
    domain_root = _compute_domain_root(domain).resolve(strict=False)
    candidate = Path(path).resolve(strict=False)
    try:
        candidate.relative_to(domain_root)
    except ValueError as exc:
        raise ComputeDispatcherError(
            f"Path '{candidate}' is outside the allowed compute namespace for domain '{domain}'."
        ) from exc
    return candidate


def _load_record(domain: str, path: Path | str) -> dict[str, Any]:
    record_path = _ensure_domain_record_path(domain, path)
    try:
        payload = json.loads(record_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ComputeDispatcherError(f"Compute record not found: {record_path}") from None
    except (OSError, json.JSONDecodeError) as exc:
        raise ComputeDispatcherError(f"Compute record is not readable: {record_path}") from exc
    if not isinstance(payload, dict):
        raise ComputeDispatcherError(f"Compute record must be a JSON object: {record_path}")
    return payload


def _task_ref(source_task_id: object | None) -> str | None:
    normalized = _normalize_text(source_task_id)
    if not normalized:
        return None
    if normalized.startswith(_TASK_REF_PREFIX):
        return normalized
    return f"{_TASK_REF_PREFIX}{normalized}"


def _task_id_from_refs(refs: object) -> str | None:
    if not isinstance(refs, list):
        return None
    for ref in refs:
        normalized = _normalize_text(ref)
        if normalized.startswith(_TASK_REF_PREFIX):
            return normalized[len(_TASK_REF_PREFIX) :]
    return None


def _job_storage_payload(job: ComputeJob) -> dict[str, Any]:
    payload = job.to_dict()
    payload["id"] = job.job_id
    payload["logical_id"] = job.job_id
    return payload


def _result_storage_payload(result_payload: dict[str, Any]) -> dict[str, Any]:
    payload = dict(result_payload)
    payload["id"] = _normalize_text(payload.get("job_id"))
    payload["logical_id"] = _normalize_text(payload.get("job_id"))
    return payload


def _trace_storage_payload(
    *,
    job: ComputeJob,
    transition: str,
    job_path: Path,
    source_task_id: str | None = None,
    result_path: Path | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": f"{job.job_id}-{transition}",
        "logical_id": f"{job.job_id}-{transition}",
        "job_id": job.job_id,
        "job_type": job.job_type,
        "mode": job.mode,
        "status": job.status,
        "domain": job.domain,
        "requested_by": job.requested_by,
        "job_ref": str(job_path),
        "storage_path": compute_job_contract_path(job.domain, job.job_id),
        "timestamp": job.updated_at,
    }
    if source_task_id:
        payload["source_task_id"] = source_task_id
    if result_path is not None:
        payload["result_ref"] = str(result_path)
    if job.result is not None:
        payload["result"] = job.result.to_dict()
    return payload


def _job_registry_entry(job_id: str) -> dict[str, Any] | None:
    matches: list[dict[str, Any]] = []
    for domain in sorted(ALLOWED_DOMAINS):
        entry = get_artifact_by_logical_key(compute_artifact_key(domain, JOB_ARTIFACT_TYPE, job_id))
        if isinstance(entry, dict):
            matches.append(entry)
    if not matches:
        return None
    if len(matches) > 1:
        raise ComputeDispatcherError(f"compute job lookup is ambiguous for job_id='{job_id}'")
    return matches[0]


def _job_context(job_id: str) -> tuple[ComputeJob, dict[str, Any], Path, str | None]:
    entry = _job_registry_entry(job_id)
    if not isinstance(entry, dict):
        raise ComputeDispatcherError(f"compute job not found: {job_id}")
    domain = _normalize_text(entry.get("domain"))
    record = _load_record(domain, _normalize_text(entry.get("local_path")))
    payload = record.get("payload")
    if not isinstance(payload, dict):
        raise ComputeDispatcherError(f"compute job payload is malformed for job_id='{job_id}'")
    job = compute_job_from_mapping(payload)
    return job, record, Path(str(entry["local_path"])), _task_id_from_refs(record.get("refs"))


def _persist_job(job: ComputeJob, *, source_task_id: str | None = None, result_path: Path | None = None) -> Path:
    refs = []
    task_ref = _task_ref(source_task_id)
    if task_ref:
        refs.append(task_ref)
    if result_path is not None:
        refs.append(str(result_path))
    return save_artifact(
        job.domain,
        JOB_ARTIFACT_TYPE,
        _job_storage_payload(job),
        overwrite=True,
        artifact_status="active",
        relative_path=compute_job_relative_path(job.job_id),
        domain_root_override=_compute_domain_root(job.domain),
        refs_override=refs,
    )


def _persist_result_artifact(
    job: ComputeJob,
    *,
    source_task_id: str | None,
) -> Path:
    if job.result is None:
        raise ComputeDispatcherError("terminal compute jobs must include a result artifact.")
    refs = []
    task_ref = _task_ref(source_task_id)
    if task_ref:
        refs.append(task_ref)
    return save_artifact(
        job.domain,
        RESULT_ARTIFACT_TYPE,
        _result_storage_payload(job.result.to_dict()),
        overwrite=True,
        artifact_status="active",
        relative_path=compute_result_relative_path(job.job_id),
        domain_root_override=_compute_domain_root(job.domain),
        refs_override=refs,
    )


def _persist_trace(
    job: ComputeJob,
    *,
    transition: str,
    job_path: Path,
    source_task_id: str | None = None,
    result_path: Path | None = None,
) -> Path:
    refs = [str(job_path)]
    task_ref = _task_ref(source_task_id)
    if task_ref:
        refs.append(task_ref)
    if result_path is not None:
        refs.append(str(result_path))
    return save_artifact(
        job.domain,
        TRACE_ARTIFACT_TYPE,
        _trace_storage_payload(
            job=job,
            transition=transition,
            job_path=job_path,
            source_task_id=source_task_id,
            result_path=result_path,
        ),
        overwrite=True,
        artifact_status="active",
        relative_path=compute_trace_relative_path(job.job_id, transition),
        domain_root_override=_compute_domain_root(job.domain),
        refs_override=refs,
    )


def _persist_canonical_state(job: ComputeJob, trace_path: Path) -> None:
    set_state(
        "compute_job",
        job.job_id,
        job.status,
        str(trace_path),
        domain=job.domain,
    )


def _assert_status(job: ComputeJob, expected: str) -> None:
    if job.status != expected:
        raise ComputeJobTransitionError(
            f"compute job '{job.job_id}' must be in '{expected}' state before transition; found '{job.status}'."
        )


def create_compute_job(
    *,
    job_type: object,
    mode: object,
    requested_by: object,
    domain: object,
    payload: object,
    job_id: object | None = None,
    source_task_id: object | None = None,
) -> dict[str, Any]:
    job = new_compute_job(
        job_type=job_type,
        mode=mode,
        requested_by=requested_by,
        domain=domain,
        payload=payload,
        job_id=job_id,
    )
    task_id = _normalize_text(source_task_id) or None
    job_path = _persist_job(job, source_task_id=task_id)
    trace_path = _persist_trace(job, transition="queued", job_path=job_path, source_task_id=task_id)
    _persist_canonical_state(job, trace_path)
    print(f"[COMPUTE] queued job={job.job_id} mode={job.mode}")
    return job.to_dict()


def start_compute_job(job_id: object) -> dict[str, Any]:
    normalized_job_id = _safe_job_id(job_id)
    job, _record, _existing_job_path, task_id = _job_context(normalized_job_id)
    _assert_status(job, "queued")
    updated_job = transitioned_compute_job(job, status="running")
    job_path = _persist_job(updated_job, source_task_id=task_id)
    trace_path = _persist_trace(
        updated_job,
        transition="running",
        job_path=job_path,
        source_task_id=task_id,
    )
    _persist_canonical_state(updated_job, trace_path)
    print(f"[COMPUTE] running job={updated_job.job_id}")
    return updated_job.to_dict()


def complete_compute_job(job_id: object, result_payload: object) -> dict[str, Any]:
    normalized_job_id = _safe_job_id(job_id)
    job, _record, _existing_job_path, task_id = _job_context(normalized_job_id)
    _assert_status(job, "running")
    result_mapping = dict(result_payload) if isinstance(result_payload, dict) else {}
    result = build_compute_result_artifact(
        job_id=job.job_id,
        status="completed",
        output_ref=_normalize_text(result_mapping.get("output_ref")) or job.payload.output_ref,
        metrics=result_mapping.get("metrics"),
        error=None,
    )
    updated_job = transitioned_compute_job(job, status="completed", result=result)
    result_path = _persist_result_artifact(updated_job, source_task_id=task_id)
    job_path = _persist_job(updated_job, source_task_id=task_id, result_path=result_path)
    trace_path = _persist_trace(
        updated_job,
        transition="completed",
        job_path=job_path,
        source_task_id=task_id,
        result_path=result_path,
    )
    _persist_canonical_state(updated_job, trace_path)
    print(f"[COMPUTE] completed job={updated_job.job_id}")
    return updated_job.to_dict()


def fail_compute_job(job_id: object, error_payload: object) -> dict[str, Any]:
    normalized_job_id = _safe_job_id(job_id)
    job, _record, _existing_job_path, task_id = _job_context(normalized_job_id)
    _assert_status(job, "running")
    error_mapping = dict(error_payload) if isinstance(error_payload, dict) else {}
    error_value = (
        error_mapping
        if error_mapping
        else {"message": _normalize_text(error_payload) or "compute_job_failed"}
    )
    result = build_compute_result_artifact(
        job_id=job.job_id,
        status="failed",
        output_ref=_normalize_text(error_mapping.get("output_ref")) or job.payload.output_ref,
        metrics=error_mapping.get("metrics"),
        error=error_value,
    )
    updated_job = transitioned_compute_job(job, status="failed", result=result)
    result_path = _persist_result_artifact(updated_job, source_task_id=task_id)
    job_path = _persist_job(updated_job, source_task_id=task_id, result_path=result_path)
    trace_path = _persist_trace(
        updated_job,
        transition="failed",
        job_path=job_path,
        source_task_id=task_id,
        result_path=result_path,
    )
    _persist_canonical_state(updated_job, trace_path)
    print(f"[COMPUTE] failed job={updated_job.job_id}")
    return updated_job.to_dict()


def get_compute_job(job_id: object) -> dict[str, Any] | None:
    normalized_job_id = _safe_job_id(job_id)
    entry = _job_registry_entry(normalized_job_id)
    if not isinstance(entry, dict):
        return None
    record = _load_record(_normalize_text(entry.get("domain")), _normalize_text(entry.get("local_path")))
    payload = record.get("payload")
    if not isinstance(payload, dict):
        raise ComputeDispatcherError(f"compute job payload is malformed for job_id='{normalized_job_id}'")
    return compute_job_from_mapping(payload).to_dict()
