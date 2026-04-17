from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
import uuid
from typing import Any

from app.storage.storage_adapter import resolve_path


ALLOWED_DOMAINS = frozenset({"dev", "ownerbox"})
ALLOWED_JOB_TYPES = frozenset({"training", "eval", "embedding_batch", "rerank_batch"})
ALLOWED_JOB_MODES = frozenset({"local_gpu", "remote_gpu"})
ALLOWED_JOB_STATUSES = frozenset({"queued", "running", "completed", "failed"})
ALLOWED_RESULT_STATUSES = frozenset({"completed", "failed"})
_SAFE_COMPONENT_PATTERN = re.compile(r"[^A-Za-z0-9._-]+")
_OWNERBOX_REMOTE_SEGMENT = "owner"


class ComputeJobError(ValueError):
    """Raised when a compute job packet or artifact is malformed."""


def _utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_mapping(value: object, *, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ComputeJobError(f"{field_name} must be a dict.")
    return dict(value)


def _normalize_domain(value: object) -> str:
    normalized = _normalize_text(value).lower()
    if normalized == _OWNERBOX_REMOTE_SEGMENT:
        return "ownerbox"
    if normalized not in ALLOWED_DOMAINS:
        allowed = ", ".join(sorted(ALLOWED_DOMAINS))
        raise ComputeJobError(f"domain must be one of: {allowed}")
    return normalized


def _normalize_enum(
    value: object,
    *,
    field_name: str,
    allowed: frozenset[str],
) -> str:
    normalized = _normalize_text(value).lower()
    if normalized not in allowed:
        allowed_display = ", ".join(sorted(allowed))
        raise ComputeJobError(f"{field_name} must be one of: {allowed_display}")
    return normalized


def _safe_component(value: object, *, field_name: str) -> str:
    normalized = _normalize_text(value)
    if not normalized:
        raise ComputeJobError(f"{field_name} must not be empty.")
    safe_value = _SAFE_COMPONENT_PATTERN.sub("_", normalized).strip("._")
    if not safe_value:
        raise ComputeJobError(f"{field_name} must resolve to a safe path component.")
    return safe_value


def _remote_domain_segment(domain: str) -> str:
    return _OWNERBOX_REMOTE_SEGMENT if domain == "ownerbox" else "dev"


def local_compute_root(domain: object) -> Path:
    normalized_domain = _normalize_domain(domain)
    return resolve_path(normalized_domain, "_compute_probe").parent.parent


def compute_job_relative_path(job_id: object) -> Path:
    return Path("compute") / "jobs" / f"{_safe_component(job_id, field_name='job_id')}.json"


def compute_result_relative_path(job_id: object) -> Path:
    return Path("compute") / "results" / f"{_safe_component(job_id, field_name='job_id')}.json"


def compute_trace_relative_path(job_id: object, transition: object) -> Path:
    return (
        Path("compute")
        / "traces"
        / _safe_component(job_id, field_name="job_id")
        / f"{_safe_component(transition, field_name='transition')}.json"
    )


def compute_job_contract_path(domain: object, job_id: object) -> str:
    normalized_domain = _normalize_domain(domain)
    safe_job_id = _safe_component(job_id, field_name="job_id")
    return f"DF/{_remote_domain_segment(normalized_domain)}/compute/jobs/{safe_job_id}.json"


def normalize_compute_job_id(job_id: object) -> str:
    return _safe_component(job_id, field_name="job_id")


@dataclass(frozen=True, slots=True)
class ComputeJobPayload:
    dataset_ref: str
    model_ref: str
    output_ref: str
    params: dict[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "dataset_ref", _normalize_text(self.dataset_ref))
        object.__setattr__(self, "model_ref", _normalize_text(self.model_ref))
        object.__setattr__(self, "output_ref", _normalize_text(self.output_ref))
        object.__setattr__(
            self,
            "params",
            dict(self.params) if isinstance(self.params, dict) else {},
        )

        if not self.dataset_ref:
            raise ComputeJobError("payload.dataset_ref must not be empty.")
        if not self.model_ref:
            raise ComputeJobError("payload.model_ref must not be empty.")
        if not self.output_ref:
            raise ComputeJobError("payload.output_ref must not be empty.")

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_ref": self.dataset_ref,
            "model_ref": self.model_ref,
            "output_ref": self.output_ref,
            "params": dict(self.params),
        }


@dataclass(frozen=True, slots=True)
class ComputeResultArtifact:
    type: str
    job_id: str
    status: str
    output_ref: str
    metrics: dict[str, Any]
    error: dict[str, Any] | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "type", _normalize_text(self.type) or "compute_result")
        object.__setattr__(self, "job_id", _safe_component(self.job_id, field_name="job_id"))
        object.__setattr__(
            self,
            "status",
            _normalize_enum(
                self.status,
                field_name="status",
                allowed=ALLOWED_RESULT_STATUSES,
            ),
        )
        object.__setattr__(self, "output_ref", _normalize_text(self.output_ref))
        object.__setattr__(
            self,
            "metrics",
            dict(self.metrics) if isinstance(self.metrics, dict) else {},
        )
        object.__setattr__(
            self,
            "error",
            dict(self.error) if isinstance(self.error, dict) else None,
        )

        if self.type != "compute_result":
            raise ComputeJobError("result.type must be 'compute_result'.")
        if not self.output_ref:
            raise ComputeJobError("result.output_ref must not be empty.")
        if self.status == "completed" and self.error is not None:
            raise ComputeJobError("completed compute results must not carry an error payload.")
        if self.status == "failed" and not isinstance(self.error, dict):
            raise ComputeJobError("failed compute results must carry an error payload.")

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "job_id": self.job_id,
            "status": self.status,
            "output_ref": self.output_ref,
            "metrics": dict(self.metrics),
            "error": dict(self.error) if isinstance(self.error, dict) else None,
        }


@dataclass(frozen=True, slots=True)
class ComputeJob:
    job_id: str
    job_type: str
    mode: str
    status: str
    created_at: str
    updated_at: str
    requested_by: str
    domain: str
    payload: ComputeJobPayload
    result: ComputeResultArtifact | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "job_id", _safe_component(self.job_id, field_name="job_id"))
        object.__setattr__(
            self,
            "job_type",
            _normalize_enum(
                self.job_type,
                field_name="job_type",
                allowed=ALLOWED_JOB_TYPES,
            ),
        )
        object.__setattr__(
            self,
            "mode",
            _normalize_enum(self.mode, field_name="mode", allowed=ALLOWED_JOB_MODES),
        )
        object.__setattr__(
            self,
            "status",
            _normalize_enum(
                self.status,
                field_name="status",
                allowed=ALLOWED_JOB_STATUSES,
            ),
        )
        object.__setattr__(self, "created_at", _normalize_text(self.created_at))
        object.__setattr__(self, "updated_at", _normalize_text(self.updated_at))
        object.__setattr__(self, "requested_by", _normalize_text(self.requested_by))
        object.__setattr__(self, "domain", _normalize_domain(self.domain))
        object.__setattr__(
            self,
            "payload",
            self.payload
            if isinstance(self.payload, ComputeJobPayload)
            else compute_job_payload_from_mapping(self.payload),
        )
        object.__setattr__(
            self,
            "result",
            self.result
            if isinstance(self.result, ComputeResultArtifact) or self.result is None
            else compute_result_artifact_from_mapping(self.result),
        )

        if not self.created_at:
            raise ComputeJobError("created_at must not be empty.")
        if not self.updated_at:
            raise ComputeJobError("updated_at must not be empty.")
        if not self.requested_by:
            raise ComputeJobError("requested_by must not be empty.")
        if self.status in {"queued", "running"} and self.result is not None:
            raise ComputeJobError("queued and running jobs must not carry a result artifact.")
        if self.status in {"completed", "failed"} and self.result is None:
            raise ComputeJobError("completed and failed jobs must carry a result artifact.")
        if self.result is not None and self.result.status != self.status:
            raise ComputeJobError("job status must match result.status for terminal jobs.")

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "job_type": self.job_type,
            "mode": self.mode,
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "requested_by": self.requested_by,
            "domain": self.domain,
            "payload": self.payload.to_dict(),
            "result": self.result.to_dict() if isinstance(self.result, ComputeResultArtifact) else None,
        }


def compute_job_payload_from_mapping(value: object) -> ComputeJobPayload:
    mapped = _normalize_mapping(value, field_name="payload")
    return ComputeJobPayload(
        dataset_ref=mapped.get("dataset_ref"),
        model_ref=mapped.get("model_ref"),
        output_ref=mapped.get("output_ref"),
        params=mapped.get("params") if isinstance(mapped.get("params"), dict) else {},
    )


def compute_result_artifact_from_mapping(value: object) -> ComputeResultArtifact:
    mapped = _normalize_mapping(value, field_name="result")
    return ComputeResultArtifact(
        type=mapped.get("type") or "compute_result",
        job_id=mapped.get("job_id"),
        status=mapped.get("status"),
        output_ref=mapped.get("output_ref"),
        metrics=mapped.get("metrics") if isinstance(mapped.get("metrics"), dict) else {},
        error=mapped.get("error") if isinstance(mapped.get("error"), dict) else None,
    )


def compute_job_from_mapping(value: object) -> ComputeJob:
    mapped = _normalize_mapping(value, field_name="compute_job")
    return ComputeJob(
        job_id=mapped.get("job_id") or mapped.get("id") or uuid.uuid4().hex,
        job_type=mapped.get("job_type"),
        mode=mapped.get("mode"),
        status=mapped.get("status"),
        created_at=mapped.get("created_at") or _utc_timestamp(),
        updated_at=mapped.get("updated_at") or mapped.get("created_at") or _utc_timestamp(),
        requested_by=mapped.get("requested_by"),
        domain=mapped.get("domain"),
        payload=compute_job_payload_from_mapping(mapped.get("payload")),
        result=(
            compute_result_artifact_from_mapping(mapped.get("result"))
            if isinstance(mapped.get("result"), dict)
            else None
        ),
    )


def new_compute_job(
    *,
    job_type: object,
    mode: object,
    requested_by: object,
    domain: object,
    payload: object,
    job_id: object | None = None,
) -> ComputeJob:
    timestamp = _utc_timestamp()
    return ComputeJob(
        job_id=_safe_component(job_id or uuid.uuid4().hex, field_name="job_id"),
        job_type=job_type,
        mode=mode,
        status="queued",
        created_at=timestamp,
        updated_at=timestamp,
        requested_by=requested_by,
        domain=domain,
        payload=compute_job_payload_from_mapping(payload),
        result=None,
    )


def build_compute_result_artifact(
    *,
    job_id: object,
    status: object,
    output_ref: object,
    metrics: object | None = None,
    error: object | None = None,
) -> ComputeResultArtifact:
    return ComputeResultArtifact(
        type="compute_result",
        job_id=_safe_component(job_id, field_name="job_id"),
        status=status,
        output_ref=output_ref,
        metrics=dict(metrics) if isinstance(metrics, dict) else {},
        error=dict(error) if isinstance(error, dict) else None,
    )


def transitioned_compute_job(
    job: ComputeJob | dict[str, Any],
    *,
    status: object,
    result: ComputeResultArtifact | dict[str, Any] | None = None,
) -> ComputeJob:
    current = job if isinstance(job, ComputeJob) else compute_job_from_mapping(job)
    updated_result = (
        result
        if isinstance(result, ComputeResultArtifact) or result is None
        else compute_result_artifact_from_mapping(result)
    )
    return ComputeJob(
        job_id=current.job_id,
        job_type=current.job_type,
        mode=current.mode,
        status=status,
        created_at=current.created_at,
        updated_at=_utc_timestamp(),
        requested_by=current.requested_by,
        domain=current.domain,
        payload=current.payload,
        result=updated_result,
    )
