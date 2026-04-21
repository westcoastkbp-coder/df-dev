from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import re
import uuid
from typing import Any

from app.compute.compute_dispatcher import create_compute_job


REPO_ROOT = Path(__file__).resolve().parents[2]
SHARED_ROOT = REPO_ROOT / "shared"
ALLOWED_DATASET_TYPES = frozenset({"memory_ranking", "routing", "policy", "execution"})
ALLOWED_DOMAINS = frozenset({"dev", "ownerbox"})
DATASET_VERSION = 1
_SAFE_COMPONENT_PATTERN = re.compile(r"[^A-Za-z0-9._-]+")


class DatasetBuilderError(RuntimeError):
    """Raised when the dataset pipeline cannot build or load a dataset."""


def _utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _safe_component(value: object, *, field_name: str) -> str:
    normalized = _normalize_text(value)
    if not normalized:
        raise DatasetBuilderError(f"{field_name} must not be empty.")
    safe_value = _SAFE_COMPONENT_PATTERN.sub("_", normalized).strip("._")
    if not safe_value:
        raise DatasetBuilderError(
            f"{field_name} must resolve to a safe path component."
        )
    return safe_value


def _normalize_dataset_type(dataset_type: object) -> str:
    normalized = _normalize_text(dataset_type).lower()
    if normalized not in ALLOWED_DATASET_TYPES:
        allowed = ", ".join(sorted(ALLOWED_DATASET_TYPES))
        raise DatasetBuilderError(f"dataset_type must be one of: {allowed}")
    return normalized


def _normalize_domain(value: object) -> str:
    normalized = _normalize_text(value).lower()
    if normalized == "owner":
        normalized = "ownerbox"
    if normalized not in ALLOWED_DOMAINS:
        raise DatasetBuilderError("record domain must be dev or ownerbox.")
    return normalized


def _shared_root(shared_root: Path | str | None = None) -> Path:
    if shared_root is None:
        return SHARED_ROOT
    return Path(shared_root)


def training_input_dir(
    dataset_type: object, *, shared_root: Path | str | None = None
) -> Path:
    return (
        _shared_root(shared_root) / "training" / _normalize_dataset_type(dataset_type)
    )


def dataset_output_dir(
    dataset_type: object, *, shared_root: Path | str | None = None
) -> Path:
    return (
        _shared_root(shared_root) / "datasets" / _normalize_dataset_type(dataset_type)
    )


def dataset_relative_path(dataset_type: object, dataset_id: object) -> Path:
    return (
        Path("datasets")
        / _normalize_dataset_type(dataset_type)
        / f"{_safe_component(dataset_id, field_name='dataset_id')}.json"
    )


def dataset_contract_path(dataset_type: object, dataset_id: object) -> str:
    normalized_type = _normalize_dataset_type(dataset_type)
    normalized_id = _safe_component(dataset_id, field_name="dataset_id")
    return f"DF/shared/datasets/{normalized_type}/{normalized_id}.json"


def _dataset_id(dataset_type: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{dataset_type}-{timestamp}-{uuid.uuid4().hex[:8]}"


def _record_domain(record: dict[str, Any]) -> str:
    for candidate in (
        record.get("domain"),
        record.get("memory_context", {}).get("domain")
        if isinstance(record.get("memory_context"), dict)
        else None,
        record.get("payload", {}).get("domain")
        if isinstance(record.get("payload"), dict)
        else None,
    ):
        normalized = _normalize_text(candidate)
        if normalized:
            return _normalize_domain(normalized)
    raise DatasetBuilderError("record domain is missing.")


def _record_task_type(record: dict[str, Any]) -> str:
    for candidate in (
        record.get("task_type"),
        record.get("type"),
        record.get("memory_context", {}).get("type")
        if isinstance(record.get("memory_context"), dict)
        else None,
        record.get("payload", {}).get("task_type")
        if isinstance(record.get("payload"), dict)
        else None,
        record.get("payload", {}).get("type")
        if isinstance(record.get("payload"), dict)
        else None,
    ):
        normalized = _normalize_text(candidate)
        if normalized:
            return normalized
    raise DatasetBuilderError("record task_type is missing.")


def _record_id(record: dict[str, Any], *, source_file: Path, line_number: int) -> str:
    for candidate in (
        record.get("record_id"),
        record.get("id"),
        record.get("task_id"),
        record.get("execution_id"),
        record.get("trace_id"),
    ):
        normalized = _normalize_text(candidate)
        if normalized:
            return _safe_component(normalized, field_name="record_id")
    return (
        f"{_safe_component(source_file.stem, field_name='source_file')}-{line_number}"
    )


def normalize_training_record(
    raw_record: object,
    *,
    source_file: Path,
    line_number: int,
) -> dict[str, Any] | None:
    if not isinstance(raw_record, dict):
        return None
    record = dict(raw_record)
    try:
        domain = _record_domain(record)
        task_type = _record_task_type(record)
    except DatasetBuilderError:
        return None

    normalized: dict[str, Any] = {
        "record_id": _record_id(
            record, source_file=source_file, line_number=line_number
        ),
        "domain": domain,
        "task_type": task_type,
        "source_file": source_file.name,
        "source_line": line_number,
        "payload": record,
    }
    collected_at = _normalize_text(
        record.get("created_at") or record.get("timestamp") or record.get("updated_at")
    )
    if collected_at:
        normalized["collected_at"] = collected_at
    source_task_id = _normalize_text(
        record.get("source_task_id") or record.get("task_id")
    )
    if source_task_id:
        normalized["source_task_id"] = source_task_id
    return normalized


def _iter_training_records(source_dir: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if not source_dir.exists():
        return records

    for path in sorted(source_dir.glob("*.jsonl")):
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError as exc:
            raise DatasetBuilderError(
                f"training input is not readable: {path}"
            ) from exc
        for line_number, raw_line in enumerate(lines, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            normalized = normalize_training_record(
                payload,
                source_file=path,
                line_number=line_number,
            )
            if normalized is not None:
                records.append(normalized)
    return records


def _dataset_stats(records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "num_records": len(records),
        "domains": sorted({str(record["domain"]) for record in records}),
        "task_types": sorted({str(record["task_type"]) for record in records}),
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def build_dataset(
    dataset_type: object, *, shared_root: Path | str | None = None
) -> dict[str, Any]:
    normalized_type = _normalize_dataset_type(dataset_type)
    source_dir = training_input_dir(normalized_type, shared_root=shared_root)
    records = _iter_training_records(source_dir)
    dataset_id = _dataset_id(normalized_type)
    dataset = {
        "dataset_id": dataset_id,
        "dataset_type": normalized_type,
        "version": DATASET_VERSION,
        "created_at": _utc_timestamp(),
        "records": records,
        "stats": _dataset_stats(records),
    }
    target_path = (
        dataset_output_dir(normalized_type, shared_root=shared_root)
        / f"{dataset_id}.json"
    )
    _write_json(target_path, dataset)
    print(f"[DATASET] built id={dataset_id} records={len(records)}")
    return dataset


def _load_dataset(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise DatasetBuilderError(f"dataset not found: {path}") from None
    except (OSError, json.JSONDecodeError) as exc:
        raise DatasetBuilderError(f"dataset is not readable: {path}") from exc
    if not isinstance(payload, dict):
        raise DatasetBuilderError(f"dataset must contain a JSON object: {path}")
    return payload


def _find_dataset_path(
    dataset_id: object, *, shared_root: Path | str | None = None
) -> Path:
    normalized_id = _safe_component(dataset_id, field_name="dataset_id")
    root = _shared_root(shared_root) / "datasets"
    matches = sorted(root.glob(f"*/{normalized_id}.json"))
    if not matches:
        raise DatasetBuilderError(f"dataset not found for dataset_id='{normalized_id}'")
    if len(matches) > 1:
        raise DatasetBuilderError(
            f"dataset lookup is ambiguous for dataset_id='{normalized_id}'"
        )
    return matches[0]


def get_dataset(
    dataset_id: object, *, shared_root: Path | str | None = None
) -> dict[str, Any]:
    return _load_dataset(_find_dataset_path(dataset_id, shared_root=shared_root))


def create_training_job(
    dataset_id: object,
    model_type: object,
    *,
    shared_root: Path | str | None = None,
    mode: str = "remote_gpu",
    domain: str = "dev",
    requested_by: str = "training_dataset_pipeline",
) -> dict[str, Any]:
    dataset_path = _find_dataset_path(dataset_id, shared_root=shared_root)
    dataset = _load_dataset(dataset_path)
    normalized_dataset_id = _safe_component(
        dataset.get("dataset_id"), field_name="dataset_id"
    )
    normalized_model_type = _safe_component(model_type, field_name="model_type")
    dataset_type = _normalize_dataset_type(dataset.get("dataset_type"))
    job = create_compute_job(
        job_type="training",
        mode=mode,
        requested_by=requested_by,
        domain=domain,
        payload={
            "dataset_ref": normalized_dataset_id,
            "model_ref": normalized_model_type,
            "output_ref": f"model://training/{normalized_model_type}/{normalized_dataset_id}",
            "params": {
                "model_type": normalized_model_type,
                "dataset_type": dataset_type,
                "dataset_version": int(
                    dataset.get("version", DATASET_VERSION) or DATASET_VERSION
                ),
                "dataset_contract_path": dataset_contract_path(
                    dataset_type, normalized_dataset_id
                ),
                "dataset_local_path": str(dataset_path),
            },
        },
    )
    print(f"[DATASET] training_job_created id={job['job_id']}")
    return job
