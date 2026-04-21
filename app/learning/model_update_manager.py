from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from app.compute.compute_dispatcher import create_compute_job
import app.learning.model_loader as model_loader_module
from app.learning.model_update_config import (
    load_model_update_config,
    write_model_update_config,
)
import app.training.dataset_builder as dataset_builder_module
from app.training.dataset_builder import (
    build_dataset,
    dataset_contract_path,
    training_input_dir,
)


class ModelUpdateManagerError(RuntimeError):
    """Raised when the model update loop cannot validate or persist control state."""


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_dataset_type(dataset_type: object) -> str:
    return dataset_builder_module._normalize_dataset_type(dataset_type)


def _dataset_settings(config: dict[str, Any], dataset_type: str) -> dict[str, int]:
    raw_settings = config.get(dataset_type)
    if not isinstance(raw_settings, dict):
        raw_settings = {}
    try:
        min_new_records = max(0, int(raw_settings.get("min_new_records", 50)))
    except (TypeError, ValueError):
        min_new_records = 50
    try:
        last_dataset_size = max(0, int(raw_settings.get("last_dataset_size", 0)))
    except (TypeError, ValueError):
        last_dataset_size = 0
    return {
        "min_new_records": min_new_records,
        "last_dataset_size": last_dataset_size,
    }


def _count_training_records(
    dataset_type: str, *, shared_root: Path | str | None = None
) -> int:
    source_dir = training_input_dir(dataset_type, shared_root=shared_root)
    return len(dataset_builder_module._iter_training_records(source_dir))


def _model_prefix_for_dataset_type(dataset_type: str) -> str:
    if dataset_type == "memory_ranking":
        return "memory_ranker"
    return f"{dataset_type}_model"


def _model_version(model_id: str, *, prefix: str) -> int | None:
    match = re.fullmatch(rf"{re.escape(prefix)}_v(\d+)", _normalize_text(model_id))
    if match is None:
        return None
    return int(match.group(1))


def _next_model_id(dataset_type: str, config: dict[str, Any]) -> str:
    prefix = _model_prefix_for_dataset_type(dataset_type)
    versions: list[int] = []
    for candidate in (
        _normalize_text(config.get("active_model")),
        _normalize_text(config.get("candidate_model")),
    ):
        version = _model_version(candidate, prefix=prefix)
        if version is not None:
            versions.append(version)

    models_root = Path(model_loader_module.MODELS_ROOT)
    if models_root.exists():
        for path in sorted(models_root.glob(f"{prefix}_v*.json")):
            version = _model_version(path.stem, prefix=prefix)
            if version is not None:
                versions.append(version)

    next_version = max(versions, default=0) + 1
    return f"{prefix}_v{next_version}"


def check_update_needed(
    dataset_type: object,
    *,
    shared_root: Path | str | None = None,
) -> bool:
    normalized_type = _normalize_dataset_type(dataset_type)
    config = load_model_update_config()
    settings = _dataset_settings(config, normalized_type)
    current_records = _count_training_records(normalized_type, shared_root=shared_root)
    new_data = max(0, current_records - settings["last_dataset_size"])
    return new_data > settings["min_new_records"]


def trigger_model_update(
    dataset_type: object,
    *,
    shared_root: Path | str | None = None,
    mode: str = "remote_gpu",
    domain: str = "dev",
    requested_by: str = "model_update_manager",
) -> dict[str, Any] | None:
    normalized_type = _normalize_dataset_type(dataset_type)
    config = load_model_update_config()
    settings = _dataset_settings(config, normalized_type)
    current_records = _count_training_records(normalized_type, shared_root=shared_root)
    new_data = max(0, current_records - settings["last_dataset_size"])
    if new_data <= settings["min_new_records"]:
        return None

    dataset = build_dataset(normalized_type, shared_root=shared_root)
    dataset_id = _normalize_text(dataset.get("dataset_id"))
    if not dataset_id:
        raise ModelUpdateManagerError("built dataset is missing dataset_id.")

    candidate_model = _next_model_id(normalized_type, config)
    output_path = f"DF/shared/models/{candidate_model}.json"
    dataset_local_path = (
        dataset_builder_module.dataset_output_dir(
            normalized_type, shared_root=shared_root
        )
        / f"{dataset_id}.json"
    )
    job = create_compute_job(
        job_type="training",
        mode=mode,
        requested_by=requested_by,
        domain=domain,
        payload={
            "dataset_ref": dataset_id,
            "model_ref": candidate_model,
            "output_ref": output_path,
            "params": {
                "model_type": "memory_ranker",
                "dataset_type": normalized_type,
                "dataset_version": int(
                    dataset.get("version", dataset_builder_module.DATASET_VERSION)
                ),
                "dataset_contract_path": dataset_contract_path(
                    normalized_type, dataset_id
                ),
                "dataset_local_path": str(dataset_local_path),
                "expected_model_id": candidate_model,
                "model_output_path": output_path,
                "active_model": _normalize_text(config.get("active_model")),
            },
        },
    )

    config["candidate_model"] = candidate_model
    config[normalized_type] = {
        "min_new_records": settings["min_new_records"],
        "last_dataset_size": int(
            dataset.get("stats", {}).get("num_records", current_records)
        ),
    }
    write_model_update_config(config)

    print(f"[MODEL_UPDATE] triggered dataset={dataset_id}")
    print(f"[MODEL_UPDATE] new model candidate={candidate_model}")
    return {
        "dataset": dataset,
        "job": job,
        "candidate_model": candidate_model,
    }
