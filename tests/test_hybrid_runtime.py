from __future__ import annotations

from pathlib import Path

import pytest

from app.config.hybrid_runtime import load_runtime_config
from app.context.shared_context_store import prepare_shared_context_store, shared_context_contract
from scripts import start_df


def test_load_runtime_config_defaults_to_local_dev(tmp_path: Path) -> None:
    config = load_runtime_config(root_dir=tmp_path, environ={})

    assert config.role == "local_dev"
    assert config.host == "127.0.0.1"
    assert config.port == 9000
    assert config.storage_paths.storage_root == tmp_path / "runtime" / "local_dev"


def test_load_runtime_config_uses_remote_role_settings(tmp_path: Path) -> None:
    config = load_runtime_config(
        root_dir=tmp_path,
        environ={
            "ENV_ROLE": "remote_runtime",
            "DF_HOST": "0.0.0.0",
            "DF_PORT": "9200",
            "DF_STORAGE_ROOT": str(tmp_path / "shared"),
            "DF_REMOTE_BASE_URL": "https://zephyrus.internal",
        },
    )

    assert config.role == "remote_runtime"
    assert config.host == "0.0.0.0"
    assert config.port == 9200
    assert config.storage_paths.storage_root == tmp_path / "shared"
    assert config.remote_endpoint.enabled is False


def test_prepare_shared_context_store_creates_contract_files(tmp_path: Path) -> None:
    config = load_runtime_config(root_dir=tmp_path, environ={"ENV_ROLE": "remote_runtime"})

    contract = prepare_shared_context_store(config)

    assert contract == shared_context_contract(config)
    assert config.storage_paths.global_context_file.exists()
    assert config.storage_paths.system_context_file.exists()
    assert config.storage_paths.decisions_file.exists()
    assert config.storage_paths.interactions_file.exists()
    assert config.storage_paths.audit_file.exists()
    assert config.storage_paths.active_threads_dir.exists()


def test_startup_report_for_local_dev_marks_remote_endpoint_optional(tmp_path: Path) -> None:
    report = start_df.build_startup_report(
        root_dir=tmp_path,
        environ={"ENV_ROLE": "local_dev"},
    )

    assert report["role"] == "local_dev"
    assert report["startup_mode"] == "local_dev"
    assert report["architecture_mode"] == "hybrid_dev"
    assert any(
        check["name"] == "remote:endpoint"
        and check["detail"] == "optional"
        for check in report["checks"]
    )


def test_startup_report_for_remote_runtime_requires_remote_service_dirs(tmp_path: Path) -> None:
    report = start_df.build_startup_report(
        root_dir=tmp_path,
        environ={"ENV_ROLE": "remote_runtime"},
    )

    assert report["role"] == "remote_runtime"
    assert report["startup_mode"] == "remote_runtime"
    assert report["system_status"] == "ready"
    assert any(
        check["name"] == "dir:verification"
        and check["status"] == "ok"
        for check in report["checks"]
    )


def test_invalid_role_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        load_runtime_config(root_dir=tmp_path, environ={"ENV_ROLE": "edge_case"})
