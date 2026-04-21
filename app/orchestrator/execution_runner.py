from __future__ import annotations

import app.orchestrator.task_factory as task_factory_module
from app.execution import paths as execution_paths_module
from app.execution.product_runtime import assert_product_runtime_executor
from app.orchestrator.task_memory import store_task_result
from app.orchestrator.task_state_store import (
    claim_execution_record,
    complete_execution_record,
    read_execution_record,
)
from runtime.pipeline import managed_execution as managed_execution_module

_DEFAULT_TASK_SYSTEM_FILE = task_factory_module.TASK_SYSTEM_FILE


def _sync_managed_execution_bindings() -> None:
    if task_factory_module.TASK_SYSTEM_FILE == _DEFAULT_TASK_SYSTEM_FILE:
        task_factory_module.TASK_SYSTEM_FILE = execution_paths_module.TASKS_FILE
    managed_execution_module.task_factory_module = task_factory_module
    managed_execution_module.store_task_result = store_task_result
    managed_execution_module.assert_product_runtime_executor = (
        assert_product_runtime_executor
    )
    managed_execution_module.claim_execution_record = claim_execution_record
    managed_execution_module.complete_execution_record = complete_execution_record
    managed_execution_module.read_execution_record = read_execution_record


def run_execution(task_data: dict[str, object], **kwargs):
    _sync_managed_execution_bindings()
    return managed_execution_module.run_execution(task_data, **kwargs)


__all__ = [
    "assert_product_runtime_executor",
    "claim_execution_record",
    "complete_execution_record",
    "read_execution_record",
    "run_execution",
    "store_task_result",
    "task_factory_module",
]
