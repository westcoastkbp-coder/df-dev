from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

import pytest

from app.execution.action_result import build_action_result
from app.execution.external_modules import (
    ExternalModuleRegistry,
    ExternalModuleRequest,
    ExternalModuleValidationError,
    build_external_module_request,
    execute_external_module,
)
from app.orchestrator.execution_runner import run_execution
import app.execution.paths as paths_module
import app.orchestrator.task_factory as task_factory_module
import app.orchestrator.task_lifecycle as task_lifecycle_module
import app.orchestrator.task_memory as task_memory_module
import app.orchestrator.task_queue as task_queue_module
import app.orchestrator.task_state_store as task_state_store_module
import app.policy.policy_gate as policy_gate_module
import runtime.system_log as system_log_module
import runtime.token_efficiency as token_efficiency_module
import runtime.token_telemetry as token_telemetry_module
from runtime.decision.evaluator import reset_runtime_decision_history
from runtime.decision.stability import reset_runtime_decision_stabilizer
from runtime.network.monitor import reset_network_monitor


_MAX_FAILURE_RUNTIME_MS = 2_000


@dataclass(frozen=True, slots=True)
class FailureRuntime:
    store_path: Path


@dataclass(frozen=True, slots=True)
class FailureScenario:
    task_id: str
    injection_type: str
    expected_error_code: str
    expected_error_fragment: str

    def build_adapter(self) -> "FailureAdapter":
        if self.injection_type == "TIMEOUT":
            return FailureAdapter(error=TimeoutError("estimate call timed out"))
        if self.injection_type == "NETWORK_ERROR":
            return FailureAdapter(error=ConnectionError("network unreachable"))
        if self.injection_type == "INVALID_RESPONSE":
            return FailureAdapter(
                raw_result={
                    "request_id": f"EXT-{self.task_id}",
                    "status": "success",
                    "module_type": "estimate_service",
                    "operation": "generate_estimate",
                    "result_payload": {
                        "estimate_id": "est-001",
                    },
                    "duration_ms": 11,
                }
            )
        raise AssertionError(f"unsupported failure scenario: {self.injection_type}")


SCENARIOS = (
    FailureScenario(
        task_id="DF-EXT-TIMEOUT-V1",
        injection_type="TIMEOUT",
        expected_error_code="external_module_timeout",
        expected_error_fragment="timed out",
    ),
    FailureScenario(
        task_id="DF-EXT-NETWORK-V1",
        injection_type="NETWORK_ERROR",
        expected_error_code="external_module_failed",
        expected_error_fragment="network unreachable",
    ),
    FailureScenario(
        task_id="DF-EXT-MALFORMED-V1",
        injection_type="INVALID_RESPONSE",
        expected_error_code="external_module_invalid_result",
        expected_error_fragment="result_payload missing required fields: price_band",
    ),
)


class SequencedClock:
    def __init__(self) -> None:
        self._values = [
            "2026-04-05T01:00:00Z",
            "2026-04-05T01:00:01Z",
            "2026-04-05T01:00:02Z",
            "2026-04-05T01:00:03Z",
        ]
        self._index = 0

    def __call__(self) -> str:
        if self._index < len(self._values):
            value = self._values[self._index]
            self._index += 1
            return value
        return self._values[-1]


class FailureAdapter:
    module_type = "estimate_service"

    def __init__(self, *, raw_result: object | None = None, error: Exception | None = None) -> None:
        self.raw_result = raw_result
        self.error = error

    def validate_request_payload(self, operation: str, payload: dict[str, object]) -> None:
        if operation != "generate_estimate":
            raise ExternalModuleValidationError("unsupported operation for estimate_service")
        required_fields = {"estimate_scope", "address"}
        missing_fields = sorted(required_fields - set(payload))
        if missing_fields:
            raise ExternalModuleValidationError(
                "payload missing required fields: " + ", ".join(missing_fields)
            )

    def validate_result_payload(self, operation: str, payload: dict[str, object]) -> None:
        if operation != "generate_estimate":
            raise ExternalModuleValidationError("unsupported operation for estimate_service")
        required_fields = {"estimate_id", "price_band"}
        missing_fields = sorted(required_fields - set(payload))
        if missing_fields:
            raise ExternalModuleValidationError(
                "result_payload missing required fields: " + ", ".join(missing_fields)
            )

    def execute(self, request: ExternalModuleRequest) -> object:
        if self.error is not None:
            raise self.error
        return self.raw_result or {
            "request_id": request.request_id,
            "status": "success",
            "module_type": request.module_type,
            "operation": request.operation,
            "result_payload": {
                "estimate_id": "est-001",
                "price_band": "250k-300k",
            },
            "duration_ms": 25,
        }


def _reset_runtime_globals() -> None:
    task_factory_module.clear_task_runtime_store()
    task_queue_module.task_queue.clear()
    reset_runtime_decision_history()
    reset_runtime_decision_stabilizer()
    reset_network_monitor()
    with token_telemetry_module._RUN_STATE_LOCK:
        token_telemetry_module._RUN_STATE.clear()


def _configure_runtime(monkeypatch, root_dir: Path) -> FailureRuntime:
    data_dir = root_dir / "data"
    store_path = data_dir / "task_system.json"
    monkeypatch.setattr(paths_module, "ROOT_DIR", root_dir)
    monkeypatch.setattr(paths_module, "DATA_DIR", data_dir)
    monkeypatch.setattr(paths_module, "TASK_SYSTEM_FILE", store_path)
    monkeypatch.setattr(paths_module, "TASKS_FILE", data_dir / "tasks.json")
    monkeypatch.setattr(paths_module, "MEMORY_FILE", data_dir / "memory.json")
    monkeypatch.setattr(paths_module, "CONTACTS_FILE", data_dir / "contacts.json")
    monkeypatch.setattr(task_factory_module, "TASK_SYSTEM_FILE", store_path)
    monkeypatch.setattr(task_state_store_module, "ROOT_DIR", root_dir)
    monkeypatch.setattr(
        task_state_store_module,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )
    monkeypatch.setattr(task_memory_module, "ROOT_DIR", root_dir)
    monkeypatch.setattr(
        task_memory_module,
        "TASK_MEMORY_FILE",
        Path("runtime/state/task_memory.json"),
    )
    monkeypatch.setattr(
        task_lifecycle_module,
        "TASK_LIFECYCLE_LOG_FILE",
        root_dir / "runtime" / "logs" / "task_lifecycle.jsonl",
    )
    monkeypatch.setattr(
        task_queue_module,
        "TASK_QUEUE_FILE",
        root_dir / "runtime" / "state" / "task_queue.json",
    )
    monkeypatch.setattr(
        task_queue_module,
        "TASK_LOG_FILE",
        root_dir / "runtime" / "logs" / "tasks.log",
    )
    monkeypatch.setattr(
        system_log_module,
        "SYSTEM_LOG_FILE",
        root_dir / "runtime" / "logs" / "system.log",
    )
    monkeypatch.setattr(
        system_log_module,
        "TASK_LOG_FILE",
        root_dir / "runtime" / "logs" / "tasks.log",
    )
    monkeypatch.setattr(
        policy_gate_module,
        "POLICY_LOG_FILE",
        root_dir / "runtime" / "logs" / "policy.log",
    )
    monkeypatch.setattr(
        token_telemetry_module,
        "TOKEN_USAGE_LOG_FILE",
        root_dir / "runtime" / "logs" / "token_usage.jsonl",
    )
    monkeypatch.setattr(
        token_efficiency_module,
        "TOKEN_EFFICIENCY_LOG_FILE",
        root_dir / "runtime" / "logs" / "token_efficiency.jsonl",
    )
    _reset_runtime_globals()
    task_state_store_module.initialize_database(store_path)
    return FailureRuntime(store_path=store_path)


def _build_task(store_path: Path, *, task_id: str) -> dict[str, object]:
    task = task_factory_module.create_task(
        {
            "task_id": task_id,
            "status": "created",
            "intent": "new_lead",
            "payload": {"summary": "external failure verification"},
        },
        store_path=store_path,
    )
    task["status"] = "VALIDATED"
    return task_factory_module.save_task(task, store_path=store_path)


def _request_for(task_data: dict[str, object]) -> ExternalModuleRequest:
    return build_external_module_request(
        {
            "request_id": f"EXT-{task_data['task_id']}",
            "task_id": task_data["task_id"],
            "task_type": task_data["intent"],
            "module_type": "estimate_service",
            "operation": "generate_estimate",
            "payload": {
                "estimate_scope": "detached_adu",
                "address": "123 Main St",
            },
            "correlation_id": f"corr-{task_data['task_id']}",
            "timeout_ms": 15000,
            "metadata": {
                "schema_version": "v1",
                "request_source": "df_core",
                "capability": "estimate_generation",
                "priority": "normal",
                "tags": ["estimate", "external"],
            },
        }
    )


def _executor_for(registry: ExternalModuleRegistry):
    def executor(task_data: dict[str, object]) -> dict[str, object]:
        result = execute_external_module(_request_for(task_data), registry=registry)
        if result.status == "success":
            return build_action_result(
                status="completed",
                task_id=task_data.get("task_id"),
                action_type="NEW_LEAD",
                result_payload=dict(result.to_dict()),
                error_code="",
                error_message="",
                source="test_external_failure_executor",
            )
        return build_action_result(
            status="failed",
            task_id=task_data.get("task_id"),
            action_type="NEW_LEAD",
            result_payload=dict(result.to_dict()),
            error_code=result.error_code,
            error_message=result.error_message,
            source="test_external_failure_executor",
        )

    executor.__module__ = "test_external_failure_executor"
    return executor


def _persist(task_data: dict[str, object]) -> None:
    task_factory_module.save_task(task_data, store_path=task_factory_module.TASK_SYSTEM_FILE)


def _status_transitions(task_data: dict[str, object]) -> tuple[tuple[str, str], ...]:
    transitions: list[tuple[str, str]] = []
    for entry in list(task_data.get("history", [])):
        if str(entry.get("event", "")).strip() != "status_transition":
            continue
        transitions.append(
            (
                str(entry.get("from_status", "")).strip(),
                str(entry.get("to_status", "")).strip(),
            )
        )
    return tuple(transitions)


def _history_events(task_data: dict[str, object]) -> tuple[str, ...]:
    return tuple(str(entry.get("event", "")).strip() for entry in list(task_data.get("history", [])))


def _observe_failure(
    monkeypatch,
    tmp_path: Path,
    scenario: FailureScenario,
    *,
    run_label: str,
) -> dict[str, object]:
    runtime = _configure_runtime(monkeypatch, tmp_path / scenario.injection_type.lower() / run_label)
    task = _build_task(runtime.store_path, task_id=scenario.task_id)
    registry = ExternalModuleRegistry()
    registry.register(scenario.build_adapter())

    started_at = perf_counter()
    executed = run_execution(
        task,
        now=SequencedClock(),
        persist=_persist,
        executor=_executor_for(registry),
    )
    elapsed_ms = int(round((perf_counter() - started_at) * 1000))

    restored = task_factory_module.get_task(scenario.task_id, runtime.store_path)
    execution_key = task_state_store_module.build_execution_key(
        task_id=scenario.task_id,
        action_type="NEW_LEAD",
    )
    execution_record = task_state_store_module.read_execution_record(
        execution_key,
        store_path=runtime.store_path,
    )

    result = dict(executed.get("result", {}) or {})
    ledger_result = dict((execution_record or {}).get("action_result", {}) or {})
    history_events = _history_events(restored or {})
    history_transitions = _status_transitions(restored or {})
    error_message = str(result.get("error_message", "")).strip()

    false_success_occurred = any(
        status.lower() in {"completed", "success"}
        for status in (
            str(executed.get("status", "")).strip(),
            str((restored or {}).get("status", "")).strip(),
            str(result.get("status", "")).strip(),
            str(ledger_result.get("status", "")).strip(),
        )
        if status
    )
    pipeline_continued_safely = (
        execution_record is not None
        and str(execution_record.get("status", "")).strip() == "executed"
        and str(ledger_result.get("status", "")).strip() == "failed"
        and str(ledger_result.get("error_code", "")).strip() == str(result.get("error_code", "")).strip()
        and "execution_started" in history_events
        and "execution_failed" in history_events
    )
    valid_failure_state = (
        str((restored or {}).get("status", "")).strip() == "FAILED"
        and str((restored or {}).get("failed_at", "")).strip() != ""
        and str((restored or {}).get("error", "")).strip() != ""
        and history_transitions[-2:] == (("VALIDATED", "EXECUTING"), ("EXECUTING", "FAILED"))
    )

    return {
        "elapsed_ms": elapsed_ms,
        "observed_task_state": str(executed.get("status", "")).strip(),
        "persisted_task_state": str((restored or {}).get("status", "")).strip(),
        "action_result_status": str(result.get("status", "")).strip(),
        "ledger_status": str((execution_record or {}).get("status", "")).strip(),
        "error_code": str(result.get("error_code", "")).strip(),
        "error_message": error_message,
        "pipeline_continued_safely": pipeline_continued_safely,
        "false_success_occurred": false_success_occurred,
        "valid_failure_state": valid_failure_state,
        "terminal_signature": (
            str(executed.get("status", "")).strip(),
            str((restored or {}).get("status", "")).strip(),
            str(result.get("status", "")).strip(),
            str(result.get("error_code", "")).strip(),
            error_message,
            str((execution_record or {}).get("status", "")).strip(),
            history_events,
            history_transitions,
        ),
    }


@pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda scenario: scenario.injection_type.lower())
def test_external_failure_modes_reach_deterministic_failed_state(
    monkeypatch,
    tmp_path: Path,
    scenario: FailureScenario,
) -> None:
    first = _observe_failure(monkeypatch, tmp_path, scenario, run_label="first")
    second = _observe_failure(monkeypatch, tmp_path, scenario, run_label="second")

    for observed in (first, second):
        assert observed["elapsed_ms"] < _MAX_FAILURE_RUNTIME_MS
        assert observed["observed_task_state"] == "FAILED"
        assert observed["persisted_task_state"] == "FAILED"
        assert observed["action_result_status"] == "failed"
        assert observed["ledger_status"] == "executed"
        assert observed["error_code"] == scenario.expected_error_code
        assert scenario.expected_error_fragment in str(observed["error_message"])
        assert observed["pipeline_continued_safely"] is True
        assert observed["false_success_occurred"] is False
        assert observed["valid_failure_state"] is True

    assert first["terminal_signature"] == second["terminal_signature"]
