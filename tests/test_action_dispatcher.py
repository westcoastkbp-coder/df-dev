from __future__ import annotations

import json
import time
from pathlib import Path

import app.adapters.browser_adapter as browser_adapter_module
import app.adapters.email_adapter as email_adapter_module
from app.execution.action_contract import build_action_result_contract, validate_action_result_contract
from app.execution.action_dispatcher import dispatch_action
from app.memory import memory_registry
from app.orchestrator import task_memory, task_state_store
import app.execution.action_dispatcher as action_dispatcher_module
import app.execution.idempotency_store as idempotency_store_module
import runtime.system_log as system_log_module


def _configure_runtime(monkeypatch, tmp_path: Path) -> Path:
    system_log_file = tmp_path / "runtime" / "logs" / "system.log"
    task_log_file = tmp_path / "runtime" / "logs" / "tasks.log"
    task_memory_file = tmp_path / "runtime" / "state" / "task_memory.json"
    registry_file = tmp_path / "df-system" / "memory_registry.json"

    monkeypatch.setattr(system_log_module, "SYSTEM_LOG_FILE", system_log_file)
    monkeypatch.setattr(system_log_module, "TASK_LOG_FILE", task_log_file)
    monkeypatch.setattr(action_dispatcher_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(task_memory, "TASK_MEMORY_FILE", task_memory_file)
    monkeypatch.setattr(task_state_store, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store,
        "TASK_STATE_DB_FILE",
        Path("runtime/task_state.sqlite3"),
    )
    monkeypatch.setattr(memory_registry, "REGISTRY_FILE", registry_file)
    monkeypatch.setattr(idempotency_store_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        idempotency_store_module,
        "IDEMPOTENCY_DB_FILE",
        Path("runtime/state/idempotency.sqlite3"),
    )
    action_dispatcher_module._ACTION_OUTCOME_CACHE.clear()
    return system_log_file


def _trace_entries(system_log_file: Path) -> list[dict[str, object]]:
    if not system_log_file.exists():
        return []
    entries: list[dict[str, object]] = []
    for line in system_log_file.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if payload.get("event_type") != "trace":
            continue
        entries.append(dict(payload.get("details", {})))
    return entries


def _valid_openai_action_contract(*, execution_mode: str = "live") -> dict[str, object]:
    return {
        "action_id": "act-openai-dispatch-001",
        "action_type": "openai_request",
        "target_type": "adapter",
        "target_ref": "openai",
        "parameters": {
            "model": "gpt-5-mini",
            "prompt": "Summarize the change in one sentence.",
            "max_tokens": 64,
            "temperature": 0.2,
        },
        "execution_mode": execution_mode,
        "confirmation_policy": "not_required",
        "idempotency_key": "df:openai:act-openai-dispatch-001",
        "requested_by": "df_core",
        "timestamp": "2026-04-14T12:00:00Z",
        "schema_version": "v1",
    }


def _valid_send_email_action_contract() -> dict[str, object]:
    return {
        "action_id": "act-email-001",
        "action_type": "send_email",
        "target_type": "gateway",
        "target_ref": "gmail_gateway",
        "parameters": {
            "template_id": "lead_followup_v1",
            "lead_id": "lead-001",
            "metadata": {"priority": "high"},
        },
        "execution_mode": "live",
        "confirmation_policy": "required",
        "idempotency_key": "lead-001:workflow:send_email:abcd1234",
        "requested_by": "df_core",
        "timestamp": "2026-04-14T12:00:00Z",
        "schema_version": "v1",
    }


def _valid_browser_action_contract(*, execution_mode: str = "live") -> dict[str, object]:
    return {
        "action_id": "act-browser-001",
        "action_type": "browser_action",
        "target_type": "adapter",
        "target_ref": "browser",
        "parameters": {
            "operation": "click_element",
            "url": "https://example.com/form",
            "selector": "#submit",
            "timeout_seconds": 10,
        },
        "execution_mode": execution_mode,
        "confirmation_policy": "required",
        "idempotency_key": "owner:browser:001",
        "requested_by": "ownerbox_interaction_v1",
        "timestamp": "2026-04-14T12:00:00Z",
        "schema_version": "v1",
    }


def _valid_email_action_contract(*, execution_mode: str = "live") -> dict[str, object]:
    return {
        "action_id": "act-email-action-001",
        "action_type": "email_action",
        "target_type": "adapter",
        "target_ref": "email",
        "parameters": {
            "operation": "send_email",
            "to": ["owner@example.com"],
            "subject": "Owner update",
            "body": "Bounded body",
            "attachments": ["artifact:owner-update"],
        },
        "execution_mode": execution_mode,
        "confirmation_policy": "required",
        "idempotency_key": "owner:email:001",
        "requested_by": "ownerbox_interaction_v1",
        "timestamp": "2026-04-14T12:00:00Z",
        "schema_version": "v1",
    }


def _valid_printer_action_contract(*, execution_mode: str = "live") -> dict[str, object]:
    return {
        "action_id": "act-print-action-001",
        "action_type": "print_document",
        "target_type": "adapter",
        "target_ref": "printer",
        "parameters": {
            "operation": "print_document",
            "document_title": "Owner Review",
            "document_text": "Bounded printable document text.",
            "copies": 1,
            "printer_name": "Zephyrus_Main",
        },
        "execution_mode": execution_mode,
        "confirmation_policy": "required",
        "idempotency_key": "owner:print:001",
        "requested_by": "ownerbox_interaction_v1",
        "timestamp": "2026-04-14T12:00:00Z",
        "schema_version": "v1",
    }


def test_valid_openai_request_executes_end_to_end(monkeypatch, tmp_path: Path) -> None:
    system_log_file = _configure_runtime(monkeypatch, tmp_path)
    calls: list[dict[str, object]] = []

    def openai_executor(action_contract: object) -> dict[str, object]:
        assert isinstance(action_contract, dict)
        calls.append(dict(action_contract))
        return build_action_result_contract(
            action_id="act-openai-dispatch-001",
            status="success",
            result_type="text_generation",
            payload={
                "text": "Normalized model output",
                "metadata": {
                    "latency_ms": 21,
                    "input_tokens": 12,
                    "output_tokens": 6,
                    "total_tokens": 18,
                },
            },
        )

    result = dispatch_action(
        _valid_openai_action_contract(),
        openai_executor=openai_executor,
    )

    assert len(calls) == 1
    assert validate_action_result_contract(result) == result
    assert result["status"] == "success"
    assert result["result_type"] == "text_generation"
    metadata = result["payload"]["metadata"]
    assert metadata["adapter_used"] == "openai_adapter"
    assert metadata["dispatcher_latency_ms"] >= 0
    assert metadata["trace_artifact_path"]
    assert metadata["memory_evidence_registered"] is True

    trace_entries = _trace_entries(system_log_file)
    assert trace_entries
    trace = trace_entries[-1]
    assert trace["action_id"] == "act-openai-dispatch-001"
    assert trace["action_type"] == "OPENAI_REQUEST"
    assert trace["adapter_used"] == "openai_adapter"
    assert trace["result_status"] == "success"
    assert trace["latency_ms"] >= 0
    assert trace["usage"] == {
        "input_tokens": 12,
        "output_tokens": 6,
        "total_tokens": 18,
    }

    trace_path = Path(str(metadata["trace_artifact_path"]))
    assert trace_path.exists()
    trace_record = json.loads(trace_path.read_text(encoding="utf-8"))
    assert trace_record["type"] == "execution_trace"
    assert trace_record["payload"]["action_id"] == "act-openai-dispatch-001"
    registry_entry = memory_registry.get_artifact_by_logical_key(
        memory_registry.compute_artifact_key("dev", "execution_trace", "act-openai-dispatch-001")
    )
    assert registry_entry is not None
    history = task_memory.get_task_history("act-openai-dispatch-001")
    assert len(history) == 1
    assert history[0]["result_type"] == "text_generation"


def test_invalid_action_is_blocked(monkeypatch, tmp_path: Path) -> None:
    system_log_file = _configure_runtime(monkeypatch, tmp_path)
    called = False

    def openai_executor(action_contract: object) -> dict[str, object]:
        nonlocal called
        called = True
        raise AssertionError("unsupported action must not hit adapter")

    result = dispatch_action(
        _valid_send_email_action_contract(),
        openai_executor=openai_executor,
    )

    assert called is False
    assert validate_action_result_contract(result) == result
    assert result["status"] == "blocked"
    assert result["error_code"] == "unsupported_operation"
    assert result["error_message"] == "unsupported action_type for dispatcher: SEND_EMAIL"
    trace_entries = _trace_entries(system_log_file)
    assert trace_entries[-1]["result_status"] == "blocked"
    assert trace_entries[-1]["error_code"] == "unsupported_operation"


def test_dry_run_path_skips_adapter_and_still_traces(monkeypatch, tmp_path: Path) -> None:
    system_log_file = _configure_runtime(monkeypatch, tmp_path)
    called = False

    def openai_executor(action_contract: object) -> dict[str, object]:
        nonlocal called
        called = True
        raise AssertionError("dry_run must not call adapter")

    result = dispatch_action(
        _valid_openai_action_contract(execution_mode="dry_run"),
        openai_executor=openai_executor,
    )

    assert called is False
    assert validate_action_result_contract(result) == result
    assert result["status"] == "success"
    assert result["result_type"] == "simulation"
    assert result["payload"]["metadata"]["dry_run"] is True
    assert _trace_entries(system_log_file)[-1]["execution_mode"] == "dry_run"


def test_adapter_errors_are_handled_without_crash(monkeypatch, tmp_path: Path) -> None:
    system_log_file = _configure_runtime(monkeypatch, tmp_path)

    def openai_executor(action_contract: object) -> dict[str, object]:
        raise RuntimeError("adapter crashed")

    result = dispatch_action(
        _valid_openai_action_contract(),
        openai_executor=openai_executor,
    )

    assert validate_action_result_contract(result) == result
    assert result["status"] == "failed"
    assert result["error_code"] == "unknown_error"
    assert result["error_message"] == "action dispatch failed"
    trace_entries = _trace_entries(system_log_file)
    assert trace_entries[-1]["result_status"] == "failed"
    assert trace_entries[-1]["adapter_used"] == "openai_adapter"


def test_successful_action_is_returned_from_idempotency_cache(monkeypatch, tmp_path: Path) -> None:
    system_log_file = _configure_runtime(monkeypatch, tmp_path)
    calls: list[str] = []

    def email_executor(action_contract: object) -> dict[str, object]:
        action_id = str(dict(action_contract)["action_id"])
        calls.append(action_id)
        return build_action_result_contract(
            action_id=action_id,
            status="success",
            result_type="email_send",
            payload={
                "summary": "Sent owner email",
                "references": {"message_id": "message-001"},
                "metadata": {"operation": "send_email"},
            },
        )

    first = dispatch_action(_valid_email_action_contract(), email_executor=email_executor)
    second = dispatch_action(_valid_email_action_contract(), email_executor=email_executor)

    assert calls == ["act-email-action-001"]
    assert first["status"] == "success"
    assert second["status"] == "success"
    assert second["payload"]["metadata"]["idempotency_cache_hit"] is True
    assert second["payload"]["metadata"]["memory_evidence_registered"] is False
    traces = _trace_entries(system_log_file)
    assert traces[-1]["idempotency_cache_hit"] is True


def test_idempotency_survives_restart(monkeypatch, tmp_path: Path) -> None:
    _configure_runtime(monkeypatch, tmp_path)
    calls: list[str] = []

    def email_executor(action_contract: object) -> dict[str, object]:
        action_id = str(dict(action_contract)["action_id"])
        calls.append(action_id)
        return build_action_result_contract(
            action_id=action_id,
            status="success",
            result_type="email_send",
            payload={
                "summary": "Sent owner email",
                "metadata": {"operation": "send_email"},
            },
        )

    first = dispatch_action(_valid_email_action_contract(), email_executor=email_executor)
    action_dispatcher_module._ACTION_OUTCOME_CACHE.clear()
    second = dispatch_action(_valid_email_action_contract(), email_executor=email_executor)

    assert calls == ["act-email-action-001"]
    assert first["status"] == "success"
    assert second["status"] == "success"
    assert second["payload"]["metadata"]["idempotency_cache_hit"] is True


def test_failed_non_retryable_action_is_not_replayed_after_restart(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _configure_runtime(monkeypatch, tmp_path)
    calls: list[str] = []

    def openai_executor(action_contract: object) -> dict[str, object]:
        action_id = str(dict(action_contract)["action_id"])
        calls.append(action_id)
        return build_action_result_contract(
            action_id=action_id,
            status="failed",
            result_type="text_generation",
            payload={"metadata": {"operation": "generate"}},
            error_code="provider_error",
            error_message="provider failed",
        )

    first = dispatch_action(_valid_openai_action_contract(), openai_executor=openai_executor)
    action_dispatcher_module._ACTION_OUTCOME_CACHE.clear()
    second = dispatch_action(_valid_openai_action_contract(), openai_executor=openai_executor)

    assert calls == ["act-openai-dispatch-001"]
    assert first["status"] == "failed"
    assert second["status"] == "failed"
    assert second["error_code"] == "provider_error"
    assert second["payload"]["metadata"]["idempotency_cache_hit"] is True


def test_idempotency_store_failure_returns_normalized_failure(monkeypatch, tmp_path: Path) -> None:
    _configure_runtime(monkeypatch, tmp_path)

    class FailingIdempotencyStore:
        def get(self, *, action_id: object, idempotency_key: object):
            raise idempotency_store_module.IdempotencyStoreError(
                code="persistence_error",
                action_id=action_id,
                operation="get",
                reason="idempotency store unavailable",
            )

    called = False

    def openai_executor(action_contract: object) -> dict[str, object]:
        nonlocal called
        called = True
        raise AssertionError("dispatcher must fail closed before execution")

    result = dispatch_action(
        _valid_openai_action_contract(),
        openai_executor=openai_executor,
        idempotency_store=FailingIdempotencyStore(),
    )

    assert called is False
    assert result["status"] == "failed"
    assert result["error_code"] == "persistence_error"
    assert result["error_message"] == "idempotency store unavailable"


def test_dispatch_timeout_is_classified_as_timeout(monkeypatch, tmp_path: Path) -> None:
    system_log_file = _configure_runtime(monkeypatch, tmp_path)

    def openai_executor(action_contract: object) -> dict[str, object]:
        time.sleep(0.05)
        return build_action_result_contract(
            action_id=str(dict(action_contract)["action_id"]),
            status="success",
            result_type="text_generation",
            payload={"text": "too late"},
        )

    result = dispatch_action(
        _valid_openai_action_contract(),
        openai_executor=openai_executor,
        dispatch_context={"step_timeout_seconds": "0.01"},
    )

    assert validate_action_result_contract(result) == result
    assert result["status"] == "failed"
    assert result["error_code"] == "timeout"
    assert result["error_message"] == "action dispatch timed out"
    assert result["payload"]["metadata"]["step_timeout_seconds"] == "0.01"
    traces = _trace_entries(system_log_file)
    assert traces[-1]["error_code"] == "timeout"


def test_browser_and_email_routes_execute_with_trace_and_bounded_memory(
    monkeypatch, tmp_path: Path
) -> None:
    system_log_file = _configure_runtime(monkeypatch, tmp_path)

    def browser_executor(action_contract: object) -> dict[str, object]:
        assert dict(action_contract)["action_type"] == "BROWSER_ACTION"
        return build_action_result_contract(
            action_id="act-browser-001",
            status="success",
            result_type="browser_click",
            payload={
                "summary": "Clicked submit",
                "references": {"selector": "#submit"},
                "metadata": {"operation": "click_element"},
            },
        )

    def email_executor(action_contract: object) -> dict[str, object]:
        assert dict(action_contract)["action_type"] == "EMAIL_ACTION"
        return build_action_result_contract(
            action_id="act-email-action-001",
            status="success",
            result_type="email_send",
            payload={
                "summary": "Sent owner email",
                "references": {"message_id": "message-001"},
                "metadata": {"operation": "send_email"},
            },
        )

    browser_result = dispatch_action(
        _valid_browser_action_contract(),
        browser_executor=browser_executor,
        dispatch_context={
            "owner_id": "owner-001",
            "trust_class": "high",
            "approval_id": "owner-approval-001",
        },
    )
    email_result = dispatch_action(
        _valid_email_action_contract(),
        email_executor=email_executor,
        dispatch_context={
            "owner_id": "owner-001",
            "trust_class": "high",
            "approval_id": "owner-approval-002",
        },
    )

    assert browser_result["payload"]["metadata"]["adapter_used"] == "browser_adapter"
    assert browser_result["payload"]["metadata"]["operation"] == "click_element"
    assert browser_result["payload"]["metadata"]["approval_id"] == "owner-approval-001"
    assert email_result["payload"]["metadata"]["adapter_used"] == "email_adapter"
    assert email_result["payload"]["metadata"]["owner_id"] == "owner-001"

    traces = _trace_entries(system_log_file)
    assert traces[-2]["action_type"] == "BROWSER_ACTION"
    assert traces[-2]["operation"] == "click_element"
    assert traces[-2]["approval_id"] == "owner-approval-001"
    assert traces[-1]["action_type"] == "EMAIL_ACTION"
    assert traces[-1]["trust_class"] == "high"

    browser_history = task_memory.get_task_history("act-browser-001")
    email_history = task_memory.get_task_history("act-email-action-001")
    assert browser_history[0]["result_summary"] == (
        f"action_type=BROWSER_ACTION operation=click_element status=success "
        f"result_type=browser_click trace={tmp_path / 'runtime/out/traces/actions/act-browser-001.json'}"
    )
    assert "Bounded body" not in email_history[0]["result_summary"]


def test_printer_route_executes_with_trace_and_bounded_memory(
    monkeypatch, tmp_path: Path
) -> None:
    system_log_file = _configure_runtime(monkeypatch, tmp_path)

    def printer_executor(action_contract: object) -> dict[str, object]:
        assert dict(action_contract)["action_type"] == "PRINT_DOCUMENT"
        return build_action_result_contract(
            action_id="act-print-action-001",
            status="success",
            result_type="print_document",
            payload={
                "summary": "Printed 'Owner Review' on Zephyrus_Main",
                "references": {"artifact_path": "runtime/out/artifacts/printer_jobs/act-print-action-001.txt"},
                "metadata": {
                    "operation": "print_document",
                    "backend_used": "stub_printer",
                    "printer_name": "Zephyrus_Main",
                    "job_status": "submitted",
                    "pages": 1,
                },
            },
        )

    result = dispatch_action(
        _valid_printer_action_contract(),
        printer_executor=printer_executor,
        dispatch_context={
            "owner_id": "owner-001",
            "trust_class": "critical",
            "approval_id": "owner-approval-003",
            "workflow_id": "owner-workflow-001",
            "step_id": "owner-workflow-001.step2",
            "scenario_type": "owner_generate_review_and_print_document",
        },
    )

    assert result["status"] == "success"
    assert result["payload"]["metadata"]["adapter_used"] == "printer_adapter"
    assert result["payload"]["metadata"]["printer_name"] == "Zephyrus_Main"
    traces = _trace_entries(system_log_file)
    assert traces[-1]["action_type"] == "PRINT_DOCUMENT"
    assert traces[-1]["printer_name"] == "Zephyrus_Main"
    assert traces[-1]["scenario_type"] == "owner_generate_review_and_print_document"
    assert traces[-1]["approval_id"] == "owner-approval-003"
    print_history = task_memory.get_task_history("act-print-action-001")
    assert len(print_history) == 1
    assert "Bounded printable document text." not in print_history[0]["result_summary"]


def test_dispatcher_routes_to_default_live_capable_adapters(monkeypatch, tmp_path: Path) -> None:
    system_log_file = _configure_runtime(monkeypatch, tmp_path)

    class BrowserRuntime:
        backend_name = "playwright"

        def open_page(self, *, url: str, timeout_seconds: int):
            raise AssertionError("unexpected operation")

        def extract_text(self, *, url: str, selector: str, timeout_seconds: int):
            raise AssertionError("unexpected operation")

        def fill_form(self, *, url: str, fields: dict[str, str], selector: str | None, timeout_seconds: int):
            raise AssertionError("unexpected operation")

        def click_element(self, *, url: str, selector: str, timeout_seconds: int):
            return browser_adapter_module.BrowserExecution(
                result_type="browser_click",
                summary="Clicked via live browser backend",
                references={"selector": selector, "url": url},
            )

        def submit_form(self, *, url: str, selector: str, timeout_seconds: int):
            raise AssertionError("unexpected operation")

    class EmailRuntime:
        backend_name = "gmail"

        def create_draft(self, *, to: list[str], subject: str, body: str, attachments: list[str]):
            raise AssertionError("unexpected operation")

        def send_email(self, *, to: list[str], subject: str, body: str, attachments: list[str]):
            return email_adapter_module.EmailExecution(
                result_type="email_send",
                summary="Sent via live email backend",
                references={"message_id": "message-live-001", "recipient_count": len(to)},
            )

        def reply_email(
            self,
            *,
            reply_to_id: str,
            body: str,
            to: list[str],
            subject: str | None,
            attachments: list[str],
        ):
            raise AssertionError("unexpected operation")

    monkeypatch.setattr(
        browser_adapter_module,
        "_build_default_browser_runtime",
        lambda config: (BrowserRuntime(), "playwright"),
    )
    monkeypatch.setattr(
        email_adapter_module,
        "_build_default_email_runtime",
        lambda config: (EmailRuntime(), "gmail"),
    )

    browser_result = dispatch_action(_valid_browser_action_contract())
    email_result = dispatch_action(_valid_email_action_contract())

    assert browser_result["status"] == "success"
    assert browser_result["payload"]["metadata"]["backend_used"] == "playwright"
    assert email_result["status"] == "success"
    assert email_result["payload"]["metadata"]["backend_used"] == "gmail"

    traces = _trace_entries(system_log_file)
    assert traces[-2]["backend_used"] == "playwright"
    assert traces[-1]["backend_used"] == "gmail"


def test_browser_and_email_dry_run_paths_use_adapter_simulation(monkeypatch, tmp_path: Path) -> None:
    _configure_runtime(monkeypatch, tmp_path)

    browser_result = dispatch_action(_valid_browser_action_contract(execution_mode="dry_run"))
    email_result = dispatch_action(_valid_email_action_contract(execution_mode="dry_run"))

    assert browser_result["status"] == "success"
    assert browser_result["result_type"] == "simulation"
    assert browser_result["payload"]["metadata"]["adapter_used"] == "browser_adapter"
    assert browser_result["payload"]["metadata"]["dry_run"] is True
    assert email_result["status"] == "success"
    assert email_result["result_type"] == "simulation"
    assert email_result["payload"]["metadata"]["adapter_used"] == "email_adapter"
    assert email_result["payload"]["metadata"]["dry_run"] is True


def test_trace_is_created_for_malformed_contract(monkeypatch, tmp_path: Path) -> None:
    system_log_file = _configure_runtime(monkeypatch, tmp_path)

    result = dispatch_action({"action_type": "openai_request"})

    assert validate_action_result_contract(result) == result
    assert result["status"] == "blocked"
    assert result["error_code"] == "validation_error"
    traces = _trace_entries(system_log_file)
    assert traces
    assert traces[-1]["action_id"] == "unknown_action"
    assert traces[-1]["result_status"] == "blocked"
