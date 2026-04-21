from __future__ import annotations

from pathlib import Path

from app.adapters.printer_adapter import (
    PrinterAdapterError,
    PrinterExecution,
    execute_printer_action,
)


def _valid_printer_action_contract(
    *, execution_mode: str = "live"
) -> dict[str, object]:
    return {
        "action_id": "act-print-001",
        "action_type": "print_document",
        "target_type": "adapter",
        "target_ref": "printer",
        "parameters": {
            "operation": "print_document",
            "document_title": "Owner Review",
            "document_text": "Bounded printable document text.",
            "copies": 1,
        },
        "execution_mode": execution_mode,
        "confirmation_policy": "required",
        "idempotency_key": "owner-print:001",
        "requested_by": "ownerbox_interaction_v1",
        "timestamp": "2026-04-14T12:00:00Z",
        "schema_version": "v1",
    }


def test_printer_adapter_missing_config_fails_closed(monkeypatch) -> None:
    monkeypatch.delenv("DF_PRINTER_ENABLED", raising=False)
    monkeypatch.delenv("DF_PRINTER_NAME", raising=False)
    monkeypatch.delenv("DF_PRINTER_TIMEOUT_SECONDS", raising=False)

    result = execute_printer_action(_valid_printer_action_contract())

    assert result["status"] == "failed"
    assert result["error_code"] == "provider_not_configured"
    assert result["error_message"] == "printer adapter is not enabled"


def test_printer_adapter_dry_run_never_calls_runtime() -> None:
    called = False

    class FailingRuntime:
        backend_name = "failing_runtime"

        def print_document(self, **kwargs: object) -> PrinterExecution:
            nonlocal called
            called = True
            raise AssertionError("dry_run must not call printer runtime")

    result = execute_printer_action(
        _valid_printer_action_contract(execution_mode="dry_run"),
        runtime=FailingRuntime(),
        config={
            "enabled": True,
            "printer_name": "Zephyrus_Main",
            "timeout_seconds": 30,
        },
    )

    assert called is False
    assert result["status"] == "success"
    assert result["result_type"] == "simulation"
    assert result["payload"]["metadata"]["dry_run"] is True
    assert result["payload"]["metadata"]["printer_name"] == "Zephyrus_Main"


def test_printer_adapter_normalizes_device_unavailable_failure(tmp_path: Path) -> None:
    class UnavailableRuntime:
        backend_name = "stub_unavailable"

        def print_document(
            self,
            *,
            action_id: str,
            printer_name: str,
            document_title: str,
            artifact_path: Path,
            timeout_seconds: int,
        ) -> PrinterExecution:
            raise PrinterAdapterError(
                "device_not_available",
                "printer is not available",
                diagnostic={"artifact_path": str(artifact_path)},
            )

    result = execute_printer_action(
        _valid_printer_action_contract(),
        runtime=UnavailableRuntime(),
        config={
            "enabled": True,
            "printer_name": "Zephyrus_Main",
            "timeout_seconds": 30,
        },
    )

    assert result["status"] == "failed"
    assert result["error_code"] == "device_not_available"
    assert result["error_message"] == "printer is not available"
    assert result["payload"]["metadata"]["printer_name"] == "Zephyrus_Main"
