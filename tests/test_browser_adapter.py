from __future__ import annotations

import pytest

import app.adapters.browser_adapter as browser_adapter_module
from app.adapters.browser_adapter import (
    BrowserAdapterError,
    BrowserExecution,
    execute_browser_action,
)
from app.execution.action_contract import (
    ActionContractViolation,
    validate_action_contract,
    validate_action_result_contract,
)


def _valid_browser_action_contract(
    *, operation: str = "open_page", execution_mode: str = "live"
) -> dict[str, object]:
    parameters: dict[str, object] = {
        "operation": operation,
        "url": "https://example.com/form",
        "timeout_seconds": 10,
    }
    if operation in {"extract_text", "click_element", "submit_form"}:
        parameters["selector"] = "#target"
    if operation == "fill_form":
        parameters["fields"] = {"email": "owner@example.com", "name": "Anton"}
        parameters["selector"] = "#lead-form"
    return {
        "action_id": "act-browser-001",
        "action_type": "browser_action",
        "target_type": "adapter",
        "target_ref": "browser",
        "parameters": parameters,
        "execution_mode": execution_mode,
        "confirmation_policy": "not_required",
        "idempotency_key": "owner:browser:001",
        "requested_by": "ownerbox_interaction_v1",
        "timestamp": "2026-04-14T12:00:00Z",
        "schema_version": "v1",
    }


class StubBrowserRuntime:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    def open_page(self, *, url: str, timeout_seconds: int) -> BrowserExecution:
        self.calls.append(
            ("open_page", {"url": url, "timeout_seconds": timeout_seconds})
        )
        return BrowserExecution(
            result_type="browser_open",
            summary=f"Opened {url}",
            references={"url": url},
        )

    def extract_text(
        self, *, url: str, selector: str, timeout_seconds: int
    ) -> BrowserExecution:
        self.calls.append(
            (
                "extract_text",
                {"url": url, "selector": selector, "timeout_seconds": timeout_seconds},
            )
        )
        return BrowserExecution(
            result_type="browser_extract",
            summary=f"Extracted text from {selector}",
            references={"url": url, "selector": selector},
        )

    def fill_form(
        self,
        *,
        url: str,
        fields: dict[str, str],
        selector: str | None,
        timeout_seconds: int,
    ) -> BrowserExecution:
        self.calls.append(
            (
                "fill_form",
                {
                    "url": url,
                    "fields": dict(fields),
                    "selector": selector,
                    "timeout_seconds": timeout_seconds,
                },
            )
        )
        return BrowserExecution(
            result_type="browser_fill",
            summary="Filled form",
            references={"url": url, "selector": selector, "field_count": len(fields)},
        )

    def click_element(
        self, *, url: str, selector: str, timeout_seconds: int
    ) -> BrowserExecution:
        self.calls.append(
            (
                "click_element",
                {"url": url, "selector": selector, "timeout_seconds": timeout_seconds},
            )
        )
        return BrowserExecution(
            result_type="browser_click",
            summary=f"Clicked {selector}",
            references={"url": url, "selector": selector},
        )

    def submit_form(
        self, *, url: str, selector: str, timeout_seconds: int
    ) -> BrowserExecution:
        self.calls.append(
            (
                "submit_form",
                {"url": url, "selector": selector, "timeout_seconds": timeout_seconds},
            )
        )
        return BrowserExecution(
            result_type="browser_submit",
            summary=f"Submitted {selector}",
            references={"url": url, "selector": selector},
        )


def test_valid_browser_action_executes() -> None:
    runtime = StubBrowserRuntime()

    result = execute_browser_action(_valid_browser_action_contract(), runtime=runtime)

    assert len(runtime.calls) == 1
    assert runtime.calls[0][0] == "open_page"
    assert validate_action_result_contract(result) == result
    assert result["status"] == "success"
    assert result["result_type"] == "browser_open"
    assert result["payload"]["summary"] == "Opened https://example.com/form"
    assert result["payload"]["metadata"]["operation"] == "open_page"


def test_invalid_browser_operation_rejected() -> None:
    payload = _valid_browser_action_contract()
    payload["parameters"] = {"operation": "hover", "url": "https://example.com/form"}

    with pytest.raises(
        ActionContractViolation, match="unsupported browser operation: hover"
    ):
        validate_action_contract(payload)


def test_malformed_browser_params_rejected() -> None:
    payload = _valid_browser_action_contract(operation="click_element")
    payload["parameters"] = {
        "operation": "click_element",
        "url": "https://example.com/form",
        "selector": "#target\n.secondary",
    }

    result = execute_browser_action(payload)

    assert validate_action_result_contract(result) == result
    assert result["status"] == "failed"
    assert result["error_code"] == "validation_error"
    assert (
        result["error_message"] == "parameters.selector contains unsupported characters"
    )


def test_browser_dry_run_does_not_execute_runtime() -> None:
    runtime = StubBrowserRuntime()

    result = execute_browser_action(
        _valid_browser_action_contract(execution_mode="dry_run"),
        runtime=runtime,
    )

    assert runtime.calls == []
    assert validate_action_result_contract(result) == result
    assert result["status"] == "success"
    assert result["result_type"] == "simulation"
    assert result["payload"]["metadata"]["dry_run"] is True


def test_browser_dry_run_does_not_resolve_live_backend(monkeypatch) -> None:
    monkeypatch.setattr(
        browser_adapter_module,
        "_build_default_browser_runtime",
        lambda config: (_ for _ in ()).throw(
            AssertionError("live backend must not be resolved")
        ),
    )

    result = execute_browser_action(
        _valid_browser_action_contract(execution_mode="dry_run")
    )

    assert result["status"] == "success"
    assert result["result_type"] == "simulation"


def test_browser_missing_provider_config_returns_provider_not_configured(
    monkeypatch,
) -> None:
    monkeypatch.delenv("DIGITAL_FOREMAN_BROWSER_BACKEND", raising=False)

    result = execute_browser_action(_valid_browser_action_contract())

    assert validate_action_result_contract(result) == result
    assert result["status"] == "failed"
    assert result["error_code"] == "provider_not_configured"


def test_browser_live_backend_path_uses_provider_factory(monkeypatch) -> None:
    runtime = StubBrowserRuntime()
    captured: dict[str, object] = {}

    def build_runtime(config):
        captured["runtime_mode"] = config.runtime_mode
        return runtime, "playwright"

    monkeypatch.setattr(
        browser_adapter_module, "_build_default_browser_runtime", build_runtime
    )

    result = execute_browser_action(
        _valid_browser_action_contract(),
        config={"runtime_mode": "playwright"},
    )

    assert runtime.calls[0][0] == "open_page"
    assert captured["runtime_mode"] == "playwright"
    assert result["payload"]["metadata"]["backend_used"] == "playwright"


def test_browser_timeout_is_normalized() -> None:
    class TimeoutRuntime(StubBrowserRuntime):
        def open_page(self, *, url: str, timeout_seconds: int) -> BrowserExecution:
            raise TimeoutError("slow provider")

    result = execute_browser_action(
        _valid_browser_action_contract(), runtime=TimeoutRuntime()
    )

    assert validate_action_result_contract(result) == result
    assert result["status"] == "failed"
    assert result["error_code"] == "timeout"


def test_browser_runtime_errors_are_normalized() -> None:
    class FailingRuntime(StubBrowserRuntime):
        def open_page(self, *, url: str, timeout_seconds: int) -> BrowserExecution:
            raise BrowserAdapterError("transport_error", "browser transport failed")

    result = execute_browser_action(
        _valid_browser_action_contract(), runtime=FailingRuntime()
    )

    assert validate_action_result_contract(result) == result
    assert result["status"] == "failed"
    assert result["error_code"] == "transport_error"
    assert result["error_message"] == "browser transport failed"
