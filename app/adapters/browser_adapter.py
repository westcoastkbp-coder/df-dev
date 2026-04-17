from __future__ import annotations

import os
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha1
from typing import Protocol
from urllib.parse import urlparse

from app.execution.action_contract import (
    ActionContractViolation,
    build_action_result_contract,
    validate_action_contract,
)


SUPPORTED_ACTION_TYPE = "BROWSER_ACTION"
SUPPORTED_TARGET_REFS = frozenset({"browser", "browser_adapter"})
SUPPORTED_PARAMETER_FIELDS = frozenset(
    {"operation", "url", "selector", "fields", "timeout_seconds"}
)
SUPPORTED_OPERATIONS = frozenset(
    {"open_page", "extract_text", "fill_form", "click_element", "submit_form"}
)
SUPPORTED_RUNTIME_MODES = frozenset({"auto", "stub", "playwright"})
DEFAULT_RUNTIME_MODE = "auto"
DEFAULT_TIMEOUT_SECONDS = 10
MAX_TIMEOUT_SECONDS = 60
MAX_URL_LENGTH = 512
MAX_SELECTOR_LENGTH = 256
MAX_TEXT_LENGTH = 240
MAX_FIELD_COUNT = 20
BACKEND_ENV_VAR = "DIGITAL_FOREMAN_BROWSER_BACKEND"
PLAYWRIGHT_BROWSERS_PATH_ENV_VAR = "PLAYWRIGHT_BROWSERS_PATH"
_SELECTOR_FORBIDDEN_PATTERN = re.compile(r"[\x00\r\n]")


@dataclass(frozen=True, slots=True)
class BrowserAdapterConfig:
    runtime_mode: str = DEFAULT_RUNTIME_MODE
    headless: bool = True
    browsers_path: str | None = None


@dataclass(frozen=True, slots=True)
class BrowserExecution:
    result_type: str
    summary: str
    references: dict[str, object]


class BrowserAdapterError(RuntimeError):
    def __init__(
        self,
        error_code: str,
        message: str,
        *,
        diagnostic: Mapping[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.message = message
        self.diagnostic = _normalize_mapping(diagnostic)


class BrowserRuntime(Protocol):
    def open_page(self, *, url: str, timeout_seconds: int) -> BrowserExecution:
        ...

    def extract_text(
        self,
        *,
        url: str,
        selector: str,
        timeout_seconds: int,
    ) -> BrowserExecution:
        ...

    def fill_form(
        self,
        *,
        url: str,
        fields: dict[str, str],
        selector: str | None,
        timeout_seconds: int,
    ) -> BrowserExecution:
        ...

    def click_element(
        self,
        *,
        url: str,
        selector: str,
        timeout_seconds: int,
    ) -> BrowserExecution:
        ...

    def submit_form(
        self,
        *,
        url: str,
        selector: str,
        timeout_seconds: int,
    ) -> BrowserExecution:
        ...


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_mapping(value: Mapping[str, object] | None) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _truncate_text(value: object, *, limit: int = MAX_TEXT_LENGTH) -> str:
    normalized = _normalize_text(value)
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3].rstrip() + "..."


def _bounded_text(value: object, *, field_name: str, max_length: int = MAX_TEXT_LENGTH) -> str:
    normalized = _normalize_text(value)
    if not normalized:
        raise ActionContractViolation(f"{field_name} must not be empty")
    if len(normalized) > max_length:
        raise ActionContractViolation(f"{field_name} exceeds max length")
    return normalized


def _normalize_optional_selector(value: object, *, required: bool) -> str | None:
    normalized = _normalize_text(value)
    if not normalized:
        if required:
            raise ActionContractViolation("parameters.selector must not be empty")
        return None
    if len(normalized) > MAX_SELECTOR_LENGTH:
        raise ActionContractViolation("parameters.selector exceeds max length")
    if _SELECTOR_FORBIDDEN_PATTERN.search(normalized):
        raise ActionContractViolation("parameters.selector contains unsupported characters")
    return normalized


def _validate_url(value: object, *, field_name: str = "parameters.url") -> str:
    normalized = _bounded_text(value, field_name=field_name, max_length=MAX_URL_LENGTH)
    parsed = urlparse(normalized)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ActionContractViolation(f"{field_name} must be an absolute http or https URL")
    return normalized


def _validate_timeout_seconds(value: object) -> int:
    if value is None:
        return DEFAULT_TIMEOUT_SECONDS
    if isinstance(value, bool) or not isinstance(value, int):
        raise ActionContractViolation("parameters.timeout_seconds must be an integer")
    if value <= 0 or value > MAX_TIMEOUT_SECONDS:
        raise ActionContractViolation(
            f"parameters.timeout_seconds must be between 1 and {MAX_TIMEOUT_SECONDS}"
        )
    return value


def _validate_fields(value: object, *, required: bool) -> dict[str, str]:
    if value is None:
        if required:
            raise ActionContractViolation("parameters.fields must be a dict")
        return {}
    if not isinstance(value, dict):
        raise ActionContractViolation("parameters.fields must be a dict")
    if required and not value:
        raise ActionContractViolation("parameters.fields must not be empty")
    if len(value) > MAX_FIELD_COUNT:
        raise ActionContractViolation("parameters.fields exceeds max size")
    normalized: dict[str, str] = {}
    for key in sorted(value):
        normalized_key = _bounded_text(key, field_name="parameters.fields key", max_length=64)
        item = value[key]
        if not isinstance(item, str):
            raise ActionContractViolation(
                f"parameters.fields.{normalized_key} must be a string"
            )
        normalized[normalized_key] = _bounded_text(
            item,
            field_name=f"parameters.fields.{normalized_key}",
            max_length=MAX_TEXT_LENGTH,
        )
    return normalized


def _operation_result_type(operation: str) -> str:
    mapping = {
        "open_page": "browser_open",
        "extract_text": "browser_extract",
        "fill_form": "browser_fill",
        "click_element": "browser_click",
        "submit_form": "browser_submit",
    }
    return mapping[operation]


def validate_browser_action_parameters(parameters: Mapping[str, object]) -> dict[str, object]:
    normalized = dict(parameters)
    unexpected_fields = sorted(set(normalized) - SUPPORTED_PARAMETER_FIELDS)
    if unexpected_fields:
        raise ActionContractViolation(
            "parameters contains unsupported fields: " + ", ".join(unexpected_fields)
        )

    operation = _bounded_text(normalized.get("operation"), field_name="parameters.operation")
    if operation not in SUPPORTED_OPERATIONS:
        raise ActionContractViolation(f"unsupported browser operation: {operation}")

    timeout_seconds = _validate_timeout_seconds(normalized.get("timeout_seconds"))
    requires_selector = operation in {"extract_text", "click_element", "submit_form"}
    requires_fields = operation == "fill_form"

    url = _validate_url(normalized.get("url"))
    selector = _normalize_optional_selector(
        normalized.get("selector"),
        required=requires_selector,
    )
    fields = _validate_fields(normalized.get("fields"), required=requires_fields)

    if operation != "fill_form" and normalized.get("fields") not in (None, {}):
        raise ActionContractViolation(f"parameters.fields is not supported for {operation}")
    if operation == "open_page" and selector is not None:
        raise ActionContractViolation("parameters.selector is not supported for open_page")
    if operation == "extract_text" and normalized.get("fields") not in (None, {}):
        raise ActionContractViolation("parameters.fields is not supported for extract_text")
    if operation == "click_element" and normalized.get("fields") not in (None, {}):
        raise ActionContractViolation("parameters.fields is not supported for click_element")
    if operation == "submit_form" and normalized.get("fields") not in (None, {}):
        raise ActionContractViolation("parameters.fields is not supported for submit_form")

    return {
        "operation": operation,
        "url": url,
        "selector": selector,
        "fields": fields,
        "timeout_seconds": timeout_seconds,
    }


class StubBrowserRuntime:
    backend_name = "stub"

    def open_page(self, *, url: str, timeout_seconds: int) -> BrowserExecution:
        page_ref = f"page-{sha1(url.encode('utf-8')).hexdigest()[:12]}"
        return BrowserExecution(
            result_type="browser_open",
            summary=_truncate_text(f"Opened page {url}"),
            references={"url": url, "page_ref": page_ref, "timeout_seconds": timeout_seconds},
        )

    def extract_text(
        self,
        *,
        url: str,
        selector: str,
        timeout_seconds: int,
    ) -> BrowserExecution:
        extracted_text = _truncate_text(f"Extracted text from {selector} on {url}")
        return BrowserExecution(
            result_type="browser_extract",
            summary=extracted_text,
            references={
                "url": url,
                "selector": selector,
                "text_preview": extracted_text,
                "timeout_seconds": timeout_seconds,
            },
        )

    def fill_form(
        self,
        *,
        url: str,
        fields: dict[str, str],
        selector: str | None,
        timeout_seconds: int,
    ) -> BrowserExecution:
        return BrowserExecution(
            result_type="browser_fill",
            summary=f"Filled {len(fields)} form fields on {url}",
            references={
                "url": url,
                "selector": selector,
                "field_names": sorted(fields),
                "field_count": len(fields),
                "timeout_seconds": timeout_seconds,
            },
        )

    def click_element(
        self,
        *,
        url: str,
        selector: str,
        timeout_seconds: int,
    ) -> BrowserExecution:
        return BrowserExecution(
            result_type="browser_click",
            summary=f"Clicked {selector} on {url}",
            references={"url": url, "selector": selector, "timeout_seconds": timeout_seconds},
        )

    def submit_form(
        self,
        *,
        url: str,
        selector: str,
        timeout_seconds: int,
    ) -> BrowserExecution:
        return BrowserExecution(
            result_type="browser_submit",
            summary=f"Submitted form {selector} on {url}",
            references={"url": url, "selector": selector, "timeout_seconds": timeout_seconds},
        )


class PlaywrightBrowserRuntime:
    backend_name = "playwright"

    def __init__(self, *, headless: bool = True, browsers_path: str | None = None) -> None:
        self._headless = headless
        self._browsers_path = _normalize_text(browsers_path) or None

    def _sync_api(self):
        try:
            from playwright.sync_api import Error as PlaywrightError
            from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise BrowserAdapterError(
                "provider_not_configured",
                "playwright browser backend is not installed",
            ) from exc
        return sync_playwright, PlaywrightError, PlaywrightTimeoutError

    def _run_page_operation(
        self,
        *,
        url: str,
        timeout_seconds: int,
        operation: str,
        callback,
    ) -> BrowserExecution:
        if self._browsers_path:
            os.environ.setdefault(PLAYWRIGHT_BROWSERS_PATH_ENV_VAR, self._browsers_path)
        sync_playwright, PlaywrightError, PlaywrightTimeoutError = self._sync_api()
        timeout_ms = int(timeout_seconds) * 1000
        try:
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(headless=self._headless)
                try:
                    page = browser.new_page()
                    page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                    return callback(page, timeout_ms)
                finally:
                    browser.close()
        except PlaywrightTimeoutError as exc:
            raise BrowserAdapterError(
                "timeout",
                "browser action timed out",
                diagnostic={"operation": operation, "reason": _truncate_text(exc)},
            ) from exc
        except PlaywrightError as exc:
            raise BrowserAdapterError(
                "provider_error",
                "browser provider failed",
                diagnostic={"operation": operation, "reason": _truncate_text(exc)},
            ) from exc
        except OSError as exc:
            raise BrowserAdapterError(
                "transport_error",
                "browser transport failed",
                diagnostic={"operation": operation, "reason": _truncate_text(exc)},
            ) from exc

    def _page_snapshot(self, page, *, requested_url: str) -> tuple[str, str]:
        final_url = _truncate_text(getattr(page, "url", requested_url), limit=MAX_URL_LENGTH)
        try:
            title = _truncate_text(page.title(), limit=MAX_TEXT_LENGTH)
        except Exception:
            title = ""
        return final_url or requested_url, title

    def _field_selector(self, field_name: str) -> str:
        if field_name.startswith(("#", ".", "[", "/", "css=", "xpath=")):
            return field_name
        return f'[name="{field_name}"]'

    def open_page(self, *, url: str, timeout_seconds: int) -> BrowserExecution:
        def callback(page, _timeout_ms: int) -> BrowserExecution:
            final_url, title = self._page_snapshot(page, requested_url=url)
            page_ref = f"page-{sha1(final_url.encode('utf-8')).hexdigest()[:12]}"
            return BrowserExecution(
                result_type="browser_open",
                summary=_truncate_text(f"Opened page {final_url}"),
                references={
                    "url": final_url,
                    "requested_url": url,
                    "page_ref": page_ref,
                    "title": title,
                    "timeout_seconds": timeout_seconds,
                },
            )

        return self._run_page_operation(
            url=url,
            timeout_seconds=timeout_seconds,
            operation="open_page",
            callback=callback,
        )

    def extract_text(
        self,
        *,
        url: str,
        selector: str,
        timeout_seconds: int,
    ) -> BrowserExecution:
        def callback(page, timeout_ms: int) -> BrowserExecution:
            locator = page.locator(selector).first
            text = _truncate_text(locator.inner_text(timeout=timeout_ms))
            final_url, title = self._page_snapshot(page, requested_url=url)
            return BrowserExecution(
                result_type="browser_extract",
                summary=text or f"Extracted text from {selector}",
                references={
                    "url": final_url,
                    "requested_url": url,
                    "selector": selector,
                    "title": title,
                    "text_preview": text,
                    "timeout_seconds": timeout_seconds,
                },
            )

        return self._run_page_operation(
            url=url,
            timeout_seconds=timeout_seconds,
            operation="extract_text",
            callback=callback,
        )

    def fill_form(
        self,
        *,
        url: str,
        fields: dict[str, str],
        selector: str | None,
        timeout_seconds: int,
    ) -> BrowserExecution:
        def callback(page, timeout_ms: int) -> BrowserExecution:
            if selector:
                page.locator(selector).first.wait_for(state="attached", timeout=timeout_ms)
            for field_name, value in sorted(fields.items()):
                page.locator(self._field_selector(field_name)).first.fill(
                    value,
                    timeout=timeout_ms,
                )
            final_url, title = self._page_snapshot(page, requested_url=url)
            return BrowserExecution(
                result_type="browser_fill",
                summary=f"Filled {len(fields)} form fields on {final_url}",
                references={
                    "url": final_url,
                    "requested_url": url,
                    "selector": selector,
                    "title": title,
                    "field_names": sorted(fields),
                    "field_count": len(fields),
                    "timeout_seconds": timeout_seconds,
                },
            )

        return self._run_page_operation(
            url=url,
            timeout_seconds=timeout_seconds,
            operation="fill_form",
            callback=callback,
        )

    def click_element(
        self,
        *,
        url: str,
        selector: str,
        timeout_seconds: int,
    ) -> BrowserExecution:
        def callback(page, timeout_ms: int) -> BrowserExecution:
            page.locator(selector).first.click(timeout=timeout_ms)
            final_url, title = self._page_snapshot(page, requested_url=url)
            return BrowserExecution(
                result_type="browser_click",
                summary=f"Clicked {selector} on {final_url}",
                references={
                    "url": final_url,
                    "requested_url": url,
                    "selector": selector,
                    "title": title,
                    "timeout_seconds": timeout_seconds,
                },
            )

        return self._run_page_operation(
            url=url,
            timeout_seconds=timeout_seconds,
            operation="click_element",
            callback=callback,
        )

    def submit_form(
        self,
        *,
        url: str,
        selector: str,
        timeout_seconds: int,
    ) -> BrowserExecution:
        def callback(page, timeout_ms: int) -> BrowserExecution:
            form = page.locator(selector).first
            form.evaluate("(form) => form.requestSubmit ? form.requestSubmit() : form.submit()")
            page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
            final_url, title = self._page_snapshot(page, requested_url=url)
            return BrowserExecution(
                result_type="browser_submit",
                summary=f"Submitted form {selector} on {final_url}",
                references={
                    "url": final_url,
                    "requested_url": url,
                    "selector": selector,
                    "title": title,
                    "timeout_seconds": timeout_seconds,
                },
            )

        return self._run_page_operation(
            url=url,
            timeout_seconds=timeout_seconds,
            operation="submit_form",
            callback=callback,
        )


def _validate_browser_action_contract(action_contract: object) -> dict[str, object]:
    validated = validate_action_contract(action_contract)
    if validated["action_type"] != SUPPORTED_ACTION_TYPE:
        raise ActionContractViolation(
            f"unsupported action_type for browser adapter: {validated['action_type']}"
        )
    target_ref = _normalize_text(validated.get("target_ref")).lower()
    if target_ref not in SUPPORTED_TARGET_REFS:
        raise ActionContractViolation(
            "target_ref must be one of: " + ", ".join(sorted(SUPPORTED_TARGET_REFS))
        )
    return {
        **validated,
        "parameters": validate_browser_action_parameters(
            _normalize_mapping(validated.get("parameters"))
        ),
    }


def _normalize_runtime_mode(value: object) -> str:
    normalized = _normalize_text(value).lower() or DEFAULT_RUNTIME_MODE
    if normalized not in SUPPORTED_RUNTIME_MODES:
        raise BrowserAdapterError(
            "validation_error",
            "runtime_mode must be one of: " + ", ".join(sorted(SUPPORTED_RUNTIME_MODES)),
        )
    return normalized


def _resolve_browser_adapter_config(
    config: BrowserAdapterConfig | Mapping[str, object] | None,
) -> BrowserAdapterConfig:
    if config is None:
        return BrowserAdapterConfig()
    if isinstance(config, BrowserAdapterConfig):
        return config
    if not isinstance(config, Mapping):
        raise BrowserAdapterError("validation_error", "browser adapter config must be a mapping")
    payload = dict(config)
    unexpected_fields = sorted(set(payload) - {"runtime_mode", "headless", "browsers_path"})
    if unexpected_fields:
        raise BrowserAdapterError(
            "validation_error",
            "browser adapter config contains unsupported fields: "
            + ", ".join(unexpected_fields),
        )
    headless = payload.get("headless", True)
    if not isinstance(headless, bool):
        raise BrowserAdapterError("validation_error", "headless must be a boolean")
    browsers_path = payload.get("browsers_path")
    if browsers_path is not None and not isinstance(browsers_path, str):
        raise BrowserAdapterError("validation_error", "browsers_path must be a string")
    return BrowserAdapterConfig(
        runtime_mode=_normalize_runtime_mode(payload.get("runtime_mode")),
        headless=headless,
        browsers_path=_normalize_text(browsers_path) or None,
    )


def _env_backend() -> str:
    normalized = _normalize_text(os.getenv(BACKEND_ENV_VAR)).lower()
    if normalized in SUPPORTED_RUNTIME_MODES - {"auto"}:
        return normalized
    return ""


def _build_default_browser_runtime(
    config: BrowserAdapterConfig,
) -> tuple[BrowserRuntime, str]:
    backend = config.runtime_mode
    if backend == "auto":
        backend = _env_backend()
        if not backend:
            raise BrowserAdapterError(
                "provider_not_configured",
                "browser backend is not configured",
            )
    if backend == "stub":
        return StubBrowserRuntime(), "stub"
    if backend == "playwright":
        return (
            PlaywrightBrowserRuntime(
                headless=config.headless,
                browsers_path=config.browsers_path,
            ),
            "playwright",
        )
    raise BrowserAdapterError(
        "provider_not_configured",
        f"browser backend `{backend}` is not configured",
    )


def _runtime_backend_name(runtime: BrowserRuntime | None, config: BrowserAdapterConfig) -> str:
    candidate = _normalize_text(getattr(runtime, "backend_name", ""))
    if candidate:
        return candidate
    if config.runtime_mode != "auto":
        return config.runtime_mode
    return "custom"


def _simulation_payload(action_id: str, parameters: Mapping[str, object]) -> dict[str, object]:
    operation = _normalize_text(parameters.get("operation"))
    return build_action_result_contract(
        action_id=action_id,
        status="success",
        result_type="simulation",
        payload={
            "note": f"dry run: {operation}",
            "summary": f"Simulated browser operation {operation}",
            "references": {
                "url": parameters.get("url"),
                "selector": parameters.get("selector"),
                "field_names": sorted(_normalize_mapping(parameters.get("fields"))),
            },
            "metadata": {
                "dry_run": True,
                "simulation_mode": "dry_run",
                "operation": operation,
                "timeout_seconds": parameters.get("timeout_seconds"),
                "provider": "simulation",
                "backend_used": "dry_run",
            },
        },
    )


def _failure_result(
    *,
    action_id: str,
    error_code: str,
    error_message: str,
    backend_used: str,
    parameters: Mapping[str, object] | None = None,
    diagnostic: Mapping[str, object] | None = None,
) -> dict[str, object]:
    metadata = {
        "operation": _normalize_text((parameters or {}).get("operation")),
        "provider": backend_used,
        "backend_used": backend_used,
    }
    payload: dict[str, object] = {"metadata": metadata}
    normalized_diagnostic = _normalize_mapping(diagnostic)
    if normalized_diagnostic:
        payload["diagnostic"] = normalized_diagnostic
    operation = _normalize_text((parameters or {}).get("operation")) or "open_page"
    result_type = _operation_result_type(operation) if operation in SUPPORTED_OPERATIONS else "browser_action"
    return build_action_result_contract(
        action_id=action_id,
        status="failed",
        result_type=result_type,
        payload=payload,
        error_code=error_code,
        error_message=error_message,
    )


def execute_browser_action(
    action_contract: object,
    *,
    runtime: BrowserRuntime | None = None,
    config: BrowserAdapterConfig | Mapping[str, object] | None = None,
) -> dict[str, object]:
    started_at = time.monotonic()
    action_id = _normalize_text(_normalize_mapping(action_contract).get("action_id")) or "unknown_action"
    raw_parameters = _normalize_mapping(_normalize_mapping(action_contract).get("parameters"))
    effective_config = _resolve_browser_adapter_config(config)
    backend_used = _runtime_backend_name(runtime, effective_config)

    try:
        validated = _validate_browser_action_contract(action_contract)
        action_id = str(validated["action_id"])
        parameters = dict(validated["parameters"])
        operation = str(parameters["operation"])

        if validated["execution_mode"] == "dry_run":
            return _simulation_payload(action_id, parameters)

        if runtime is None:
            runtime, backend_used = _build_default_browser_runtime(effective_config)
        else:
            backend_used = _runtime_backend_name(runtime, effective_config)

        if operation == "open_page":
            execution = runtime.open_page(
                url=str(parameters["url"]),
                timeout_seconds=int(parameters["timeout_seconds"]),
            )
        elif operation == "extract_text":
            execution = runtime.extract_text(
                url=str(parameters["url"]),
                selector=str(parameters["selector"]),
                timeout_seconds=int(parameters["timeout_seconds"]),
            )
        elif operation == "fill_form":
            execution = runtime.fill_form(
                url=str(parameters["url"]),
                fields={str(key): str(value) for key, value in dict(parameters["fields"]).items()},
                selector=_normalize_text(parameters.get("selector")) or None,
                timeout_seconds=int(parameters["timeout_seconds"]),
            )
        elif operation == "click_element":
            execution = runtime.click_element(
                url=str(parameters["url"]),
                selector=str(parameters["selector"]),
                timeout_seconds=int(parameters["timeout_seconds"]),
            )
        else:
            execution = runtime.submit_form(
                url=str(parameters["url"]),
                selector=str(parameters["selector"]),
                timeout_seconds=int(parameters["timeout_seconds"]),
            )
    except ActionContractViolation as exc:
        return _failure_result(
            action_id=action_id,
            error_code="validation_error",
            error_message=str(exc),
            backend_used=backend_used,
            parameters=raw_parameters,
        )
    except BrowserAdapterError as exc:
        return _failure_result(
            action_id=action_id,
            error_code=exc.error_code,
            error_message=exc.message,
            backend_used=backend_used,
            parameters=raw_parameters,
            diagnostic=exc.diagnostic,
        )
    except TimeoutError:
        return _failure_result(
            action_id=action_id,
            error_code="timeout",
            error_message="browser action timed out",
            backend_used=backend_used,
            parameters=raw_parameters,
        )
    except Exception as exc:
        return _failure_result(
            action_id=action_id,
            error_code="unknown_error",
            error_message="browser action failed",
            backend_used=backend_used,
            parameters=raw_parameters,
            diagnostic={"exception_type": type(exc).__name__},
        )

    latency_ms = max(0, int(round((time.monotonic() - started_at) * 1000)))
    return build_action_result_contract(
        action_id=action_id,
        status="success",
        result_type=execution.result_type,
        payload={
            "summary": execution.summary,
            "references": dict(execution.references),
            "metadata": {
                "operation": operation,
                "provider": backend_used,
                "backend_used": backend_used,
                "timeout_seconds": parameters["timeout_seconds"],
                "latency_ms": latency_ms,
            },
        },
    )
