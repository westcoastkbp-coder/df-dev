from __future__ import annotations

import json
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path
from typing import Final

import httpx

from app.execution.action_result import build_action_result
from app.execution.execution_boundary import require_execution_boundary
from app.execution.paths import LOGS_DIR, ROOT_DIR
from app.policy.policy_gate import evaluate_policy
from runtime.system_log import log_event, write_json_log


BROWSER_TOOL_ACTION: Final[str] = "BROWSER_TOOL"
OPEN_URL_OPERATION: Final[str] = "open_url"
GET_PAGE_TEXT_OPERATION: Final[str] = "get_page_text"
FILL_FORM_OPERATION: Final[str] = "fill_form"
CLICK_OPERATION: Final[str] = "click"
SUPPORTED_BROWSER_OPERATIONS: Final[tuple[str, ...]] = (
    OPEN_URL_OPERATION,
    GET_PAGE_TEXT_OPERATION,
    FILL_FORM_OPERATION,
    CLICK_OPERATION,
)
BROWSER_TOOL_LOG_FILE = ROOT_DIR / LOGS_DIR / "browser_tool.jsonl"
_BROWSER_TOOL_MANIFEST_FILE = ROOT_DIR / "app" / "config" / "browser_tool_manifest.json"


class BrowserToolValidationError(ValueError):
    """Raised when a browser tool request violates the static contract."""


class _HTMLTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._parts: list[str] = []

    def handle_data(self, data: str) -> None:
        normalized = str(data or "").strip()
        if normalized:
            self._parts.append(normalized)

    def get_text(self) -> str:
        return "\n".join(self._parts)


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_mapping(value: object, *, field_name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise BrowserToolValidationError(f"{field_name} must be a dict")
    return dict(value)


def _manifest() -> dict[str, object]:
    raw = json.loads(_BROWSER_TOOL_MANIFEST_FILE.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise BrowserToolValidationError("browser tool manifest must be a dict")
    return raw


def _allowed_pages() -> dict[str, dict[str, object]]:
    pages = _manifest().get("allowed_pages")
    if not isinstance(pages, dict):
        raise BrowserToolValidationError("browser tool manifest allowed_pages must be a dict")
    normalized: dict[str, dict[str, object]] = {}
    for url, config in pages.items():
        normalized[_normalize_text(url)] = _normalize_mapping(
            config,
            field_name=f"allowed_pages[{url}]",
        )
    return normalized


def _page_config(url: object) -> dict[str, object]:
    normalized_url = _normalize_text(url)
    config = _allowed_pages().get(normalized_url)
    if config is None:
        raise BrowserToolValidationError(f"url `{normalized_url or '(empty)'}` is not allowlisted")
    return config


def _normalize_steps(payload: dict[str, object]) -> list[dict[str, object]]:
    raw_steps = payload.get("steps")
    if raw_steps is None:
        return [
            {
                "operation": payload.get("operation"),
                "url": payload.get("url"),
                "selector": payload.get("selector"),
                "value": payload.get("value"),
            }
        ]
    if not isinstance(raw_steps, list):
        raise BrowserToolValidationError("payload.steps must be a list")
    if not raw_steps:
        raise BrowserToolValidationError("payload.steps must not be empty")
    normalized_steps: list[dict[str, object]] = []
    for index, item in enumerate(raw_steps):
        step = _normalize_mapping(item, field_name=f"payload.steps[{index}]")
        normalized_steps.append(
            {
                "operation": step.get("operation"),
                "url": step.get("url"),
                "selector": step.get("selector"),
                "value": step.get("value"),
            }
        )
    return normalized_steps


def validate_browser_tool_payload(payload: object) -> dict[str, object]:
    normalized_payload = _normalize_mapping(payload, field_name="payload")
    task_id = _normalize_text(normalized_payload.get("task_id"))
    if not task_id:
        raise BrowserToolValidationError("payload.task_id must not be empty")

    normalized_steps: list[dict[str, object]] = []
    for index, raw_step in enumerate(_normalize_steps(normalized_payload)):
        operation = _normalize_text(raw_step.get("operation")).lower()
        if operation not in SUPPORTED_BROWSER_OPERATIONS:
            raise BrowserToolValidationError(
                f"payload.steps[{index}].operation must be one of: {', '.join(SUPPORTED_BROWSER_OPERATIONS)}"
            )
        normalized_step = {"operation": operation}
        if operation == OPEN_URL_OPERATION:
            url = _normalize_text(raw_step.get("url"))
            if not url:
                raise BrowserToolValidationError(f"payload.steps[{index}].url must not be empty")
            _page_config(url)
            normalized_step["url"] = url
        elif operation == GET_PAGE_TEXT_OPERATION:
            pass
        elif operation == FILL_FORM_OPERATION:
            selector = _normalize_text(raw_step.get("selector"))
            value = _normalize_text(raw_step.get("value"))
            if not selector:
                raise BrowserToolValidationError(f"payload.steps[{index}].selector must not be empty")
            if not value:
                raise BrowserToolValidationError(f"payload.steps[{index}].value must not be empty")
            normalized_step["selector"] = selector
            normalized_step["value"] = value
        elif operation == CLICK_OPERATION:
            selector = _normalize_text(raw_step.get("selector"))
            if not selector:
                raise BrowserToolValidationError(f"payload.steps[{index}].selector must not be empty")
            normalized_step["selector"] = selector
        normalized_steps.append(normalized_step)

    validated = {
        "task_id": task_id,
        "steps": normalized_steps,
    }
    if "confirmed" in normalized_payload:
        validated["confirmed"] = bool(normalized_payload.get("confirmed"))
    return validated


def _log_browser_action(
    *,
    task_id: str,
    operation: str,
    status: str,
    details: dict[str, object],
) -> None:
    entry = {
        "operation": operation,
        "status": status,
        **dict(details),
    }
    write_json_log(
        BROWSER_TOOL_LOG_FILE,
        task_id=task_id,
        event_type="browser_tool_action",
        status=status,
        details=entry,
    )
    log_event(
        "action" if status == "completed" else "validation",
        {
            "task_id": task_id,
            "tool": BROWSER_TOOL_ACTION,
            **entry,
        },
        task_id=task_id,
        status=status,
    )


@dataclass(slots=True)
class BrowserTool:
    task_id: str
    client: httpx.Client = field(default_factory=lambda: httpx.Client(timeout=15.0, follow_redirects=False))
    current_url: str = ""
    current_html: str = ""
    form_values: dict[str, str] = field(default_factory=dict)

    def _current_page_config(self) -> dict[str, object]:
        if not self.current_url:
            raise BrowserToolValidationError("open_url must be called before interacting with the page")
        return _page_config(self.current_url)

    def _set_page(self, *, url: str, html: str, status_code: int) -> dict[str, object]:
        self.current_url = url
        self.current_html = html
        self.form_values = {}
        result = {
            "url": url,
            "status_code": int(status_code),
        }
        _log_browser_action(
            task_id=self.task_id,
            operation=OPEN_URL_OPERATION,
            status="completed",
            details=result,
        )
        return result

    def open_url(self, url: str) -> dict[str, object]:
        allowed_url = _normalize_text(url)
        _page_config(allowed_url)
        response = self.client.get(allowed_url)
        return self._set_page(
            url=allowed_url,
            html=response.text,
            status_code=response.status_code,
        )

    def get_page_text(self) -> dict[str, object]:
        if not self.current_html:
            raise BrowserToolValidationError("open_url must be called before get_page_text")
        parser = _HTMLTextExtractor()
        parser.feed(self.current_html)
        text = parser.get_text()
        result = {
            "url": self.current_url,
            "text": text,
        }
        _log_browser_action(
            task_id=self.task_id,
            operation=GET_PAGE_TEXT_OPERATION,
            status="completed",
            details={"url": self.current_url, "text_length": len(text)},
        )
        return result

    def fill_form(self, selector: str, value: str) -> dict[str, object]:
        page_config = self._current_page_config()
        allowed_selectors = {
            _normalize_text(item)
            for item in list(page_config.get("allowed_form_selectors", []) or [])
            if _normalize_text(item)
        }
        normalized_selector = _normalize_text(selector)
        normalized_value = _normalize_text(value)
        if normalized_selector not in allowed_selectors:
            raise BrowserToolValidationError(
                f"selector `{normalized_selector or '(empty)'}` is not allowed for {self.current_url}"
            )
        self.form_values[normalized_selector] = normalized_value
        result = {
            "url": self.current_url,
            "selector": normalized_selector,
            "value_length": len(normalized_value),
        }
        _log_browser_action(
            task_id=self.task_id,
            operation=FILL_FORM_OPERATION,
            status="completed",
            details=result,
        )
        return result

    def click(self, selector: str) -> dict[str, object]:
        page_config = self._current_page_config()
        click_targets = _normalize_mapping(
            page_config.get("click_targets", {}),
            field_name=f"click_targets[{self.current_url}]",
        )
        normalized_selector = _normalize_text(selector)
        target = click_targets.get(normalized_selector)
        if not isinstance(target, dict):
            raise BrowserToolValidationError(
                f"selector `{normalized_selector or '(empty)'}` is not clickable for {self.current_url}"
            )
        method = _normalize_text(target.get("method")).upper() or "GET"
        target_url = _normalize_text(target.get("target_url"))
        if method not in {"GET", "POST"}:
            raise BrowserToolValidationError(f"unsupported click method: {method or '(empty)'}")
        _page_config(target_url)
        if method == "GET":
            response = self.client.get(target_url)
        else:
            response = self.client.post(target_url, data=dict(self.form_values))
        self.current_url = target_url
        self.current_html = response.text
        result = {
            "selector": normalized_selector,
            "method": method,
            "url": target_url,
            "status_code": int(response.status_code),
        }
        _log_browser_action(
            task_id=self.task_id,
            operation=CLICK_OPERATION,
            status="completed",
            details=result,
        )
        return result

    def close(self) -> None:
        self.client.close()


def execute_browser_action(payload: object) -> dict[str, object]:
    scope = require_execution_boundary(
        component="browser_tool.execute_browser_action",
        reason="direct_browser_tool_call_blocked",
    )
    request = validate_browser_tool_payload(payload)
    if scope.task_id != request["task_id"]:
        raise BrowserToolValidationError("payload.task_id must match the active execution scope")
    policy_result = evaluate_policy(
        {
            "action_type": BROWSER_TOOL_ACTION,
            "payload": request,
        },
        {"task_id": scope.task_id, "status": "running"},
    )
    if not policy_result.execution_allowed:
        return build_action_result(
            status="policy_blocked",
            task_id=scope.task_id,
            action_type=BROWSER_TOOL_ACTION,
            result_payload={
                "result_type": "POLICY_VIOLATION",
                "policy_trace": dict(policy_result.policy_trace),
            },
            error_code="POLICY_VIOLATION",
            error_message=policy_result.reason,
            source="app.execution.browser_tool",
        )

    browser_tool = BrowserTool(task_id=request["task_id"])
    step_results: list[dict[str, object]] = []
    try:
        for step in list(request["steps"]):
            operation = str(step["operation"])
            if operation == OPEN_URL_OPERATION:
                step_results.append(browser_tool.open_url(str(step["url"])))
            elif operation == GET_PAGE_TEXT_OPERATION:
                step_results.append(browser_tool.get_page_text())
            elif operation == FILL_FORM_OPERATION:
                step_results.append(
                    browser_tool.fill_form(
                        str(step["selector"]),
                        str(step["value"]),
                    )
                )
            elif operation == CLICK_OPERATION:
                step_results.append(browser_tool.click(str(step["selector"])))
            else:
                raise BrowserToolValidationError(f"unsupported operation: {operation}")
    finally:
        browser_tool.close()

    return build_action_result(
        status="completed",
        task_id=request["task_id"],
        action_type=BROWSER_TOOL_ACTION,
        result_payload={
            "steps": step_results,
            "final_url": browser_tool.current_url,
        },
        error_code="",
        error_message="",
        source="app.execution.browser_tool",
        diagnostic_message=f"browser tool completed {len(step_results)} step(s)",
    )
