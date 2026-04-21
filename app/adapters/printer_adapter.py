from __future__ import annotations

import os
import subprocess
import time
from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha1
from pathlib import Path
from typing import Protocol

from app.execution.action_contract import (
    ActionContractViolation,
    build_action_result_contract,
    validate_action_contract,
)
from app.execution.paths import OUTPUT_DIR, ROOT_DIR


SUPPORTED_ACTION_TYPE = "PRINT_DOCUMENT"
SUPPORTED_TARGET_REFS = frozenset({"printer", "printer_adapter"})
SUPPORTED_OPERATIONS = frozenset({"print_document"})
SUPPORTED_PARAMETER_FIELDS = frozenset(
    {"copies", "document_text", "document_title", "operation", "printer_name"}
)
DEFAULT_TIMEOUT_SECONDS = 30
MAX_TIMEOUT_SECONDS = 300
MAX_DOCUMENT_TITLE_LENGTH = 120
MAX_DOCUMENT_TEXT_LENGTH = 512
MAX_PRINTER_NAME_LENGTH = 120
PRINT_ARTIFACT_DIR = OUTPUT_DIR / "artifacts" / "printer_jobs"
PRINTER_ENABLED_ENV_VAR = "DF_PRINTER_ENABLED"
PRINTER_NAME_ENV_VAR = "DF_PRINTER_NAME"
PRINTER_TIMEOUT_ENV_VAR = "DF_PRINTER_TIMEOUT_SECONDS"


@dataclass(frozen=True, slots=True)
class PrinterAdapterConfig:
    enabled: bool | None = None
    printer_name: str | None = None
    timeout_seconds: int | None = None


@dataclass(frozen=True, slots=True)
class PrinterExecution:
    result_type: str
    summary: str
    references: dict[str, object]
    printer_name: str
    job_status: str
    pages: int | None


class PrinterAdapterError(RuntimeError):
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


class PrinterRuntime(Protocol):
    def print_document(
        self,
        *,
        action_id: str,
        printer_name: str,
        document_title: str,
        artifact_path: Path,
        timeout_seconds: int,
    ) -> PrinterExecution: ...


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_mapping(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _bounded_text(value: object, *, field_name: str, max_length: int) -> str:
    normalized = _normalize_text(value)
    if not normalized:
        raise ActionContractViolation(f"{field_name} must not be empty")
    if len(normalized) > max_length:
        raise ActionContractViolation(f"{field_name} exceeds max length")
    if any(character in normalized for character in ("\x00", "\r")):
        raise ActionContractViolation(f"{field_name} contains unsupported characters")
    return normalized


def _normalize_optional_printer_name(value: object) -> str | None:
    normalized = _normalize_text(value)
    if not normalized:
        return None
    return _bounded_text(
        normalized,
        field_name="parameters.printer_name",
        max_length=MAX_PRINTER_NAME_LENGTH,
    )


def _validate_copies(value: object) -> int:
    if value in (None, ""):
        return 1
    if isinstance(value, bool) or not isinstance(value, int):
        raise ActionContractViolation("parameters.copies must be an integer")
    if value != 1:
        raise ActionContractViolation(
            "parameters.copies must equal 1 for printer adapter v1"
        )
    return value


def validate_printer_action_parameters(
    parameters: Mapping[str, object],
) -> dict[str, object]:
    normalized = dict(parameters)
    unexpected_fields = sorted(set(normalized) - SUPPORTED_PARAMETER_FIELDS)
    if unexpected_fields:
        raise ActionContractViolation(
            "parameters contains unsupported fields: " + ", ".join(unexpected_fields)
        )

    operation = _bounded_text(
        normalized.get("operation"),
        field_name="parameters.operation",
        max_length=32,
    ).lower()
    if operation not in SUPPORTED_OPERATIONS:
        raise ActionContractViolation(f"unsupported printer operation: {operation}")

    return {
        "operation": operation,
        "document_title": _bounded_text(
            normalized.get("document_title"),
            field_name="parameters.document_title",
            max_length=MAX_DOCUMENT_TITLE_LENGTH,
        ),
        "document_text": _bounded_text(
            normalized.get("document_text"),
            field_name="parameters.document_text",
            max_length=MAX_DOCUMENT_TEXT_LENGTH,
        ),
        "copies": _validate_copies(normalized.get("copies")),
        "printer_name": _normalize_optional_printer_name(
            normalized.get("printer_name")
        ),
    }


class PowerShellPrinterRuntime:
    backend_name = "powershell_out_printer"

    def _windows_path(self, file_path: Path) -> str:
        try:
            completed = subprocess.run(
                ["wslpath", "-w", str(file_path)],
                capture_output=True,
                text=True,
                check=True,
            )
        except (FileNotFoundError, subprocess.CalledProcessError) as exc:
            raise PrinterAdapterError(
                "provider_not_configured",
                "printer backend path conversion is unavailable",
                diagnostic={"exception_type": type(exc).__name__},
            ) from exc
        return _normalize_text(completed.stdout)

    def print_document(
        self,
        *,
        action_id: str,
        printer_name: str,
        document_title: str,
        artifact_path: Path,
        timeout_seconds: int,
    ) -> PrinterExecution:
        windows_path = self._windows_path(artifact_path)
        escaped_printer = printer_name.replace("'", "''")
        escaped_path = windows_path.replace("'", "''")
        script = (
            "$ErrorActionPreference='Stop';"
            f"Get-Printer -Name '{escaped_printer}' -ErrorAction Stop | Out-Null;"
            f"Get-Content -LiteralPath '{escaped_path}' -Raw -ErrorAction Stop | "
            f"Out-Printer -Name '{escaped_printer}' -ErrorAction Stop"
        )
        try:
            completed = subprocess.run(
                [
                    "powershell.exe",
                    "-NoProfile",
                    "-NonInteractive",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-Command",
                    script,
                ],
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                check=False,
            )
        except FileNotFoundError as exc:
            raise PrinterAdapterError(
                "provider_not_configured",
                "powershell printer backend is not available",
                diagnostic={"exception_type": type(exc).__name__},
            ) from exc
        except subprocess.TimeoutExpired as exc:
            raise PrinterAdapterError(
                "timeout",
                "printer action timed out",
                diagnostic={
                    "timeout_seconds": timeout_seconds,
                    "reason": _normalize_text(exc),
                },
            ) from exc
        if completed.returncode != 0:
            stderr = _normalize_text(completed.stderr or completed.stdout)
            error_code = (
                "device_not_available"
                if any(
                    token in stderr.lower()
                    for token in (
                        "cannot find any printers",
                        "cannot find a printer",
                        "cannot find the printer",
                        "no msft_printer objects found",
                    )
                )
                else "provider_error"
            )
            message = (
                "printer is not available"
                if error_code == "device_not_available"
                else "printer backend failed"
            )
            raise PrinterAdapterError(
                error_code,
                message,
                diagnostic={"stderr": stderr[:240], "returncode": completed.returncode},
            )
        job_id = sha1(
            f"{action_id}|{printer_name}|{document_title}".encode("utf-8")
        ).hexdigest()[:12]
        return PrinterExecution(
            result_type="print_document",
            summary=f"Printed '{document_title}' on {printer_name}",
            references={
                "artifact_path": str(artifact_path),
                "job_id": f"print-job-{job_id}",
            },
            printer_name=printer_name,
            job_status="submitted",
            pages=1,
        )


def _validate_printer_action_contract(action_contract: object) -> dict[str, object]:
    validated = validate_action_contract(action_contract)
    if validated["action_type"] != SUPPORTED_ACTION_TYPE:
        raise ActionContractViolation(
            f"unsupported action_type for printer adapter: {validated['action_type']}"
        )
    target_ref = _normalize_text(validated.get("target_ref")).lower()
    if target_ref not in SUPPORTED_TARGET_REFS:
        raise ActionContractViolation(
            "target_ref must be one of: " + ", ".join(sorted(SUPPORTED_TARGET_REFS))
        )
    return {
        **validated,
        "parameters": validate_printer_action_parameters(
            _normalize_mapping(validated.get("parameters"))
        ),
    }


def _env_enabled() -> bool | None:
    raw = os.getenv(PRINTER_ENABLED_ENV_VAR)
    if raw is None:
        return None
    return _normalize_text(raw).lower() in {"1", "true", "yes", "on"}


def _env_timeout_seconds() -> int | None:
    raw = os.getenv(PRINTER_TIMEOUT_ENV_VAR)
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError as exc:
        raise PrinterAdapterError(
            "validation_error",
            f"{PRINTER_TIMEOUT_ENV_VAR} must be an integer",
        ) from exc


def _resolve_timeout_seconds(value: object | None) -> int:
    candidate = DEFAULT_TIMEOUT_SECONDS if value in (None, "") else value
    if isinstance(candidate, bool) or not isinstance(candidate, int):
        raise PrinterAdapterError(
            "validation_error", "printer timeout_seconds must be an integer"
        )
    if candidate <= 0 or candidate > MAX_TIMEOUT_SECONDS:
        raise PrinterAdapterError(
            "validation_error",
            f"printer timeout_seconds must be between 1 and {MAX_TIMEOUT_SECONDS}",
        )
    return candidate


def _resolve_printer_adapter_config(
    config: PrinterAdapterConfig | Mapping[str, object] | None,
) -> PrinterAdapterConfig:
    if config is None:
        return PrinterAdapterConfig()
    if isinstance(config, PrinterAdapterConfig):
        return config
    if not isinstance(config, Mapping):
        raise PrinterAdapterError(
            "validation_error", "printer adapter config must be a mapping"
        )
    payload = dict(config)
    unexpected_fields = sorted(
        set(payload) - {"enabled", "printer_name", "timeout_seconds"}
    )
    if unexpected_fields:
        raise PrinterAdapterError(
            "validation_error",
            "printer adapter config contains unsupported fields: "
            + ", ".join(unexpected_fields),
        )
    enabled = payload.get("enabled")
    if enabled is not None and not isinstance(enabled, bool):
        raise PrinterAdapterError(
            "validation_error", "printer adapter enabled must be a boolean"
        )
    printer_name = payload.get("printer_name")
    if printer_name is not None and not isinstance(printer_name, str):
        raise PrinterAdapterError(
            "validation_error", "printer adapter printer_name must be a string"
        )
    timeout_seconds = payload.get("timeout_seconds")
    if timeout_seconds is not None and (
        isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, int)
    ):
        raise PrinterAdapterError(
            "validation_error", "printer adapter timeout_seconds must be an integer"
        )
    return PrinterAdapterConfig(
        enabled=enabled,
        printer_name=_normalize_text(printer_name) or None,
        timeout_seconds=timeout_seconds,
    )


def _effective_printer_settings(
    *,
    config: PrinterAdapterConfig,
    parameters: Mapping[str, object],
) -> tuple[str, int]:
    enabled = config.enabled if config.enabled is not None else _env_enabled()
    if enabled is not True:
        raise PrinterAdapterError(
            "provider_not_configured",
            "printer adapter is not enabled",
        )
    printer_name = _normalize_text(parameters.get("printer_name")) or _normalize_text(
        config.printer_name
    )
    if not printer_name:
        printer_name = _normalize_text(os.getenv(PRINTER_NAME_ENV_VAR))
    if not printer_name:
        raise PrinterAdapterError(
            "provider_not_configured",
            "printer name is not configured",
        )
    timeout_seconds = _resolve_timeout_seconds(
        config.timeout_seconds
        if config.timeout_seconds is not None
        else _env_timeout_seconds()
    )
    return printer_name, timeout_seconds


def _runtime_backend_name(runtime: PrinterRuntime | None) -> str:
    candidate = _normalize_text(getattr(runtime, "backend_name", ""))
    return candidate or "custom"


def _simulation_payload(
    *,
    action_id: str,
    parameters: Mapping[str, object],
    printer_name: str,
    timeout_seconds: int,
) -> dict[str, object]:
    return build_action_result_contract(
        action_id=action_id,
        status="success",
        result_type="simulation",
        payload={
            "note": "dry run: print_document",
            "summary": f"Simulated print for '{parameters['document_title']}'",
            "references": {
                "document_title": parameters.get("document_title"),
                "printer_name": printer_name,
            },
            "metadata": {
                "dry_run": True,
                "simulation_mode": "dry_run",
                "operation": "print_document",
                "backend_used": "dry_run",
                "provider": "simulation",
                "printer_name": printer_name,
                "timeout_seconds": timeout_seconds,
                "job_status": "simulated",
                "pages": 1,
            },
        },
    )


def _failure_result(
    *,
    action_id: str,
    error_code: str,
    error_message: str,
    backend_used: str,
    printer_name: str | None = None,
    diagnostic: Mapping[str, object] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "metadata": {
            "operation": "print_document",
            "provider": backend_used,
            "backend_used": backend_used,
            "printer_name": printer_name,
        }
    }
    normalized_diagnostic = _normalize_mapping(diagnostic)
    if normalized_diagnostic:
        payload["diagnostic"] = normalized_diagnostic
    return build_action_result_contract(
        action_id=action_id,
        status="failed",
        result_type="print_document",
        payload=payload,
        error_code=error_code,
        error_message=error_message,
    )


def _printable_artifact_path(action_id: str) -> Path:
    safe_action_id = "".join(
        character if character.isalnum() or character in "._-" else "_"
        for character in action_id
    )
    return ROOT_DIR / PRINT_ARTIFACT_DIR / f"{safe_action_id}.txt"


def _write_printable_artifact(
    *,
    action_id: str,
    document_title: str,
    document_text: str,
) -> Path:
    artifact_path = _printable_artifact_path(action_id)
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    rendered_document = (
        f"{document_title}\n{'=' * len(document_title)}\n\n{document_text}\n"
    )
    artifact_path.write_text(rendered_document, encoding="utf-8")
    return artifact_path


def execute_printer_action(
    action_contract: object,
    *,
    runtime: PrinterRuntime | None = None,
    config: PrinterAdapterConfig | Mapping[str, object] | None = None,
) -> dict[str, object]:
    started_at = time.monotonic()
    action_id = (
        _normalize_text(_normalize_mapping(action_contract).get("action_id"))
        or "unknown_action"
    )
    backend_used = _runtime_backend_name(runtime)
    printer_name: str | None = None
    try:
        validated = _validate_printer_action_contract(action_contract)
        action_id = str(validated["action_id"])
        parameters = dict(validated["parameters"])
        effective_config = _resolve_printer_adapter_config(config)
        printer_name, timeout_seconds = _effective_printer_settings(
            config=effective_config,
            parameters=parameters,
        )
        if validated["execution_mode"] == "dry_run":
            return _simulation_payload(
                action_id=action_id,
                parameters=parameters,
                printer_name=printer_name,
                timeout_seconds=timeout_seconds,
            )
        if runtime is None:
            runtime = PowerShellPrinterRuntime()
        backend_used = _runtime_backend_name(runtime)
        artifact_path = _write_printable_artifact(
            action_id=action_id,
            document_title=str(parameters["document_title"]),
            document_text=str(parameters["document_text"]),
        )
        execution = runtime.print_document(
            action_id=action_id,
            printer_name=printer_name,
            document_title=str(parameters["document_title"]),
            artifact_path=artifact_path,
            timeout_seconds=timeout_seconds,
        )
    except ActionContractViolation as exc:
        return _failure_result(
            action_id=action_id,
            error_code="validation_error",
            error_message=str(exc),
            backend_used=backend_used,
            printer_name=printer_name,
        )
    except PrinterAdapterError as exc:
        return _failure_result(
            action_id=action_id,
            error_code=exc.error_code,
            error_message=exc.message,
            backend_used=backend_used,
            printer_name=printer_name,
            diagnostic=exc.diagnostic,
        )
    except Exception as exc:
        return _failure_result(
            action_id=action_id,
            error_code="unknown_error",
            error_message="printer action failed",
            backend_used=backend_used,
            printer_name=printer_name,
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
                "operation": "print_document",
                "provider": backend_used,
                "backend_used": backend_used,
                "printer_name": execution.printer_name,
                "timeout_seconds": timeout_seconds,
                "job_status": execution.job_status,
                "pages": execution.pages,
                "latency_ms": latency_ms,
            },
        },
    )
