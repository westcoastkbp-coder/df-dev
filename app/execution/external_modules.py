from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Protocol, TypeAlias, TypedDict, cast

from app.execution.execution_boundary import require_execution_boundary


ExternalScalar: TypeAlias = str | int | float | bool | None
ExternalValue: TypeAlias = ExternalScalar | tuple["ExternalValue", ...] | Mapping[str, "ExternalValue"]

ALLOWED_EXTERNAL_MODULE_STATUSES = {
    "success",
    "failed",
    "timeout",
    "invalid_result",
    "unavailable",
}
REQUEST_FIELDS = {
    "request_id",
    "task_id",
    "task_type",
    "module_type",
    "operation",
    "payload",
    "correlation_id",
    "timeout_ms",
    "metadata",
}
REQUEST_REQUIRED_FIELDS = REQUEST_FIELDS
RESULT_FIELDS = {
    "request_id",
    "status",
    "module_type",
    "operation",
    "result_payload",
    "error_code",
    "error_message",
    "raw_reference",
    "duration_ms",
}
RESULT_REQUIRED_FIELDS = {
    "request_id",
    "status",
    "module_type",
    "operation",
    "result_payload",
    "duration_ms",
}
REQUEST_METADATA_FIELDS = {
    "schema_version",
    "request_source",
    "capability",
    "priority",
    "tags",
}
RAW_REFERENCE_FIELDS = {
    "reference_type",
    "reference_id",
    "preview",
}
SIGNAL_ERROR_CODES = {
    "timeout": "external_module_timeout",
    "unavailable": "external_module_unavailable",
    "invalid_result": "external_module_invalid_result",
    "failed": "external_module_failed",
}


class ExternalModuleValidationError(ValueError):
    """Raised when the external module request or result contract is invalid."""


class ExternalModuleUnavailableError(RuntimeError):
    """Raised when a requested external module adapter is not available."""


class ExternalModuleMetadata(TypedDict, total=False):
    schema_version: str
    request_source: str
    capability: str
    priority: str
    tags: list[str]


class ExternalRawReference(TypedDict, total=False):
    reference_type: str
    reference_id: str
    preview: str


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_mapping(value: object, *, field_name: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise ExternalModuleValidationError(f"{field_name} must be a dict")
    return dict(value)


def _deep_clone_json_like(value: object, *, field_name: str) -> ExternalValue:
    if value is None or isinstance(value, (str, int, float, bool)):
        return cast(ExternalValue, value)
    if isinstance(value, Mapping):
        cloned: dict[str, ExternalValue] = {}
        for key, item in value.items():
            normalized_key = _normalize_text(key)
            if not normalized_key:
                raise ExternalModuleValidationError(f"{field_name} contains an empty key")
            cloned[normalized_key] = _deep_clone_json_like(
                item,
                field_name=f"{field_name}.{normalized_key}",
            )
        return MappingProxyType(cloned)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(
            _deep_clone_json_like(item, field_name=f"{field_name}[]")
            for item in value
        )
    raise ExternalModuleValidationError(
        f"{field_name} must contain only structured JSON-like values"
    )


def _deep_unfreeze(value: ExternalValue) -> object:
    if isinstance(value, Mapping):
        return {key: _deep_unfreeze(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_deep_unfreeze(item) for item in value]
    return value


def _validate_identifier(value: object, *, field_name: str) -> str:
    normalized = _normalize_text(value)
    if not normalized:
        raise ExternalModuleValidationError(f"{field_name} must not be empty")
    return normalized


def _validate_timeout_ms(value: object) -> int:
    if not isinstance(value, int):
        raise ExternalModuleValidationError("timeout_ms must be an integer")
    if value <= 0:
        raise ExternalModuleValidationError("timeout_ms must be greater than 0")
    if value > 300_000:
        raise ExternalModuleValidationError("timeout_ms must be less than or equal to 300000")
    return value


def _validate_duration_ms(value: object) -> int:
    if not isinstance(value, int):
        raise ExternalModuleValidationError("duration_ms must be an integer")
    if value < 0:
        raise ExternalModuleValidationError("duration_ms must be greater than or equal to 0")
    return value


def _validate_metadata(value: object) -> ExternalModuleMetadata:
    normalized = _normalize_mapping(value, field_name="metadata")
    unexpected_fields = sorted(set(normalized) - REQUEST_METADATA_FIELDS)
    if unexpected_fields:
        raise ExternalModuleValidationError(
            "metadata contains unsupported fields: " + ", ".join(unexpected_fields)
        )

    metadata: ExternalModuleMetadata = {}
    for field_name in ("schema_version", "request_source", "capability", "priority"):
        if field_name in normalized:
            metadata[field_name] = _validate_identifier(
                normalized.get(field_name),
                field_name=f"metadata.{field_name}",
            )
    if "tags" in normalized:
        tags_value = normalized.get("tags")
        if not isinstance(tags_value, Sequence) or isinstance(tags_value, (str, bytes, bytearray)):
            raise ExternalModuleValidationError("metadata.tags must be a list")
        tags = [_validate_identifier(tag, field_name="metadata.tags[]") for tag in tags_value]
        metadata["tags"] = tags
    return metadata


def _validate_raw_reference(value: object) -> ExternalRawReference:
    normalized = _normalize_mapping(value, field_name="raw_reference")
    unexpected_fields = sorted(set(normalized) - RAW_REFERENCE_FIELDS)
    if unexpected_fields:
        raise ExternalModuleValidationError(
            "raw_reference contains unsupported fields: " + ", ".join(unexpected_fields)
        )
    raw_reference: ExternalRawReference = {}
    for field_name in ("reference_type", "reference_id"):
        if field_name in normalized:
            raw_reference[field_name] = _validate_identifier(
                normalized.get(field_name),
                field_name=f"raw_reference.{field_name}",
            )
    if "preview" in normalized:
        preview = _validate_identifier(normalized.get("preview"), field_name="raw_reference.preview")
        if len(preview) > 256:
            raise ExternalModuleValidationError("raw_reference.preview must be 256 characters or fewer")
        raw_reference["preview"] = preview
    return raw_reference


@dataclass(frozen=True, slots=True)
class ExternalModuleRequest:
    request_id: str
    task_id: str
    task_type: str
    module_type: str
    operation: str
    payload: Mapping[str, ExternalValue]
    correlation_id: str
    timeout_ms: int
    metadata: ExternalModuleMetadata

    def to_dict(self) -> dict[str, object]:
        return {
            "request_id": self.request_id,
            "task_id": self.task_id,
            "task_type": self.task_type,
            "module_type": self.module_type,
            "operation": self.operation,
            "payload": _deep_unfreeze(cast(ExternalValue, self.payload)),
            "correlation_id": self.correlation_id,
            "timeout_ms": self.timeout_ms,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class ExternalModuleResult:
    request_id: str
    status: str
    module_type: str
    operation: str
    result_payload: Mapping[str, ExternalValue]
    error_code: str = ""
    error_message: str = ""
    raw_reference: ExternalRawReference | None = None
    duration_ms: int = 0

    def to_dict(self) -> dict[str, object]:
        return {
            "request_id": self.request_id,
            "status": self.status,
            "module_type": self.module_type,
            "operation": self.operation,
            "result_payload": _deep_unfreeze(cast(ExternalValue, self.result_payload)),
            "error_code": self.error_code,
            "error_message": self.error_message,
            "raw_reference": dict(self.raw_reference) if self.raw_reference else None,
            "duration_ms": self.duration_ms,
        }


class ExternalModuleAdapter(Protocol):
    module_type: str

    def execute(self, request: ExternalModuleRequest) -> Mapping[str, object] | ExternalModuleResult:
        ...

    def validate_request_payload(self, operation: str, payload: Mapping[str, object]) -> None:
        ...

    def validate_result_payload(self, operation: str, payload: Mapping[str, object]) -> None:
        ...


class ExternalModuleRegistry:
    def __init__(self) -> None:
        self._adapters: dict[str, ExternalModuleAdapter] = {}

    def register(self, adapter: ExternalModuleAdapter) -> None:
        module_type = _validate_identifier(
            getattr(adapter, "module_type", ""),
            field_name="adapter.module_type",
        )
        self._adapters[module_type] = adapter

    def resolve(self, module_type: str) -> ExternalModuleAdapter:
        adapter = self._adapters.get(module_type)
        if adapter is None:
            raise ExternalModuleUnavailableError(
                f"external module adapter unavailable: {module_type}"
            )
        return adapter


def build_external_module_request(payload: object) -> ExternalModuleRequest:
    normalized = _normalize_mapping(payload, field_name="external_module_request")
    missing_fields = sorted(REQUEST_REQUIRED_FIELDS - set(normalized))
    if missing_fields:
        raise ExternalModuleValidationError(
            "external_module_request missing required fields: " + ", ".join(missing_fields)
        )
    unexpected_fields = sorted(set(normalized) - REQUEST_FIELDS)
    if unexpected_fields:
        raise ExternalModuleValidationError(
            "external_module_request contains unsupported fields: "
            + ", ".join(unexpected_fields)
        )

    request_payload = _normalize_mapping(normalized.get("payload"), field_name="payload")
    metadata_payload = normalized.get("metadata", {})
    if metadata_payload is None:
        metadata_payload = {}
    if not isinstance(metadata_payload, Mapping):
        raise ExternalModuleValidationError("metadata must be a dict")

    return ExternalModuleRequest(
        request_id=_validate_identifier(normalized.get("request_id"), field_name="request_id"),
        task_id=_validate_identifier(normalized.get("task_id"), field_name="task_id"),
        task_type=_validate_identifier(normalized.get("task_type"), field_name="task_type"),
        module_type=_validate_identifier(normalized.get("module_type"), field_name="module_type"),
        operation=_validate_identifier(normalized.get("operation"), field_name="operation"),
        payload=cast(
            Mapping[str, ExternalValue],
            _deep_clone_json_like(request_payload, field_name="payload"),
        ),
        correlation_id=_validate_identifier(
            normalized.get("correlation_id"),
            field_name="correlation_id",
        ),
        timeout_ms=_validate_timeout_ms(normalized.get("timeout_ms")),
        metadata=_validate_metadata(metadata_payload),
    )


def build_external_module_result(payload: object) -> ExternalModuleResult:
    normalized = _normalize_mapping(payload, field_name="external_module_result")
    missing_fields = sorted(RESULT_REQUIRED_FIELDS - set(normalized))
    if missing_fields:
        raise ExternalModuleValidationError(
            "external_module_result missing required fields: " + ", ".join(missing_fields)
        )
    unexpected_fields = sorted(set(normalized) - RESULT_FIELDS)
    if unexpected_fields:
        raise ExternalModuleValidationError(
            "external_module_result contains unsupported fields: "
            + ", ".join(unexpected_fields)
        )

    status = _validate_identifier(normalized.get("status"), field_name="status").lower()
    if status not in ALLOWED_EXTERNAL_MODULE_STATUSES:
        raise ExternalModuleValidationError(f"unsupported external module status: {status}")

    raw_reference_value = normalized.get("raw_reference")
    raw_reference: ExternalRawReference | None = None
    if raw_reference_value is not None:
        raw_reference = _validate_raw_reference(raw_reference_value)

    error_code = _normalize_text(normalized.get("error_code"))
    error_message = _normalize_text(normalized.get("error_message"))
    if status == "success" and (error_code or error_message):
        raise ExternalModuleValidationError(
            "success result must not include error_code or error_message"
        )

    result_payload = _normalize_mapping(normalized.get("result_payload"), field_name="result_payload")

    return ExternalModuleResult(
        request_id=_validate_identifier(normalized.get("request_id"), field_name="request_id"),
        status=status,
        module_type=_validate_identifier(normalized.get("module_type"), field_name="module_type"),
        operation=_validate_identifier(normalized.get("operation"), field_name="operation"),
        result_payload=cast(
            Mapping[str, ExternalValue],
            _deep_clone_json_like(result_payload, field_name="result_payload"),
        ),
        error_code=error_code,
        error_message=error_message,
        raw_reference=raw_reference,
        duration_ms=_validate_duration_ms(normalized.get("duration_ms")),
    )


def _result_signal(
    request: ExternalModuleRequest,
    *,
    status: str,
    error_code: str,
    error_message: str,
    duration_ms: int = 0,
) -> ExternalModuleResult:
    return ExternalModuleResult(
        request_id=request.request_id,
        status=status,
        module_type=request.module_type,
        operation=request.operation,
        result_payload=MappingProxyType({}),
        error_code=error_code,
        error_message=error_message,
        duration_ms=duration_ms,
    )


def validate_external_module_result_for_df(
    result: object,
    *,
    request: ExternalModuleRequest,
    registry: ExternalModuleRegistry,
) -> ExternalModuleResult:
    try:
        validated = build_external_module_result(result)
    except ExternalModuleValidationError as exc:
        return _result_signal(
            request,
            status="invalid_result",
            error_code=SIGNAL_ERROR_CODES["invalid_result"],
            error_message=str(exc),
        )

    if validated.request_id != request.request_id:
        return _result_signal(
            request,
            status="invalid_result",
            error_code=SIGNAL_ERROR_CODES["invalid_result"],
            error_message="result request_id does not match request",
            duration_ms=validated.duration_ms,
        )
    if validated.module_type != request.module_type:
        return _result_signal(
            request,
            status="invalid_result",
            error_code=SIGNAL_ERROR_CODES["invalid_result"],
            error_message="result module_type does not match request",
            duration_ms=validated.duration_ms,
        )
    if validated.operation != request.operation:
        return _result_signal(
            request,
            status="invalid_result",
            error_code=SIGNAL_ERROR_CODES["invalid_result"],
            error_message="result operation does not match request",
            duration_ms=validated.duration_ms,
        )

    adapter = registry.resolve(request.module_type)
    if validated.status == "success":
        try:
            adapter.validate_result_payload(
                request.operation,
                cast(dict[str, object], _deep_unfreeze(cast(ExternalValue, validated.result_payload))),
            )
        except ExternalModuleValidationError as exc:
            return _result_signal(
                request,
                status="invalid_result",
                error_code=SIGNAL_ERROR_CODES["invalid_result"],
                error_message=str(exc),
                duration_ms=validated.duration_ms,
            )
    return validated


def execute_external_module(
    request: ExternalModuleRequest,
    *,
    registry: ExternalModuleRegistry,
) -> ExternalModuleResult:
    require_execution_boundary(
        component="external_modules.execute_external_module",
        task_id=request.task_id,
        reason="direct_external_module_call_blocked",
    )
    try:
        adapter = registry.resolve(request.module_type)
    except ExternalModuleUnavailableError as exc:
        return _result_signal(
            request,
            status="unavailable",
            error_code=SIGNAL_ERROR_CODES["unavailable"],
            error_message=str(exc),
        )

    try:
        adapter.validate_request_payload(
            request.operation,
            cast(dict[str, object], _deep_unfreeze(cast(ExternalValue, request.payload))),
        )
    except ExternalModuleValidationError as exc:
        raise ExternalModuleValidationError(f"external module request payload invalid: {exc}") from exc

    try:
        raw_result = adapter.execute(request)
    except TimeoutError as exc:
        return _result_signal(
            request,
            status="timeout",
            error_code=SIGNAL_ERROR_CODES["timeout"],
            error_message=str(exc).strip() or "external module timed out",
        )
    except ExternalModuleUnavailableError as exc:
        return _result_signal(
            request,
            status="unavailable",
            error_code=SIGNAL_ERROR_CODES["unavailable"],
            error_message=str(exc),
        )
    except Exception as exc:
        return _result_signal(
            request,
            status="failed",
            error_code=SIGNAL_ERROR_CODES["failed"],
            error_message=str(exc).strip() or "external module execution failed",
        )

    return validate_external_module_result_for_df(
        raw_result,
        request=request,
        registry=registry,
    )
