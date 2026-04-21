from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from app.execution.product_box_manifest import load_product_box_manifest
from runtime.system_log import log_event


class ProductRuntimeBoundaryError(ValueError):
    pass


_PRODUCT_BOX_MANIFEST = load_product_box_manifest()

PRODUCT_RUNTIME_ALLOWLIST = tuple(_PRODUCT_BOX_MANIFEST["product_runtime_allowlist"])
PRODUCT_RUNTIME_BLOCKLIST = tuple(
    str(entry).strip().replace("\\", "/").removesuffix("/*").replace("/", ".")
    for entry in _PRODUCT_BOX_MANIFEST["blocked_modules"]
)
PRODUCT_RUNTIME_ALLOWED_ACTIONS = set(_PRODUCT_BOX_MANIFEST["allowed_actions"])
PRODUCT_RUNTIME_BLOCKED_ACTIONS = set(_PRODUCT_BOX_MANIFEST["blocked_actions"])


def _matches_prefix(module_name: str, prefix: str) -> bool:
    return module_name == prefix or module_name.startswith(prefix + ".")


def _boundary_error(*, context: str, detail: str) -> ProductRuntimeBoundaryError:
    message = f"product runtime boundary violation: {context}: {detail}"
    log_event("validation", message)
    return ProductRuntimeBoundaryError(message)


def assert_product_runtime_module(module_name: object, *, context: str) -> str:
    normalized_module_name = str(module_name or "").strip()
    if not normalized_module_name:
        raise _boundary_error(context=context, detail="module name is required")
    if normalized_module_name.startswith("test_"):
        return normalized_module_name
    for blocked in PRODUCT_RUNTIME_BLOCKLIST:
        if _matches_prefix(normalized_module_name, blocked):
            raise _boundary_error(
                context=context,
                detail=f"blocked module `{normalized_module_name}`",
            )
    for allowed in PRODUCT_RUNTIME_ALLOWLIST:
        if _matches_prefix(normalized_module_name, allowed):
            return normalized_module_name
    raise _boundary_error(
        context=context,
        detail=f"module `{normalized_module_name}` is outside allowlist",
    )


def assert_product_runtime_executor(
    executor: Callable[..., object],
    *,
    context: str,
) -> str:
    executor_module = str(getattr(executor, "__module__", "") or "").strip()
    return assert_product_runtime_module(executor_module, context=context)


def assert_product_runtime_action(
    *,
    descriptor_action: object,
    descriptor_path: object = "",
    request_source: object = "",
    context: str,
) -> str:
    normalized_action = str(descriptor_action or "").strip().upper()
    normalized_descriptor_path = str(descriptor_path or "").strip()
    normalized_request_source = str(request_source or "").strip()
    if not normalized_action:
        raise _boundary_error(context=context, detail="descriptor action is required")
    if normalized_action in PRODUCT_RUNTIME_BLOCKED_ACTIONS:
        raise _boundary_error(
            context=context,
            detail=f"blocked action `{normalized_action}` from `{normalized_request_source or 'unknown'}`",
        )
    if normalized_action not in PRODUCT_RUNTIME_ALLOWED_ACTIONS:
        raise _boundary_error(
            context=context,
            detail=f"action `{normalized_action}` is outside allowlist",
        )
    normalized_path = normalized_descriptor_path.replace("\\", "/").lower()
    if normalized_path.startswith("scripts/") or normalized_path.startswith("tests/"):
        raise _boundary_error(
            context=context,
            detail=f"blocked descriptor path `{normalized_descriptor_path}`",
        )
    return normalized_action


def assert_product_runtime_output_path(path: object, *, context: str) -> str:
    normalized_path = str(path or "").strip()
    if not normalized_path:
        return normalized_path
    normalized = Path(normalized_path.replace("\\", "/")).as_posix().lower()
    if normalized.startswith("scripts/") or normalized.startswith("tests/"):
        raise _boundary_error(
            context=context,
            detail=f"blocked output path `{normalized_path}`",
        )
    return normalized_path
