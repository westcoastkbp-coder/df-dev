from __future__ import annotations

from app.product.command_parser import CommandParserError, parse_command
from app.product.intake import MAX_CHAIN_LENGTH, build_product_task_request
from app.product.runner import execute_product_task_request
from app.product.task_registry import (
    ALLOWED_ACTIONS,
    TASK_CHAINS,
    TASK_REGISTRY,
    TaskRegistryError,
    get_chain,
    get_descriptor,
    get_descriptor_action,
    validate_task_id,
)


def owner_command(*args, **kwargs):
    from app.product.owner_command import owner_command as _owner_command

    return _owner_command(*args, **kwargs)


def __getattr__(name: str):
    if name == "OwnerCommandError":
        from app.product.owner_command import OwnerCommandError

        return OwnerCommandError
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "ALLOWED_ACTIONS",
    "CommandParserError",
    "MAX_CHAIN_LENGTH",
    "OwnerCommandError",
    "TASK_CHAINS",
    "TASK_REGISTRY",
    "TaskRegistryError",
    "build_product_task_request",
    "execute_product_task_request",
    "owner_command",
    "get_chain",
    "get_descriptor",
    "get_descriptor_action",
    "parse_command",
    "validate_task_id",
]
