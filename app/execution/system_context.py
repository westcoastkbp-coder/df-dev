from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from pydantic import ValidationError

from app.execution.context_types import ContextSnapshot, OperationalSettings

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SYSTEM_CONTEXT_PATH = ROOT / "config" / "system_context.yaml"
IMMUTABLE_TOP_LEVEL_FIELDS = ("system", "product_box", "interaction")


class ContextLoadError(RuntimeError):
    pass


class ContextValidationError(ValueError):
    pass


class ContextUpdateError(RuntimeError):
    pass


def _parse_scalar(raw_value: str) -> object:
    value = str(raw_value or "").strip()
    if not value:
        return ""
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if value.isdigit():
        return int(value)
    if value.startswith(("'", '"')) and value.endswith(("'", '"')):
        return value[1:-1]
    return value


def _significant_lines(text: str) -> list[tuple[int, str]]:
    lines: list[tuple[int, str]] = []
    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        lines.append((indent, raw_line.lstrip(" ")))
    return lines


def _parse_yaml_block(
    lines: list[tuple[int, str]],
    start_index: int,
    indent: int,
) -> tuple[object, int]:
    if start_index >= len(lines):
        return {}, start_index

    current_indent, current_content = lines[start_index]
    if current_indent != indent:
        raise ContextValidationError("invalid yaml indentation structure")

    if current_content.startswith("- "):
        items: list[object] = []
        index = start_index
        while index < len(lines):
            line_indent, content = lines[index]
            if line_indent != indent or not content.startswith("- "):
                break
            item_content = content[2:].strip()
            if item_content:
                items.append(_parse_scalar(item_content))
                index += 1
                continue
            index += 1
            if index >= len(lines) or lines[index][0] <= indent:
                items.append({})
                continue
            item_value, index = _parse_yaml_block(lines, index, lines[index][0])
            items.append(item_value)
        return items, index

    mapping: dict[str, object] = {}
    index = start_index
    while index < len(lines):
        line_indent, content = lines[index]
        if line_indent != indent or content.startswith("- "):
            break
        key, separator, remainder = content.partition(":")
        if separator != ":":
            raise ContextValidationError(f"invalid yaml key line: {content}")
        normalized_key = key.strip()
        normalized_remainder = remainder.strip()
        if normalized_remainder:
            mapping[normalized_key] = _parse_scalar(normalized_remainder)
            index += 1
            continue
        index += 1
        if index >= len(lines) or lines[index][0] <= indent:
            mapping[normalized_key] = {}
            continue
        nested_value, index = _parse_yaml_block(lines, index, lines[index][0])
        mapping[normalized_key] = nested_value
    return mapping, index


def load_yaml_file(path: Path) -> dict[str, object]:
    target_path = Path(path)
    if not target_path.exists():
        raise ContextLoadError(f"system context file not found: {target_path}")
    try:
        raw_text = target_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ContextLoadError(f"failed to read system context: {target_path}") from exc

    lines = _significant_lines(raw_text)
    if not lines:
        raise ContextValidationError("system context file is empty")

    parsed, next_index = _parse_yaml_block(lines, 0, lines[0][0])
    if next_index != len(lines):
        raise ContextValidationError("system context file has trailing invalid content")
    if not isinstance(parsed, dict):
        raise ContextValidationError("system context root must be a mapping")
    return parsed


def dump_yaml_value(value: object, *, indent: int = 0) -> list[str]:
    prefix = " " * indent
    if isinstance(value, dict):
        lines: list[str] = []
        for key, nested_value in value.items():
            if isinstance(nested_value, (dict, list, tuple)):
                lines.append(f"{prefix}{key}:")
                lines.extend(dump_yaml_value(nested_value, indent=indent + 2))
            else:
                rendered = "true" if nested_value is True else "false" if nested_value is False else nested_value
                lines.append(f"{prefix}{key}: {rendered}")
        return lines
    if isinstance(value, (list, tuple)):
        lines = []
        for item in value:
            if isinstance(item, (dict, list, tuple)):
                lines.append(f"{prefix}-")
                lines.extend(dump_yaml_value(item, indent=indent + 2))
            else:
                rendered = "true" if item is True else "false" if item is False else item
                lines.append(f"{prefix}- {rendered}")
        return lines
    rendered = "true" if value is True else "false" if value is False else value
    return [f"{prefix}{rendered}"]


def dump_yaml_file(path: Path, payload: Mapping[str, object]) -> None:
    lines = dump_yaml_value(dict(payload))
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")


def _validate_snapshot(payload: Mapping[str, object]) -> ContextSnapshot:
    try:
        return ContextSnapshot.model_validate(dict(payload))
    except ValidationError as exc:
        raise ContextValidationError(str(exc)) from exc


def build_system_rules_envelope(snapshot: ContextSnapshot) -> dict[str, object]:
    return {
        "dev_environment": snapshot.system.dev_environment,
        "product_environment": snapshot.system.product_environment,
        "strict_separation": snapshot.system.architecture_separation.value == "strict",
        "product_box_runtime_scope": snapshot.product_box.runtime_scope.value,
        "product_box_company_name": snapshot.product_box.company_name,
        "product_box_description": snapshot.product_box.description,
        "product_box_is_dev_environment": snapshot.product_box.is_dev_environment,
        "product_box_code_generation_allowed": snapshot.product_box.code_generation_allowed,
        "product_box_roles": [role.value for role in snapshot.product_box.roles],
        "primary_channels": [channel.value for channel in snapshot.interaction.primary_channels],
    }


def _normalize_operational_updates(updates: Mapping[str, object]) -> dict[str, object]:
    payload = dict(updates)
    if not payload:
        return {}
    if any(field in payload for field in IMMUTABLE_TOP_LEVEL_FIELDS):
        raise ContextUpdateError("immutable system fields cannot be updated via operational path")
    if "context" in payload:
        raise ContextUpdateError("context metadata cannot be updated via operational path")
    if "operational" in payload:
        nested = payload.pop("operational")
        if not isinstance(nested, Mapping):
            raise ContextUpdateError("operational update payload must be a mapping")
        payload.update(dict(nested))

    allowed_fields = set(OperationalSettings.model_fields)
    unknown_fields = sorted(set(payload) - allowed_fields)
    if unknown_fields:
        raise ContextUpdateError(
            "unsupported operational field(s): " + ", ".join(unknown_fields)
        )
    return payload


class SystemContext:
    def __init__(self, snapshot: ContextSnapshot, *, path: Path) -> None:
        self._snapshot = snapshot
        self._path = Path(path)

    @classmethod
    def load(cls, path: Path | None = None) -> "SystemContext":
        target_path = Path(path or DEFAULT_SYSTEM_CONTEXT_PATH)
        payload = load_yaml_file(target_path)
        snapshot = _validate_snapshot(payload)
        return cls(snapshot, path=target_path)

    def snapshot(self) -> ContextSnapshot:
        return ContextSnapshot.model_validate(self._snapshot.model_dump(mode="python"))

    def context_snapshot(self) -> dict[str, object]:
        snapshot = self.snapshot()
        payload = snapshot.model_dump(mode="python")
        payload["source_path"] = str(self._path)
        return payload

    def get_dev_environment(self) -> str:
        return self._snapshot.system.dev_environment

    def get_product_environment(self) -> str:
        return self._snapshot.system.product_environment

    def is_strict_separation_enabled(self) -> bool:
        return self._snapshot.system.architecture_separation.value == "strict"

    def is_product_box_dev_environment(self) -> bool:
        return self._snapshot.product_box.is_dev_environment

    def is_code_generation_allowed_for_product_box(self) -> bool:
        return self._snapshot.product_box.code_generation_allowed

    def get_product_box_runtime_scope(self) -> str:
        return self._snapshot.product_box.runtime_scope.value

    def get_product_box_company_name(self) -> str:
        return self._snapshot.product_box.company_name

    def get_product_box_roles(self) -> tuple[str, ...]:
        return tuple(role.value for role in self._snapshot.product_box.roles)

    def get_primary_channels(self) -> tuple[str, ...]:
        return tuple(channel.value for channel in self._snapshot.interaction.primary_channels)

    def update_operational_context(
        self,
        updates: Mapping[str, object],
        *,
        actor_role: object,
    ) -> ContextSnapshot:
        snapshot = self.snapshot()
        normalized_actor_role = str(actor_role or "").strip()
        if normalized_actor_role != snapshot.operational.owner_role:
            raise ContextUpdateError("operational context updates require actor role = owner")

        normalized_updates = _normalize_operational_updates(updates)
        if not normalized_updates:
            return snapshot

        payload = snapshot.model_dump(mode="python")
        payload["operational"].update(normalized_updates)
        payload["context"]["revision"] = snapshot.context.revision + 1
        updated_snapshot = _validate_snapshot(payload)
        dump_yaml_file(self._path, updated_snapshot.model_dump(mode="python"))
        self._snapshot = updated_snapshot
        return self.snapshot()


def load_system_context(path: Path | None = None) -> SystemContext:
    return SystemContext.load(path)

