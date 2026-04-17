from __future__ import annotations

import copy
import hashlib
import json
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
MEMORY_DIR = REPO_ROOT / "memory"
SCOPED_MEMORY_NAMES = (
    "google_context",
    "web_context",
    "dev_context",
)
ROLE_TO_SCOPED_MEMORY = {
    "architect_agent": ("dev_context",),
    "coder_agent": ("dev_context",),
    "reviewer_agent": ("dev_context",),
    "qa_agent": ("dev_context",),
    "memory_agent": ("dev_context",),
    "google_agent": ("google_context",),
    "google_docs_agent": ("google_context",),
    "google_drive_agent": ("google_context",),
    "browser_agent": ("web_context",),
    "web_agent": ("web_context",),
    "web_operator": ("web_context",),
}
MEMORY_FILES = {
    "decisions": MEMORY_DIR / "decisions.json",
    "architecture": MEMORY_DIR / "architecture.json",
    "owner_memory": MEMORY_DIR / "owner_memory.json",
    "project_state": MEMORY_DIR / "project_state.json",
}
DEFAULT_MEMORY = {
    "decisions": {
        "decisions": [],
    },
    "architecture": {
        "system_rules": [],
        "hardware_baseline": {},
        "tool_strategy": [],
    },
    "owner_memory": {
        "important_notes": [],
        "long_term_goals": [],
        "owner_name": "",
        "priorities": [],
        "strategic_focus": "",
        "public_positioning": "",
        "business_relation": "",
        "product_relation": "",
    },
    "project_state": {
        "current_stage": "",
        "active_block": "",
        "core_status": "",
        "operating_phase": "",
        "system_mode": "",
        "focus": "",
        "next_step": "",
        "next_steps": [],
    },
}
DEFAULT_SHARED_SYSTEM_CONTEXT = {
    "system_name": "Digital Foreman",
    "business_name": "West Coast KBP",
    "product_name": "Digital Foreman",
    "system_type": "Execution OS / Execution Control System",
    "assistant_policy": {
        "access_level": "assistant_context",
        "reserved_for": "top-level assistant / context guardian",
        "can_read": [
            "decisions",
            "architecture",
            "owner_memory",
            "project_state",
            "shared_system_context",
            "scoped/*",
        ],
    },
    "agent_policy": {
        "access_level": "agent_context",
        "default_behavior": "role-scoped memory only",
        "full_memory_requires_explicit_need": True,
        "restricted_context": [
            "owner_memory",
            "business_relation",
            "product_relation",
            "public_positioning",
            "strategic_focus",
        ],
    },
    "operating_model": [
        "Dev Box -> Business Box -> Product Box",
        "Business Box validates real operations before productization",
    ],
    "system_rules": [
        "Digital Foreman / Execution OS Core is one system across dev, business, and product surfaces.",
        "Real business use is the validation layer for productization.",
        "Agents receive only scoped memory relevant to the role unless full access is explicitly required.",
    ],
}
EXECUTION_CONTEXT_VERSION = 1
EXECUTION_CONTEXT_MAX_BYTES = 64 * 1024
EXECUTION_CONTEXT_CHECKSUM_LENGTH = 64
EXECUTION_CONTEXT_CHECKSUM_PLACEHOLDER = "0" * EXECUTION_CONTEXT_CHECKSUM_LENGTH
DEFAULT_EXECUTION_SYSTEM_CONTEXT = {
    "system_state": {
        "owner": {
            "name": "Anton Vorontsov",
            "role": "owner",
        },
        "business": {
            "name": "West Coast KBP",
            "type": "real operating business",
        },
        "product": {
            "name": "Execution OS",
            "status": "active development",
        },
        "architecture": {
            "model": "Execution OS",
            "rules": [
                "external-first",
                "role-based execution",
                "human-as-verifier",
                "adapters-as-hands",
            ],
        },
        "current_stage": {
            "phase": "system integration",
            "priority": "global context",
        },
    },
    "active_tasks": [],
    "last_actions": [],
    "metadata": {
        "version": EXECUTION_CONTEXT_VERSION,
        "updated_at": "",
        "checksum": "",
    },
}
DEFAULT_SCOPED_MEMORY = {
    "google_context": {
        "scope": "google_context",
        "intended_roles": [
            "google_agent",
            "google_docs_agent",
            "google_drive_agent",
        ],
        "focus": [
            "Google Drive / Docs / Sheets / Slides tasks",
            "document accuracy and bounded edits",
            "connector-safe operations",
        ],
        "guardrails": [
            "Use only Google-related context needed for the current task.",
            "Do not request owner/business/product memory unless explicitly required.",
        ],
    },
    "web_context": {
        "scope": "web_context",
        "intended_roles": [
            "browser_agent",
            "web_agent",
            "web_operator",
        ],
        "focus": [
            "browser navigation and form interactions",
            "external page state and live web execution",
            "bounded capture of page results",
        ],
        "guardrails": [
            "Use only web-task context needed for the action.",
            "Do not inherit full owner/business/product memory by default.",
        ],
    },
    "dev_context": {
        "scope": "dev_context",
        "intended_roles": [
            "architect_agent",
            "coder_agent",
            "reviewer_agent",
            "qa_agent",
            "memory_agent",
        ],
        "focus": [
            "repository implementation and validation",
            "system rules and execution constraints",
            "recent task memory and code-change quality",
        ],
        "guardrails": [
            "Stay inside task scope, constraints, and system rules.",
            "Do not rely on owner/business/product context unless explicitly required.",
        ],
    },
}

OWNER_CONTEXT_FIELDS = (
    "owner_name",
    "strategic_focus",
    "public_positioning",
    "business_relation",
    "product_relation",
)
SYSTEM_CONTEXT_SUMMARY_ITEMS = 3
SYSTEM_CONTEXT_ACTION_MAX_CHARS = 140
CONTEXT_NOT_LOADED = "CONTEXT_NOT_LOADED"
CONTEXT_INVALID = "CONTEXT_INVALID"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _base_memory_dir(memory_dir: Path | str | None = None) -> Path:
    return MEMORY_DIR if memory_dir is None else Path(memory_dir)


def _scoped_memory_dir(memory_dir: Path | str | None = None) -> Path:
    return _base_memory_dir(memory_dir) / "scoped"


def _shared_system_context_path(memory_dir: Path | str | None = None) -> Path:
    return _base_memory_dir(memory_dir) / "shared_system_context.json"


def _execution_system_context_path(memory_dir: Path | str | None = None) -> Path:
    return _base_memory_dir(memory_dir) / "system_context.json"


def _normalize_memory_name(memory_name: str) -> str:
    normalized_name = str(memory_name or "").strip().lower()
    if normalized_name not in MEMORY_FILES:
        raise ValueError("INVALID_MEMORY_NAME")
    return normalized_name


def _memory_path(memory_name: str, memory_dir: Path | str | None = None) -> Path:
    normalized_name = _normalize_memory_name(memory_name)
    base_dir = _base_memory_dir(memory_dir)
    return base_dir / f"{normalized_name}.json"


def _default_memory_payload(memory_name: str) -> dict[str, Any]:
    return copy.deepcopy(DEFAULT_MEMORY[_normalize_memory_name(memory_name)])


def _normalize_scoped_memory_name(scope_name: str) -> str:
    normalized_name = str(scope_name or "").strip().lower()
    if normalized_name not in DEFAULT_SCOPED_MEMORY:
        raise ValueError("INVALID_SCOPED_MEMORY_NAME")
    return normalized_name


def _scoped_memory_path(scope_name: str, memory_dir: Path | str | None = None) -> Path:
    normalized_name = _normalize_scoped_memory_name(scope_name)
    return _scoped_memory_dir(memory_dir) / f"{normalized_name}.json"


def _default_scoped_memory_payload(scope_name: str) -> dict[str, Any]:
    return copy.deepcopy(DEFAULT_SCOPED_MEMORY[_normalize_scoped_memory_name(scope_name)])


def ensure_memory_files(memory_dir: Path | str | None = None) -> None:
    base_dir = _base_memory_dir(memory_dir)
    base_dir.mkdir(parents=True, exist_ok=True)
    for memory_name, default_payload in DEFAULT_MEMORY.items():
        path = base_dir / f"{memory_name}.json"
        if path.is_file():
            continue
        path.write_text(
            json.dumps(default_payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    shared_path = _shared_system_context_path(memory_dir)
    if not shared_path.is_file():
        shared_path.write_text(
            json.dumps(DEFAULT_SHARED_SYSTEM_CONTEXT, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    scoped_dir = _scoped_memory_dir(memory_dir)
    scoped_dir.mkdir(parents=True, exist_ok=True)
    for scope_name in SCOPED_MEMORY_NAMES:
        path = scoped_dir / f"{scope_name}.json"
        if path.is_file():
            continue
        path.write_text(
            json.dumps(_default_scoped_memory_payload(scope_name), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


def _read_memory_file(memory_name: str, memory_dir: Path | str | None = None) -> dict[str, Any]:
    ensure_memory_files(memory_dir)
    path = _memory_path(memory_name, memory_dir)
    default_payload = _default_memory_payload(memory_name)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        path.write_text(
            json.dumps(default_payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return default_payload
    if not isinstance(payload, dict):
        path.write_text(
            json.dumps(default_payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return default_payload
    return payload


def _write_memory_file(
    memory_name: str,
    payload: dict[str, Any],
    memory_dir: Path | str | None = None,
) -> Path:
    ensure_memory_files(memory_dir)
    path = _memory_path(memory_name, memory_dir)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def _read_json_file(path: Path, default_payload: dict[str, Any]) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        path.write_text(
            json.dumps(default_payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return copy.deepcopy(default_payload)
    if not isinstance(payload, dict):
        path.write_text(
            json.dumps(default_payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return copy.deepcopy(default_payload)
    return payload


def _execution_context_validation_error() -> RuntimeError:
    return RuntimeError(CONTEXT_INVALID)


def _expect_dict(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _execution_context_validation_error()
    return value


def _expect_exact_keys(payload: dict[str, Any], expected_keys: set[str]) -> None:
    if set(payload) != expected_keys:
        raise _execution_context_validation_error()


def _expect_string(value: Any) -> str:
    if not isinstance(value, str):
        raise _execution_context_validation_error()
    return value


def _expect_string_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        raise _execution_context_validation_error()
    normalized_values: list[str] = []
    for value in values:
        if not isinstance(value, str):
            raise _execution_context_validation_error()
        normalized_value = _short_context_line(value)
        if normalized_value:
            normalized_values.append(normalized_value)
    return normalized_values


def _validated_system_state(payload: Any) -> dict[str, Any]:
    system_state = _expect_dict(payload)
    _expect_exact_keys(
        system_state,
        {"owner", "business", "product", "architecture", "current_stage"},
    )
    owner = _expect_dict(system_state.get("owner"))
    _expect_exact_keys(owner, {"name", "role"})
    business = _expect_dict(system_state.get("business"))
    _expect_exact_keys(business, {"name", "type"})
    product = _expect_dict(system_state.get("product"))
    _expect_exact_keys(product, {"name", "status"})
    architecture = _expect_dict(system_state.get("architecture"))
    _expect_exact_keys(architecture, {"model", "rules"})
    current_stage = _expect_dict(system_state.get("current_stage"))
    _expect_exact_keys(current_stage, {"phase", "priority"})
    return {
        "owner": {
            "name": _expect_string(owner.get("name")).strip(),
            "role": _expect_string(owner.get("role")).strip(),
        },
        "business": {
            "name": _expect_string(business.get("name")).strip(),
            "type": _expect_string(business.get("type")).strip(),
        },
        "product": {
            "name": _expect_string(product.get("name")).strip(),
            "status": _expect_string(product.get("status")).strip(),
        },
        "architecture": {
            "model": _expect_string(architecture.get("model")).strip(),
            "rules": _expect_string_list(architecture.get("rules")),
        },
        "current_stage": {
            "phase": _expect_string(current_stage.get("phase")).strip(),
            "priority": _expect_string(current_stage.get("priority")).strip(),
        },
    }


def _execution_context_bytes(payload: dict[str, Any]) -> int:
    return len((json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8"))


def _execution_context_checksum_source(payload: dict[str, Any]) -> bytes:
    checksum_payload = copy.deepcopy(payload)
    checksum_payload["metadata"]["checksum"] = ""
    return json.dumps(
        checksum_payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _execution_context_checksum(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_execution_context_checksum_source(payload)).hexdigest()


def _validated_execution_system_context_payload(
    payload: Any,
    *,
    verify_checksum: bool = True,
) -> dict[str, Any]:
    context_payload = _expect_dict(payload)
    _expect_exact_keys(
        context_payload,
        {"system_state", "active_tasks", "last_actions", "metadata"},
    )
    metadata = _expect_dict(context_payload.get("metadata"))
    _expect_exact_keys(metadata, {"version", "updated_at", "checksum"})
    version = metadata.get("version")
    if not isinstance(version, int) or version != EXECUTION_CONTEXT_VERSION:
        raise _execution_context_validation_error()
    normalized_payload = {
        "system_state": _validated_system_state(context_payload.get("system_state")),
        "active_tasks": _expect_string_list(context_payload.get("active_tasks")),
        "last_actions": _expect_string_list(context_payload.get("last_actions")),
        "metadata": {
            "version": version,
            "updated_at": _expect_string(metadata.get("updated_at")).strip(),
            "checksum": _expect_string(metadata.get("checksum")).strip(),
        },
    }
    checksum = normalized_payload["metadata"]["checksum"]
    if verify_checksum and len(checksum) != EXECUTION_CONTEXT_CHECKSUM_LENGTH:
        raise _execution_context_validation_error()
    if verify_checksum:
        computed_checksum = _execution_context_checksum(normalized_payload)
        if checksum != computed_checksum:
            warnings.warn(
                "Execution system context checksum mismatch",
                RuntimeWarning,
                stacklevel=2,
            )
            raise _execution_context_validation_error()
    return normalized_payload


def _trim_execution_system_context(
    payload: dict[str, Any],
) -> dict[str, Any]:
    trimmed_payload = copy.deepcopy(payload)
    while _execution_context_bytes(trimmed_payload) > EXECUTION_CONTEXT_MAX_BYTES:
        if trimmed_payload["last_actions"]:
            trimmed_payload["last_actions"].pop(0)
            continue
        if trimmed_payload["active_tasks"]:
            trimmed_payload["active_tasks"].pop(0)
            continue
        raise ValueError("CONTEXT_SIZE_LIMIT_EXCEEDED")
    return trimmed_payload


def _prepare_execution_system_context_for_write(payload: Any) -> dict[str, Any]:
    validated_payload = _validated_execution_system_context_payload(
        payload,
        verify_checksum=False,
    )
    prepared_payload = copy.deepcopy(validated_payload)
    prepared_payload["metadata"]["version"] = EXECUTION_CONTEXT_VERSION
    prepared_payload["metadata"]["updated_at"] = _utc_now_iso()
    prepared_payload["metadata"]["checksum"] = EXECUTION_CONTEXT_CHECKSUM_PLACEHOLDER
    while True:
        prepared_payload = _trim_execution_system_context(prepared_payload)
        prepared_payload["metadata"]["checksum"] = _execution_context_checksum(prepared_payload)
        if _execution_context_bytes(prepared_payload) <= EXECUTION_CONTEXT_MAX_BYTES:
            break
        prepared_payload["metadata"]["checksum"] = EXECUTION_CONTEXT_CHECKSUM_PLACEHOLDER
    return prepared_payload


def _write_execution_system_context_file(path: Path, payload: dict[str, Any]) -> None:
    temp_path = path.with_name(f"{path.name}.tmp")
    serialized_payload = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    try:
        temp_path.write_bytes(serialized_payload.encode("utf-8"))
        temp_path.replace(path)
    finally:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass


def _read_execution_system_context_from_disk(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise RuntimeError(CONTEXT_NOT_LOADED)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError) as error:
        raise _execution_context_validation_error() from error
    normalized_payload = _validated_execution_system_context_payload(payload)
    if _execution_context_bytes(normalized_payload) > EXECUTION_CONTEXT_MAX_BYTES:
        normalized_payload = _prepare_execution_system_context_for_write(normalized_payload)
        _write_execution_system_context_file(path, normalized_payload)
    return normalized_payload


def _merge_default_payload(
    payload: Any,
    default_payload: Any,
) -> Any:
    if isinstance(default_payload, dict):
        merged_payload = copy.deepcopy(default_payload)
        if not isinstance(payload, dict):
            return merged_payload
        for key, value in payload.items():
            if key in merged_payload:
                merged_payload[key] = _merge_default_payload(value, merged_payload[key])
            else:
                merged_payload[key] = copy.deepcopy(value)
        return merged_payload
    if isinstance(default_payload, list):
        if not isinstance(payload, list):
            return copy.deepcopy(default_payload)
        return copy.deepcopy(payload)
    if payload is None:
        return copy.deepcopy(default_payload)
    return copy.deepcopy(payload)


def _short_context_line(value: Any, *, max_chars: int = SYSTEM_CONTEXT_ACTION_MAX_CHARS) -> str:
    normalized_value = " ".join(str(value or "").split()).strip()
    if len(normalized_value) <= max_chars:
        return normalized_value
    if max_chars <= 3:
        return normalized_value[:max_chars]
    return f"{normalized_value[: max_chars - 3].rstrip()}..."


def read_memory(
    memory_name: str | None = None,
    *,
    memory_dir: Path | str | None = None,
) -> dict[str, Any]:
    if memory_name is None:
        ensure_memory_files(memory_dir)
        return {
            name: _read_memory_file(name, memory_dir)
            for name in MEMORY_FILES
        }
    return _read_memory_file(memory_name, memory_dir)


def read_shared_system_context(*, memory_dir: Path | str | None = None) -> dict[str, Any]:
    ensure_memory_files(memory_dir)
    return _read_json_file(
        _shared_system_context_path(memory_dir),
        DEFAULT_SHARED_SYSTEM_CONTEXT,
    )


def read_execution_system_context(*, memory_dir: Path | str | None = None) -> dict[str, Any]:
    ensure_memory_files(memory_dir)
    return _read_execution_system_context_from_disk(
        _execution_system_context_path(memory_dir)
    )


def read_context(*, memory_dir: Path | str | None = None) -> dict[str, Any]:
    return read_execution_system_context(memory_dir=memory_dir)


def load_required_execution_system_context(
    *,
    memory_dir: Path | str | None = None,
) -> dict[str, Any]:
    return read_execution_system_context(memory_dir=memory_dir)


def write_execution_system_context(
    payload: dict[str, Any],
    *,
    memory_dir: Path | str | None = None,
) -> dict[str, Any]:
    ensure_memory_files(memory_dir)
    path = _execution_system_context_path(memory_dir)
    normalized_payload = _prepare_execution_system_context_for_write(payload)
    _write_execution_system_context_file(path, normalized_payload)
    return normalized_payload


def write_context(
    payload: dict[str, Any],
    *,
    memory_dir: Path | str | None = None,
) -> dict[str, Any]:
    return write_execution_system_context(payload, memory_dir=memory_dir)


def update_context(
    updater: Any,
    *,
    memory_dir: Path | str | None = None,
) -> dict[str, Any]:
    current_payload = read_execution_system_context(memory_dir=memory_dir)
    candidate_payload = copy.deepcopy(current_payload)
    updated_payload = updater(candidate_payload)
    if updated_payload is not None:
        candidate_payload = updated_payload
    if not isinstance(candidate_payload, dict):
        raise ValueError("SYSTEM_CONTEXT_PAYLOAD_INVALID")
    return write_context(candidate_payload, memory_dir=memory_dir)


def build_execution_system_context_summary(
    system_context: dict[str, Any] | None = None,
    *,
    memory_dir: Path | str | None = None,
) -> dict[str, Any]:
    payload = (
        read_execution_system_context(memory_dir=memory_dir)
        if not isinstance(system_context, dict)
        else _validated_execution_system_context_payload(system_context)
    )
    system_state = payload["system_state"]
    return {
        "owner": {
            "name": str(system_state["owner"].get("name") or "").strip(),
            "role": str(system_state["owner"].get("role") or "").strip(),
        },
        "business": {
            "name": str(system_state["business"].get("name") or "").strip(),
            "type": str(system_state["business"].get("type") or "").strip(),
        },
        "product": {
            "name": str(system_state["product"].get("name") or "").strip(),
            "status": str(system_state["product"].get("status") or "").strip(),
        },
        "architecture": {
            "model": str(system_state["architecture"].get("model") or "").strip(),
            "rules": _normalized_strings(system_state["architecture"].get("rules")),
        },
        "current_stage": {
            "phase": str(system_state["current_stage"].get("phase") or "").strip(),
            "priority": str(system_state["current_stage"].get("priority") or "").strip(),
        },
        "active_flows": _normalized_strings(payload.get("active_tasks"))[:SYSTEM_CONTEXT_SUMMARY_ITEMS],
        "recent_actions": _normalized_strings(payload.get("last_actions"))[-SYSTEM_CONTEXT_SUMMARY_ITEMS:],
    }


def append_execution_system_context_recent_action(
    action_line: str,
    *,
    memory_dir: Path | str | None = None,
) -> dict[str, Any]:
    normalized_action = _short_context_line(action_line)
    if not normalized_action:
        return read_execution_system_context(memory_dir=memory_dir)

    def _append_action(payload: dict[str, Any]) -> dict[str, Any]:
        last_actions = _normalized_strings(payload.get("last_actions"))
        last_actions.append(normalized_action)
        payload["last_actions"] = last_actions
        return payload

    return update_context(_append_action, memory_dir=memory_dir)


def read_scoped_memory(
    scope_name: str | None = None,
    *,
    memory_dir: Path | str | None = None,
) -> dict[str, Any]:
    ensure_memory_files(memory_dir)
    if scope_name is None:
        return {
            name: _read_json_file(_scoped_memory_path(name, memory_dir), _default_scoped_memory_payload(name))
            for name in SCOPED_MEMORY_NAMES
        }
    normalized_name = _normalize_scoped_memory_name(scope_name)
    return _read_json_file(
        _scoped_memory_path(normalized_name, memory_dir),
        _default_scoped_memory_payload(normalized_name),
    )


def write_memory(
    memory_name: str,
    payload: dict[str, Any],
    *,
    memory_dir: Path | str | None = None,
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("MEMORY_PAYLOAD_INVALID")

    normalized_name = _normalize_memory_name(memory_name)
    if normalized_name == "decisions":
        decision = str(payload.get("decision") or "").strip()
        if not decision:
            raise ValueError("DECISION_REQUIRED")
        entry = {
            "decision": decision,
            "reason": str(payload.get("reason") or "").strip(),
            "timestamp": str(payload.get("timestamp") or "").strip() or _utc_now_iso(),
        }
        current_payload = _read_memory_file("decisions", memory_dir)
        decisions = current_payload.get("decisions")
        if not isinstance(decisions, list):
            decisions = []
        decisions.append(entry)
        updated_payload = {
            "decisions": decisions,
        }
        _write_memory_file("decisions", updated_payload, memory_dir)
        return entry

    current_payload = _read_memory_file(normalized_name, memory_dir)
    updated_payload = copy.deepcopy(current_payload)
    updated_payload.update(copy.deepcopy(payload))
    _write_memory_file(normalized_name, updated_payload, memory_dir)
    return updated_payload


def get_project_state(*, memory_dir: Path | str | None = None) -> dict[str, Any]:
    return _read_memory_file("project_state", memory_dir)


def _normalized_strings(values: object) -> list[str]:
    if not isinstance(values, list):
        return []
    return [str(value).strip() for value in values if str(value).strip()]


def _owner_context_summary(owner_memory: dict[str, Any]) -> dict[str, str]:
    return {
        field: str(owner_memory.get(field) or "").strip()
        for field in OWNER_CONTEXT_FIELDS
        if str(owner_memory.get(field) or "").strip()
    }


def _build_memory_summary_from_payload(memory_payload: dict[str, Any]) -> dict[str, Any]:
    decision_entries = memory_payload["decisions"].get("decisions")
    if not isinstance(decision_entries, list):
        decision_entries = []

    architecture_rules = memory_payload["architecture"].get("system_rules")
    architecture_rules = _normalized_strings(architecture_rules)

    owner_memory = memory_payload.get("owner_memory")
    if not isinstance(owner_memory, dict):
        owner_memory = {}
    owner_priorities = _normalized_strings(owner_memory.get("priorities"))
    owner_context = _owner_context_summary(owner_memory)

    project_state = memory_payload["project_state"]
    current_stage = ""
    active_block = ""
    core_status = ""
    operating_phase = ""
    system_mode = ""
    focus = ""
    next_step = ""
    next_steps: list[str] = []
    if isinstance(project_state, dict):
        current_stage = str(project_state.get("current_stage") or "").strip()
        active_block = str(project_state.get("active_block") or "").strip()
        core_status = str(project_state.get("core_status") or "").strip()
        operating_phase = str(project_state.get("operating_phase") or "").strip()
        system_mode = str(project_state.get("system_mode") or "").strip()
        focus = str(project_state.get("focus") or "").strip()
        next_step = str(project_state.get("next_step") or "").strip()
        next_steps = _normalized_strings(project_state.get("next_steps"))

    if not next_step and next_steps:
        next_step = next_steps[0]

    summary = {
        "active_block": active_block,
        "architecture_rules": architecture_rules,
        "core_status": core_status,
        "current_stage": current_stage,
        "focus": focus,
        "last_decisions": [
            str(entry.get("decision") or "").strip()
            for entry in decision_entries[-3:]
            if isinstance(entry, dict) and str(entry.get("decision") or "").strip()
        ],
        "next_step": next_step,
        "operating_phase": operating_phase,
        "owner_priorities": owner_priorities,
        "system_mode": system_mode,
    }
    if owner_context:
        summary["owner_context"] = owner_context
    return summary


def _build_restricted_memory_summary_from_payload(memory_payload: dict[str, Any]) -> dict[str, Any]:
    summary = _build_memory_summary_from_payload(memory_payload)
    summary.pop("owner_context", None)
    summary.pop("owner_priorities", None)
    return summary


def build_memory_summary(*, memory_dir: Path | str | None = None) -> dict[str, Any]:
    memory_payload = read_memory(memory_dir=memory_dir)
    return _build_memory_summary_from_payload(memory_payload)


def build_memory_snapshot(*, memory_dir: Path | str | None = None) -> dict[str, Any]:
    memory_payload = read_memory(memory_dir=memory_dir)
    snapshot = copy.deepcopy(memory_payload)
    snapshot["memory_summary"] = _build_memory_summary_from_payload(memory_payload)
    return snapshot


def scoped_memory_names_for_role(role: str) -> tuple[str, ...]:
    normalized_role = str(role or "").strip().lower()
    if normalized_role in ROLE_TO_SCOPED_MEMORY:
        return ROLE_TO_SCOPED_MEMORY[normalized_role]
    if "google" in normalized_role:
        return ("google_context",)
    if "web" in normalized_role or "browser" in normalized_role:
        return ("web_context",)
    if normalized_role.endswith("_agent"):
        return ("dev_context",)
    return ()


def build_assistant_context(*, memory_dir: Path | str | None = None) -> dict[str, Any]:
    return {
        "access_level": "assistant_context",
        "shared_memory": build_memory_snapshot(memory_dir=memory_dir),
        "shared_system_context": read_shared_system_context(memory_dir=memory_dir),
        "scoped_memory": read_scoped_memory(memory_dir=memory_dir),
    }


def build_agent_context(
    role: str,
    *,
    memory_dir: Path | str | None = None,
    include_full_memory: bool = False,
) -> dict[str, Any]:
    normalized_role = str(role or "").strip().lower()
    memory_payload = read_memory(memory_dir=memory_dir)
    scope_names = scoped_memory_names_for_role(normalized_role)
    context = {
        "access_level": "agent_context",
        "role": normalized_role,
        "shared_system_context": read_shared_system_context(memory_dir=memory_dir),
        "shared_memory_summary": _build_restricted_memory_summary_from_payload(memory_payload),
        "scoped_memory": {
            scope_name: read_scoped_memory(scope_name, memory_dir=memory_dir)
            for scope_name in scope_names
        },
    }
    if include_full_memory:
        context["assistant_context"] = build_assistant_context(memory_dir=memory_dir)
    return context
