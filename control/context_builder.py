from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from integrations.google_drive_reader import read_google_drive_file

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONTEXT_DIR = REPO_ROOT / "tasks" / "context"
DEFAULT_CONSTRAINTS = "Do not break existing code. Modify only necessary parts."
DEFAULT_SUCCESS_CRITERIA = "Code runs without errors and matches task description"
MAX_INCLUDED_FILES = 6
SMALL_FILE_SIZE_BYTES = 4096
EXCERPT_CHAR_LIMIT = 1200
EXPLICIT_FILE_KEYS = (
    "file_paths",
    "files",
    "paths",
    "related_files",
    "config_files",
)
PIPELINE_FALLBACK_FILES = (
    Path("control/task_to_codex.py"),
    Path("scripts/run_codex_task.py"),
    Path("control/task_guard.py"),
    Path("control/validate_task.py"),
    Path("control/metrics_logger.py"),
)
PERSONAL_CONTEXT_FALLBACK_FILES = (
    Path("modules/personal/schema.json"),
    Path("personal/personal_context.json"),
    Path("scripts/update_personal_context.py"),
)


def _load_task_source(task_source: dict[str, Any] | Path | str) -> dict[str, Any]:
    if isinstance(task_source, dict):
        return dict(task_source)

    path = Path(task_source)
    return json.loads(path.read_text(encoding="utf-8"))


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _build_instruction(task_source: dict[str, Any]) -> str:
    instruction = _normalize_text(task_source.get("instruction"))
    if instruction:
        return instruction

    title = _normalize_text(task_source.get("title"))
    body = _normalize_text(task_source.get("body"))
    if title and body:
        return f"{title}\n\n{body}"
    return title or body


def _build_title(task_source: dict[str, Any], instruction: str) -> str:
    title = _normalize_text(task_source.get("title"))
    if title:
        return title

    for line in instruction.splitlines():
        if line.strip():
            return line.strip()
    return ""


def _task_id(task_source: dict[str, Any]) -> int:
    if "task_id" in task_source:
        return int(task_source["task_id"])
    return int(task_source["issue_number"])


def _candidate_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        for key in ("path", "relative_path", "file", "filename"):
            candidate = value.get(key)
            if candidate:
                return [str(candidate)]
        return []
    if isinstance(value, (list, tuple)):
        candidates: list[str] = []
        for item in value:
            candidates.extend(_candidate_values(item))
        return candidates
    return []


def _resolve_repo_file(
    candidate: str,
    *,
    repo_root: Path,
) -> Path | None:
    normalized = str(candidate or "").strip()
    if not normalized:
        return None

    candidate_path = Path(normalized)
    path = (
        candidate_path if candidate_path.is_absolute() else repo_root / candidate_path
    )
    resolved = path.resolve(strict=False)

    try:
        resolved.relative_to(repo_root.resolve())
    except ValueError:
        return None

    if not resolved.exists() or not resolved.is_file():
        return None
    return resolved


def _explicit_files(task_source: dict[str, Any], repo_root: Path) -> list[Path]:
    resolved_paths: list[Path] = []
    seen: set[str] = set()

    for key in EXPLICIT_FILE_KEYS:
        for candidate in _candidate_values(task_source.get(key)):
            path = _resolve_repo_file(candidate, repo_root=repo_root)
            if path is None:
                continue

            relative_path = path.relative_to(repo_root.resolve()).as_posix()
            if relative_path in seen:
                continue
            seen.add(relative_path)
            resolved_paths.append(path)

    return resolved_paths


def _fallback_files(repo_root: Path) -> list[Path]:
    selected: list[Path] = []
    seen: set[str] = set()

    config_dir = repo_root / "config"
    if config_dir.exists():
        for path in sorted(config_dir.glob("*.json")):
            if not path.is_file():
                continue
            relative_path = path.relative_to(repo_root.resolve()).as_posix()
            if relative_path in seen:
                continue
            seen.add(relative_path)
            selected.append(path)

    readme_path = repo_root / "README.md"
    if readme_path.is_file():
        relative_path = readme_path.relative_to(repo_root.resolve()).as_posix()
        if relative_path not in seen:
            seen.add(relative_path)
            selected.append(readme_path)

    for relative_path in PIPELINE_FALLBACK_FILES:
        path = repo_root / relative_path
        if not path.is_file():
            continue
        normalized = path.relative_to(repo_root.resolve()).as_posix()
        if normalized in seen:
            continue
        seen.add(normalized)
        selected.append(path)

    return selected


def _personal_fallback_files(repo_root: Path) -> list[Path]:
    selected: list[Path] = []
    for relative_path in PERSONAL_CONTEXT_FALLBACK_FILES:
        path = repo_root / relative_path
        if not path.is_file():
            continue
        selected.append(path)
    return selected


def _is_personal_context_task(task_source: dict[str, Any]) -> bool:
    return str(
        task_source.get("task_type") or ""
    ).strip() == "personal_context_update" or isinstance(
        task_source.get("personal_context_update"), dict
    )


def _selected_files(
    task_source: dict[str, Any], repo_root: Path
) -> tuple[list[Path], str]:
    explicit_paths = _explicit_files(task_source, repo_root)
    if explicit_paths:
        return explicit_paths[:MAX_INCLUDED_FILES], "explicit_paths"
    if _is_personal_context_task(task_source):
        return _personal_fallback_files(repo_root)[
            :MAX_INCLUDED_FILES
        ], "personal_context_fallback"
    fallback_paths = _fallback_files(repo_root)
    if fallback_paths:
        return fallback_paths[:MAX_INCLUDED_FILES], "pipeline_fallback"
    return [], "clarification_required"


def _google_drive_file_ids(task_source: dict[str, Any]) -> list[str]:
    external_context = task_source.get("external_context")
    if not isinstance(external_context, dict):
        return []

    raw_value = external_context.get("google_drive")
    raw_items: list[Any]
    if isinstance(raw_value, str):
        raw_items = [raw_value]
    elif isinstance(raw_value, (list, tuple)):
        raw_items = list(raw_value)
    else:
        return []

    file_ids: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        candidate = ""
        if isinstance(item, str):
            candidate = item
        elif isinstance(item, dict):
            candidate = str(item.get("drive_file_id") or item.get("file_id") or "")

        normalized = candidate.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        file_ids.append(normalized)
    return file_ids


def _external_files(task_source: dict[str, Any]) -> list[dict[str, Any]]:
    external_files: list[dict[str, Any]] = []
    for file_id in _google_drive_file_ids(task_source):
        try:
            entry = read_google_drive_file({"drive_file_id": file_id})
        except Exception:
            return []
        if entry is not None:
            external_files.append(entry)
    return external_files


def _file_entry(path: Path, repo_root: Path) -> dict[str, Any]:
    size_bytes = path.stat().st_size
    text = path.read_text(encoding="utf-8", errors="replace")
    use_full_text = size_bytes <= SMALL_FILE_SIZE_BYTES

    return {
        "relative_path": path.relative_to(repo_root.resolve()).as_posix(),
        "size_bytes": size_bytes,
        "content": text if use_full_text else text[:EXCERPT_CHAR_LIMIT],
        "content_mode": "full_text" if use_full_text else "excerpt",
        "truncated": False if use_full_text else len(text) > EXCERPT_CHAR_LIMIT,
    }


def build_context_packet(
    task_source: dict[str, Any] | Path | str,
    *,
    repo_root: Path | str | None = None,
) -> dict[str, Any]:
    source = _load_task_source(task_source)
    root_dir = Path(repo_root) if repo_root is not None else REPO_ROOT
    instruction = _build_instruction(source)
    title = _build_title(source, instruction)
    selected_files, selection_mode = _selected_files(source, root_dir)

    related_files: list[dict[str, Any]] = []
    config_files: list[dict[str, Any]] = []
    for path in selected_files:
        entry = _file_entry(path, root_dir)
        if entry["relative_path"].startswith("config/"):
            config_files.append(entry)
        else:
            related_files.append(entry)

    packet = {
        "task_id": _task_id(source),
        "title": title,
        "instruction": instruction,
        "constraints": _normalize_text(source.get("constraints"))
        or DEFAULT_CONSTRAINTS,
        "success_criteria": _normalize_text(source.get("success_criteria"))
        or DEFAULT_SUCCESS_CRITERIA,
        "related_files": related_files,
        "config_files": config_files,
        "external_files": _external_files(source),
        "notes": (
            "Selection mode: clarification_required. "
            "No repository files were auto-selected because the task did not provide explicit scope."
            if selection_mode == "clarification_required"
            else (
                "Selection mode: "
                f"{selection_mode}. Limit: {MAX_INCLUDED_FILES}. "
                "Fallback order: config/*.json, README.md, fixed control/scripts pipeline files."
            )
        ),
    }
    if source.get("parent_task_id") is not None:
        packet["parent_task_id"] = source["parent_task_id"]
    if source.get("subtask_id") is not None:
        packet["subtask_id"] = str(source["subtask_id"])
    if source.get("subtask_type") is not None:
        packet["subtask_type"] = str(source["subtask_type"])
    return packet


def context_output_path(
    task_id: int,
    output_dir: Path | str | None = None,
    *,
    subtask_id: str | None = None,
) -> Path:
    base_dir = Path(output_dir) if output_dir is not None else DEFAULT_CONTEXT_DIR
    identifier = str(subtask_id or task_id)
    return base_dir / f"task-{identifier}-context.json"


def write_context_packet(
    context_packet: dict[str, Any],
    output_dir: Path | str | None = None,
) -> Path:
    path = context_output_path(
        int(context_packet["task_id"]),
        output_dir=output_dir,
        subtask_id=(
            str(context_packet["subtask_id"])
            if context_packet.get("subtask_id") is not None
            else None
        ),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(context_packet, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def build_and_write_context_packet(
    task_source: dict[str, Any] | Path | str,
    *,
    output_dir: Path | str | None = None,
    repo_root: Path | str | None = None,
) -> tuple[dict[str, Any], Path]:
    context_packet = build_context_packet(task_source, repo_root=repo_root)
    path = write_context_packet(context_packet, output_dir=output_dir)
    return context_packet, path
