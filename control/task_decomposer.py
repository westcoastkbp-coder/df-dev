from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SUBTASKS_DIR = REPO_ROOT / "tasks" / "subtasks"
MAX_SUBTASKS = 5
STRUCTURED_SINGLE_STEP_TASK_TYPES = {
    "personal_context_update",
    "external_write_google_doc",
    "drive_to_google_doc",
}

CREATE_HINTS = (
    "create",
    "add",
    "write",
    "generate",
    "new file",
)
MODIFY_HINTS = (
    "modify",
    "update",
    "edit",
    "change",
    "refactor",
    "fix",
    "implement",
)
SETUP_HINTS = (
    "setup",
    "set up",
    "configure",
    "install",
    "prepare",
    "initialize",
    "bootstrap",
)
ANALYSIS_HINTS = (
    "analyze",
    "review",
    "inspect",
    "test",
    "validate",
    "verify",
    "check",
)
ACTION_VERBS = tuple(
    dict.fromkeys(
        CREATE_HINTS
        + MODIFY_HINTS
        + SETUP_HINTS
        + ANALYSIS_HINTS
        + ("build", "run")
    )
)
VERB_GROUP = "|".join(
    sorted((re.escape(verb) for verb in ACTION_VERBS), key=len, reverse=True)
)
FILE_PATTERN = re.compile(r"(?:[A-Za-z]:[\\/])?[\w./\\-]+\.[A-Za-z0-9]+")
LIST_MARKER_PATTERN = re.compile(r"^\s*(?:[-*]|\d+[.)])\s+")
CLAUSE_SPLIT_PATTERN = re.compile(r"\s*(?:\r?\n|;)\s*")
ACTION_SPLIT_PATTERN = re.compile(
    rf"\b(?:and then|then|after that|afterwards|next|and)\b(?=\s+(?:{VERB_GROUP})\b)",
    re.IGNORECASE,
)
COMMA_ACTION_SPLIT_PATTERN = re.compile(rf",\s*(?=(?:{VERB_GROUP})\b)", re.IGNORECASE)


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


def _task_id(task_source: dict[str, Any]) -> int | str:
    if "task_id" in task_source:
        value = task_source["task_id"]
    else:
        value = task_source["issue_number"]

    try:
        return int(value)
    except (TypeError, ValueError):
        return str(value)


def _clean_clause(text: str) -> str:
    normalized = " ".join(str(text or "").replace("\r", "\n").split())
    return normalized.strip(" ,.;:")


def _is_structured_single_step_task(task_source: dict[str, Any]) -> bool:
    task_type = str(task_source.get("task_type") or "").strip()
    return task_type in STRUCTURED_SINGLE_STEP_TASK_TYPES or isinstance(
        task_source.get("personal_context_update"),
        dict,
    ) or isinstance(task_source.get("tool_call"), dict) or isinstance(
        task_source.get("pipeline"),
        list,
    )


def _bullet_clauses(instruction: str) -> list[str]:
    clauses: list[str] = []
    for line in str(instruction or "").splitlines():
        if not LIST_MARKER_PATTERN.match(line):
            continue
        clause = _clean_clause(LIST_MARKER_PATTERN.sub("", line))
        if clause:
            clauses.append(clause)
    return clauses


def _split_action_clauses(text: str) -> list[str]:
    pieces = [_clean_clause(text)]
    for pattern in (COMMA_ACTION_SPLIT_PATTERN, ACTION_SPLIT_PATTERN):
        next_pieces: list[str] = []
        for piece in pieces:
            next_pieces.extend(pattern.split(piece))
        pieces = next_pieces
    return [_clean_clause(piece) for piece in pieces if _clean_clause(piece)]


def _candidate_clauses(instruction: str) -> list[str]:
    bullet_clauses = _bullet_clauses(instruction)
    if bullet_clauses:
        return bullet_clauses

    clauses: list[str] = []
    for piece in CLAUSE_SPLIT_PATTERN.split(str(instruction or "")):
        cleaned_piece = _clean_clause(piece)
        if not cleaned_piece:
            continue
        clauses.extend(_split_action_clauses(cleaned_piece))
    return clauses


def _split_clause_by_files(clause: str) -> list[str]:
    matches = list(FILE_PATTERN.finditer(clause))
    if len(matches) < 2:
        return [clause]

    prefix = _clean_clause(clause[: matches[0].start()])
    if not prefix:
        return [clause]

    suffix = _clean_clause(clause[matches[-1].end() :])
    split_clauses: list[str] = []
    for match in matches:
        parts = [prefix, match.group(0)]
        if suffix:
            parts.append(suffix)
        split_clauses.append(_clean_clause(" ".join(parts)))
    return split_clauses


def _classify_subtask_type(instruction: str) -> str:
    lower_instruction = _clean_clause(instruction).lower()

    if any(f" {hint} " in f" {lower_instruction} " for hint in SETUP_HINTS):
        return "setup"
    if any(f" {hint} " in f" {lower_instruction} " for hint in CREATE_HINTS):
        return "create_file"
    if any(f" {hint} " in f" {lower_instruction} " for hint in MODIFY_HINTS):
        return "modify_file"
    return "analysis"


def _merge_excess_clauses(clauses: list[str]) -> list[str]:
    if len(clauses) <= MAX_SUBTASKS:
        return clauses

    kept = list(clauses[: MAX_SUBTASKS - 1])
    kept.append("; ".join(clauses[MAX_SUBTASKS - 1 :]))
    return kept


def decompose_task(task_source: dict[str, Any] | Path | str) -> dict[str, Any]:
    source = _load_task_source(task_source)
    parent_task_id = _task_id(source)
    instruction = _build_instruction(source)

    if _is_structured_single_step_task(source):
        return {
            "parent_task_id": parent_task_id,
            "subtasks": [
                {
                    "subtask_id": f"{parent_task_id}-1",
                    "instruction": _clean_clause(instruction),
                    "type": _classify_subtask_type(instruction),
                    "priority": 1,
                }
            ],
        }

    raw_clauses = _candidate_clauses(instruction)
    if not raw_clauses:
        raw_clauses = [_clean_clause(instruction)]

    split_clauses: list[str] = []
    for clause in raw_clauses:
        expanded = _split_clause_by_files(clause)
        if expanded:
            split_clauses.extend(expanded)

    normalized_clauses = [clause for clause in map(_clean_clause, split_clauses) if clause]
    if not normalized_clauses:
        normalized_clauses = [_clean_clause(instruction)]

    limited_clauses = _merge_excess_clauses(normalized_clauses or [_clean_clause(instruction)])
    subtasks: list[dict[str, Any]] = []
    for index, clause in enumerate(limited_clauses, start=1):
        subtasks.append(
            {
                "subtask_id": f"{parent_task_id}-{index}",
                "instruction": clause,
                "type": _classify_subtask_type(clause),
                "priority": index,
            }
        )

    return {
        "parent_task_id": parent_task_id,
        "subtasks": subtasks or [
            {
                "subtask_id": f"{parent_task_id}-1",
                "instruction": _clean_clause(instruction),
                "type": _classify_subtask_type(instruction),
                "priority": 1,
            }
        ],
    }


def subtasks_output_path(
    parent_task_id: int | str,
    output_dir: Path | str | None = None,
) -> Path:
    base_dir = Path(output_dir) if output_dir is not None else DEFAULT_SUBTASKS_DIR
    return base_dir / f"task-{parent_task_id}-subtasks.json"


def write_subtask_record(
    record: dict[str, Any],
    output_dir: Path | str | None = None,
) -> Path:
    path = subtasks_output_path(record["parent_task_id"], output_dir=output_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(record, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path
