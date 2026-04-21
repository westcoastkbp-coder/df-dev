from __future__ import annotations

import argparse
import copy
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from control.context_builder import build_and_write_context_packet

DEFAULT_CODEX_TASK_DIR = REPO_ROOT / "tasks" / "codex"
DEFAULT_CONSTRAINTS = (
    "Do not break existing code. Modify only necessary parts. "
    "Do not full-repo scan unless the task explicitly requires it. "
    "Operate on specific files or functions only. "
    "Limit edits to 1-2 files and avoid broad refactors. "
    "Do not repeat scanning or analysis without making changes."
)
DEFAULT_SUCCESS_CRITERIA = (
    "Behavior is deterministic and stable. "
    "If the task scope is unclear, return a clarification request instead of guessing. "
    "Output must include changed files, diff, and a short explanation."
)
PERSONAL_CONTEXT_TASK_TYPE = "personal_context_update"
EXTERNAL_WRITE_GOOGLE_DOC_TASK_TYPE = "external_write_google_doc"
PERSONAL_CONTEXT_FILE_PATHS = [
    "modules/personal/schema.json",
    "personal/personal_context.json",
    "scripts/update_personal_context.py",
]
EXPLICIT_SCOPE_KEYS = (
    "file_paths",
    "files",
    "paths",
    "related_files",
    "config_files",
)


def read_task_packet(packet_path: Path | str) -> dict[str, Any]:
    path = Path(packet_path)
    return json.loads(path.read_text(encoding="utf-8"))


def build_instruction(title: str, body: str) -> str:
    normalized_title = str(title or "").strip()
    normalized_body = str(body or "").strip()

    if normalized_title and normalized_body:
        return f"{normalized_title}\n\n{normalized_body}"
    return normalized_title or normalized_body


def _has_explicit_scope(task_packet: dict[str, Any]) -> bool:
    for key in EXPLICIT_SCOPE_KEYS:
        value = task_packet.get(key)
        if isinstance(value, str) and value.strip():
            return True
        if isinstance(value, dict) and value:
            return True
        if isinstance(value, (list, tuple)) and any(
            str(item or "").strip() for item in value
        ):
            return True
    return False


def _requires_clarification(task_packet: dict[str, Any]) -> bool:
    task_type = str(task_packet.get("task_type") or "").strip()
    if task_type in {PERSONAL_CONTEXT_TASK_TYPE, EXTERNAL_WRITE_GOOGLE_DOC_TASK_TYPE}:
        return False
    if isinstance(task_packet.get("personal_context_update"), dict):
        return False
    if isinstance(task_packet.get("external_context"), dict):
        return False
    if isinstance(task_packet.get("tool_call"), dict):
        return False
    if isinstance(task_packet.get("pipeline"), list):
        return False
    return not _has_explicit_scope(task_packet)


def _build_clarification_instruction(title: str, body: str) -> str:
    requested_task = build_instruction(title=title, body=body)
    lines = [
        "CLARIFICATION REQUIRED",
        "",
        "The task does not name the exact file or function to change.",
        "Ask for the specific target before editing code.",
        "Do not scan the repository to guess the scope.",
    ]
    if requested_task:
        lines.extend(["", f"Requested task:\n{requested_task}"])
    return "\n".join(lines)


def build_codex_task(task_packet: dict[str, Any]) -> dict[str, Any]:
    issue_number = int(task_packet["issue_number"])
    task_type = str(task_packet.get("task_type") or "").strip()
    title = str(task_packet.get("title") or "")
    body = str(task_packet.get("body") or "")
    labels = [str(label) for label in task_packet.get("labels", [])]
    instruction = (
        _build_clarification_instruction(title=title, body=body)
        if _requires_clarification(task_packet)
        else build_instruction(title=title, body=body)
    )

    codex_task = {
        "task_id": issue_number,
        "instruction": instruction,
        "constraints": DEFAULT_CONSTRAINTS,
        "success_criteria": DEFAULT_SUCCESS_CRITERIA,
        "labels": labels,
    }
    if task_type:
        codex_task["task_type"] = task_type
    if task_type == PERSONAL_CONTEXT_TASK_TYPE:
        codex_task["task_type"] = PERSONAL_CONTEXT_TASK_TYPE
        if task_packet.get("file_paths") is None:
            codex_task["file_paths"] = list(PERSONAL_CONTEXT_FILE_PATHS)
    if isinstance(task_packet.get("personal_context_update"), dict):
        provided_file_paths = task_packet.get("file_paths")
        codex_task["task_type"] = PERSONAL_CONTEXT_TASK_TYPE
        codex_task["personal_context_update"] = copy.deepcopy(
            task_packet["personal_context_update"]
        )
        if provided_file_paths is None:
            codex_task["file_paths"] = list(PERSONAL_CONTEXT_FILE_PATHS)
        elif isinstance(provided_file_paths, (list, tuple)):
            codex_task["file_paths"] = list(provided_file_paths)
        else:
            codex_task["file_paths"] = [str(provided_file_paths)]
    elif task_packet.get("file_paths") is not None:
        codex_task["file_paths"] = copy.deepcopy(task_packet["file_paths"])
    if task_type == EXTERNAL_WRITE_GOOGLE_DOC_TASK_TYPE:
        codex_task["title"] = str(task_packet.get("title") or "").strip()
        if task_packet.get("document_title") is not None:
            codex_task["document_title"] = str(task_packet["document_title"])
        codex_task["content"] = str(
            task_packet.get("content")
            if task_packet.get("content") is not None
            else task_packet.get("body") or ""
        )
    if isinstance(task_packet.get("external_context"), dict):
        codex_task["external_context"] = copy.deepcopy(task_packet["external_context"])
    if isinstance(task_packet.get("tool_call"), dict):
        codex_task["tool_call"] = copy.deepcopy(task_packet["tool_call"])
    return codex_task


def output_path_for_task(task_id: int, output_dir: Path | str | None = None) -> Path:
    base_dir = Path(output_dir) if output_dir is not None else DEFAULT_CODEX_TASK_DIR
    return base_dir / f"task-{task_id}.json"


def write_codex_task(
    codex_task: dict[str, Any],
    output_dir: Path | str | None = None,
) -> Path:
    path = output_path_for_task(int(codex_task["task_id"]), output_dir=output_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(codex_task, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def transform_task_packet_to_codex(
    packet_path: Path | str,
    output_dir: Path | str | None = None,
    context_output_dir: Path | str | None = None,
    repo_root: Path | str | None = None,
) -> tuple[dict[str, Any], Path]:
    task_packet = read_task_packet(packet_path)
    codex_task = build_codex_task(task_packet)
    context_source = dict(task_packet)
    context_source.update(codex_task)
    _, context_path = build_and_write_context_packet(
        context_source,
        output_dir=context_output_dir,
        repo_root=repo_root,
    )
    codex_task["context_packet_path"] = str(context_path)
    path = write_codex_task(codex_task, output_dir=output_dir)
    return codex_task, path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert a normalized task packet into a Codex-ready task file.",
    )
    parser.add_argument(
        "packet_path",
        help="Path to the input task packet JSON.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional output directory for the generated Codex task JSON.",
    )
    parser.add_argument(
        "--context-output-dir",
        default=None,
        help="Optional output directory for the generated task context JSON.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    codex_task, path = transform_task_packet_to_codex(
        packet_path=args.packet_path,
        output_dir=args.output_dir,
        context_output_dir=getattr(args, "context_output_dir", None),
    )
    print(f"CODEX_TASK_WRITTEN: {path}")
    print(json.dumps(codex_task, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
