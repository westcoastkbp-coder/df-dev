from __future__ import annotations

from collections.abc import Mapping, Sequence


DEFAULT_VENDOR = "openai"
KNOWN_VENDORS = {"openai", "google", "claude", "codex"}

GOOGLE_KEYWORDS = (
    "calendar",
    "doc",
    "docs",
    "document",
    "drive",
    "email",
    "gmail",
    "gdoc",
    "google",
    "sheet",
    "sheets",
    "spreadsheet",
)
CLAUDE_KEYWORDS = (
    "browser",
    "form",
    "page",
    "url",
    "web",
    "website",
)
CODEX_KEYWORDS = (
    ".c",
    ".cpp",
    ".go",
    ".java",
    ".js",
    ".json",
    ".md",
    ".py",
    ".rb",
    ".rs",
    ".sql",
    ".ts",
    "code",
    "commit",
    "git",
    "patch",
    "pull request",
    "pull_request",
    "repo",
    "repository",
    "src/",
    "test",
    "tests/",
)


def _normalize_text(value: object) -> str:
    return " ".join(str(value or "").strip().split())


def normalize_vendor(value: object) -> str:
    normalized = _normalize_text(value).lower()
    if normalized in KNOWN_VENDORS:
        return normalized
    return DEFAULT_VENDOR


def _normalize_mapping(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _normalize_sequence(value: object) -> list[object]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return list(value)
    return []


def _collect_text_fragments(value: object) -> list[str]:
    if isinstance(value, Mapping):
        fragments: list[str] = []
        for key, item in value.items():
            key_text = _normalize_text(key).lower()
            if key_text:
                fragments.append(key_text)
            fragments.extend(_collect_text_fragments(item))
        return fragments
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        fragments: list[str] = []
        for item in value:
            fragments.extend(_collect_text_fragments(item))
        return fragments
    text = _normalize_text(value).lower()
    return [text] if text else []


def _signals(
    task: object,
    context: object,
    action_plan: object,
) -> str:
    normalized_task = _normalize_mapping(task)
    normalized_context = _normalize_mapping(context)
    normalized_plan = _normalize_mapping(action_plan)
    context_hints = {
        "command_name": normalized_context.get("command_name"),
        "mode": normalized_context.get("mode"),
        "task_state": _normalize_mapping(normalized_context.get("task_state")),
    }
    fragments: list[str] = []
    fragments.extend(_collect_text_fragments(normalized_plan))
    fragments.extend(_collect_text_fragments(normalized_task))
    fragments.extend(_collect_text_fragments(context_hints))
    return " ".join(fragment for fragment in fragments if fragment)


def _contains_keyword(signal_text: str, keywords: Sequence[str]) -> bool:
    return any(keyword in signal_text for keyword in keywords)


def route(
    task: object = None,
    context: object = None,
    action_plan: object = None,
) -> str:
    normalized_plan = _normalize_mapping(action_plan)
    raw_vendor = _normalize_text(normalized_plan.get("vendor"))
    explicit_vendor = normalize_vendor(raw_vendor)
    if raw_vendor and explicit_vendor != DEFAULT_VENDOR:
        return explicit_vendor

    signal_text = _signals(task, context, normalized_plan)
    if _contains_keyword(signal_text, GOOGLE_KEYWORDS):
        return "google"
    if _contains_keyword(signal_text, CLAUDE_KEYWORDS):
        return "claude"
    if _contains_keyword(signal_text, CODEX_KEYWORDS):
        return "codex"
    return DEFAULT_VENDOR
