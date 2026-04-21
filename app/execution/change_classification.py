from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Iterable

from app.execution.role_routing import route_for_change_type

CRITICAL_PATHS = (
    "AGENTS.md",
    ".githooks",
    ".github/workflows",
    "core",
    "memory",
    "orchestrator",
)

REVIEW_PATHS = (
    "Makefile",
    "scripts",
)

CONFIG_SUFFIXES = {
    ".cfg",
    ".conf",
    ".ini",
    ".json",
    ".toml",
    ".yaml",
    ".yml",
}

RISK_LEVELS = {
    "safe": "low",
    "review": "medium",
    "critical": "high",
}

COMMIT_PREFIXES = {
    "safe": "",
    "review": "[review] ",
    "critical": "[critical] ",
}


@dataclass(frozen=True)
class ChangeClassification:
    change_type: str
    risk_level: str
    affected_areas: tuple[str, ...]
    changed_paths: tuple[str, ...]
    classified_paths: tuple[str, ...]
    recommended_commit_prefix: str
    roles_used: tuple[str, ...]
    decision_path: tuple[str, ...]
    reviewer_required: bool
    qa_repeat_allowed: bool

    @property
    def has_changes(self) -> bool:
        return bool(self.changed_paths)


def normalize_path(path: str) -> str:
    normalized = path.replace("\\", "/").strip()
    if normalized.startswith("./"):
        normalized = normalized[2:]
    return PurePosixPath(normalized).as_posix()


def is_descendant(path: str, prefix: str) -> bool:
    return path == prefix or path.startswith(f"{prefix}/")


def is_critical_path(path: str) -> bool:
    return any(is_descendant(path, prefix) for prefix in CRITICAL_PATHS)


def is_config_path(path: str) -> bool:
    normalized = normalize_path(path)
    pure_path = PurePosixPath(normalized)
    name = pure_path.name

    if name.startswith(".env"):
        return True

    return pure_path.suffix.lower() in CONFIG_SUFFIXES


def is_review_path(path: str) -> bool:
    return any(
        is_descendant(path, prefix) for prefix in REVIEW_PATHS
    ) or is_config_path(path)


def affected_area(path: str) -> str:
    normalized = normalize_path(path)
    parts = normalized.split("/", 1)
    return parts[0]


def classify_paths(paths: Iterable[str]) -> ChangeClassification:
    normalized_paths = tuple(
        sorted({normalize_path(path) for path in paths if normalize_path(path)})
    )

    critical_paths = tuple(path for path in normalized_paths if is_critical_path(path))
    review_paths = tuple(path for path in normalized_paths if is_review_path(path))

    if critical_paths:
        change_type = "critical"
        classified_paths = critical_paths
    elif review_paths:
        change_type = "review"
        classified_paths = review_paths
    else:
        change_type = "safe"
        classified_paths = normalized_paths

    route = route_for_change_type(change_type)

    return ChangeClassification(
        change_type=change_type,
        risk_level=RISK_LEVELS[change_type],
        affected_areas=tuple(
            sorted({affected_area(path) for path in normalized_paths})
        ),
        changed_paths=normalized_paths,
        classified_paths=classified_paths,
        recommended_commit_prefix=COMMIT_PREFIXES[change_type],
        roles_used=route.roles_used,
        decision_path=route.decision_path,
        reviewer_required=route.reviewer_required,
        qa_repeat_allowed=route.qa_repeat_allowed,
    )

