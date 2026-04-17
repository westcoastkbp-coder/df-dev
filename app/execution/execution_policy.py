from __future__ import annotations

from typing import Iterable

LINEAR_PIPELINE_STAGES = ("format", "validate", "commit")
TIMEOUT_PYTEST = 120
TIMEOUT_HOOKS = 15
MAX_EXECUTION_TIME = 10
MAX_RETRY_ATTEMPTS = 2
DEFAULT_VALIDATION_TIMEOUT_SECONDS = 60


def pipeline_is_linear(stages: Iterable[object]) -> bool:
    normalized = tuple(str(stage or "").strip().lower() for stage in stages)
    return normalized == LINEAR_PIPELINE_STAGES


def bounded_pytest_timeout(
    *,
    validation_timeout_seconds: int,
    test_files_count: int,
) -> int:
    normalized_count = max(1, int(test_files_count))
    return min(
        TIMEOUT_PYTEST,
        max(
            int(validation_timeout_seconds),
            DEFAULT_VALIDATION_TIMEOUT_SECONDS * normalized_count,
        ),
    )
