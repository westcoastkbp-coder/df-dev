from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class HybridServiceBoundary:
    local_only: tuple[str, ...]
    remote_capable: tuple[str, ...]
    shared_state: tuple[str, ...]


HYBRID_SERVICE_BOUNDARY = HybridServiceBoundary(
    local_only=(
        "dev_entrypoint",
        "source_workspace",
        "approval_and_policy_authority",
        "core_architecture_decisions",
        "local_quick_tests",
    ),
    remote_capable=(
        "verification_jobs",
        "runtime_workers",
        "log_aggregation",
        "context_storage",
    ),
    shared_state=(
        "interaction_records",
        "decision_memory",
        "audit_logs",
        "system_context",
        "active_thread_context",
        "global_context",
    ),
)
