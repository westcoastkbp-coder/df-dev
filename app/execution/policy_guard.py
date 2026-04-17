from __future__ import annotations

from collections.abc import Mapping

from app.execution.context_types import ContextSnapshot
from app.execution.system_context import build_system_rules_envelope, load_system_context


class PolicyViolationError(ValueError):
    pass


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _as_mapping(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _collect_policy_violations(
    plan: Mapping[str, object],
    snapshot: ContextSnapshot,
) -> tuple[str, ...]:
    payload = dict(plan)
    violations: list[str] = []
    target_environment = _normalize_text(
        payload.get("target_environment") or payload.get("environment")
    )
    treat_as_dev_environment = bool(
        payload.get("treat_as_dev_environment") or payload.get("is_dev_environment")
    )
    code_generation_requested = bool(
        payload.get("allow_code_generation") or payload.get("code_generation")
    )

    if (
        target_environment == snapshot.system.product_environment
        and treat_as_dev_environment
    ):
        violations.append("product box cannot be treated as a dev environment")

    if (
        target_environment == snapshot.system.product_environment
        and code_generation_requested
        and not snapshot.product_box.code_generation_allowed
    ):
        violations.append("code generation is forbidden inside product box")

    assumptions = _as_mapping(payload.get("assumptions"))
    session_context = _as_mapping(payload.get("session_context"))
    session_product_box = _as_mapping(session_context.get("product_box"))

    if bool(assumptions.get("product_box_is_dev_environment")) or bool(
        session_product_box.get("is_dev_environment")
    ):
        violations.append("system context overrides session assumption about product box dev status")

    if bool(assumptions.get("product_box_code_generation_allowed")) or bool(
        session_product_box.get("code_generation_allowed")
    ):
        violations.append("system context overrides session assumption about product box code generation")

    if assumptions.get("strict_separation") is False:
        violations.append("system context overrides weaker session isolation assumptions")

    return tuple(dict.fromkeys(violations))


def validate_plan_against_system_context(
    plan: Mapping[str, object],
    snapshot: ContextSnapshot,
) -> None:
    violations = _collect_policy_violations(plan, snapshot)
    if violations:
        raise PolicyViolationError("; ".join(violations))


def load_validated_system_context(
    plan: Mapping[str, object],
) -> tuple[ContextSnapshot, dict[str, object]]:
    context = load_system_context()
    snapshot = context.snapshot()
    validate_plan_against_system_context(plan, snapshot)
    return snapshot, build_system_rules_envelope(snapshot)

