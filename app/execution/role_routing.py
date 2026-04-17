from __future__ import annotations

from dataclasses import dataclass

VALID_CHANGE_TYPES = ("safe", "review", "critical")

ROLE_PURPOSES = {
    "Architect": "defines the solution",
    "Implementer": "writes the code",
    "Reviewer": "reviews the changes",
    "QA": "runs tests and checks behavior",
    "Reporter": "records the result",
}

LOGICAL_TO_RUNTIME_ROLE = {
    "Architect": "architect_agent",
    "Implementer": "coder_agent",
    "Reviewer": "reviewer_agent",
    "QA": "qa_agent",
    "Reporter": "memory_agent",
}

RUNTIME_TO_LOGICAL_ROLE = {
    runtime_role: logical_role
    for logical_role, runtime_role in LOGICAL_TO_RUNTIME_ROLE.items()
}


@dataclass(frozen=True)
class RoleRouting:
    change_type: str
    roles_used: tuple[str, ...]
    decision_path: tuple[str, ...]
    reviewer_required: bool
    qa_repeat_allowed: bool

    @property
    def runtime_roles(self) -> tuple[str, ...]:
        return tuple(LOGICAL_TO_RUNTIME_ROLE[role] for role in self.roles_used)

    @property
    def route_summary(self) -> str:
        if self.change_type == "critical":
            return (
                "Architect -> Implementer -> Reviewer -> QA"
                " -> Reviewer -> QA (repeat if needed) -> Reporter"
            )
        return " -> ".join(self.roles_used)


ROUTES = {
    "safe": RoleRouting(
        change_type="safe",
        roles_used=("Implementer", "QA", "Reporter"),
        decision_path=(
            "Implementer -> QA",
            "QA -> Reporter",
        ),
        reviewer_required=False,
        qa_repeat_allowed=False,
    ),
    "review": RoleRouting(
        change_type="review",
        roles_used=("Architect", "Implementer", "Reviewer", "QA", "Reporter"),
        decision_path=(
            "Architect -> Implementer",
            "Implementer -> Reviewer",
            "Reviewer -> QA",
            "QA -> Reporter",
        ),
        reviewer_required=False,
        qa_repeat_allowed=False,
    ),
    "critical": RoleRouting(
        change_type="critical",
        roles_used=("Architect", "Implementer", "Reviewer", "QA", "Reporter"),
        decision_path=(
            "Architect -> Implementer",
            "Implementer -> Reviewer",
            "Reviewer -> QA",
            "QA -> Reviewer (repeat if needed)",
            "Reviewer -> QA (re-validate after repeat)",
            "QA -> Reporter",
        ),
        reviewer_required=True,
        qa_repeat_allowed=True,
    ),
}


def normalize_change_type(change_type: str) -> str:
    normalized = str(change_type or "").strip().lower()
    if normalized not in VALID_CHANGE_TYPES:
        raise ValueError(f"unsupported change_type: {change_type}")
    return normalized


def route_for_change_type(change_type: str) -> RoleRouting:
    return ROUTES[normalize_change_type(change_type)]


def logical_role_for_runtime_role(role: str) -> str:
    return RUNTIME_TO_LOGICAL_ROLE.get(role, role)


def runtime_role_for_logical_role(role: str) -> str:
    return LOGICAL_TO_RUNTIME_ROLE[role]


def build_routed_steps(change_type: str) -> list[dict]:
    route = route_for_change_type(change_type)
    return [
        {
            "step": index,
            "role": runtime_role_for_logical_role(logical_role),
            "logical_role": logical_role,
            "name": logical_role.lower(),
        }
        for index, logical_role in enumerate(route.roles_used, start=1)
    ]


def ordered_subset(expected: tuple[str, ...], provided: tuple[str, ...]) -> bool:
    if not expected:
        return True

    cursor = 0
    for item in provided:
        if item == expected[cursor]:
            cursor += 1
            if cursor == len(expected):
                return True

    return False


def validate_logged_route(
    *, change_type: str, roles_used: list[str], decision_path: list[str]
) -> list[str]:
    route = route_for_change_type(change_type)
    normalized_roles = tuple(role.strip() for role in roles_used if role.strip())
    normalized_path = tuple(step.strip() for step in decision_path if step.strip())
    errors: list[str] = []

    if not normalized_roles:
        errors.append("missing roles_used items")
    else:
        missing_roles = [
            role for role in route.roles_used if role not in normalized_roles
        ]
        if missing_roles:
            errors.append("missing routed role(s): " + ", ".join(missing_roles))
        if not ordered_subset(route.roles_used, normalized_roles):
            errors.append(
                "roles_used must follow routed order: " + " -> ".join(route.roles_used)
            )
        if route.reviewer_required and "Reviewer" not in normalized_roles:
            errors.append("critical route requires `Reviewer` in roles_used")

    if not normalized_path:
        errors.append("missing decision_path items")
    elif not ordered_subset(route.decision_path, normalized_path):
        errors.append(
            "decision_path must document the routed path for "
            f"`{route.change_type}` changes"
        )

    return errors
