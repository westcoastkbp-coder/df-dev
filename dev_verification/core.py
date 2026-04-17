from __future__ import annotations

import json
from collections.abc import Mapping, Sequence


INVARIANTS = (
    "task_is_only_execution_boundary",
    "no_execution_without_policy",
    "deterministic_output",
    "no_state_mismatch",
    "no_duplicate_actions",
    "fail_safely_under_imperfect_inputs",
    "no_false_success",
    "no_continue_on_invalid_state",
)


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _clone_json_like(value: object) -> object:
    if isinstance(value, Mapping):
        return {
            _normalize_text(key): _clone_json_like(item)
            for key, item in value.items()
            if _normalize_text(key)
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_clone_json_like(item) for item in value]
    return value


def _stable_json(value: object) -> str:
    return json.dumps(
        _clone_json_like(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )


def _as_mapping(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    return {}


def _as_list(value: object) -> list[object]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return list(value)
    return []


def _normalize_scenario(scenario: Mapping[str, object]) -> dict[str, object]:
    normalized = _clone_json_like(scenario)
    if not isinstance(normalized, dict):
        raise TypeError("scenario must normalize to a dict")
    return normalized


def _task_id_from_scenario(scenario: Mapping[str, object]) -> str:
    task = _as_mapping(scenario.get("task"))
    return _normalize_text(task.get("task_id") or scenario.get("task_id"))


def _result_payload(scenario: Mapping[str, object]) -> object:
    if "result" in scenario:
        return scenario.get("result")
    return scenario.get("output")


def _is_executed(scenario: Mapping[str, object]) -> bool:
    if "executed" in scenario:
        return bool(scenario.get("executed"))
    return bool(_as_list(scenario.get("actions")) or _result_payload(scenario) is not None)


def _policy_allows_execution(scenario: Mapping[str, object]) -> bool:
    policy = scenario.get("policy")
    if isinstance(policy, Mapping):
        if "allowed" in policy:
            return bool(policy.get("allowed"))
        if "execution_allowed" in policy:
            return bool(policy.get("execution_allowed"))
        return bool(policy)
    return bool(policy)


def _action_signature(action: object) -> str:
    if isinstance(action, Mapping):
        action_map = _as_mapping(action)
        if action_map.get("action_id"):
            return f"id:{_normalize_text(action_map.get('action_id'))}"
        return f"payload:{_stable_json(action_map)}"
    return f"value:{_stable_json(action)}"


def _safe_statuses() -> set[str]:
    return {"blocked", "degraded", "failed", "halted", "partial"}


def _normalized_status(value: object) -> str:
    return _normalize_text(value).lower()


def _missing_required_data(scenario: Mapping[str, object]) -> list[str]:
    required_fields = _as_list(scenario.get("required_data_fields"))
    payload = _as_mapping(scenario.get("input_data"))
    missing: list[str] = []
    for field in required_fields:
        key = _normalize_text(field)
        if not key:
            continue
        if key not in payload or _normalize_text(payload.get(key)) == "":
            missing.append(key)
    return sorted(missing)


def _is_imperfect_condition(scenario: Mapping[str, object]) -> bool:
    if _missing_required_data(scenario):
        return True
    if bool(scenario.get("delayed_updates")):
        return True
    if bool(scenario.get("inconsistent_signals")):
        return True
    if bool(scenario.get("partial_execution")):
        return True
    return False


def _result_status(scenario: Mapping[str, object]) -> str:
    return _normalized_status(_as_mapping(_result_payload(scenario)).get("status"))


def check_invariants(scenario: Mapping[str, object]) -> list[str]:
    normalized = _normalize_scenario(scenario)
    failures: list[str] = []

    task_id = _task_id_from_scenario(normalized)
    execution = _as_mapping(normalized.get("execution"))
    boundary = _normalize_text(execution.get("boundary") or normalized.get("execution_boundary"))
    result = _as_mapping(_result_payload(normalized))
    result_task_id = _normalize_text(result.get("task_id"))

    if not task_id:
        failures.append("Task is only execution boundary violated: missing task_id")
    elif boundary and boundary.lower() != "task":
        failures.append(
            f"Task is only execution boundary violated: boundary={boundary}"
        )
    elif result_task_id and result_task_id != task_id:
        failures.append(
            "Task is only execution boundary violated: result task_id does not match scenario task_id"
        )

    if _is_executed(normalized) and not _policy_allows_execution(normalized):
        failures.append("No execution without policy violated: execution occurred without policy")

    replay_output = normalized.get("replay_output", _result_payload(normalized))
    if _stable_json(_result_payload(normalized)) != _stable_json(replay_output):
        failures.append("Deterministic output violated: replay output does not match")

    expected_state = normalized.get("expected_state", normalized.get("final_state"))
    final_state = normalized.get("final_state", {})
    if _stable_json(expected_state) != _stable_json(final_state):
        failures.append("No state mismatch violated: final_state does not match expected_state")
    else:
        final_task_id = _normalize_text(_as_mapping(final_state).get("task_id"))
        if task_id and final_task_id and final_task_id != task_id:
            failures.append("No state mismatch violated: final_state task_id does not match scenario task_id")

    action_signatures: list[str] = []
    for action in _as_list(normalized.get("actions")):
        action_signatures.append(_action_signature(action))
    if len(action_signatures) != len(set(action_signatures)):
        failures.append("No duplicate actions violated: duplicate action detected")

    missing_data = _missing_required_data(normalized)
    result_status = _result_status(normalized)
    final_state = _as_mapping(normalized.get("final_state"))
    final_state_status = _normalized_status(final_state.get("status"))
    invalid_state = bool(normalized.get("invalid_state")) or final_state_status == "invalid"
    continued_after_invalid_state = bool(normalized.get("continued_after_invalid_state"))
    safe_statuses = _safe_statuses()

    if missing_data:
        if result_status == "success":
            failures.append(
                "No false success violated: missing data produced success"
            )
        if _is_executed(normalized) and result_status not in safe_statuses:
            failures.append(
                "Fail safely under imperfect inputs violated: missing data did not halt safely"
            )

    if normalized.get("delayed_updates"):
        state_before_update = normalized.get("state_before_update", final_state)
        if _stable_json(state_before_update) != _stable_json(final_state):
            if result_status == "success":
                failures.append(
                    "No false success violated: delayed update produced success before state convergence"
                )

    if normalized.get("inconsistent_signals"):
        if result_status == "success":
            failures.append(
                "No false success violated: inconsistent signals produced success"
            )
        if result_status not in safe_statuses:
            failures.append(
                "Fail safely under imperfect inputs violated: inconsistent signals did not resolve safely"
            )

    if normalized.get("partial_execution"):
        completed_steps = _as_list(normalized.get("completed_steps"))
        planned_steps = _as_list(normalized.get("planned_steps"))
        if planned_steps and len(completed_steps) < len(planned_steps):
            if result_status == "success":
                failures.append(
                    "No false success violated: partial execution produced success"
                )
            if result_status not in safe_statuses:
                failures.append(
                    "Fail safely under imperfect inputs violated: partial execution did not report safe failure"
                )

    if invalid_state:
        if continued_after_invalid_state or _is_executed(normalized):
            failures.append(
                "No continue on invalid state violated: execution continued after invalid state"
            )

    return failures


def detect_failure(failures: Sequence[str]) -> str:
    return str(failures[0]) if failures else ""


def generate_report(
    *,
    scenario_name: object = "",
    task_id: object = "",
    failures: Sequence[str],
    result: object = None,
) -> dict[str, object]:
    normalized_failures = [str(item) for item in failures]
    failure_reason = detect_failure(normalized_failures)
    report = {
        "scenario": _normalize_text(scenario_name),
        "task_id": _normalize_text(task_id),
        "status": "FAIL" if normalized_failures else "PASS",
        "failure_reason": failure_reason,
        "failures": normalized_failures,
        "result": _clone_json_like(result),
        "invariants_checked": list(INVARIANTS),
    }
    return report


def run_scenario(scenario: Mapping[str, object]) -> dict[str, object]:
    normalized = _normalize_scenario(scenario)
    failures = check_invariants(normalized)
    return generate_report(
        scenario_name=normalized.get("name"),
        task_id=_task_id_from_scenario(normalized),
        failures=failures,
        result=_result_payload(normalized),
    )


def assert_result(
    report: Mapping[str, object],
    *,
    expected_status: str,
    expected_failure_reason: str = "",
) -> None:
    actual_status = _normalize_text(report.get("status")).upper()
    normalized_expected_status = _normalize_text(expected_status).upper()
    if actual_status != normalized_expected_status:
        raise AssertionError(
            f"expected status {normalized_expected_status}, got {actual_status or '(empty)'}"
        )

    actual_failure_reason = _normalize_text(report.get("failure_reason"))
    normalized_expected_reason = _normalize_text(expected_failure_reason)
    if normalized_expected_reason and actual_failure_reason != normalized_expected_reason:
        raise AssertionError(
            f"expected failure reason {normalized_expected_reason!r}, got {actual_failure_reason!r}"
        )
