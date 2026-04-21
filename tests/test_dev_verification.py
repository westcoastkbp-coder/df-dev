from __future__ import annotations

import pytest

from dev_verification import (
    assert_result,
    check_invariants,
    detect_failure,
    generate_report,
    run_scenario,
)


def test_run_scenario_returns_pass_for_valid_dev_only_scenario() -> None:
    scenario = {
        "name": "dev verification happy path",
        "task": {"task_id": "DF-DEV-VERIFICATION-LAYER-V1"},
        "execution": {"boundary": "task"},
        "policy": {"allowed": True},
        "executed": True,
        "actions": [
            {"action_id": "a-1", "type": "validate"},
            {"action_id": "a-2", "type": "report"},
        ],
        "result": {
            "task_id": "DF-DEV-VERIFICATION-LAYER-V1",
            "status": "completed",
            "payload": {"summary": "ok"},
        },
        "replay_output": {
            "task_id": "DF-DEV-VERIFICATION-LAYER-V1",
            "status": "completed",
            "payload": {"summary": "ok"},
        },
        "final_state": {
            "task_id": "DF-DEV-VERIFICATION-LAYER-V1",
            "status": "verified",
        },
        "expected_state": {
            "task_id": "DF-DEV-VERIFICATION-LAYER-V1",
            "status": "verified",
        },
    }

    report = run_scenario(scenario)

    assert report["status"] == "PASS"
    assert report["failure_reason"] == ""
    assert report["failures"] == []
    assert_result(report, expected_status="PASS")


@pytest.mark.parametrize(
    ("scenario", "expected_reason"),
    [
        (
            {
                "task": {"task_id": "DF-DEV-BOUNDARY-V1"},
                "execution": {"boundary": "session"},
                "policy": {"allowed": True},
                "executed": True,
                "result": {"task_id": "DF-DEV-BOUNDARY-V1"},
                "final_state": {"task_id": "DF-DEV-BOUNDARY-V1"},
            },
            "Task is only execution boundary violated: boundary=session",
        ),
        (
            {
                "task": {"task_id": "DF-DEV-POLICY-V1"},
                "execution": {"boundary": "task"},
                "policy": {"allowed": False},
                "executed": True,
                "result": {"task_id": "DF-DEV-POLICY-V1"},
                "final_state": {"task_id": "DF-DEV-POLICY-V1"},
            },
            "No execution without policy violated: execution occurred without policy",
        ),
        (
            {
                "task": {"task_id": "DF-DEV-DETERMINISTIC-V1"},
                "execution": {"boundary": "task"},
                "policy": {"allowed": True},
                "executed": True,
                "actions": [{"action_id": "a-1", "type": "write"}],
                "result": {"task_id": "DF-DEV-DETERMINISTIC-V1", "value": "first"},
                "replay_output": {
                    "task_id": "DF-DEV-DETERMINISTIC-V1",
                    "value": "second",
                },
                "final_state": {"task_id": "DF-DEV-DETERMINISTIC-V1"},
            },
            "Deterministic output violated: replay output does not match",
        ),
        (
            {
                "task": {"task_id": "DF-DEV-DUPLICATE-V1"},
                "execution": {"boundary": "task"},
                "policy": {"allowed": True},
                "executed": True,
                "actions": [
                    {"action_id": "dup-1", "type": "write"},
                    {"action_id": "dup-1", "type": "write"},
                ],
                "result": {"task_id": "DF-DEV-DUPLICATE-V1"},
                "final_state": {"task_id": "DF-DEV-DUPLICATE-V1"},
            },
            "No duplicate actions violated: duplicate action detected",
        ),
        (
            {
                "task": {"task_id": "DF-DEV-STATE-V1"},
                "execution": {"boundary": "task"},
                "policy": {"allowed": True},
                "executed": True,
                "result": {"task_id": "DF-DEV-STATE-V1"},
                "final_state": {"task_id": "DF-DEV-STATE-V1", "status": "stale"},
                "expected_state": {"task_id": "DF-DEV-STATE-V1", "status": "verified"},
            },
            "No state mismatch violated: final_state does not match expected_state",
        ),
    ],
)
def test_run_scenario_returns_fail_with_requested_reason(
    scenario: dict[str, object],
    expected_reason: str,
) -> None:
    report = run_scenario(scenario)

    assert report["status"] == "FAIL"
    assert report["failure_reason"] == expected_reason
    assert_result(
        report,
        expected_status="FAIL",
        expected_failure_reason=expected_reason,
    )


def test_check_invariants_and_generate_report_are_stable() -> None:
    scenario = {
        "name": "deterministic report",
        "task": {"task_id": "DF-DEV-STABLE-V1"},
        "execution": {"boundary": "task"},
        "policy": {"allowed": True},
        "executed": True,
        "actions": [{"action_id": "a-1", "type": "noop"}],
        "result": {"task_id": "DF-DEV-STABLE-V1", "status": "completed"},
        "final_state": {"task_id": "DF-DEV-STABLE-V1", "status": "verified"},
    }

    failures = check_invariants(scenario)
    first = generate_report(
        scenario_name=scenario["name"],
        task_id="DF-DEV-STABLE-V1",
        failures=failures,
        result=scenario["result"],
    )
    second = generate_report(
        scenario_name=scenario["name"],
        task_id="DF-DEV-STABLE-V1",
        failures=failures,
        result=scenario["result"],
    )

    assert failures == []
    assert detect_failure(failures) == ""
    assert first == second


@pytest.mark.parametrize(
    ("scenario", "expected_reason"),
    [
        (
            {
                "name": "missing data should fail safely",
                "task": {"task_id": "DF-REALITY-MISSING-DATA-V1"},
                "execution": {"boundary": "task"},
                "policy": {"allowed": True},
                "executed": True,
                "required_data_fields": ["customer_id", "scope"],
                "input_data": {"scope": "adu conversion"},
                "result": {
                    "task_id": "DF-REALITY-MISSING-DATA-V1",
                    "status": "success",
                },
                "final_state": {
                    "task_id": "DF-REALITY-MISSING-DATA-V1",
                    "status": "running",
                },
            },
            "No false success violated: missing data produced success",
        ),
        (
            {
                "name": "delayed update should not claim success early",
                "task": {"task_id": "DF-REALITY-DELAYED-UPDATES-V1"},
                "execution": {"boundary": "task"},
                "policy": {"allowed": True},
                "executed": True,
                "delayed_updates": True,
                "state_before_update": {
                    "task_id": "DF-REALITY-DELAYED-UPDATES-V1",
                    "status": "pending",
                },
                "final_state": {
                    "task_id": "DF-REALITY-DELAYED-UPDATES-V1",
                    "status": "verified",
                },
                "result": {
                    "task_id": "DF-REALITY-DELAYED-UPDATES-V1",
                    "status": "success",
                },
            },
            "No false success violated: delayed update produced success before state convergence",
        ),
        (
            {
                "name": "inconsistent signals should not pass",
                "task": {"task_id": "DF-REALITY-INCONSISTENT-SIGNALS-V1"},
                "execution": {"boundary": "task"},
                "policy": {"allowed": True},
                "executed": True,
                "inconsistent_signals": True,
                "result": {
                    "task_id": "DF-REALITY-INCONSISTENT-SIGNALS-V1",
                    "status": "success",
                },
                "final_state": {
                    "task_id": "DF-REALITY-INCONSISTENT-SIGNALS-V1",
                    "status": "running",
                },
            },
            "No false success violated: inconsistent signals produced success",
        ),
        (
            {
                "name": "partial execution should not pass",
                "task": {"task_id": "DF-REALITY-PARTIAL-EXECUTION-V1"},
                "execution": {"boundary": "task"},
                "policy": {"allowed": True},
                "executed": True,
                "partial_execution": True,
                "planned_steps": ["validate", "execute", "report"],
                "completed_steps": ["validate"],
                "result": {
                    "task_id": "DF-REALITY-PARTIAL-EXECUTION-V1",
                    "status": "success",
                },
                "final_state": {
                    "task_id": "DF-REALITY-PARTIAL-EXECUTION-V1",
                    "status": "running",
                },
            },
            "No false success violated: partial execution produced success",
        ),
        (
            {
                "name": "invalid state must halt execution",
                "task": {"task_id": "DF-REALITY-INVALID-STATE-V1"},
                "execution": {"boundary": "task"},
                "policy": {"allowed": True},
                "executed": True,
                "invalid_state": True,
                "continued_after_invalid_state": True,
                "result": {
                    "task_id": "DF-REALITY-INVALID-STATE-V1",
                    "status": "failed",
                },
                "final_state": {
                    "task_id": "DF-REALITY-INVALID-STATE-V1",
                    "status": "invalid",
                },
            },
            "No continue on invalid state violated: execution continued after invalid state",
        ),
    ],
)
def test_imperfect_real_world_conditions_fail_for_the_right_reason(
    scenario: dict[str, object],
    expected_reason: str,
) -> None:
    report = run_scenario(scenario)

    assert report["status"] == "FAIL"
    assert report["failure_reason"] == expected_reason
    assert_result(
        report,
        expected_status="FAIL",
        expected_failure_reason=expected_reason,
    )


def test_imperfect_inputs_can_pass_when_system_fails_safely() -> None:
    scenario = {
        "name": "safe degradation with missing data",
        "task": {"task_id": "DF-REALITY-SAFE-FAIL-V1"},
        "execution": {"boundary": "task"},
        "policy": {"allowed": True},
        "executed": True,
        "required_data_fields": ["customer_id", "scope"],
        "input_data": {"scope": "adu conversion"},
        "result": {"task_id": "DF-REALITY-SAFE-FAIL-V1", "status": "blocked"},
        "final_state": {"task_id": "DF-REALITY-SAFE-FAIL-V1", "status": "blocked"},
        "expected_state": {"task_id": "DF-REALITY-SAFE-FAIL-V1", "status": "blocked"},
    }

    report = run_scenario(scenario)

    assert report["status"] == "PASS"
    assert report["failure_reason"] == ""
