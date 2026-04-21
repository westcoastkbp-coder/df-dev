from __future__ import annotations

import pytest

from tests.office_scenario_pack import (
    SCENARIOS,
    configure_scenario_runtime,
    run_scenario,
)


@pytest.mark.parametrize(
    "scenario", SCENARIOS, ids=[scenario.scenario_id for scenario in SCENARIOS]
)
def test_office_scenario_pack_matches_expected_flow(
    monkeypatch, tmp_path, scenario
) -> None:
    store_path = configure_scenario_runtime(monkeypatch, tmp_path)

    output = run_scenario(scenario, store_path=store_path)

    assert output["scenario_id"] == scenario.scenario_id
    assert output["task_sequence"] == scenario.expected_task_sequence
    assert output["transitions"] == scenario.expected_transitions
    assert output["actions"] == scenario.expected_actions
    assert output["final_state"] == scenario.expected_final_state


@pytest.mark.parametrize(
    "scenario", SCENARIOS, ids=[scenario.scenario_id for scenario in SCENARIOS]
)
def test_office_scenario_pack_uses_only_allowed_status_progression(
    monkeypatch, tmp_path, scenario
) -> None:
    store_path = configure_scenario_runtime(monkeypatch, tmp_path)

    output = run_scenario(scenario, store_path=store_path)

    for transition in output["transitions"]:
        _, states = transition.split(":", 1)
        current_status, next_status = states.split("->", 1)
        assert (current_status, next_status) in {
            ("created", "confirmed"),
            ("confirmed", "pending"),
            ("pending", "running"),
            ("running", "completed"),
        }


def test_office_scenario_pack_detected_gaps_are_documented() -> None:
    detected_gaps = {
        scenario.scenario_id: scenario.detected_gap
        for scenario in SCENARIOS
        if scenario.detected_gap
    }

    assert detected_gaps == {
        "permit_required_to_project": "No dedicated permit-submission action exists; scenario validates structural task flow and lifecycle only.",
        "project_to_procurement_execution": "No separate execution task type exists; execution is representable only as procurement lifecycle completion.",
        "project_to_payment": "No payment-specific execution logic exists; scenario validates explicit task lineage and lifecycle only.",
    }
