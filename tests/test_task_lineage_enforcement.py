from __future__ import annotations

from pathlib import Path

import pytest

from app.orchestrator import task_factory
from app.orchestrator import task_state_store


def _configure_state_backend(monkeypatch, tmp_path: Path) -> Path:
    monkeypatch.setattr(task_state_store, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )
    task_factory.clear_task_runtime_store()
    return tmp_path / "data" / "task_system.json"


def _create_office_task(
    *,
    store_path: Path,
    task_id: str,
    task_type: str,
    parent_task_id: str = "",
) -> dict[str, object]:
    task_input: dict[str, object] = {
        "task_id": task_id,
        "status": "created",
        "intent": "generic_task",
        "task_type": task_type,
        "payload": {"summary": task_type},
    }
    if parent_task_id:
        task_input["parent_task_id"] = parent_task_id
        task_input["payload"] = {
            "summary": task_type,
            "parent_task_id": parent_task_id,
        }
    return task_factory.create_task(task_input, store_path=store_path)


def test_valid_office_lineage_chain_is_accepted(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)

    lead = _create_office_task(
        store_path=store_path,
        task_id="DF-LINEAGE-LEAD-V1",
        task_type="lead",
    )
    estimate = _create_office_task(
        store_path=store_path,
        task_id="DF-LINEAGE-ESTIMATE-V1",
        task_type="estimate",
        parent_task_id=str(lead["task_id"]),
    )
    follow_up = _create_office_task(
        store_path=store_path,
        task_id="DF-LINEAGE-FOLLOWUP-V1",
        task_type="follow_up",
        parent_task_id=str(estimate["task_id"]),
    )
    permit = _create_office_task(
        store_path=store_path,
        task_id="DF-LINEAGE-PERMIT-V1",
        task_type="permit",
        parent_task_id=str(follow_up["task_id"]),
    )
    project = _create_office_task(
        store_path=store_path,
        task_id="DF-LINEAGE-PROJECT-V1",
        task_type="project",
        parent_task_id=str(permit["task_id"]),
    )
    procurement = _create_office_task(
        store_path=store_path,
        task_id="DF-LINEAGE-PROCUREMENT-V1",
        task_type="procurement",
        parent_task_id=str(project["task_id"]),
    )
    payment = _create_office_task(
        store_path=store_path,
        task_id="DF-LINEAGE-PAYMENT-V1",
        task_type="payment",
        parent_task_id=str(project["task_id"]),
    )

    assert lead["task_type"] == "lead"
    assert estimate["parent_task_type"] == "lead"
    assert follow_up["parent_task_type"] == "estimate"
    assert permit["parent_task_type"] == "follow_up"
    assert project["parent_task_type"] == "permit"
    assert procurement["parent_task_type"] == "project"
    assert payment["parent_task_type"] == "project"


def test_invalid_office_lineage_transition_is_rejected(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    lead = _create_office_task(
        store_path=store_path,
        task_id="DF-LINEAGE-INVALID-LEAD-V1",
        task_type="lead",
    )

    with pytest.raises(
        ValueError,
        match="policy gate blocked task creation: invalid office lineage transition: lead -> project",
    ):
        _create_office_task(
            store_path=store_path,
            task_id="DF-LINEAGE-INVALID-PROJECT-V1",
            task_type="project",
            parent_task_id=str(lead["task_id"]),
        )


def test_orphan_office_task_is_rejected(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)

    with pytest.raises(
        ValueError,
        match="policy gate blocked task creation: office task `permit` requires parent_task_id",
    ):
        _create_office_task(
            store_path=store_path,
            task_id="DF-LINEAGE-ORPHAN-PERMIT-V1",
            task_type="permit",
        )


def test_duplicate_invalid_child_is_rejected(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    lead = _create_office_task(
        store_path=store_path,
        task_id="DF-LINEAGE-DUP-LEAD-V1",
        task_type="lead",
    )
    _create_office_task(
        store_path=store_path,
        task_id="DF-LINEAGE-DUP-ESTIMATE-1",
        task_type="estimate",
        parent_task_id=str(lead["task_id"]),
    )

    with pytest.raises(
        ValueError,
        match="policy gate blocked task creation: duplicate office child task type not allowed: lead -> estimate",
    ):
        _create_office_task(
            store_path=store_path,
            task_id="DF-LINEAGE-DUP-ESTIMATE-2",
            task_type="estimate",
            parent_task_id=str(lead["task_id"]),
        )


def test_non_office_task_behavior_remains_unchanged(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)

    task = task_factory.create_task(
        {
            "task_id": "DF-LINEAGE-GENERIC-V1",
            "status": "created",
            "intent": "generic_task",
            "payload": {"summary": "unchanged"},
        },
        store_path=store_path,
    )

    assert task["intent"] == "generic_task"
    assert task.get("task_type", "") == ""
    assert task.get("parent_task_id", "") == ""


def test_create_task_blocks_conflicting_active_decision_before_persist(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)

    task_factory.create_task(
        {
            "task_id": "DF-CONFLICT-ACTIVE-V1",
            "status": "created",
            "intent": "business_signal_response",
            "task_type": "lead",
            "payload": {
                "summary": "active decision",
                "decision": {
                    "decision_id": "dec-active",
                    "decision_type": "resource_lock",
                    "priority": "high",
                },
                "recommended_action": "request_more_reviews",
                "resource_id": "crew-west",
                "priority": "high",
                "domain": "operations",
            },
        },
        store_path=store_path,
    )

    with pytest.raises(
        ValueError,
        match="policy gate blocked task creation: decision conflict flagged: resource conflict with active decision DF-CONFLICT-ACTIVE-V1",
    ):
        task_factory.create_task(
            {
                "task_id": "DF-CONFLICT-NEW-V1",
                "status": "created",
                "intent": "business_signal_response",
                "task_type": "lead",
                "payload": {
                    "summary": "new decision",
                    "decision": {
                        "decision_id": "dec-new",
                        "decision_type": "resource_lock",
                        "priority": "high",
                    },
                    "recommended_action": "request_more_reviews",
                    "resource_id": "crew-west",
                    "priority": "high",
                    "domain": "operations",
                },
            },
            store_path=store_path,
        )


def test_create_task_blocks_resource_conflict_before_persist(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)

    with pytest.raises(
        ValueError,
        match="policy gate blocked task creation: alternative_option",
    ):
        task_factory.create_task(
            {
                "task_id": "DF-RESOURCE-CREATE-BLOCK-V1",
                "status": "created",
                "intent": "generic_task",
                "payload": {
                    "summary": "blocked resource",
                    "resource": {
                        "id": "crew-west",
                        "type": "crew",
                        "availability": "unavailable",
                        "current_load": 0,
                        "max_load": 2,
                    },
                    "candidate_resources": [
                        {
                            "id": "crew-east",
                            "type": "crew",
                            "availability": "available",
                            "current_load": 0,
                            "max_load": 2,
                        }
                    ],
                },
            },
            store_path=store_path,
        )
