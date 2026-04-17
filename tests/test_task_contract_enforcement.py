from __future__ import annotations

from pathlib import Path

import pytest

from app.execution.task_schema import TASK_CONTRACT_VERSION
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


def test_create_task_persists_contract_version(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)

    created = task_factory.create_task(
        {
            "task_id": "DF-CONTRACT-CREATE-V1",
            "status": "created",
            "intent": "new_lead",
            "payload": {"summary": "contract-boundary"},
        },
        store_path=store_path,
    )

    assert created["task_contract_version"] == TASK_CONTRACT_VERSION
    assert created["history"][0]["event"] == "created"


def test_save_task_rejects_unsupported_top_level_field(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    created = task_factory.create_task(
        {
            "task_id": "DF-CONTRACT-EXTRA-FIELD-V1",
            "status": "created",
            "intent": "new_lead",
            "payload": {"summary": "contract-boundary"},
        },
        store_path=store_path,
    )
    created["unexpected_field"] = "drift"

    with pytest.raises(ValueError, match="task contains unsupported fields: unexpected_field"):
        task_factory.save_task(created, store_path)


def test_save_task_rejects_previously_silently_dropped_fields(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    created = task_factory.create_task(
        {
            "task_id": "DF-CONTRACT-SILENT-DROP-V1",
            "status": "created",
            "intent": "new_lead",
            "payload": {"summary": "contract-boundary"},
        },
        store_path=store_path,
    )
    created["source"] = "voice"

    with pytest.raises(ValueError, match="task contains unsupported fields: source"):
        task_factory.save_task(created, store_path)


def test_task_state_store_write_rejects_contract_bypass(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)

    with pytest.raises(ValueError, match="task contains unsupported fields: unexpected_field"):
        task_state_store.write_task(
            {
                "task_contract_version": TASK_CONTRACT_VERSION,
                "task_id": "DF-CONTRACT-BYPASS-V1",
                "created_at": "2026-04-04T00:00:00Z",
                "intent": "new_lead",
                "payload": {"summary": "contract-boundary"},
                "status": "created",
                "notes": [],
                "history": [],
                "unexpected_field": "blocked",
            },
            store_path=store_path,
        )


def test_load_tasks_rejects_stored_contract_drift(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    task_factory.create_task(
        {
            "task_id": "DF-CONTRACT-DRIFT-V1",
            "status": "created",
            "intent": "new_lead",
            "payload": {"summary": "stable"},
        },
        store_path=store_path,
    )

    def inject_drift(connection) -> None:
        connection.execute(
            """
            UPDATE Task
            SET descriptor = ?
            WHERE task_id = ?
            """,
            (
                (
                    '{"task_contract_version":1,"task_id":"DF-CONTRACT-DRIFT-V1",'
                    '"created_at":"2026-04-04T00:00:00Z","intent":"new_lead",'
                    '"payload":{"summary":"stable"},"status":"created","notes":[],'
                    '"history":[],"unexpected_field":"drift"}'
                ),
                "DF-CONTRACT-DRIFT-V1",
            ),
        )

    task_state_store.run_in_transaction(inject_drift, store_path=store_path)
    task_factory.clear_task_runtime_store()

    with pytest.raises(ValueError, match="task contains unsupported fields: unexpected_field"):
        task_factory.load_tasks(store_path)


def test_create_task_rejects_invalid_initial_status(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)

    with pytest.raises(
        ValueError,
        match="task must be created with status `created` or `awaiting_approval`",
    ):
        task_factory.create_task(
            {
                "task_id": "DF-CONTRACT-STATUS-V1",
                "status": "queued",
                "intent": "new_lead",
                "payload": {"summary": "contract-boundary"},
            },
            store_path=store_path,
        )


def test_create_task_accepts_awaiting_approval_contract(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)

    created = task_factory.create_task(
        {
            "task_id": "DF-CONTRACT-APPROVAL-V1",
            "status": "awaiting_approval",
            "intent": "business_signal_response",
            "execution_mode": "confirmation",
            "approval_status": "pending",
            "payload": {"summary": "await approval"},
        },
        store_path=store_path,
    )

    assert created["status"] == "AWAITING_APPROVAL"
    assert created["approval_status"] == "pending"


def test_apply_task_approval_transitions_task_to_validated(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    created = task_factory.create_task(
        {
            "task_id": "DF-CONTRACT-APPROVAL-APPLY-V1",
            "status": "awaiting_approval",
            "intent": "business_signal_response",
            "execution_mode": "confirmation",
            "approval_status": "pending",
            "payload": {"summary": "await approval"},
        },
        store_path=store_path,
    )

    approved = task_factory.apply_task_approval(
        created["task_id"],
        approved=True,
        approved_by="dev_officer",
        store_path=store_path,
    )

    assert approved["status"] == "VALIDATED"
    assert approved["approval_status"] == "approved"
    assert approved["approved_by"] == "dev_officer"
