from __future__ import annotations

from pathlib import Path

from app.execution.action_contract import (
    build_action_result_contract,
    validate_action_contract,
    validate_action_result_contract,
)
from app.memory import memory_registry
from app.orchestrator import task_memory, task_state_store
from app.voice.voice_orchestrator import VoiceOrchestrator
from app.voice.voice_session import (
    create_response_plan,
    create_voice_session,
    create_voice_trace_metadata,
    create_voice_turn,
)
import app.execution.action_dispatcher as action_dispatcher_module
import runtime.system_log as system_log_module


def _configure_dispatcher_runtime(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        system_log_module,
        "SYSTEM_LOG_FILE",
        tmp_path / "runtime" / "logs" / "system.log",
    )
    monkeypatch.setattr(
        system_log_module,
        "TASK_LOG_FILE",
        tmp_path / "runtime" / "logs" / "tasks.log",
    )
    monkeypatch.setattr(action_dispatcher_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_memory,
        "TASK_MEMORY_FILE",
        tmp_path / "runtime" / "state" / "task_memory.json",
    )
    monkeypatch.setattr(task_state_store, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store,
        "TASK_STATE_DB_FILE",
        Path("runtime/task_state.sqlite3"),
    )
    monkeypatch.setattr(
        memory_registry,
        "REGISTRY_FILE",
        tmp_path / "df-system" / "memory_registry.json",
    )


def test_voice_session_can_be_created() -> None:
    session = create_voice_session(
        caller_id="caller-001",
        channel_type="phone",
        active_language="en-us",
        language_profile={"preferred_locale": "en-US"},
        context_ref="lead:123",
        session_id="voice-session-001",
        started_at="2026-04-14T20:51:00Z",
    )

    assert session.session_id == "voice-session-001"
    assert session.caller_id == "caller-001"
    assert session.channel_type == "phone"
    assert session.active_language == "en-US"
    assert session.language_profile == {"preferred_locale": "en-US"}
    assert session.context_ref == "lead:123"
    assert session.started_at == "2026-04-14T20:51:00Z"
    assert session.last_turn_at == "2026-04-14T20:51:00Z"
    assert session.session_status == "active"


def test_voice_turn_can_be_created_and_linked_to_session() -> None:
    session = create_voice_session(
        caller_id="caller-002",
        channel_type="phone",
        active_language="en-US",
        session_id="voice-session-002",
        started_at="2026-04-14T20:51:00Z",
    )
    response_plan = create_response_plan(
        response_type="spoken_text",
        target_language="en-us",
        text_payload="Call scheduled.",
        action_refs=["voice-action-001"],
        requires_confirmation=False,
        execution_mode="live",
        metadata={"result_status": "success"},
    )
    trace_metadata = create_voice_trace_metadata(
        session_id=session.session_id,
        turn_id="voice-turn-001",
        caller_id=session.caller_id,
        detected_language="en-US",
        action_id="voice-action-001",
        result_status="success",
        started_at="2026-04-14T20:51:01Z",
        completed_at="2026-04-14T20:51:02Z",
        latency_ms=12,
    )

    turn = create_voice_turn(
        turn_id="voice-turn-001",
        session_id=session.session_id,
        input_text=" Call the client tomorrow. ",
        detected_language="en-us",
        normalized_intent_ref="call_client",
        response_plan=response_plan,
        output_text=" Call scheduled. ",
        turn_status="completed",
        created_at="2026-04-14T20:51:01Z",
        trace_metadata=trace_metadata,
    )

    assert turn.session_id == session.session_id
    assert turn.input_text == "Call the client tomorrow."
    assert turn.detected_language == "en-US"
    assert turn.normalized_intent_ref == "call_client"
    assert turn.output_text == "Call scheduled."
    assert turn.response_plan.action_refs == ("voice-action-001",)
    assert turn.trace_metadata is not None
    assert turn.trace_metadata.session_id == session.session_id


def test_voice_orchestrator_routes_through_action_dispatcher(monkeypatch, tmp_path: Path) -> None:
    _configure_dispatcher_runtime(monkeypatch, tmp_path)
    orchestrator = VoiceOrchestrator()

    result = orchestrator.process_turn(
        input_text="Call client tomorrow morning.",
        caller_id="caller-003",
        channel_type="phone",
        active_language="ru-RU",
        detected_language="ru-RU",
        execution_mode="dry_run",
    )

    assert result.action_contract is not None
    assert result.action_result is not None
    assert validate_action_contract(result.action_contract) == result.action_contract
    assert validate_action_result_contract(result.action_result) == result.action_result
    assert result.action_contract["action_type"] == "OPENAI_REQUEST"
    assert result.action_result["result_type"] == "simulation"
    assert result.action_result["payload"]["metadata"]["adapter_used"] == "openai_adapter"
    assert result.turn.response_plan.action_refs == (result.action_contract["action_id"],)


def test_response_plan_is_deterministic_and_contract_bound() -> None:
    dispatched_contracts: list[dict[str, object]] = []

    def dispatcher(action_contract: object) -> dict[str, object]:
        validated = validate_action_contract(action_contract)
        dispatched_contracts.append(validated)
        return build_action_result_contract(
            action_id=str(validated["action_id"]),
            status="success",
            result_type="text_generation",
            payload={
                "text": "  Confirmed follow-up is scheduled.  ",
                "metadata": {
                    "dispatcher_latency_ms": 15,
                },
            },
        )

    result = VoiceOrchestrator(dispatcher=dispatcher).process_turn(
        input_text="Please follow up with the client.",
        caller_id="caller-004",
        channel_type="phone",
        active_language="en-US",
        detected_language="en-US",
    )

    assert len(dispatched_contracts) == 1
    assert result.turn.response_plan.response_type == "spoken_text"
    assert result.turn.response_plan.target_language == "en-US"
    assert result.turn.response_plan.text_payload == "Confirmed follow-up is scheduled."
    assert result.turn.output_text == "Confirmed follow-up is scheduled."
    assert result.turn.response_plan.metadata["result_status"] == "success"
    assert result.turn.response_plan.metadata["latency_ms"] == 15
    assert result.turn.trace_metadata is not None
    assert result.turn.trace_metadata.action_id == dispatched_contracts[0]["action_id"]


def test_trace_metadata_and_multilingual_fields_are_preserved() -> None:
    def dispatcher(action_contract: object) -> dict[str, object]:
        validated = validate_action_contract(action_contract)
        return build_action_result_contract(
            action_id=str(validated["action_id"]),
            status="success",
            result_type="text_generation",
            payload={
                "text": "Подтверждение отправлено.",
                "metadata": {
                    "dispatcher_latency_ms": 9,
                },
            },
        )

    result = VoiceOrchestrator(dispatcher=dispatcher).process_turn(
        input_text="Напиши клиенту подтверждение.",
        caller_id="caller-005",
        channel_type="phone",
        active_language="ru-RU",
        detected_language="ru-RU",
        context_ref="lead:voice-005",
    )

    trace = result.turn.trace_metadata
    assert trace is not None
    assert result.session.active_language == "ru-RU"
    assert result.turn.detected_language == "ru-RU"
    assert result.turn.response_plan.target_language == "ru-RU"
    assert trace.session_id == result.session.session_id
    assert trace.turn_id == result.turn.turn_id
    assert trace.caller_id == "caller-005"
    assert trace.detected_language == "ru-RU"
    assert trace.action_id == result.action_contract["action_id"]
    assert trace.result_status == "success"
    assert trace.latency_ms == 9


def test_malformed_input_is_normalized_safely() -> None:
    calls: list[object] = []

    def dispatcher(action_contract: object) -> dict[str, object]:
        calls.append(action_contract)
        raise AssertionError("malformed input must not dispatch")

    result = VoiceOrchestrator(dispatcher=dispatcher).process_turn(
        input_text="   \n\t   ",
        caller_id="caller-006",
        channel_type="phone",
        active_language="en-US",
    )

    assert calls == []
    assert result.action_contract is None
    assert result.action_result is None
    assert result.turn.input_text == ""
    assert result.turn.turn_status == "blocked"
    assert result.turn.response_plan.response_type == "input_error"
    assert result.turn.response_plan.text_payload == "Voice input was empty after normalization."
    assert result.turn.trace_metadata is not None
    assert result.turn.trace_metadata.action_id is None
    assert result.turn.trace_metadata.result_status == "blocked"
