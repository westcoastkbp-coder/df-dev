from __future__ import annotations

import json
from pathlib import Path

from app.execution.action_contract import build_action_result_contract
from app.execution.action_dispatcher import dispatch_action
from app.memory import memory_registry
from app.orchestrator import task_memory, task_state_store
from app.ownerbox.domain import (
    create_owner_action_scope,
    create_owner_domain,
    create_owner_memory_scope,
    create_owner_trust_profile,
)
from app.ownerbox.operational_scenarios import (
    OPERATIONAL_SCENARIO_TYPES,
    OwnerOperationalScenarioOrchestrator,
    SCENARIO_REGISTRY,
    compile_owner_operational_scenario,
    create_owner_operational_scenario_request,
)
from app.ownerbox.owner_orchestrator import OwnerOrchestrator
from app.ownerbox.workflow_orchestrator import OwnerWorkflowOrchestrator
import app.execution.action_dispatcher as action_dispatcher_module
import app.execution.idempotency_store as idempotency_store_module
import app.ownerbox.owner_orchestrator as owner_orchestrator_module
import app.ownerbox.workflow_orchestrator as workflow_orchestrator_module
import app.ownerbox.workflow_state_store as workflow_state_store_module
import runtime.system_log as system_log_module


def _configure_dispatch_runtime(monkeypatch, tmp_path: Path) -> None:
    system_log_file = tmp_path / "runtime" / "logs" / "system.log"
    task_log_file = tmp_path / "runtime" / "logs" / "tasks.log"
    task_memory_file = tmp_path / "runtime" / "state" / "task_memory.json"
    registry_file = tmp_path / "df-system" / "memory_registry.json"

    monkeypatch.setattr(system_log_module, "SYSTEM_LOG_FILE", system_log_file)
    monkeypatch.setattr(system_log_module, "TASK_LOG_FILE", task_log_file)
    monkeypatch.setattr(action_dispatcher_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(owner_orchestrator_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(workflow_orchestrator_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(task_memory, "TASK_MEMORY_FILE", task_memory_file)
    monkeypatch.setattr(task_state_store, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store,
        "TASK_STATE_DB_FILE",
        Path("runtime/task_state.sqlite3"),
    )
    monkeypatch.setattr(memory_registry, "REGISTRY_FILE", registry_file)
    monkeypatch.setattr(idempotency_store_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        idempotency_store_module,
        "IDEMPOTENCY_DB_FILE",
        Path("runtime/state/idempotency.sqlite3"),
    )
    monkeypatch.setattr(workflow_state_store_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        workflow_state_store_module,
        "WORKFLOW_STATE_DB_FILE",
        Path("runtime/state/ownerbox_workflow_state.sqlite3"),
    )
    action_dispatcher_module._ACTION_OUTCOME_CACHE.clear()


def _trace_entries(tmp_path: Path) -> list[dict[str, object]]:
    system_log_file = tmp_path / "runtime" / "logs" / "system.log"
    if not system_log_file.exists():
        return []
    entries: list[dict[str, object]] = []
    for line in system_log_file.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if payload.get("event_type") != "trace":
            continue
        entries.append(dict(payload.get("details", {})))
    return entries


def _owner_boundary_bundle() -> tuple[object, object, object, object]:
    memory_scope = create_owner_memory_scope(
        scope_id="owner-memory-scope-v1",
        allowed_refs=["artifact", "state"],
    )
    action_scope = create_owner_action_scope(scope_id="owner-action-scope-v1")
    trust_profile = create_owner_trust_profile(
        trust_profile_id="owner-trust-v1",
        owner_id="owner-001",
        confirmation_policy_ref="owner-confirmation-v1",
    )
    owner_domain = create_owner_domain(
        domain_id="ownerbox-main",
        owner_id="owner-001",
        trust_level="high",
        memory_scope_ref=memory_scope.scope_id,
        action_scope_ref=action_scope.scope_id,
        policy_scope_ref="owner-policy-scope-v1",
    )
    return owner_domain, memory_scope, action_scope, trust_profile


def _scenario_orchestrator(
    *,
    openai_calls: list[dict[str, object]],
    browser_calls: list[tuple[str, dict[str, object]]],
    email_calls: list[tuple[str, dict[str, object]]],
    printer_calls: list[tuple[str, dict[str, object]]],
) -> OwnerOperationalScenarioOrchestrator:
    def openai_executor(action_contract: object) -> dict[str, object]:
        parameters = dict(dict(action_contract)["parameters"])
        openai_calls.append(dict(parameters))
        return build_action_result_contract(
            action_id=str(dict(action_contract)["action_id"]),
            status="success",
            result_type="text_generation",
            payload={"text": "Generated browser update body."},
        )

    def browser_executor(action_contract: object) -> dict[str, object]:
        parameters = dict(dict(action_contract)["parameters"])
        operation = str(parameters["operation"])
        browser_calls.append((operation, dict(parameters)))
        payload = {
            "summary": f"Browser step completed: {operation}.",
            "metadata": {"operation": operation},
        }
        if operation == "extract_text":
            payload["summary"] = "Extracted bounded page summary."
        return build_action_result_contract(
            action_id=str(dict(action_contract)["action_id"]),
            status="success",
            result_type=f"browser_{operation}",
            payload=payload,
        )

    def email_executor(action_contract: object) -> dict[str, object]:
        parameters = dict(dict(action_contract)["parameters"])
        operation = str(parameters["operation"])
        email_calls.append((operation, dict(parameters)))
        return build_action_result_contract(
            action_id=str(dict(action_contract)["action_id"]),
            status="success",
            result_type=f"email_{operation}",
            payload={
                "summary": f"Email step completed: {operation}.",
                "metadata": {"operation": operation},
            },
        )

    def printer_executor(action_contract: object) -> dict[str, object]:
        parameters = dict(dict(action_contract)["parameters"])
        operation = str(parameters["operation"])
        printer_calls.append((operation, dict(parameters)))
        return build_action_result_contract(
            action_id=str(dict(action_contract)["action_id"]),
            status="success",
            result_type="print_document",
            payload={
                "summary": (
                    f"Printed '{parameters['document_title']}' on "
                    f"{parameters.get('printer_name') or 'configured_printer'}"
                ),
                "references": {
                    "artifact_path": "runtime/out/artifacts/printer_jobs/demo.txt"
                },
                "metadata": {
                    "operation": operation,
                    "backend_used": "stub_printer",
                    "printer_name": parameters.get("printer_name")
                    or "configured_printer",
                    "job_status": "submitted",
                    "pages": 1,
                },
            },
        )

    def dispatcher(action_contract: object, **kwargs: object) -> dict[str, object]:
        return dispatch_action(
            action_contract,
            openai_executor=openai_executor,
            browser_executor=browser_executor,
            email_executor=email_executor,
            printer_executor=printer_executor,
            **kwargs,
        )

    return OwnerOperationalScenarioOrchestrator(
        workflow_orchestrator=OwnerWorkflowOrchestrator(
            owner_orchestrator=OwnerOrchestrator(dispatcher=dispatcher)
        )
    )


def test_scenario_registry_is_explicit_and_compile_is_deterministic() -> None:
    assert set(SCENARIO_REGISTRY) == set(OPERATIONAL_SCENARIO_TYPES)

    request = create_owner_operational_scenario_request(
        scenario_type="owner_draft_then_browser_update",
        owner_id="owner-001",
        target_url="https://example.com/update",
        target_fields={"status": "ready"},
        structured_inputs={
            "form_selector": "#update-form",
            "draft_field_name": "message",
        },
        prompt_input="Draft a concise owner update.",
    )
    compilation = compile_owner_operational_scenario(request)

    assert compilation.workflow_type == "openai_then_browser_open_fill_submit"
    assert (
        compilation.workflow_metadata["scenario_type"]
        == "owner_draft_then_browser_update"
    )
    assert compilation.workflow_payload["fill_action"]["action_parameters"][
        "fields"
    ] == {
        "message": "{{step1.result_summary}}",
        "status": "ready",
    }


def test_malformed_scenario_input_returns_validation_error_shape() -> None:
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    result = OwnerOperationalScenarioOrchestrator().execute_scenario(
        {
            "scenario_type": "owner_web_form_review_and_submit",
            "owner_id": "owner-001",
            "target_url": "not-a-url",
            "target_fields": {"company": "Digital Foreman"},
            "structured_inputs": {"form_selector": "#lead-form"},
        },
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
    )

    assert result.workflow_id is None
    assert result.workflow_status == "failed"
    assert (
        result.failure_reason
        == "validation_error: target_url must be an absolute http or https URL"
    )
    assert result.pending_approval_ids == ()


def test_email_review_and_send_previews_before_send_approval(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    email_calls: list[tuple[str, dict[str, object]]] = []
    orchestrator = _scenario_orchestrator(
        openai_calls=[],
        browser_calls=[],
        email_calls=email_calls,
        printer_calls=[],
    )

    result = orchestrator.execute_scenario(
        create_owner_operational_scenario_request(
            scenario_type="owner_email_review_and_send",
            owner_id="owner-001",
            target_fields={},
            structured_inputs={
                "to": ["owner@example.com"],
                "subject": "Owner update",
                "attachments": [],
            },
            draft_content="Review-ready draft body.",
        ),
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
    )

    assert [operation for operation, _parameters in email_calls] == ["create_draft"]
    assert result.scenario_type == "owner_email_review_and_send"
    assert result.workflow_status == "blocked"
    assert result.preview_text == "Current step: Send Email"
    assert len(result.pending_approval_ids) == 1
    assert result.failure_reason is None


def test_web_form_review_and_submit_resumes_without_duplicate_submit(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    browser_calls: list[tuple[str, dict[str, object]]] = []
    orchestrator = _scenario_orchestrator(
        openai_calls=[],
        browser_calls=browser_calls,
        email_calls=[],
        printer_calls=[],
    )

    created = orchestrator.execute_scenario(
        create_owner_operational_scenario_request(
            scenario_type="owner_web_form_review_and_submit",
            owner_id="owner-001",
            target_url="https://example.com/form",
            target_fields={"company": "Digital Foreman"},
            structured_inputs={
                "form_selector": "#lead-form",
                "extract_selector": "main",
            },
        ),
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
    )

    assert [operation for operation, _parameters in browser_calls] == [
        "open_page",
        "extract_text",
        "fill_form",
    ]
    assert created.workflow_status == "blocked"
    approval_id = created.pending_approval_ids[0]
    workflow_id = created.workflow_id

    restored = _scenario_orchestrator(
        openai_calls=[],
        browser_calls=browser_calls,
        email_calls=[],
        printer_calls=[],
    ).resume_scenario(workflow_id)
    first = _scenario_orchestrator(
        openai_calls=[],
        browser_calls=browser_calls,
        email_calls=[],
        printer_calls=[],
    ).approve_scenario(approval_id)
    second = _scenario_orchestrator(
        openai_calls=[],
        browser_calls=browser_calls,
        email_calls=[],
        printer_calls=[],
    ).approve_scenario(approval_id)

    assert restored.scenario_type == "owner_web_form_review_and_submit"
    assert restored.workflow_status == "blocked"
    assert [operation for operation, _parameters in browser_calls] == [
        "open_page",
        "extract_text",
        "fill_form",
        "submit_form",
    ]
    assert first.workflow_status == "completed"
    assert first.final_result_summary == "Browser step completed: submit_form."
    assert second.workflow_status == "completed"


def test_page_review_and_extract_completes_without_approval(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    browser_calls: list[tuple[str, dict[str, object]]] = []
    orchestrator = _scenario_orchestrator(
        openai_calls=[],
        browser_calls=browser_calls,
        email_calls=[],
        printer_calls=[],
    )

    result = orchestrator.execute_scenario(
        create_owner_operational_scenario_request(
            scenario_type="owner_page_review_and_extract",
            owner_id="owner-001",
            target_url="https://example.com/page",
            structured_inputs={"extract_selector": "article"},
        ),
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
    )

    assert [operation for operation, _parameters in browser_calls] == [
        "open_page",
        "extract_text",
    ]
    assert result.workflow_status == "completed"
    assert result.pending_approval_ids == ()
    assert result.final_result_summary == "Extracted bounded page summary."
    assert result.failure_reason is None


def test_draft_then_browser_update_uses_generated_text_in_fill_step(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    openai_calls: list[dict[str, object]] = []
    browser_calls: list[tuple[str, dict[str, object]]] = []
    orchestrator = _scenario_orchestrator(
        openai_calls=openai_calls,
        browser_calls=browser_calls,
        email_calls=[],
        printer_calls=[],
    )

    result = orchestrator.execute_scenario(
        create_owner_operational_scenario_request(
            scenario_type="owner_draft_then_browser_update",
            owner_id="owner-001",
            target_url="https://example.com/update",
            target_fields={"status": "ready"},
            structured_inputs={
                "form_selector": "#update-form",
                "draft_field_name": "message",
            },
            prompt_input="Draft a concise owner-facing update.",
        ),
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
    )

    fill_parameters = next(
        parameters
        for operation, parameters in browser_calls
        if operation == "fill_form"
    )

    assert len(openai_calls) == 1
    assert fill_parameters["fields"] == {
        "message": "Generated browser update body.",
        "status": "ready",
    }
    assert result.workflow_status == "blocked"
    assert list(result.to_dict()) == [
        "scenario_type",
        "workflow_id",
        "current_step",
        "preview_text",
        "pending_approval_ids",
        "final_result_summary",
        "failure_reason",
        "workflow_status",
    ]


def test_print_scenario_validation_fails_on_malformed_input() -> None:
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    result = OwnerOperationalScenarioOrchestrator().execute_scenario(
        {
            "scenario_type": "owner_generate_review_and_print_document",
            "owner_id": "owner-001",
            "structured_inputs": {
                "document_title": "Owner Review",
                "copies": 2,
            },
            "draft_content": "Bounded printable document text.",
        },
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
    )

    assert result.workflow_id is None
    assert result.workflow_status == "failed"
    assert (
        result.failure_reason
        == "validation_error: structured_inputs.copies must be one of: 1"
    )


def test_print_scenario_previews_before_approval_and_survives_restart(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    printer_calls: list[tuple[str, dict[str, object]]] = []
    orchestrator = _scenario_orchestrator(
        openai_calls=[],
        browser_calls=[],
        email_calls=[],
        printer_calls=printer_calls,
    )

    created = orchestrator.execute_scenario(
        create_owner_operational_scenario_request(
            scenario_type="owner_generate_review_and_print_document",
            owner_id="owner-001",
            structured_inputs={
                "document_title": "Owner Review",
                "printer_name": "Zephyrus_Main",
            },
            draft_content="Bounded printable document text.",
        ),
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
    )

    assert printer_calls == []
    assert created.workflow_status == "blocked"
    assert len(created.pending_approval_ids) == 1
    assert (
        created.preview_text
        == "Preview ready: Owner Review Bounded printable document text."
    )

    restored = _scenario_orchestrator(
        openai_calls=[],
        browser_calls=[],
        email_calls=[],
        printer_calls=printer_calls,
    ).resume_scenario(created.workflow_id)

    assert restored.workflow_status == "blocked"
    assert restored.pending_approval_ids == created.pending_approval_ids
    assert restored.preview_text == created.preview_text
    assert printer_calls == []


def test_print_scenario_executes_once_and_does_not_replay(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    printer_calls: list[tuple[str, dict[str, object]]] = []
    orchestrator = _scenario_orchestrator(
        openai_calls=[],
        browser_calls=[],
        email_calls=[],
        printer_calls=printer_calls,
    )

    created = orchestrator.execute_scenario(
        create_owner_operational_scenario_request(
            scenario_type="owner_generate_review_and_print_document",
            owner_id="owner-001",
            structured_inputs={
                "document_title": "Owner Review",
                "printer_name": "Zephyrus_Main",
            },
            draft_content="Bounded printable document text.",
        ),
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
    )

    approval_id = created.pending_approval_ids[0]
    first = _scenario_orchestrator(
        openai_calls=[],
        browser_calls=[],
        email_calls=[],
        printer_calls=printer_calls,
    ).approve_scenario(approval_id)
    second = _scenario_orchestrator(
        openai_calls=[],
        browser_calls=[],
        email_calls=[],
        printer_calls=printer_calls,
    ).approve_scenario(approval_id)
    resumed = _scenario_orchestrator(
        openai_calls=[],
        browser_calls=[],
        email_calls=[],
        printer_calls=printer_calls,
    ).resume_scenario(created.workflow_id)

    assert [operation for operation, _parameters in printer_calls] == ["print_document"]
    assert first.workflow_status == "completed"
    assert second.workflow_status == "completed"
    assert resumed.workflow_status == "completed"

    traces = _trace_entries(tmp_path)
    print_trace = next(
        trace
        for trace in reversed(traces)
        if trace.get("action_type") == "PRINT_DOCUMENT"
    )
    assert print_trace["scenario_type"] == "owner_generate_review_and_print_document"
    assert print_trace["printer_name"] == "Zephyrus_Main"
    assert print_trace["approval_id"] == approval_id

    verifier = _scenario_orchestrator(
        openai_calls=[],
        browser_calls=[],
        email_calls=[],
        printer_calls=printer_calls,
    )
    state = verifier._workflow_orchestrator._workflow_state_store.load_state(
        created.workflow_id
    )
    assert state is not None
    steps = list(state.snapshot["steps"])
    print_action_id = next(
        step["action_id"] for step in steps if step["action_type"] == "PRINT_DOCUMENT"
    )
    history = task_memory.get_task_history(print_action_id)
    assert len(history) == 1
    assert "Bounded printable document text." not in history[0]["result_summary"]


def test_print_scenario_generation_prompt_passes_generated_text_to_print(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _configure_dispatch_runtime(monkeypatch, tmp_path)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    openai_calls: list[dict[str, object]] = []
    printer_calls: list[tuple[str, dict[str, object]]] = []
    orchestrator = _scenario_orchestrator(
        openai_calls=openai_calls,
        browser_calls=[],
        email_calls=[],
        printer_calls=printer_calls,
    )

    created = orchestrator.execute_scenario(
        create_owner_operational_scenario_request(
            scenario_type="owner_generate_review_and_print_document",
            owner_id="owner-001",
            structured_inputs={
                "document_title": "Generated Review",
                "printer_name": "Zephyrus_Main",
            },
            prompt_input="Write a concise owner review note.",
        ),
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
    )

    assert len(openai_calls) == 1
    assert created.workflow_status == "blocked"
    assert "Generated browser update body." in (created.preview_text or "")

    _scenario_orchestrator(
        openai_calls=openai_calls,
        browser_calls=[],
        email_calls=[],
        printer_calls=printer_calls,
    ).approve_scenario(created.pending_approval_ids[0])

    assert len(printer_calls) == 1
    assert printer_calls[0][1]["document_text"] == "Generated browser update body."
