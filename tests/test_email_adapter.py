from __future__ import annotations

import pytest

import app.adapters.email_adapter as email_adapter_module
from app.adapters.email_adapter import EmailAdapterError, EmailExecution, execute_email_action
from app.execution.action_contract import ActionContractViolation, validate_action_contract, validate_action_result_contract


def _valid_email_action_contract(*, operation: str = "create_draft", execution_mode: str = "live") -> dict[str, object]:
    parameters: dict[str, object] = {
        "operation": operation,
        "to": ["owner@example.com"],
        "subject": "Owner update",
        "body": "Status summary",
        "attachments": ["artifact:owner-summary"],
    }
    if operation == "reply_email":
        parameters = {
            "operation": operation,
            "to": ["owner@example.com"],
            "subject": "Re: Owner update",
            "body": "Reply body",
            "reply_to_id": "thread-001",
            "attachments": [],
        }
    return {
        "action_id": "act-email-001",
        "action_type": "email_action",
        "target_type": "adapter",
        "target_ref": "email",
        "parameters": parameters,
        "execution_mode": execution_mode,
        "confirmation_policy": "required",
        "idempotency_key": "owner:email:001",
        "requested_by": "ownerbox_interaction_v1",
        "timestamp": "2026-04-14T12:00:00Z",
        "schema_version": "v1",
    }


class StubEmailRuntime:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    def create_draft(
        self,
        *,
        to: list[str],
        subject: str,
        body: str,
        attachments: list[str],
    ) -> EmailExecution:
        self.calls.append(
            ("create_draft", {"to": list(to), "subject": subject, "body": body, "attachments": list(attachments)})
        )
        return EmailExecution(
            result_type="email_draft",
            summary="Prepared draft",
            references={"draft_id": "draft-001"},
        )

    def send_email(
        self,
        *,
        to: list[str],
        subject: str,
        body: str,
        attachments: list[str],
    ) -> EmailExecution:
        self.calls.append(
            ("send_email", {"to": list(to), "subject": subject, "body": body, "attachments": list(attachments)})
        )
        return EmailExecution(
            result_type="email_send",
            summary="Sent email",
            references={"message_id": "message-001"},
        )

    def reply_email(
        self,
        *,
        reply_to_id: str,
        body: str,
        to: list[str],
        subject: str | None,
        attachments: list[str],
    ) -> EmailExecution:
        self.calls.append(
            (
                "reply_email",
                {
                    "reply_to_id": reply_to_id,
                    "body": body,
                    "to": list(to),
                    "subject": subject,
                    "attachments": list(attachments),
                },
            )
        )
        return EmailExecution(
            result_type="email_reply",
            summary="Replied to thread",
            references={"message_id": "reply-001"},
        )


def test_create_draft_works() -> None:
    runtime = StubEmailRuntime()

    result = execute_email_action(_valid_email_action_contract(), runtime=runtime)

    assert runtime.calls[0][0] == "create_draft"
    assert validate_action_result_contract(result) == result
    assert result["status"] == "success"
    assert result["result_type"] == "email_draft"
    assert result["payload"]["summary"] == "Prepared draft"


def test_send_email_works() -> None:
    runtime = StubEmailRuntime()

    result = execute_email_action(
        _valid_email_action_contract(operation="send_email"),
        runtime=runtime,
    )

    assert runtime.calls[0][0] == "send_email"
    assert validate_action_result_contract(result) == result
    assert result["status"] == "success"
    assert result["result_type"] == "email_send"


def test_invalid_recipients_rejected() -> None:
    payload = _valid_email_action_contract()
    payload["parameters"] = {
        "operation": "create_draft",
        "to": ["not-an-email"],
        "subject": "Owner update",
        "body": "Status summary",
        "attachments": [],
    }

    with pytest.raises(ActionContractViolation, match="invalid recipient: not-an-email"):
        validate_action_contract(payload)


def test_email_dry_run_does_not_send() -> None:
    runtime = StubEmailRuntime()

    result = execute_email_action(
        _valid_email_action_contract(operation="send_email", execution_mode="dry_run"),
        runtime=runtime,
    )

    assert runtime.calls == []
    assert validate_action_result_contract(result) == result
    assert result["status"] == "success"
    assert result["result_type"] == "simulation"
    assert result["payload"]["metadata"]["dry_run"] is True


def test_email_dry_run_does_not_resolve_live_provider(monkeypatch) -> None:
    monkeypatch.setattr(
        email_adapter_module,
        "_build_default_email_runtime",
        lambda config: (_ for _ in ()).throw(AssertionError("live provider must not be resolved")),
    )

    result = execute_email_action(
        _valid_email_action_contract(operation="send_email", execution_mode="dry_run")
    )

    assert result["status"] == "success"
    assert result["result_type"] == "simulation"


def test_email_missing_provider_config_returns_provider_not_configured(monkeypatch) -> None:
    monkeypatch.delenv("DIGITAL_FOREMAN_EMAIL_BACKEND", raising=False)

    result = execute_email_action(_valid_email_action_contract(operation="send_email"))

    assert validate_action_result_contract(result) == result
    assert result["status"] == "failed"
    assert result["error_code"] == "provider_not_configured"


def test_email_real_provider_path_uses_provider_factory(monkeypatch) -> None:
    runtime = StubEmailRuntime()
    captured: dict[str, object] = {}

    def build_runtime(config):
        captured["provider_mode"] = config.provider_mode
        return runtime, "gmail"

    monkeypatch.setattr(email_adapter_module, "_build_default_email_runtime", build_runtime)

    result = execute_email_action(
        _valid_email_action_contract(operation="send_email"),
        config={"provider_mode": "gmail"},
    )

    assert runtime.calls[0][0] == "send_email"
    assert captured["provider_mode"] == "gmail"
    assert result["payload"]["metadata"]["backend_used"] == "gmail"


def test_reply_email_requires_explicit_reference() -> None:
    payload = _valid_email_action_contract(operation="reply_email")
    payload["parameters"].pop("reply_to_id")

    result = execute_email_action(payload)

    assert validate_action_result_contract(result) == result
    assert result["status"] == "failed"
    assert result["error_code"] == "validation_error"


def test_email_runtime_errors_are_normalized() -> None:
    class FailingRuntime(StubEmailRuntime):
        def send_email(
            self,
            *,
            to: list[str],
            subject: str,
            body: str,
            attachments: list[str],
        ) -> EmailExecution:
            raise EmailAdapterError("provider_error", "email provider failed")

    result = execute_email_action(
        _valid_email_action_contract(operation="send_email"),
        runtime=FailingRuntime(),
    )

    assert validate_action_result_contract(result) == result
    assert result["status"] == "failed"
    assert result["error_code"] == "provider_error"
    assert result["error_message"] == "email provider failed"
