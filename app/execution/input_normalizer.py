from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Mapping


ALLOWED_CHANNELS = {"phone", "email", "sms"}
ALLOWED_INTENTS = {
    "call_client",
    "add_note",
    "follow_up",
    "service_request",
    "record_address",
    "record_materials",
    "new_lead",
    "lead_estimate_decision",
    "outbound_message",
    "client_intake_flow",
    "generic_task",
}

CLIENT_INTAKE_FLOW_NAME = "client_intake_flow"
CLIENT_INTAKE_FLOW_STEPS = [
    "add to CRM",
    "send email",
    "create doc",
    "create meeting",
]


@dataclass(frozen=True)
class IncomingMessage:
    channel: str
    contact_id: str
    raw_text: str
    timestamp: str

    def as_dict(self) -> dict[str, str]:
        return {
            "channel": self.channel,
            "contact_id": self.contact_id,
            "raw_text": self.raw_text,
            "timestamp": self.timestamp,
        }


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_channel(value: object) -> str:
    normalized = _normalize_text(value).lower()
    if normalized not in ALLOWED_CHANNELS:
        raise ValueError("channel must be one of: phone, email, sms")
    return normalized


def _normalize_intent(value: object) -> str:
    normalized = _normalize_text(value).lower()
    return normalized if normalized in ALLOWED_INTENTS else "generic_task"


def infer_intent_from_text(text: str) -> str:
    normalized = text.lower()
    if (
        "client intake flow" in normalized
        or "client_intake_flow" in normalized
        or ("lock" in normalized and "client flow" in normalized)
    ):
        return "client_intake_flow"
    if any(
        phrase in normalized
        for phrase in (
            "new lead",
            "lead",
            "new customer",
            "new client",
        )
    ):
        return "new_lead"
    if any(
        phrase in normalized
        for phrase in (
            "email client",
            "send email",
            "call client and say",
            "call customer and say",
        )
    ):
        return "outbound_message"
    if any(
        phrase in normalized
        for phrase in (
            "text client",
            "message client",
            "sms client",
            "write client",
            "send sms",
            "напиши клиенту",
            "отправь смс",
        )
    ):
        return "outbound_message"
    if any(
        phrase in normalized
        for phrase in (
            "leaking faucet",
            "leak",
            "faucet",
            "plumber",
            "plumbing",
            "fix",
            "repair",
        )
    ):
        return "service_request"
    if "call" in normalized and any(
        word in normalized for word in ("client", "customer", "owner")
    ):
        return "call_client"
    if any(
        word in normalized
        for word in ("follow up", "follow-up", "check back", "remind")
    ):
        return "follow_up"
    if "address" in normalized:
        return "record_address"
    if any(
        word in normalized for word in ("material", "materials", "supply", "supplies")
    ):
        return "record_materials"
    if any(word in normalized for word in ("note", "noted", "memo")):
        return "add_note"
    return "generic_task"


def payload_from_text(intent: str, text: str) -> dict[str, object]:
    summary = text.strip()
    payload: dict[str, object] = {"text": summary, "summary": summary}
    if intent == "client_intake_flow":
        payload["flow_name"] = CLIENT_INTAKE_FLOW_NAME
        payload["steps"] = list(CLIENT_INTAKE_FLOW_STEPS)
        payload["reusable"] = True
        payload["callable_as_one_action"] = True
    elif intent == "record_address":
        payload["address"] = summary
    elif intent == "service_request":
        payload["request"] = summary
    elif intent == "new_lead":
        payload["request"] = summary
        payload["lead_summary"] = summary
    elif intent == "outbound_message":
        payload["message_text"] = summary
        if any(phrase in summary.lower() for phrase in ("email client", "send email")):
            payload["outbound_channel"] = "email"
            payload["subject"] = "Digital Foreman update"
        elif any(
            phrase in summary.lower()
            for phrase in ("call client and say", "call customer and say")
        ):
            payload["outbound_channel"] = "phone"
        else:
            payload["outbound_channel"] = "sms"
    elif intent == "record_materials":
        payload["materials"] = [summary]
    elif intent == "add_note":
        payload["note"] = summary
    elif intent == "follow_up":
        payload["follow_up"] = summary
    return payload


def normalize_input(
    *,
    text: object = "",
    intent: object = "",
    payload: Mapping[str, object] | None = None,
) -> tuple[str, dict[str, object]]:
    normalized_payload = dict(payload or {})
    normalized_text = _normalize_text(
        text or normalized_payload.get("text") or normalized_payload.get("summary")
    )
    raw_intent = _normalize_text(intent)

    if normalized_text and not raw_intent:
        raw_intent = infer_intent_from_text(normalized_text)

    normalized_intent = _normalize_intent(raw_intent)
    if normalized_text:
        base_payload = payload_from_text(normalized_intent, normalized_text)
        base_payload.update(normalized_payload)
        normalized_payload = base_payload

    normalized_payload.setdefault(
        "summary",
        _normalize_text(normalized_payload.get("summary")) or normalized_text,
    )
    return normalized_intent, normalized_payload


def normalize_incoming_message(
    *,
    channel: object,
    raw_text: object,
    timestamp: object,
    contact_id: object = "",
    raw_contact: object = "",
) -> IncomingMessage:
    normalized_text = _normalize_text(raw_text)
    if not normalized_text:
        raise ValueError("message must not be empty")

    normalized_timestamp = _normalize_text(timestamp)
    if not normalized_timestamp:
        raise ValueError("timestamp must not be empty")

    normalized_contact = _normalize_text(contact_id) or _normalize_text(raw_contact)
    if not normalized_contact:
        raise ValueError("contact_id or raw_contact must not be empty")

    return IncomingMessage(
        channel=_normalize_channel(channel),
        contact_id=normalized_contact,
        raw_text=normalized_text,
        timestamp=normalized_timestamp,
    )
