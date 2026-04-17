from __future__ import annotations

import json
from typing import Any

from integrations.claude_tool import run_claude_analyze, run_claude_analyze_external

LINKEDIN_CREATE_POST_DRAFT_INSTRUCTION = (
    "Create a LinkedIn post draft for the provided topic and context. "
    "Return valid JSON only with keys post_text, tone, intent. "
    "Write in a grounded founder-builder voice. "
    "Keep it specific, practical, and suitable for manual review. "
    "Do not publish anything."
)
DEFAULT_TONE = "grounded, practical, founder-builder"
DEFAULT_INTENT = "share real progress and invite thoughtful feedback"


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _extract_json_object(text: str) -> dict[str, Any]:
    normalized_text = str(text or "").strip()
    if not normalized_text:
        return {}

    candidates = [normalized_text]
    if "```" in normalized_text:
        candidates.extend(
            part.strip()
            for part in normalized_text.split("```")
            if part.strip()
        )

    for candidate in candidates:
        cleaned_candidate = candidate.removeprefix("json").strip()
        try:
            payload = json.loads(cleaned_candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    return {}


def _fallback_post_text(topic: str, context: str) -> str:
    lines = [
        f"I've been working on {topic} inside Digital Foreman.",
        "The goal is to turn real execution into a system that stays controlled, visible, and useful.",
    ]
    if context:
        lines.append(f"Right now the context is: {context}.")
    lines.append(
        "I am sharing the draft thinking early so the work stays grounded in real action, not just ideas."
    )
    lines.append("Curious what part of this would matter most to you.")
    return "\n\n".join(lines)


def _fallback_output(topic: str, context: str) -> dict[str, str]:
    return {
        "post_text": _fallback_post_text(topic, context),
        "tone": DEFAULT_TONE,
        "intent": DEFAULT_INTENT,
    }


def _validated_topic_and_context(input_payload: dict[str, Any]) -> tuple[str, str]:
    if not isinstance(input_payload, dict):
        raise RuntimeError("LinkedIn tool input must be an object.")

    topic = str(input_payload.get("topic") or "").strip()
    context = str(input_payload.get("context") or "").strip()
    if not topic:
        raise RuntimeError("LinkedIn topic is required.")
    return topic, context


def run_linkedin_create_post_draft_external(input_payload: dict[str, Any]) -> dict[str, str]:
    topic, context = _validated_topic_and_context(input_payload)

    analysis = run_claude_analyze_external(
        {
            "instruction": LINKEDIN_CREATE_POST_DRAFT_INSTRUCTION,
            "text": (
                f"Topic:\n{topic}\n\n"
                f"Context:\n{context or 'No additional context provided.'}"
            ),
        }
    ).get("analysis", "")

    parsed_payload = _extract_json_object(analysis)
    post_text = str(parsed_payload.get("post_text") or "").strip()
    tone = _normalize_text(parsed_payload.get("tone")) or DEFAULT_TONE
    intent = _normalize_text(parsed_payload.get("intent")) or DEFAULT_INTENT

    if not post_text:
        raise RuntimeError("LinkedIn draft response did not include post_text.")

    return {
        "post_text": post_text,
        "tone": tone,
        "intent": intent,
    }


def run_linkedin_create_post_draft_fallback(input_payload: dict[str, Any]) -> dict[str, str]:
    topic, context = _validated_topic_and_context(input_payload)
    return _fallback_output(topic, context)


def run_linkedin_create_post_draft(input_payload: dict[str, Any]) -> dict[str, str]:
    topic, context = _validated_topic_and_context(input_payload)
    try:
        return run_linkedin_create_post_draft_external(
            {
                "topic": topic,
                "context": context,
            }
        )
    except Exception:
        return _fallback_output(topic, context)
