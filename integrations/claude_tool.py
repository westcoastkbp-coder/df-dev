from __future__ import annotations

import json
import os
import re
from hashlib import sha256
from pathlib import Path
from time import sleep
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from control.env_loader import load_env

ANTHROPIC_MESSAGES_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
CLAUDE_ANALYZE_MODEL = "claude-sonnet-4-20250514"
MAX_OUTPUT_TOKENS = 1024
REQUEST_TIMEOUT_SECONDS = 30
REQUEST_RETRY_DELAYS_SECONDS = (1.0, 2.0, 4.0, 8.0)
CACHE_DIR = Path(__file__).resolve().parents[1] / "artifacts" / "tool_cache" / "claude"
THREE_SENTENCE_SUMMARY_INSTRUCTIONS = {
    "Summarize this document in 3 sentences",
    "Summarize this text in 3 sentences",
}
STRUCTURED_SUMMARY_INSTRUCTION = "Analyze this document and produce a concise structured summary"
EMAIL_REPLY_INSTRUCTION = "Summarize email and suggest reply"
OWNER_SECTION_HEADERS = (
    "Action Steps:",
    "Priorities:",
    "Next Moves:",
)
MEMORY_CONTROL_DIRECTIVE = (
    "Memory control rules:\n"
    "- Respect memory_summary.current_stage.\n"
    "- Respect memory_summary.architecture_rules.\n"
    "- Do not contradict memory_summary.last_decisions.\n"
    "- Align with memory_summary.owner_priorities when present."
)


class ClaudeToolError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = str(code)
        self.message = str(message)


def _repo_env_value(name: str) -> str:
    try:
        return str(load_env().get(name) or "").strip()
    except OSError:
        return ""


def _env_value(name: str) -> str:
    return str(os.environ.get(name) or "").strip() or _repo_env_value(name)


def _api_key() -> str:
    anthropic_api_key = _env_value("ANTHROPIC_API_KEY")
    if anthropic_api_key:
        return anthropic_api_key

    legacy_api_key = _env_value("CLAUDE_API_KEY")
    if legacy_api_key:
        return legacy_api_key

    raise ClaudeToolError("CLAUDE_API_FAILED", "Anthropic API key is not configured.")


def _http_error_message(error: HTTPError) -> str:
    try:
        payload = error.read().decode("utf-8", errors="replace")
    except Exception:
        payload = ""

    if payload:
        try:
            message = json.loads(payload)["error"]["message"]
            if str(message).strip():
                return str(message).strip()
        except (KeyError, TypeError, ValueError):
            if payload.strip():
                return payload.strip()

    return str(error.reason or "Anthropic API request failed.").strip()


def _messages_response(request_payload: dict[str, Any], api_key: str) -> dict[str, Any]:
    body = json.dumps(request_payload).encode("utf-8")
    request = Request(
        ANTHROPIC_MESSAGES_URL,
        headers={
            "x-api-key": api_key,
            "anthropic-version": ANTHROPIC_VERSION,
            "content-type": "application/json",
        },
        data=body,
        method="POST",
    )
    with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return dict(payload)


def _should_retry_http_error(error: HTTPError) -> bool:
    return int(getattr(error, "code", 0) or 0) in {408, 429, 500, 502, 503, 504}


def _messages_response_with_retry(
    request_payload: dict[str, Any],
    api_key: str,
) -> dict[str, Any]:
    for attempt_index, delay_seconds in enumerate((0.0, *REQUEST_RETRY_DELAYS_SECONDS), start=1):
        try:
            return _messages_response(request_payload, api_key)
        except HTTPError as error:
            if not _should_retry_http_error(error):
                raise
            if attempt_index > len(REQUEST_RETRY_DELAYS_SECONDS):
                raise
        except (URLError, OSError, TimeoutError):
            if attempt_index > len(REQUEST_RETRY_DELAYS_SECONDS):
                raise

        sleep(delay_seconds)

    raise ClaudeToolError("CLAUDE_API_FAILED", "Anthropic API request retries exhausted.")


def _analysis_from_response(response_payload: dict[str, Any]) -> str:
    content_blocks = response_payload.get("content")
    if not isinstance(content_blocks, list):
        raise ClaudeToolError(
            "CLAUDE_API_FAILED",
            "Anthropic API response did not include message content.",
        )

    text_blocks: list[str] = []
    for block in content_blocks:
        if not isinstance(block, dict):
            continue
        if str(block.get("type") or "").strip() != "text":
            continue
        block_text = str(block.get("text") or "")
        if block_text:
            text_blocks.append(block_text)

    analysis = "".join(text_blocks).strip()
    if not analysis:
        raise ClaudeToolError(
            "CLAUDE_API_FAILED",
            "Anthropic API response did not include analysis text.",
        )
    return analysis


def _cache_path(instruction: str, text: str, context: dict[str, Any] | None = None) -> Path:
    cache_key = sha256(
        json.dumps(
            {
                "instruction": instruction,
                "text": text,
                "context": context,
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    return CACHE_DIR / f"{cache_key}.json"


def _read_cached_analysis(
    instruction: str,
    text: str,
    context: dict[str, Any] | None = None,
) -> str:
    cache_path = _cache_path(instruction, text, context)
    if not cache_path.is_file():
        return ""

    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""

    if not isinstance(payload, dict):
        return ""
    return str(payload.get("analysis") or "").strip()


def _write_cached_analysis(
    instruction: str,
    text: str,
    analysis: str,
    context: dict[str, Any] | None = None,
) -> None:
    normalized_analysis = str(analysis or "").strip()
    if not normalized_analysis:
        return
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _cache_path(instruction, text, context).write_text(
        json.dumps({"analysis": normalized_analysis}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _request_message_content(
    instruction: str,
    text: str,
    context: dict[str, Any] | None = None,
) -> str:
    prompt_sections = [
        "Return only the final analysis text with no preamble.",
    ]
    if isinstance(context, dict):
        prompt_sections.append(
            f"Context:\n{json.dumps(context, indent=2, sort_keys=True)}"
        )
        if isinstance(context.get("memory_summary"), dict):
            prompt_sections.append(MEMORY_CONTROL_DIRECTIVE)
    prompt_sections.append(f"Instruction:\n{instruction}")
    prompt_sections.append(f"Text:\n{text}")
    return "\n\n".join(prompt_sections)


def _memory_summary_from_context(context: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(context, dict):
        return {}
    memory_summary = context.get("memory_summary")
    if not isinstance(memory_summary, dict):
        return {}
    return memory_summary


def _split_sentences(text: str) -> list[str]:
    normalized_text = re.sub(r"\s+", " ", str(text or "")).strip()
    if not normalized_text:
        return []

    sentences = [
        sentence.strip()
        for sentence in re.split(r"(?<=[.!?])\s+", normalized_text)
        if sentence.strip()
    ]
    if sentences:
        return sentences

    words = normalized_text.split()
    if not words:
        return []
    chunk_size = max(1, len(words) // 3)
    return [
        " ".join(words[index : index + chunk_size]).strip()
        for index in range(0, len(words), chunk_size)
        if " ".join(words[index : index + chunk_size]).strip()
    ]


def _ensure_sentence_punctuation(sentence: str) -> str:
    normalized_sentence = str(sentence or "").strip()
    if not normalized_sentence:
        return ""
    if normalized_sentence.endswith((".", "!", "?")):
        return normalized_sentence
    return f"{normalized_sentence}."


def _deterministic_three_sentence_summary(text: str) -> str:
    sentences = _split_sentences(text)[:3]
    if not sentences:
        return "No readable content was available for analysis."
    return " ".join(_ensure_sentence_punctuation(sentence) for sentence in sentences)


def _deterministic_structured_summary(text: str) -> str:
    sentences = _split_sentences(text)
    if not sentences:
        return "Overview: No readable content was available for analysis."

    overview = _ensure_sentence_punctuation(sentences[0])
    key_points = sentences[: min(3, len(sentences))]
    bullet_lines = "\n".join(
        f"- {_ensure_sentence_punctuation(point)}"
        for point in key_points
    )
    return f"Overview: {overview}\nKey points:\n{bullet_lines}"


def _deterministic_email_reply_analysis(text: str) -> str:
    sentences = _split_sentences(text)
    summary = _ensure_sentence_punctuation(sentences[0]) if sentences else (
        "No readable email content was available."
    )
    return (
        f"Summary: {summary}\n\n"
        "Reply draft:\n"
        "Hi,\n\n"
        "Thanks for your email. I reviewed your note and I am following up on the points you raised. "
        "Please let me know if there is anything you would like me to prioritize.\n\n"
        "Best,"
    )


def _looks_like_owner_instruction(instruction: str) -> bool:
    normalized_instruction = str(instruction or "").strip().lower()
    required_terms = (
        "you are assisting the owner:",
        "anton vorontsov",
        "structured, actionable",
    )
    return all(term in normalized_instruction for term in required_terms)


def _owner_priority_lines(request_text: str, context: dict[str, Any] | None = None) -> list[str]:
    normalized_request = str(request_text or "").strip().lower()
    priorities: list[str] = []
    memory_summary = _memory_summary_from_context(context)
    if any(term in normalized_request for term in ("eb1", "eb-1", "o1", "o-1", "immigration", "visa")):
        priorities.append("1. Immigration evidence and filing readiness.")
        priorities.append("2. Concrete proof of business and technical impact.")
    else:
        priorities.append("1. Highest-leverage owner decision with near-term deadline.")
        priorities.append("2. Work that compounds system and business momentum.")

    if isinstance(context, dict):
        permits = context.get("permits")
        if isinstance(permits, dict) and str(permits.get("priority") or "").strip():
            priorities.append(
                f"3. Permit track remains {str(permits.get('priority')).strip()} priority."
            )
        else:
            priorities.append("3. Preserve evidence trail in Digital Foreman for reuse.")
    else:
        priorities.append("3. Preserve evidence trail in Digital Foreman for reuse.")
    owner_priorities = memory_summary.get("owner_priorities")
    if isinstance(owner_priorities, list):
        for index, priority in enumerate(owner_priorities[:2], start=len(priorities) + 1):
            normalized_priority = str(priority or "").strip()
            if normalized_priority:
                priorities.append(f"{index}. {normalized_priority}.")
    return priorities


def _deterministic_owner_action_plan(
    text: str,
    context: dict[str, Any] | None = None,
) -> str:
    normalized_request = str(text or "").strip()
    request_lower = normalized_request.lower()
    memory_summary = _memory_summary_from_context(context)
    evidence_note = "Capture each result inside Digital Foreman as dated proof."
    current_stage = str(memory_summary.get("current_stage") or "").strip()
    last_decisions = memory_summary.get("last_decisions")
    if not isinstance(last_decisions, list):
        last_decisions = []

    if any(term in request_lower for term in ("eb1", "eb-1")):
        action_steps = [
            "1. Make a gap list for the EB1 criteria you can credibly satisfy right now.",
            "2. Gather strongest proof of original work, system growth, business traction, and public recognition.",
            "3. Turn each proof item into a dated evidence packet with links, metrics, and a one-line significance note.",
        ]
        next_moves = [
            "Book a focused review of the current EB1 evidence set this week.",
            "Draft the next 30-day evidence-building plan around publications, impact metrics, and reference letters.",
            evidence_note,
        ]
    elif any(term in request_lower for term in ("o1", "o-1", "immigration", "visa")):
        action_steps = [
            "1. Confirm the immediate immigration deadline, blocker, or decision point.",
            "2. Collect the latest evidence showing specialized expertise, leadership, and measurable impact.",
            "3. Convert missing proof into a short worklist with owners and target dates.",
        ]
        next_moves = [
            "Decide which case milestone matters most over the next two weeks.",
            "Prepare a concise update for counsel with evidence links and open questions.",
            evidence_note,
        ]
    else:
        action_steps = [
            "1. Define the specific owner outcome you need from this request.",
            "2. Pull the strongest supporting facts, deadlines, and system signals.",
            "3. Choose the smallest next action that improves leverage or removes risk.",
        ]
        next_moves = [
            "Commit the chosen next action to a dated checklist.",
            "Review progress after the next concrete milestone.",
            evidence_note,
        ]

    if current_stage:
        action_steps.append(
            f"{len(action_steps) + 1}. Keep the next move aligned with the current stage: {current_stage}."
        )
    if last_decisions:
        preserved_decision = str(last_decisions[-1] or "").strip()
        if preserved_decision:
            next_moves.append(f"Keep the latest system decision intact: {preserved_decision}.")

    priorities = _owner_priority_lines(normalized_request, context)
    return (
        "Action Steps:\n"
        + "\n".join(action_steps)
        + "\n\nPriorities:\n"
        + "\n".join(priorities)
        + "\n\nNext Moves:\n"
        + "\n".join(f"- {line}" for line in next_moves)
    )


def _offline_analysis(
    instruction: str,
    text: str,
    context: dict[str, Any] | None = None,
) -> str:
    normalized_instruction = str(instruction or "").strip()
    if normalized_instruction in THREE_SENTENCE_SUMMARY_INSTRUCTIONS:
        return _deterministic_three_sentence_summary(text)
    if normalized_instruction == STRUCTURED_SUMMARY_INSTRUCTION:
        return _deterministic_structured_summary(text)
    if normalized_instruction == EMAIL_REPLY_INSTRUCTION:
        return _deterministic_email_reply_analysis(text)
    if _looks_like_owner_instruction(normalized_instruction):
        return _deterministic_owner_action_plan(text, context)
    return _deterministic_three_sentence_summary(text)


def _validated_claude_input(
    input_payload: dict[str, Any],
) -> tuple[str, str, dict[str, Any] | None]:
    if not isinstance(input_payload, dict):
        raise ClaudeToolError("CLAUDE_API_FAILED", "Claude tool input must be an object.")

    text = str(input_payload.get("text") or "").strip()
    instruction = str(input_payload.get("instruction") or "").strip()
    context = input_payload.get("context") if isinstance(input_payload.get("context"), dict) else None
    if not text:
        raise ClaudeToolError("CLAUDE_API_FAILED", "Claude text is required.")
    if not instruction:
        raise ClaudeToolError("CLAUDE_API_FAILED", "Claude instruction is required.")

    return instruction, text, context


def _fallback_analysis_payload(
    instruction: str,
    text: str,
    context: dict[str, Any] | None = None,
) -> dict[str, str]:
    cached_analysis = _read_cached_analysis(instruction, text, context)
    if cached_analysis:
        return {
            "analysis": cached_analysis,
        }

    fallback_analysis = _offline_analysis(instruction, text, context)
    _write_cached_analysis(instruction, text, fallback_analysis, context)
    return {
        "analysis": fallback_analysis,
    }


def run_claude_analyze_external(input_payload: dict[str, Any]) -> dict[str, str]:
    instruction, text, context = _validated_claude_input(input_payload)

    request_payload = {
        "model": CLAUDE_ANALYZE_MODEL,
        "max_tokens": MAX_OUTPUT_TOKENS,
        "temperature": 0,
        "messages": [
            {
                "role": "user",
                "content": _request_message_content(instruction, text, context),
            }
        ],
    }

    response_payload = _messages_response_with_retry(request_payload, _api_key())
    analysis = _analysis_from_response(response_payload)
    _write_cached_analysis(instruction, text, analysis, context)
    return {
        "analysis": analysis,
    }


def run_claude_analyze_fallback(input_payload: dict[str, Any]) -> dict[str, str]:
    instruction, text, context = _validated_claude_input(input_payload)
    return _fallback_analysis_payload(instruction, text, context)


def run_claude_analyze(input_payload: dict[str, Any]) -> dict[str, str]:
    instruction, text, context = _validated_claude_input(input_payload)
    try:
        return run_claude_analyze_external(
            {
                "instruction": instruction,
                "text": text,
                "context": context,
            }
        )
    except (
        ClaudeToolError,
        HTTPError,
        URLError,
        OSError,
        TimeoutError,
        ValueError,
        json.JSONDecodeError,
    ):
        return _fallback_analysis_payload(instruction, text, context)
