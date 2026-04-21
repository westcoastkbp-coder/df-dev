import json
import re
from typing import Any

import requests

from control.env_loader import load_env

DEFAULT_GEMINI_MODEL = "gemini-2.5-flash"
GEMINI_GENERATE_CONTENT_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
)


def _load_gemini_api_key() -> str | None:
    return load_env().get("GEMINI_API_KEY")


def _load_gemini_model() -> str:
    model = str(load_env().get("GEMINI_MODEL") or "").strip()
    return model or DEFAULT_GEMINI_MODEL


def _gemini_fallback_verifier() -> dict:
    return {
        "verdict": "NOT_VERIFIED",
        "bypass_risks": ["invalid_response"],
        "adversarial_test": "",
        "notes": "failed to parse Gemini response",
    }


def _build_gemini_request_payload(prompt: str) -> dict[str, Any]:
    return {
        "contents": [
            {
                "parts": [
                    {"text": str(prompt)},
                ]
            }
        ]
    }


def _response_json(response: requests.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError as error:
        raise ValueError("invalid_json_response") from error
    if not isinstance(payload, dict):
        raise ValueError("invalid_response_payload")
    return payload


def _extract_gemini_error(payload: dict[str, Any]) -> str:
    error_payload = payload.get("error")
    if not isinstance(error_payload, dict):
        return ""

    message = str(error_payload.get("message") or "").strip()
    status = str(error_payload.get("status") or "").strip()
    code = error_payload.get("code")

    parts = [
        part
        for part in (f"code={code}" if code is not None else "", status, message)
        if part
    ]
    return "Gemini API error: " + " | ".join(parts) if parts else "Gemini API error"


def _extract_candidate_text(payload: dict[str, Any]) -> str:
    candidates = payload.get("candidates")
    if not isinstance(candidates, list) or not candidates:
        raise ValueError("missing_candidates")

    text_parts: list[str] = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        content = candidate.get("content")
        if not isinstance(content, dict):
            continue
        parts = content.get("parts")
        if not isinstance(parts, list):
            continue
        for part in parts:
            if not isinstance(part, dict):
                continue
            text = part.get("text")
            if isinstance(text, str) and text.strip():
                text_parts.append(text)

    combined_text = "\n".join(text_parts).strip()
    if not combined_text:
        raise ValueError("missing_candidate_text")
    return combined_text


def _extract_json_from_text(text: str) -> dict[str, Any]:
    normalized_text = str(text or "").strip()
    if normalized_text.startswith("```"):
        normalized_text = re.sub(r"^```(?:json)?\s*", "", normalized_text)
        normalized_text = re.sub(r"\s*```$", "", normalized_text)

    try:
        parsed = json.loads(normalized_text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", normalized_text, re.DOTALL)
        if not match:
            raise ValueError("no_json_found") from None
        parsed = json.loads(match.group(0))

    if not isinstance(parsed, dict):
        raise ValueError("invalid_json_object")
    return parsed


def _post_gemini_prompt(prompt: str) -> dict:
    api_key = _load_gemini_api_key()
    if not api_key:
        raise ValueError("missing_api_key")

    model = _load_gemini_model()
    response = requests.post(
        GEMINI_GENERATE_CONTENT_URL.format(model=model),
        headers={
            "Content-Type": "application/json",
            "x-goog-api-key": api_key,
        },
        json=_build_gemini_request_payload(prompt),
        timeout=30,
    )
    response_payload = _response_json(response)
    if not response.ok:
        raise ValueError(
            _extract_gemini_error(response_payload)
            or f"gemini_http_{response.status_code}"
        )

    text = _extract_candidate_text(response_payload)
    return _extract_json_from_text(text)


def _build_google_operator_prompt(task: str, context: dict[str, Any]) -> str:
    return f"""
You are google_operator.

Execute the user's task using the provided context.

Rules:
- Use the full task and full context directly.
- Do not decompose the task into low-level implementation steps.
- Return ONLY valid JSON.
- Put the final useful output in the result field.
- If the task requires creating a Google Doc, do not claim that it already exists.
- Instead return the draft document payload in result.document with exactly:
  {{
    "title": "...",
    "content": "..."
  }}
- Only include fields that are already real. Never invent document IDs or URLs.

FORMAT:
{{
  "result": {{}}
}}

Task:
{task}

Context:
{json.dumps(context, indent=2, sort_keys=True)}
"""


def call_gemini_verifier(packet: dict) -> dict:
    api_key = _load_gemini_api_key()

    if not api_key:
        return {
            "verdict": "NOT_VERIFIED",
            "bypass_risks": ["missing_api_key"],
            "adversarial_test": "",
            "notes": "GEMINI_API_KEY missing",
        }

    prompt = f"""
You are Independent Verification Engineer.

IMPORTANT:
Return ONLY valid JSON.
NO text before or after.
NO explanations.

FORMAT:

{{
  "verdict": "VERIFIED" or "NOT_VERIFIED",
  "bypass_risks": [],
  "adversarial_test": "",
  "notes": ""
}}

Packet:
{json.dumps(packet, indent=2)}
"""

    try:
        return _post_gemini_prompt(prompt)

    except Exception:
        return _gemini_fallback_verifier()


def call_gemini_breaker(packet: dict):
    api_key = _load_gemini_api_key()

    if not api_key:
        return {
            "attack_vectors": [],
            "weak_points": ["missing_api_key"],
            "bypass_scenarios": [],
        }

    prompt = f"""
You are system breaker.

Your goal:
Find how to break this system.

Packet:
{json.dumps(packet, indent=2)}

Return ONLY JSON:

{{
  "attack_vectors": [],
  "weak_points": [],
  "bypass_scenarios": []
}}
"""

    try:
        return _post_gemini_prompt(prompt)
    except Exception:
        return {
            "attack_vectors": [],
            "weak_points": ["invalid_response"],
            "bypass_scenarios": [],
        }


def call_gemini_google_operator(task: str, context: dict) -> dict:
    normalized_task = str(task or "").strip()
    if not normalized_task:
        raise ValueError("missing_task")
    if not isinstance(context, dict):
        raise ValueError("invalid_context")

    prompt = _build_google_operator_prompt(normalized_task, context)
    response_payload = _post_gemini_prompt(prompt)
    if isinstance(response_payload, dict) and "result" in response_payload:
        return response_payload
    return {
        "result": response_payload,
    }
