from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from control.context_store import load_context, set_active_mode
import owner_entry as owner_entry_module
import scripts.run_command as run_command_module


def detect_mode(text: str) -> str:
    normalized_text = str(text or "").strip().lower()
    if "eb1" in normalized_text or "immigration" in normalized_text:
        return "owner"
    if "client" in normalized_text or "project" in normalized_text:
        return "business"
    return "dev"


def route_command(text: str, mode: str) -> str:
    normalized_text = " ".join(str(text or "").split()).strip()
    if not normalized_text:
        return ""

    if mode == "owner" and run_command_module.parse_command(normalized_text) is None:
        return f"owner task {normalized_text}"
    return normalized_text


def _build_response(
    *,
    mode: str,
    command: str,
    status: str,
    response: Any = "",
    error: str = "",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "mode": mode,
        "command": command,
        "status": status,
    }
    if response not in ("", None):
        payload["response"] = response
    if error:
        payload["error"] = error
    return payload


def _print_payload(payload: dict[str, Any]) -> None:
    response = payload.get("response")
    if (
        payload.get("status") == "success"
        and isinstance(response, dict)
        and str(response.get("post_text") or "").strip()
    ):
        print(response["post_text"])
        summary_parts = [
            part
            for part in (
                str(response.get("tone") or "").strip(),
                str(response.get("intent") or "").strip(),
            )
            if part
        ]
        if summary_parts:
            print(f"SUMMARY: {' | '.join(summary_parts)}")
        print("SUGGESTION: Review and approve manually before posting.")
        return

    print(json.dumps(payload, indent=2))


def _print_memory_conflict_if_supported(response: Any) -> None:
    conflict_printer = getattr(
        run_command_module, "_print_memory_conflict_if_needed", None
    )
    if callable(conflict_printer):
        conflict_printer(response)


def run_interface(text: str) -> tuple[int, dict[str, Any]]:
    user_text = " ".join(str(text or "").split()).strip()
    if not user_text:
        return 1, _build_response(
            mode="dev",
            command="",
            status="failure",
            error="EMPTY_INPUT",
        )

    detected_mode = detect_mode(user_text)
    set_active_mode(detected_mode, context_dir=run_command_module.CONTEXT_DIR)

    if detected_mode == "owner" and run_command_module.parse_command(user_text) is None:
        owner_result = owner_entry_module.handle_owner_input(user_text)
        response = owner_result.get("result")
        owner_command = f"owner task {' '.join(user_text.lower().split())}"
        if owner_result.get("status") != "success":
            error_message = ""
            if isinstance(response, dict):
                error_message = str(response.get("message") or "").strip()
            return 1, _build_response(
                mode="owner",
                command=owner_command,
                status="failure",
                error=error_message or "OWNER_ENTRY_FAILED",
            )
        if response not in ("", None):
            _print_memory_conflict_if_supported(response)
        return 0, _build_response(
            mode="owner",
            command=owner_command,
            status="success",
            response=response,
        )

    routed_command = route_command(user_text, detected_mode)
    task_payload = run_command_module.parse_command(routed_command)
    if task_payload is None:
        return 1, _build_response(
            mode=detected_mode,
            command=routed_command,
            status="failure",
            error="UNKNOWN_COMMAND",
        )

    context_payload = load_context(context_dir=run_command_module.CONTEXT_DIR)
    effective_mode = run_command_module._normalize_mode(
        task_payload.get("context_mode") or detected_mode
    )
    context_summary = run_command_module._load_system_context_summary()
    selected_context = run_command_module.get_relevant_context(
        effective_mode,
        context_payload=context_payload,
    )
    selected_context = run_command_module._context_with_memory(
        selected_context,
        context_summary=context_summary,
    )
    task_payload = run_command_module._inject_context_into_pipeline(
        task_payload,
        selected_context,
        mode=effective_mode,
    )

    command_name = str(task_payload.get("command_name") or routed_command).strip()

    try:
        task_path = run_command_module.write_task_file(task_payload)
        succeeded, doc_url, artifact_path, failure_reason = (
            run_command_module.execute_command_task_with_retry(task_path)
        )
    except Exception as error:
        failure_reason = str(error).strip() or "COMMAND_EXECUTION_FAILED"
        return 1, _build_response(
            mode=effective_mode,
            command=command_name,
            status="failure",
            error=failure_reason,
        )

    run_command_module._record_command_state(
        command_name,
        succeeded=succeeded,
        doc_url=doc_url,
        artifact_path=artifact_path,
        failure_reason=failure_reason,
        state_path=run_command_module.COMMAND_STATE_FILE,
    )

    if not succeeded:
        return 1, _build_response(
            mode=effective_mode,
            command=command_name,
            status="failure",
            error=failure_reason,
        )

    run_command_module._record_system_context_success(
        command_name,
        run_command_module._normalize_command(command_name),
    )

    if bool(task_payload.get("print_analysis")):
        response: Any = run_command_module._analysis_from_artifact(artifact_path)
        if response:
            _print_memory_conflict_if_supported(response)
    elif bool(task_payload.get("print_linkedin_post")):
        response = run_command_module._linkedin_post_from_artifact(artifact_path)
        post_text = str(response.get("post_text") or "").strip()
        if post_text:
            _print_memory_conflict_if_supported(post_text)
    elif command_name == "process email":
        response = run_command_module._process_email_result_from_artifact(artifact_path)
    else:
        response = doc_url

    return 0, _build_response(
        mode=effective_mode,
        command=command_name,
        status="success",
        response=response,
    )


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    exit_code, payload = run_interface(
        " ".join(str(argument) for argument in arguments)
    )
    _print_payload(payload)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
