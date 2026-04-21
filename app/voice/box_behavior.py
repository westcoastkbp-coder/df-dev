from __future__ import annotations

GENERIC_SCOPE_FOLLOWUPS = frozenset(
    {
        "",
        "here",
        "this",
        "these files",
        "use this",
        "use these",
        "go ahead",
        "continue",
        "продолжай",
        "вот",
        "эти файлы",
        "используй это",
        "используй эти файлы",
        "да",
    }
)


def normalize_box_state(value: object) -> str:
    normalized = str(value or "").strip().lower()
    allowed = {
        "idle",
        "guiding",
        "awaiting_scope",
        "processing",
        "presenting_result",
        "reviewing_task",
        "reviewing_history",
    }
    if normalized not in allowed:
        return "idle"
    return normalized


def resolve_box_state(
    *,
    current_mode: object,
    interaction_state: object,
    last_task_id: object,
    pending_objective: object,
) -> str:
    normalized_interaction_state = str(interaction_state or "").strip()
    normalized_mode = str(current_mode or "").strip()
    normalized_task_id = str(last_task_id or "").strip()
    normalized_pending_objective = str(pending_objective or "").strip()

    if normalized_interaction_state in {"listening", "processing", "responding"}:
        return "processing"
    if normalized_pending_objective and normalized_mode == "task_run":
        return "awaiting_scope"
    if normalized_mode == "help":
        return "guiding"
    if normalized_mode == "history_review":
        return "reviewing_history"
    if normalized_mode == "task_status":
        return "reviewing_task"
    if normalized_mode == "task_run" and normalized_task_id:
        return "presenting_result"
    return "idle"


def should_resume_scope_collection(
    *,
    session: dict[str, object] | None,
    transcript: object,
    scope_files: list[object] | tuple[object, ...],
    intent: object,
) -> bool:
    payload = dict(session or {})
    normalized_box_state = normalize_box_state(payload.get("box_state", "idle"))
    normalized_pending_objective = str(payload.get("pending_objective", "")).strip()
    normalized_transcript = " ".join(str(transcript or "").split()).strip().lower()
    normalized_intent = str(intent or "").strip()
    has_scope = any(str(item).strip() for item in scope_files)

    if normalized_box_state != "awaiting_scope":
        return False
    if not normalized_pending_objective or not has_scope:
        return False
    if normalized_intent not in {"help", "run_task"}:
        return False
    return normalized_transcript in GENERIC_SCOPE_FOLLOWUPS


def resolve_run_objective(
    *,
    transcript: object,
    session: dict[str, object] | None,
    resume_scope_collection: bool,
) -> str:
    normalized_transcript = " ".join(str(transcript or "").split()).strip()
    if normalized_transcript and not resume_scope_collection:
        return normalized_transcript
    payload = dict(session or {})
    return str(payload.get("pending_objective", "")).strip() or normalized_transcript


def device_status_text(
    *,
    box_state: object,
    interaction_state: object,
) -> str:
    normalized_box_state = normalize_box_state(box_state)
    normalized_interaction_state = str(interaction_state or "").strip()
    if normalized_interaction_state in {"listening", "processing", "responding"}:
        return "Коробка занята текущим запросом."
    return {
        "guiding": "Коробка готова принять задачу.",
        "awaiting_scope": "Коробка ждёт область работы перед запуском.",
        "presenting_result": "Коробка ждёт следующий шаг по последней задаче.",
        "reviewing_task": "Коробка озвучивает состояние выбранной задачи.",
        "reviewing_history": "Коробка показывает последние задачи.",
        "idle": "Коробка ждёт следующую команду.",
    }.get(normalized_box_state, "Коробка ждёт следующую команду.")


def box_response_meta(session: dict[str, object] | None) -> dict[str, object]:
    payload = dict(session or {})
    box_state = normalize_box_state(payload.get("box_state", "idle"))
    pending_objective = str(payload.get("pending_objective", "")).strip()
    interaction_state = str(payload.get("interaction_state", "idle")).strip() or "idle"
    return {
        "box_state": box_state,
        "device_status": device_status_text(
            box_state=box_state,
            interaction_state=interaction_state,
        ),
        "awaiting_input": "scope" if box_state == "awaiting_scope" else "none",
        "has_pending_objective": bool(pending_objective),
    }
