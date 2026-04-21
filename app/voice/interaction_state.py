from __future__ import annotations

from collections.abc import Iterable

from app.voice.session_store import update_session


def _payload_value(payload: object, name: str) -> str:
    return str(getattr(payload, name, "") or "").strip()


def session_mode_for_intent(intent: object) -> str:
    return {
        "run_task": "task_run",
        "read_task": "task_status",
        "read_task_history": "history_review",
        "help": "help",
        "repeat": "help",
    }.get(str(intent).strip(), "idle")


def session_context_block(session: dict[str, object] | None) -> dict[str, str]:
    payload = dict(session or {})
    return {
        "current_mode": str(payload.get("current_mode", "idle")).strip() or "idle",
        "interaction_state": str(payload.get("interaction_state", "idle")).strip()
        or "idle",
        "last_task": str(payload.get("last_task_id", "")).strip(),
    }


def display_short_status(
    *,
    interaction_state: object,
    current_mode: object,
    lifecycle_state: object = "",
    has_task: bool = False,
) -> str:
    normalized_interaction_state = str(interaction_state).strip()
    normalized_lifecycle_state = str(lifecycle_state).strip()
    if normalized_interaction_state == "idle":
        return "Ð¡Ð¸ÑÑ‚ÐµÐ¼Ð° Ð¶Ð´Ñ‘Ñ‚ ÑÐ»ÐµÐ´ÑƒÑŽÑ‰ÑƒÑŽ ÐºÐ¾Ð¼Ð°Ð½Ð´Ñƒ."
    if normalized_interaction_state == "listening":
        return "Ð¡Ð¸ÑÑ‚ÐµÐ¼Ð° ÑÐ»ÑƒÑˆÐ°ÐµÑ‚ Ð¿Ð¾Ð»ÑŒÐ·Ð¾Ð²Ð°Ñ‚ÐµÐ»Ñ."
    if normalized_interaction_state == "processing":
        return "Ð¡Ð¸ÑÑ‚ÐµÐ¼Ð° Ð¾Ð±Ñ€Ð°Ð±Ð°Ñ‚Ñ‹Ð²Ð°ÐµÑ‚ Ð·Ð°Ð¿Ñ€Ð¾Ñ."
    if normalized_interaction_state == "responding":
        return "Ð¡Ð¸ÑÑ‚ÐµÐ¼Ð° Ð¾Ñ‚Ð²ÐµÑ‡Ð°ÐµÑ‚ Ð¿Ð¾Ð»ÑŒÐ·Ð¾Ð²Ð°Ñ‚ÐµÐ»ÑŽ."
    if normalized_interaction_state == "waiting_for_followup":
        if has_task and normalized_lifecycle_state:
            return (
                "ÐœÐ¾Ð¶Ð½Ð¾ ÑÐ´ÐµÐ»Ð°Ñ‚ÑŒ ÑÐ»ÐµÐ´ÑƒÑŽÑ‰Ð¸Ð¹ ÑˆÐ°Ð³. ÐŸÐ¾ÑÐ»ÐµÐ´Ð½ÑÑ Ð·Ð°Ð´Ð°Ñ‡Ð° ÑÐµÐ¹Ñ‡Ð°Ñ Ð² ÑÐ¾ÑÑ‚Ð¾ÑÐ½Ð¸Ð¸ "
                f"{normalized_lifecycle_state}."
            )
        return "Ð¡Ð¸ÑÑ‚ÐµÐ¼Ð° Ð¶Ð´Ñ‘Ñ‚ Ð¾Ñ‚Ð²ÐµÑ‚Ð°. ÐœÐ¾Ð¶Ð½Ð¾ ÑÐ´ÐµÐ»Ð°Ñ‚ÑŒ ÑÐ»ÐµÐ´ÑƒÑŽÑ‰Ð¸Ð¹ ÑˆÐ°Ð³."
    return f"Ð ÐµÐ¶Ð¸Ð¼: {str(current_mode).strip() or 'idle'}."


def suggested_replies_for_context(
    *,
    current_mode: object,
    interaction_state: object,
    last_task_id: object,
) -> list[str]:
    normalized_mode = str(current_mode).strip()
    normalized_interaction_state = str(interaction_state).strip()
    normalized_last_task_id = str(last_task_id).strip()
    if normalized_interaction_state == "idle":
        return [
            "Run a scoped task",
            "ÐŸÐ¾ÐºÐ°Ð¶Ð¸ Ð¿Ð¾ÑÐ»ÐµÐ´Ð½Ð¸Ðµ Ð·Ð°Ð´Ð°Ñ‡Ð¸",
            "ÐŸÐ¾Ð¼Ð¾Ñ‰ÑŒ",
        ]
    if normalized_mode == "task_run":
        replies = [
            "ÐŸÐ¾ÐºÐ°Ð¶Ð¸ ÑÑ‚Ð°Ñ‚ÑƒÑ",
            "ÐŸÐ¾ÐºÐ°Ð¶Ð¸ Ð¿Ð¾ÑÐ»ÐµÐ´Ð½Ð¸Ðµ Ð·Ð°Ð´Ð°Ñ‡Ð¸",
        ]
        if normalized_last_task_id:
            replies[0] = f"Status for {normalized_last_task_id}"
        return replies
    if normalized_mode == "task_status":
        return [
            "Ð§Ñ‚Ð¾ Ð´Ð°Ð»ÑŒÑˆÐµ",
            "ÐŸÐ¾Ð²Ñ‚Ð¾Ñ€Ð¸",
            "ÐŸÐ¾ÐºÐ°Ð¶Ð¸ Ð¿Ð¾ÑÐ»ÐµÐ´Ð½Ð¸Ðµ Ð·Ð°Ð´Ð°Ñ‡Ð¸",
        ]
    if normalized_mode == "history_review":
        return [
            "ÐŸÐ¾ÐºÐ°Ð¶Ð¸ ÑÑ‚Ð°Ñ‚ÑƒÑ",
            "Run a scoped task",
            "ÐŸÐ¾Ð²Ñ‚Ð¾Ñ€Ð¸",
        ]
    return [
        "Run a scoped task",
        "ÐŸÐ¾ÐºÐ°Ð¶Ð¸ ÑÑ‚Ð°Ñ‚ÑƒÑ",
        "ÐŸÐ¾ÐºÐ°Ð¶Ð¸ Ð¿Ð¾ÑÐ»ÐµÐ´Ð½Ð¸Ðµ Ð·Ð°Ð´Ð°Ñ‡Ð¸",
    ]


def voice_summary_with_followup(user_summary: object, *, waiting: bool) -> str:
    summary = str(user_summary or "").strip()
    additions: list[str] = []
    if waiting:
        additions.append("SESSION:")
        additions.append("- Ð¡Ð¸ÑÑ‚ÐµÐ¼Ð° Ð¶Ð´Ñ‘Ñ‚ Ð¾Ñ‚Ð²ÐµÑ‚Ð°.")
        additions.append("- ÐœÐ¾Ð¶Ð½Ð¾ ÑÐ´ÐµÐ»Ð°Ñ‚ÑŒ ÑÐ»ÐµÐ´ÑƒÑŽÑ‰Ð¸Ð¹ ÑˆÐ°Ð³.")
    if not additions:
        return summary
    if summary:
        return summary + "\n\n" + "\n".join(additions)
    return "\n".join(additions)


def update_voice_session_state(
    *,
    session: dict[str, str],
    payload: object,
    intent: object,
    current_mode: object,
    interaction_state: object = "",
    task_id: object = "",
    response_summary: object = "",
    box_state: object | None = None,
    pending_objective: object | None = None,
) -> dict[str, str]:
    last_task_id = str(task_id).strip() or str(session.get("last_task_id", "")).strip()
    return update_session(
        session["session_id"],
        user_id=_payload_value(payload, "user_id") or session.get("user_id", ""),
        user_role=_payload_value(payload, "user_role") or session.get("user_role", ""),
        current_mode=current_mode,
        interaction_state=interaction_state
        or str(session.get("interaction_state", "")).strip()
        or "idle",
        last_task_id=last_task_id,
        last_intent=intent,
        last_response_summary=response_summary,
        box_state=(
            box_state
            if box_state is not None
            else str(session.get("box_state", "idle")).strip() or "idle"
        ),
        pending_objective=(
            pending_objective
            if pending_objective is not None
            else str(session.get("pending_objective", "")).strip()
        ),
    )


def advance_voice_session(
    *,
    session: dict[str, str],
    payload: object,
    intent: object,
    current_mode: object,
    states: Iterable[object],
    task_id: object = "",
    response_summary: object = "",
    box_state: object | None = None,
    pending_objective: object | None = None,
) -> dict[str, str]:
    updated = dict(session)
    for state in states:
        updated = update_voice_session_state(
            session=updated,
            payload=payload,
            intent=intent,
            current_mode=current_mode,
            interaction_state=state,
            task_id=task_id,
            response_summary=response_summary,
            box_state=box_state,
            pending_objective=pending_objective,
        )
    return updated
