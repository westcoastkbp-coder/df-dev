from __future__ import annotations

import asyncio
import base64
import threading
from typing import Any

from app.voice.voice_runtime_store import queue_outbound_event


class VoiceRuntimeBus:
    def __init__(self) -> None:
        self._queues: dict[str, asyncio.Queue[dict[str, Any]]] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._lock = threading.RLock()

    def register(self, call_session_id: str) -> asyncio.Queue[dict[str, Any]]:
        normalized_call_session_id = str(call_session_id).strip()
        with self._lock:
            if self._loop is None:
                try:
                    self._loop = asyncio.get_running_loop()
                except RuntimeError:
                    self._loop = None
            queue = self._queues.get(normalized_call_session_id)
            if queue is None:
                queue = asyncio.Queue()
                self._queues[normalized_call_session_id] = queue
            return queue

    async def next_event(
        self,
        call_session_id: str,
        *,
        timeout_seconds: float = 30.0,
    ) -> dict[str, Any]:
        queue = self.register(call_session_id)
        return await asyncio.wait_for(queue.get(), timeout=timeout_seconds)

    def publish(self, call_session_id: str, event: dict[str, Any]) -> None:
        normalized_call_session_id = str(call_session_id).strip()
        with self._lock:
            queue = self._queues.get(normalized_call_session_id)
            loop = self._loop
        if queue is None or loop is None:
            return
        loop.call_soon_threadsafe(queue.put_nowait, dict(event))

    def close(self, call_session_id: str) -> None:
        normalized_call_session_id = str(call_session_id).strip()
        with self._lock:
            self._queues.pop(normalized_call_session_id, None)


VOICE_RUNTIME_BUS = VoiceRuntimeBus()


def tts_chunks(text: object, *, max_chars: int = 120) -> list[str]:
    normalized = " ".join(str(text or "").split()).strip()
    if not normalized:
        return []
    words = normalized.split(" ")
    chunks: list[str] = []
    current = ""
    for word in words:
        candidate = f"{current} {word}".strip()
        if current and len(candidate) > max_chars:
            chunks.append(current)
            current = word
            continue
        current = candidate
    if current:
        chunks.append(current)
    return chunks


def outbound_event_payload(
    *,
    event_id: str,
    sequence: int,
    kind: str,
    text: str,
    call_session_id: str,
) -> dict[str, Any]:
    encoded = base64.b64encode(text.encode("utf-8")).decode("ascii")
    return {
        "event": "tts_chunk",
        "event_id": event_id,
        "sequence": sequence,
        "call_session_id": call_session_id,
        "kind": kind,
        "text": text,
        "media": {
            "encoding": "base64-text",
            "payload": encoded,
        },
    }


def publish_tts_text(
    *,
    call_session_id: str,
    text: object,
    kind: str,
    interruptible: bool = True,
) -> list[dict[str, Any]]:
    published: list[dict[str, Any]] = []
    for chunk in tts_chunks(text):
        queued = queue_outbound_event(
            call_session_id,
            kind=kind,
            text=chunk,
            interruptible=interruptible,
        )
        event = outbound_event_payload(
            event_id=str(queued.get("event_id", "")).strip(),
            sequence=int(queued.get("sequence", 0)),
            kind=str(queued.get("kind", kind)).strip(),
            text=str(queued.get("text", "")).strip(),
            call_session_id=call_session_id,
        )
        VOICE_RUNTIME_BUS.publish(call_session_id, event)
        published.append(event)
    return published

