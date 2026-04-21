from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable
from threading import Event, Thread

from app.execution.paths import LOGS_DIR, ROOT_DIR
from app.execution.product_executor import execute_product_task
from app.orchestrator.execution_runner import run_execution
from app.orchestrator.execution_router import route_execution
from app.orchestrator.escalation import decide_escalation_action, record_escalation
from app.orchestrator.offload_manager import offload_task
from app.orchestrator.stuck_tasks import enforce_stuck_tasks
from app.orchestrator.system_health import assess_system_health
from app.orchestrator.task_queue import InMemoryTaskQueue, task_queue
from app.orchestrator.task_lifecycle import set_task_state, normalize_task_status
from app.orchestrator.task_state_store import StatePersistenceError
from runtime.network.monitor import get_network_snapshot
from runtime.system_log import build_mode_trace, log_event
from runtime.telemetry import collect_runtime_metrics
from runtime.token_telemetry import get_average_token_cost


TaskFetcher = Callable[[str], dict[str, object] | None]
TaskPersister = Callable[[dict[str, object]], None]
NowProvider = Callable[[], str]
DecisionResolver = Callable[..., dict[str, object]]
ActiveTaskLoader = Callable[[], list[dict[str, object]]]

EXECUTION_PRIORITY_LOG_FILE = ROOT_DIR / LOGS_DIR / "execution_priority.jsonl"
DEFAULT_ESTIMATED_TOKENS = 1000
PRIORITY_RANK = {
    "CRITICAL": 0,
    "HIGH": 1,
    "NORMAL": 2,
    "LOW": 3,
}


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_priority(value: object) -> str:
    normalized = _normalize_text(value).upper()
    if normalized in PRIORITY_RANK:
        return normalized
    if normalized == "P0":
        return "CRITICAL"
    if normalized == "P1":
        return "HIGH"
    if normalized == "P2":
        return "LOW"
    if normalized == "MEDIUM":
        return "NORMAL"
    return "NORMAL"


def _task_priority(task_data: dict[str, object]) -> str:
    payload = dict(task_data.get("payload", {}) or {})
    if _normalize_text(task_data.get("priority")):
        return _normalize_priority(task_data.get("priority"))
    if _normalize_text(payload.get("priority")):
        return _normalize_priority(payload.get("priority"))
    if _normalize_text(task_data.get("mvp_priority")):
        return _normalize_priority(task_data.get("mvp_priority"))
    if _normalize_text(payload.get("mvp_priority")):
        return _normalize_priority(payload.get("mvp_priority"))
    return "NORMAL"


def _task_type_for_budget(task_data: dict[str, object]) -> str:
    payload = dict(task_data.get("payload", {}) or {})
    return (
        _normalize_text(task_data.get("task_type"))
        or _normalize_text(payload.get("task_type"))
        or _normalize_text(task_data.get("intent"))
        or "unknown"
    )


def _approval_allows_execution(task_data: dict[str, object]) -> bool:
    approval_status = _normalize_text(task_data.get("approval_status")).lower()
    if not approval_status:
        return True
    return approval_status == "approved"


def _estimated_token_cost(
    task_data: dict[str, object],
    *,
    token_cost_snapshot: dict[str, object],
    default_estimated_tokens: int,
) -> int:
    breakdown = dict(token_cost_snapshot.get("breakdown_per_task_type", {}) or {})
    task_bucket = dict(breakdown.get(_task_type_for_budget(task_data), {}) or {})
    if task_bucket.get("avg_tokens_per_run") not in (None, ""):
        return max(0, int(round(float(task_bucket["avg_tokens_per_run"]))))
    avg_tokens_per_run = token_cost_snapshot.get("avg_tokens_per_run")
    if avg_tokens_per_run not in (None, ""):
        return max(0, int(round(float(avg_tokens_per_run))))
    return max(0, int(default_estimated_tokens))


def _append_execution_priority_log(entry: dict[str, object]) -> None:
    EXECUTION_PRIORITY_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with EXECUTION_PRIORITY_LOG_FILE.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=True, separators=(",", ":")) + "\n")


def _log_priority_decision(
    *,
    selected_task: str,
    skipped_tasks: list[dict[str, object]],
    reason: str,
    remaining_budget: int | None,
) -> None:
    _append_execution_priority_log(
        {
            "selected_task": selected_task,
            "skipped_tasks": skipped_tasks,
            "reason": reason,
            "remaining_budget": remaining_budget,
        }
    )


def _build_budget_deferred_signal(task_id: str) -> dict[str, object]:
    return {
        "status": "budget_deferred",
        "task_id": task_id,
        "reason": "insufficient_token_budget",
    }


def _emit_mode_trace(task_data: dict[str, object], routing_decision: dict[str, object]) -> None:
    trace = build_mode_trace(
        task_id=str(task_data.get("task_id", "")).strip(),
        execution_mode=str(routing_decision.get("execution_mode", "")).strip().upper(),
        compute_mode=str(
            routing_decision.get(
                "execution_compute_mode",
                task_data.get("execution_compute_mode", "cpu_mode"),
            )
        ).strip(),
    )
    print(trace)
    log_event("mode", trace, task_id=task_data.get("task_id"))


def _default_now() -> str:
    from app.orchestrator.task_factory import now

    return now()


def _default_fetch_task(task_id: str) -> dict[str, object] | None:
    from app.orchestrator.task_factory import get_task

    return get_task(task_id)


def _default_persist(task_data: dict[str, object]) -> None:
    from app.orchestrator.task_factory import save_task

    save_task(task_data)


def _default_load_active_tasks() -> list[dict[str, object]]:
    from app.orchestrator.task_factory import get_open_tasks

    try:
        return get_open_tasks()
    except Exception:
        return []


def _select_next_task_id(
    *,
    queue: InMemoryTaskQueue,
    fetch_task: TaskFetcher,
    max_tokens_per_run: int | None,
    default_estimated_tokens: int,
    token_cost_snapshot: dict[str, object],
) -> tuple[str | None, list[dict[str, object]], int | None]:
    queue_entries = queue.queued_task_ids()
    if not queue_entries:
        return None, [], max_tokens_per_run

    candidates: list[tuple[int, str, dict[str, object], int]] = []
    for index, task_id in enumerate(queue_entries):
        task_data = fetch_task(task_id)
        if task_data is None:
            continue
        if normalize_task_status(task_data.get("status")) != "VALIDATED":
            continue
        if not _approval_allows_execution(task_data):
            continue
        candidates.append(
            (
                index,
                task_id,
                task_data,
                _estimated_token_cost(
                    task_data,
                    token_cost_snapshot=token_cost_snapshot,
                    default_estimated_tokens=default_estimated_tokens,
                ),
            )
        )

    if not candidates:
        return None, [], max_tokens_per_run

    sorted_candidates = sorted(
        candidates,
        key=lambda item: (
            PRIORITY_RANK[_task_priority(item[2])],
            item[0],
        ),
    )

    skipped_tasks: list[dict[str, object]] = []
    for _, task_id, task_data, estimated_cost in sorted_candidates:
        if max_tokens_per_run is not None and max_tokens_per_run < estimated_cost:
            skipped_tasks.append(
                {
                    "task_id": task_id,
                    "reason": "insufficient_token_budget",
                    "priority": _task_priority(task_data),
                    "estimated_cost": estimated_cost,
                }
            )
            continue
        if queue.reserve_task(task_id):
            return task_id, skipped_tasks, max_tokens_per_run

    return None, skipped_tasks, max_tokens_per_run


def process_next_queued_task(
    *,
    queue: InMemoryTaskQueue = task_queue,
    now: NowProvider = _default_now,
    fetch_task: TaskFetcher = _default_fetch_task,
    persist: TaskPersister = _default_persist,
    timeout: float = 0.1,
    executor=execute_product_task,
    telemetry_collector: Callable[[], dict[str, object]] = collect_runtime_metrics,
    network_snapshot_collector: Callable[[], dict[str, object]] = get_network_snapshot,
    decision_resolver: DecisionResolver = route_execution,
    max_tokens_per_run: int | None = None,
    default_estimated_tokens: int = DEFAULT_ESTIMATED_TOKENS,
    token_cost_resolver: Callable[..., dict[str, object]] = get_average_token_cost,
    active_task_loader: ActiveTaskLoader = _default_load_active_tasks,
    system_context: dict[str, object],
) -> dict[str, object] | None:
    if system_context is None:
        raise RuntimeError("SYSTEM_CONTEXT_REQUIRED")
    if system_context["status"] != "WORKING":
        return {
            "status": "blocked",
            "reason": "system_not_ready",
            "broken": system_context["broken"],
        }

    current_timestamp = now()
    enforce_stuck_tasks(
        active_task_loader(),
        now_timestamp=current_timestamp,
        persist=persist,
    )
    assess_system_health(
        active_task_loader(),
        now_timestamp=current_timestamp,
        phase="pre_execution",
    )
    deadline = time.monotonic() + max(0.0, float(timeout))
    skipped_tasks: list[dict[str, object]] = []
    while True:
        token_cost_snapshot = token_cost_resolver()
        task_id, skipped_tasks, remaining_budget = _select_next_task_id(
            queue=queue,
            fetch_task=fetch_task,
            max_tokens_per_run=max_tokens_per_run,
            default_estimated_tokens=default_estimated_tokens,
            token_cost_snapshot=token_cost_snapshot,
        )
        if task_id is not None:
            _log_priority_decision(
                selected_task=task_id,
                skipped_tasks=skipped_tasks,
                reason="selected_for_execution",
                remaining_budget=remaining_budget,
            )
            break
        if skipped_tasks:
            selected_task = str(skipped_tasks[0].get("task_id") or "").strip()
            _log_priority_decision(
                selected_task=selected_task,
                skipped_tasks=skipped_tasks,
                reason="insufficient_token_budget",
                remaining_budget=remaining_budget,
            )
            assess_system_health(
                active_task_loader(),
                now_timestamp=now(),
                phase="budget_deferred",
            )
            return _build_budget_deferred_signal(selected_task)
        if time.monotonic() >= deadline:
            assess_system_health(
                active_task_loader(),
                now_timestamp=now(),
                phase="idle_timeout",
            )
            return None
        time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))

    try:
        task_data = fetch_task(task_id)
        if task_data is None:
            assess_system_health(
                active_task_loader(),
                now_timestamp=now(),
                phase="task_missing",
            )
            return None
        if normalize_task_status(task_data.get("status")) != "VALIDATED":
            assess_system_health(
                active_task_loader(),
                now_timestamp=now(),
                phase="task_not_validated",
                task_data=task_data,
            )
            return task_data
        try:
            routing_decision = decision_resolver(
                task_data,
                now=now,
                persist=persist,
                telemetry_collector=telemetry_collector,
                network_snapshot_collector=network_snapshot_collector,
            )
            _emit_mode_trace(task_data, routing_decision)
            execution_mode = str(routing_decision.get("execution_mode", "")).strip().upper()
            if execution_mode == "REMOTE":
                with asyncio.Runner() as runner:
                    executed_task = runner.run(
                        offload_task(
                            task_data,
                            now=now,
                            persist=persist,
                            executor=executor,
                        )
                    )
            else:
                task_data["offload_latency"] = None
                executed_task = run_execution(
                    task_data,
                    now=now,
                    persist=persist,
                    executor=executor,
                )
        except Exception as exc:
            if isinstance(exc, StatePersistenceError):
                task_data["result"] = dict(exc.signal)
                task_data["error"] = str(exc)
                return task_data
            task_data["error"] = str(exc).strip() or "execution failed"
            try:
                set_task_state(
                    task_data,
                    "FAILED",
                    timestamp=now(),
                    details=task_data["error"],
                )
            except Exception:
                task_data["result"] = {
                    "status": "invalid_state_transition",
                    "task_id": str(task_data.get("task_id", "")).strip(),
                    "from": normalize_task_status(task_data.get("status")),
                    "to": "FAILED",
                }
            escalation_decision = decide_escalation_action(
                task_data,
                reason=task_data["error"],
                signal=dict(task_data.get("result", {}) or {}) if isinstance(task_data.get("result"), dict) else None,
            )
            if escalation_decision.get("action") == "escalate" and isinstance(escalation_decision.get("signal"), dict):
                record_escalation(task_data, escalation_decision["signal"])
            persist(task_data)
            log_event("validation", f"task worker blocked {task_id}: {task_data['error']}")
            assess_system_health(
                active_task_loader(),
                now_timestamp=now(),
                phase="post_execution",
                task_data=task_data,
            )
            return task_data
        if normalize_task_status(executed_task.get("status")) == "COMPLETED":
            from memory.storage import save_task_record

            save_task_record(executed_task)
        assess_system_health(
            active_task_loader(),
            now_timestamp=now(),
            phase="post_execution",
            task_data=executed_task,
        )
        return executed_task
    finally:
        queue.complete(task_id)


class TaskWorker:
    def __init__(
        self,
        *,
        queue: InMemoryTaskQueue = task_queue,
        poll_interval: float = 0.1,
        now: NowProvider = _default_now,
        fetch_task: TaskFetcher = _default_fetch_task,
        persist: TaskPersister = _default_persist,
        executor=execute_product_task,
        telemetry_collector: Callable[[], dict[str, object]] = collect_runtime_metrics,
        network_snapshot_collector: Callable[[], dict[str, object]] = get_network_snapshot,
        decision_resolver: DecisionResolver = route_execution,
        max_tokens_per_run: int | None = None,
        default_estimated_tokens: int = DEFAULT_ESTIMATED_TOKENS,
        token_cost_resolver: Callable[..., dict[str, object]] = get_average_token_cost,
        active_task_loader: ActiveTaskLoader = _default_load_active_tasks,
        system_context: dict[str, object],
        name: str = "operator-task-worker",
    ) -> None:
        if system_context is None:
            raise RuntimeError("SYSTEM_CONTEXT_REQUIRED")
        self._queue = queue
        self._poll_interval = poll_interval
        self._now = now
        self._fetch_task = fetch_task
        self._persist = persist
        self._executor = executor
        self._telemetry_collector = telemetry_collector
        self._network_snapshot_collector = network_snapshot_collector
        self._decision_resolver = decision_resolver
        self._max_tokens_per_run = max_tokens_per_run
        self._default_estimated_tokens = default_estimated_tokens
        self._token_cost_resolver = token_cost_resolver
        self._active_task_loader = active_task_loader
        self._system_context = dict(system_context)
        self._name = name
        self._stop_event = Event()
        self._thread: Thread | None = None

    def start(self) -> None:
        if self.is_running:
            return
        self._stop_event.clear()
        self._thread = Thread(
            target=self._run_loop,
            name=self._name,
            daemon=True,
        )
        self._thread.start()

    def stop(self, *, timeout: float = 1.0) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread is None:
            return
        thread.join(timeout=max(0.0, float(timeout)))
        if not thread.is_alive():
            self._thread = None

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            processed_task = process_next_queued_task(
                queue=self._queue,
                now=self._now,
                fetch_task=self._fetch_task,
                persist=self._persist,
                timeout=min(max(0.0, float(self._poll_interval)), 0.05),
                executor=self._executor,
                telemetry_collector=self._telemetry_collector,
                network_snapshot_collector=self._network_snapshot_collector,
                decision_resolver=self._decision_resolver,
                max_tokens_per_run=self._max_tokens_per_run,
                default_estimated_tokens=self._default_estimated_tokens,
                token_cost_resolver=self._token_cost_resolver,
                active_task_loader=self._active_task_loader,
                system_context=self._system_context,
            )
            if processed_task is None and self._poll_interval > 0.05:
                time.sleep(min(self._poll_interval - 0.05, 0.05))


__all__ = [
    "TaskWorker",
    "process_next_queued_task",
]

