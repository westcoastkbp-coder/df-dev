# Task Queue

- Added minimal file-backed queue state at `runtime/state/task_queue.json`.
- Implemented queue helpers in `app/orchestrator/task_queue.py`:
  `enqueue_task(task_id)` appends `{"task_id": "...", "status": "pending"}`
  `dequeue_task()` removes the first queued item
- Integrated existing execution flow:
  tasks are enqueued when prepared for execution
  tasks are dequeued after execution completes in the worker `finally` path
- Queue actions are appended to `runtime/logs/tasks.log` as `queue_enqueue` and `queue_dequeue`.
- Verified with test:
  `python -m pytest -q tests/test_task_queue.py`
  Result: `1 passed`
- Regression check:
  `python -m pytest -q`
  Result: `9 passed`
