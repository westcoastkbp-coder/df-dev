from app.orchestrator.orchestrator import (
    TaskRequest as DevTask,
    run_task,
)


def dev_run(task: DevTask):
    return run_task(task)
