from datetime import datetime

from control.dev_runtime import run_in_dev_env


def get_git_commit():
    try:
        return run_in_dev_env(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except Exception:
        return "no_git"


def get_git_branch():
    try:
        return run_in_dev_env(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except Exception:
        return "no_branch"


def ensure_not_main_branch():
    try:
        branch = run_in_dev_env(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except Exception as exc:
        raise RuntimeError("GIT_CONTEXT_REQUIRED") from exc

    if branch == "main":
        raise RuntimeError("DIRECT_MODIFICATION_OF_MAIN_BRANCH_FORBIDDEN")
