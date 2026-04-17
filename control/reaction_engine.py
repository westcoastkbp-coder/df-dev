[09.04.2026 19:05] Anton: import json
import os
import subprocess
import time
from datetime import datetime

from github import Github
from control.issue_router import detect_issue_type


PROCESSED_FILE = "control/processed_issues.json"
RUNTIME_DIR = "runtime"
POLICY_FILE = "control/policy.json"
REPO_NAME = "westcoastkbp-coder/jarvis-digital-foreman"
BASE_BRANCH = "clean-codex"


def load_processed():
    if os.path.exists(PROCESSED_FILE):
        with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_processed(data):
    with open(PROCESSED_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def load_policy():
    if os.path.exists(POLICY_FILE):
        with open(POLICY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "SYSTEM_TEST": {"message": "SYSTEM RESPONSE: test processed"},
        "BUG_REPORT": {"message": "SYSTEM RESPONSE: bug detected and logged"},
        "TASK": {"message": "SYSTEM RESPONSE: task accepted and executed"},
        "UNKNOWN": {"message": "SYSTEM RESPONSE: unknown type"},
    }


def run_command(cmd):
    return subprocess.run(cmd, check=False, capture_output=True, text=True)


def create_result_file(issue, issue_type):
    os.makedirs(RUNTIME_DIR, exist_ok=True)
    path = os.path.join(RUNTIME_DIR, f"issue_{issue.number}_result.txt")

    with open(path, "w", encoding="utf-8") as f:
        f.write(f"ISSUE: {issue.number}\n")
        f.write(f"TITLE: {issue.title}\n")
        f.write(f"TYPE: {issue_type}\n")
        f.write(f"TIME: {datetime.utcnow().isoformat()}Z\n")

    return path


def resolve_issue_type(issue):
    for label in issue.labels:
        if label.name.startswith("TYPE:"):
            return label.name.replace("TYPE: ", "").strip()

    return detect_issue_type(issue.title, issue.body or "")


def set_status(issue, status):
    issue = issue.repository.get_issue(number=issue.number)

    labels_to_keep = []
    for label in issue.labels:
        if not label.name.startswith("STATUS:"):
            labels_to_keep.append(label.name)

    issue.set_labels(*labels_to_keep)
    time.sleep(1)

    issue.add_to_labels(f"STATUS: {status}")
    time.sleep(1)


def git_commit_and_create_pr(path, issue_number):
    branch_name = f"issue-{issue_number}"

    print(f"GIT: checkout {BASE_BRANCH}")
    result = run_command(["git", "checkout", BASE_BRANCH])
    if result.stdout:
        print(result.stdout.strip())
    if result.stderr:
        print(result.stderr.strip())

    print(f"GIT: checkout -B {branch_name}")
    result = run_command(["git", "checkout", "-B", branch_name])
    if result.stdout:
        print(result.stdout.strip())
    if result.stderr:
        print(result.stderr.strip())

    print(f"GIT: add {path}")
    result = run_command(["git", "add", "-f", path])
    if result.stdout:
        print(result.stdout.strip())
    if result.stderr:
        print(result.stderr.strip())

    print(f"GIT: commit issue #{issue_number}")
    result = run_command(
        ["git", "commit", "-m", f"RESULT: auto commit for issue #{issue_number}"]
    )
    if result.stdout:
        print(result.stdout.strip())
    if result.stderr:
        print(result.stderr.strip())

    print(f"GIT: push {branch_name}")
    result = run_command(["git", "push", "-u", "origin", branch_name])
    if result.stdout:
        print(result.stdout.strip())
    if result.stderr:
        print(result.stderr.strip())

    print(f"GH: create PR for {branch_name}")
    result = run_command(
        [
            "gh",
            "pr",
            "create",
            "--repo",
            REPO_NAME,
            "--base",
            BASE_BRANCH,
            "--head",
            branch_name,
            "--title",
            f"RESULT: auto commit for issue #{issue_number}",
            "--body",
            f"Auto-generated PR for issue #{issue_number}",
        ]
    )
    if result.stdout:
        print(result.stdout.strip())
    if result.stderr:
        print(result.stderr.strip())


def main():
    token = os.
[09.04.2026 19:05] Anton: getenv("GITHUB_TOKEN")
    if not token:
        print("NO GITHUB TOKEN")
        return

    g = Github(token)
    repo = g.get_repo(REPO_NAME)

    processed = load_processed()
    policy = load_policy()

    print("=== REACTION ENGINE ===")

    for issue in repo.get_issues(state="open"):
        issue_id = str(issue.number)

        if processed.get(issue_id):
            print(f"SKIP #{issue.number} (already processed)")
            continue

        set_status(issue, "PROCESSING")

        issue_type = resolve_issue_type(issue)

        print(f"PROCESSING ISSUE #{issue.number}")
        print(f"ROUTED TYPE: {issue_type}")

        path = create_result_file(issue, issue_type)

        message = policy.get(issue_type, policy["UNKNOWN"])["message"]

        issue.create_comment(
            f"{message}\nType: {issue_type}\nArtifact: `{path}`"
        )

        git_commit_and_create_pr(path, issue.number)

        processed[issue_id] = True
        save_processed(processed)

        set_status(issue, "DONE")

        print(f"RESPONDED TO #{issue.number}")
        print(f"CREATED + COMMITTED: {path}")
        print(f"PR FLOW TRIGGERED FOR ISSUE #{issue.number}")


if __name__ == "__main__":
    main()