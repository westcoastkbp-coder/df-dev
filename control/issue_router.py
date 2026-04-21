def detect_issue_type(title: str, body: str = "") -> str:
    text = f"{title}\n{body}".upper()

    if "TEST" in text:
        return "SYSTEM_TEST"

    if "BUG" in text or "ERROR" in text or "FAIL" in text:
        return "BUG_REPORT"

    if "TASK" in text or "TODO" in text:
        return "TASK"

    return "UNKNOWN"


if __name__ == "__main__":
    samples = [
        ("DF SYSTEM TEST ISSUE 001", ""),
        ("BUG: router failed", ""),
        ("TASK: create policy file", ""),
        ("random note", ""),
    ]

    print("=== ISSUE ROUTER TEST ===")

    for title, body in samples:
        issue_type = detect_issue_type(title, body)
        print(f"{title} -> {issue_type}")
