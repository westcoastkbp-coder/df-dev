from control.audit_log import read_audit_log


def analyze_recent_runs(module: str, limit: int = 10):
    audit = read_audit_log(limit=limit)

    entries = [e for e in audit if e.get("module") == module]

    stats = {
        "total": len(entries),
        "fail": sum(1 for e in entries if e.get("status") == "FAIL"),
        "blocked": sum(1 for e in entries if e.get("status") == "BLOCKED"),
        "working": sum(1 for e in entries if e.get("status") == "WORKING"),
    }

    return stats


def detect_unstable_module(module: str):
    stats = analyze_recent_runs(module)

    if stats["blocked"] >= 3:
        return {
            "unstable": True,
            "reason": "repeated_blocked_reviews",
        }

    if stats["fail"] + stats["blocked"] >= 3:
        return {
            "unstable": True,
            "reason": "repeated_failures",
        }

    return {
        "unstable": False,
        "reason": "",
    }
