def classify_error(local_test: str, review: str) -> str:
    if local_test == "FAIL":
        return "execution_error"
    if review == "BLOCKED":
        return "verification_error"
    if local_test == "TIMEOUT":
        return "timeout_error"
    return "unknown_error"
