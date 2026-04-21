def build_review_packet(task_id, summary, files):
    return {
        "task_id": task_id,
        "change_summary": summary,
        "files_changed": files,
        "expected_invariant": "no_side_effects_before_execution_mode",
        "execution_mode": "test",
    }
