def build_escalation_decision(module: str, reason: str) -> dict:
    return {
        "status": "escalated",
        "module": module,
        "reason": reason,
        "action": "manual_intervention_required",
        "decision_trace": {
            "source": "memory_analysis",
            "trigger": reason,
            "confidence": "high",
        },
    }
