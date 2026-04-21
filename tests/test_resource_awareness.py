from __future__ import annotations

import app.execution.execution_boundary as execution_boundary_module
from app.product.command_parser import parse_command
from app.product.intake import build_product_task_request
from app.product.runner import dispatch_development_action
from app.orchestrator.execution_router import materialize_routing_contract
from runtime.policy_engine import decide_execution_mode


def test_parse_command_maps_resources_command() -> None:
    assert parse_command("resources") == {
        "action": "resources",
        "tasks": ["DF-RESOURCES"],
    }


def test_build_product_task_request_maps_resources_command() -> None:
    class _Payload:
        task_id = ""
        objective = "resources"
        scope_files = []

    request = build_product_task_request(_Payload())

    assert request["task_id"] == "DF-RESOURCES"
    assert request["descriptor_action"] == "RESOURCES"


def test_dispatch_development_action_returns_resource_snapshot(monkeypatch) -> None:
    snapshot = {
        "cpu": {"count": 16},
        "ram": {"total_gb": 32.0, "available_gb": 20.5},
        "gpu": {"available": True, "name": "Test GPU", "vram_gb": 8.0},
        "mode": "gpu-enabled",
    }

    import app.product.runner as runner_module

    monkeypatch.setattr(
        runner_module,
        "get_resource_snapshot",
        lambda: snapshot,
    )

    with execution_boundary_module.execution_boundary(
        {"task_id": "DF-RESOURCES", "intent": "resources"},
        policy_validated=True,
    ):
        result = dispatch_development_action(
            action_type="RESOURCES",
            task_id="DF-RESOURCES",
        )

    assert result["status"] == "completed"
    assert result["result_type"] == "resource_snapshot"
    assert result["resource_snapshot"] == snapshot
    assert "CPU 16 cores" in str(result["result_summary"])


def test_decide_execution_mode_uses_gpu_compute_mode_when_gpu_available(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "runtime.policy_engine.get_runtime_profile",
        lambda: "FULL",
    )
    monkeypatch.setattr(
        "runtime.policy_engine.build_runtime_decision",
        lambda *args, **kwargs: {
            "overall_runtime_state": "NORMAL",
            "execution_preference": "SAFE_LOCAL",
            "offload_recommended": False,
            "voice_safety_mode": False,
            "confidence": "HIGH_CONFIDENCE",
            "adaptive_thresholds": {},
            "prediction": {},
            "preemptive_actions": {},
        },
    )
    monkeypatch.setattr(
        "runtime.policy_engine.build_network_policy",
        lambda *args, **kwargs: {"policy_mode": "NORMAL"},
    )

    decision = decide_execution_mode(
        task_data={},
        metrics={
            "cpu": 12.0,
            "temperature": 44.0,
            "execution_compute_mode": "gpu_mode",
        },
        network_snapshot={},
    )

    assert decision[0] == "LOCAL"
    assert decision[1] == "gpu_mode"
    assert decision[2]["execution_compute_mode"] == "gpu_mode"


def test_decide_execution_mode_uses_cpu_compute_mode_when_gpu_unavailable(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "runtime.policy_engine.get_runtime_profile",
        lambda: "FULL",
    )
    monkeypatch.setattr(
        "runtime.policy_engine.build_runtime_decision",
        lambda *args, **kwargs: {
            "overall_runtime_state": "NORMAL",
            "execution_preference": "SAFE_LOCAL",
            "offload_recommended": False,
            "voice_safety_mode": False,
            "confidence": "HIGH_CONFIDENCE",
            "adaptive_thresholds": {},
            "prediction": {},
            "preemptive_actions": {},
        },
    )
    monkeypatch.setattr(
        "runtime.policy_engine.build_network_policy",
        lambda *args, **kwargs: {"policy_mode": "NORMAL"},
    )

    decision = decide_execution_mode(
        task_data={},
        metrics={
            "cpu": 18.0,
            "temperature": 39.0,
            "execution_compute_mode": "cpu_mode",
        },
        network_snapshot={},
    )

    assert decision[0] == "LOCAL"
    assert decision[1] == "cpu_mode"
    assert decision[2]["execution_compute_mode"] == "cpu_mode"


def test_materialize_routing_contract_includes_execution_compute_mode() -> None:
    contract = materialize_routing_contract(
        execution_mode="LOCAL",
        execution_compute_mode="gpu_mode",
        runtime_profile="FULL",
        routing_reason={"type": "runtime_decision"},
        telemetry_snapshot={"execution_compute_mode": "gpu_mode"},
        safety_override={"triggered": False},
        network_snapshot={"quality": "GOOD"},
        network_policy={"policy_mode": "NORMAL"},
        runtime_decision={"overall_runtime_state": "NORMAL"},
        runtime_validation={"state": "PASSED"},
    )

    assert contract["execution_mode"] == "LOCAL"
    assert contract["execution_compute_mode"] == "gpu_mode"
    assert contract["execution_location"] == "local"
