from __future__ import annotations

from app.orchestrator import dev_task


def test_route_intelligence_selects_openai_for_strategy_tasks() -> None:
    task = dev_task.create_dev_task(
        id="DF-ROUTE-1",
        title="Quarterly strategy memo",
        objective="Build a planning roadmap for the execution system.",
        preferred_agent="auto",
    )

    route = dev_task.route_intelligence(task)

    assert route["selected_agent"] == "codex"
    assert route["selected_model"] == "openai"
    assert route["task_classification"] == "strategy_planning"
    assert route["parallel_execution_allowed"] is False


def test_route_intelligence_selects_gemini_for_google_tasks() -> None:
    task = dev_task.create_dev_task(
        id="DF-ROUTE-2",
        title="Google Docs summary",
        objective="Prepare a Google Docs update from spreadsheet data.",
        preferred_agent="auto",
    )

    route = dev_task.route_intelligence(task)

    assert route["selected_agent"] == "gemini"
    assert route["selected_model"] == "gemini"
    assert route["task_classification"] == "google_data_docs"
    assert route["parallel_execution_allowed"] is False


def test_route_intelligence_selects_claude_for_web_tasks() -> None:
    task = dev_task.create_dev_task(
        id="DF-ROUTE-3",
        title="LinkedIn form run",
        objective="Open a web form, fill it, and submit through browser execution.",
        preferred_agent="auto",
    )

    route = dev_task.route_intelligence(task)

    assert route["selected_agent"] == "claude"
    assert route["selected_model"] == "claude"
    assert route["task_classification"] == "web_external_execution"
    assert route["parallel_execution_allowed"] is False


def test_create_dev_task_normalizes_openai_to_codex() -> None:
    task = dev_task.create_dev_task(
        id="DF-ROUTE-4",
        title="Plan",
        objective="Strategy planning task.",
        preferred_agent="openai",
    )

    assert task.preferred_agent == "codex"


def test_build_execution_plan_exposes_selected_model_and_single_model_rule() -> None:
    task = dev_task.create_dev_task(
        id="DF-ROUTE-5",
        title="Google Sheets cleanup",
        objective="Clean Google Sheets data and prepare a Docs summary.",
        preferred_agent="auto",
    )

    plan = dev_task.build_execution_plan(task)

    assert plan.selected_agent == "gemini"
    assert plan.selected_model == "gemini"
    assert plan.parallel_execution_allowed is False
    assert "Google/data/docs" in plan.routing_reason or "Google" in plan.routing_reason


def test_dispatch_executor_routes_gemini_and_returns_model_metadata() -> None:
    task = dev_task.create_dev_task(
        id="DF-ROUTE-6",
        title="Google Drive export",
        objective="Use Google Drive data to prepare a document export.",
        preferred_agent="auto",
        scope_files=("README.md",),
    )
    plan = dev_task.build_execution_plan(task)

    result = dev_task.dispatch_executor(task, plan, workspace=None)

    assert result["selected_agent"] == "gemini"
    assert result["selected_model"] == "gemini"
    assert result["parallel_execution_allowed"] is False
    assert result["status"] == "partial"
