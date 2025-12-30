"""Phase 0 Spike Tests.

These tests validate the core assumptions about CrewAI + Temporal integration.
Run with: pytest temporalio/contrib/crewai/_spike/test_spike.py -v

Requirements:
- Temporal server running locally (or use test server)
- crewai package installed
"""

import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from temporalio import workflow
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from .activities import SpikeActivities, SpikeActivityConfig, get_spike_activities
from .models import (
    LLMCallInput,
    LLMCallOutput,
    MemorySaveInput,
    MemorySearchInput,
    ToolCallInput,
)


# =============================================================================
# Test Workflows
# =============================================================================


@workflow.defn
class SimpleLLMWorkflow:
    """Test workflow that makes a simple LLM call."""

    @workflow.run
    async def run(self, prompt: str) -> str:
        result = await workflow.execute_activity(
            "spike_llm_call",
            LLMCallInput(
                model="gpt-4",
                messages=[{"role": "user", "content": prompt}],
            ),
            start_to_close_timeout=timedelta(seconds=30),
        )
        # Activity returns dict (JSON-deserialized), not dataclass
        return result["content"] or ""


@workflow.defn
class ToolCallWorkflow:
    """Test workflow that handles tool calls from LLM."""

    @workflow.run
    async def run(self, prompt: str) -> str:
        # First LLM call - may return tool_calls
        llm_result = await workflow.execute_activity(
            "spike_llm_call",
            LLMCallInput(
                model="gpt-4",
                messages=[{"role": "user", "content": prompt}],
                tools=[{
                    "name": "calculator",
                    "description": "Performs calculations",
                    "parameters": {"expression": {"type": "string"}},
                }],
            ),
            start_to_close_timeout=timedelta(seconds=30),
        )

        # Activity returns dict (JSON-deserialized), not dataclass
        # If tool calls returned, execute them
        if llm_result.get("tool_calls"):
            tool_results = []
            for tool_call in llm_result["tool_calls"]:
                # Extract tool info (format varies by LLM provider)
                func = tool_call.get("function", {})
                tool_name = func.get("name", "unknown")
                tool_args = func.get("arguments", {})

                # Execute tool as activity
                tool_result = await workflow.execute_activity(
                    "spike_tool_call",
                    ToolCallInput(tool_name=tool_name, tool_args=tool_args),
                    start_to_close_timeout=timedelta(seconds=30),
                )
                tool_results.append(tool_result["result"])

            return f"Tool results: {tool_results}"

        return llm_result.get("content") or ""


@workflow.defn
class MemoryWorkflow:
    """Test workflow that uses memory operations."""

    @workflow.run
    async def run(self, value_to_store: str, search_query: str) -> list[dict]:
        # Save to memory
        await workflow.execute_activity(
            "spike_memory_save",
            MemorySaveInput(
                storage_type="short_term",
                value=value_to_store,
                metadata={"source": "test"},
            ),
            start_to_close_timeout=timedelta(seconds=10),
        )

        # Search memory
        result = await workflow.execute_activity(
            "spike_memory_search",
            MemorySearchInput(
                storage_type="short_term",
                query=search_query,
                limit=5,
            ),
            start_to_close_timeout=timedelta(seconds=10),
        )

        # Activity returns dict (JSON-deserialized), not dataclass
        return result["results"]


# =============================================================================
# Mock LLM for Testing
# =============================================================================


class MockLLM:
    """Mock LLM that simulates CrewAI's LLM behavior."""

    def __init__(self, model: str):
        self.model = model
        self._response: str | list = "Mock response"

    def set_response(self, response: str | list):
        """Set the response to return from acall."""
        self._response = response

    async def acall(
        self,
        messages: list[dict],
        tools: list[dict] | None = None,
        available_functions: dict | None = None,
        **kwargs,
    ) -> str | list:
        """Simulate CrewAI's LLM.acall behavior.

        Key behavior being tested:
        - When available_functions is None and tools requested tool_calls,
          return the tool_calls directly (not execute them)
        """
        # If tool_calls are in the mock response, return them
        # This simulates the LLM requesting tool execution
        return self._response


# =============================================================================
# Tests
# =============================================================================


@pytest.fixture
def mock_llm_factory():
    """Factory that creates mock LLMs."""
    llms: dict[str, MockLLM] = {}

    def factory(model: str) -> MockLLM:
        if model not in llms:
            llms[model] = MockLLM(model)
        return llms[model]

    factory.llms = llms  # type: ignore
    return factory


@pytest.fixture
def tool_registry():
    """Registry of test tools."""
    return {
        "calculator": lambda expression: f"Result: {eval(expression)}",
        "greeter": lambda name: f"Hello, {name}!",
    }


@pytest.fixture
def activity_config(mock_llm_factory, tool_registry):
    """Create activity configuration for tests."""
    return SpikeActivityConfig(
        llm_factory=mock_llm_factory,
        tool_registry=tool_registry,
        memory_store={"short_term": [], "entity": []},
    )


@pytest.mark.asyncio
async def test_simple_llm_call(activity_config, mock_llm_factory):
    """Test that LLM calls are routed through activities correctly."""
    # Set up mock response
    mock_llm_factory("gpt-4").set_response("Hello from the LLM!")

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[SimpleLLMWorkflow],
            activities=get_spike_activities(activity_config),
        ):
            result = await env.client.execute_workflow(
                SimpleLLMWorkflow.run,
                "Say hello",
                id="test-simple-llm",
                task_queue="test-queue",
            )

            assert result == "Hello from the LLM!"


@pytest.mark.asyncio
async def test_tool_calls_returned(activity_config, mock_llm_factory):
    """Test that tool_calls are returned from LLM (not executed in activity)."""
    # Set up mock to return tool_calls
    mock_tool_calls = [
        {
            "function": {
                "name": "calculator",
                "arguments": {"expression": "2 + 2"},
            }
        }
    ]
    mock_llm_factory("gpt-4").set_response(mock_tool_calls)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[ToolCallWorkflow],
            activities=get_spike_activities(activity_config),
        ):
            result = await env.client.execute_workflow(
                ToolCallWorkflow.run,
                "Calculate 2+2",
                id="test-tool-calls",
                task_queue="test-queue",
            )

            # Tool should have been executed as separate activity
            assert "Tool results:" in result
            assert "Result: 4" in result


@pytest.mark.asyncio
async def test_memory_operations(activity_config):
    """Test that memory save/search work through activities."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[MemoryWorkflow],
            activities=get_spike_activities(activity_config),
        ):
            result = await env.client.execute_workflow(
                MemoryWorkflow.run,
                args=["Important information about AI", "AI"],
                id="test-memory",
                task_queue="test-queue",
            )

            # Should find the stored value
            assert len(result) == 1
            assert "Important information about AI" in result[0]["value"]


@pytest.mark.asyncio
async def test_llm_without_tools_returns_content(activity_config, mock_llm_factory):
    """Test that LLM returns content when no tools requested."""
    mock_llm_factory("gpt-4").set_response("Simple text response")

    activities = SpikeActivities(activity_config)
    result = await activities.llm_call(
        LLMCallInput(
            model="gpt-4",
            messages=[{"role": "user", "content": "Hello"}],
            tools=None,
        )
    )

    assert result.content == "Simple text response"
    assert result.tool_calls is None


@pytest.mark.asyncio
async def test_llm_with_tools_returns_tool_calls(activity_config, mock_llm_factory):
    """Test that LLM returns tool_calls when tools requested and LLM wants to use them."""
    mock_tool_calls = [{"function": {"name": "test", "arguments": {}}}]
    mock_llm_factory("gpt-4").set_response(mock_tool_calls)

    activities = SpikeActivities(activity_config)
    result = await activities.llm_call(
        LLMCallInput(
            model="gpt-4",
            messages=[{"role": "user", "content": "Use a tool"}],
            tools=[{"name": "test", "description": "Test tool"}],
        )
    )

    assert result.content is None
    assert result.tool_calls == mock_tool_calls


@pytest.mark.asyncio
async def test_tool_execution_activity(activity_config):
    """Test that tools can be executed as activities."""
    activities = SpikeActivities(activity_config)

    result = await activities.tool_call(
        ToolCallInput(tool_name="calculator", tool_args={"expression": "3 * 4"})
    )

    assert result.result == "Result: 12"
    assert result.error is None


@pytest.mark.asyncio
async def test_tool_not_found(activity_config):
    """Test handling of unknown tool."""
    activities = SpikeActivities(activity_config)

    result = await activities.tool_call(
        ToolCallInput(tool_name="unknown_tool", tool_args={})
    )

    assert result.error is not None
    assert "not found" in result.error


# =============================================================================
# Integration Test with Real CrewAI (optional, requires crewai installed)
# =============================================================================


@pytest.mark.skip(reason="Requires crewai and API keys")
@pytest.mark.asyncio
async def test_real_crewai_llm():
    """Test with real CrewAI LLM (requires API keys).

    Uncomment and run manually with:
    OPENAI_API_KEY=... pytest -k test_real_crewai_llm -v
    """
    from crewai.llm import LLM

    config = SpikeActivityConfig(
        llm_factory=lambda model: LLM(model=model),
        tool_registry={},
    )

    activities = SpikeActivities(config)

    result = await activities.llm_call(
        LLMCallInput(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "Say 'test successful'"}],
        )
    )

    assert result.content is not None
    assert "test" in result.content.lower() or "successful" in result.content.lower()
