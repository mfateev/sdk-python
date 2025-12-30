"""Phase 1 Tests: LLM Integration.

These tests verify the core LLM activity functionality:
- Model serialization/deserialization
- Activity execution with mock LLM
- LLM stub in workflows
- Tool calls returned to workflow
"""

from dataclasses import asdict
from datetime import timedelta

import pytest

from temporalio import workflow
from temporalio.contrib.crewai import (
    CrewAIActivityConfig,
    LLMActivityConfig,
    crewai_activities,
    llm_stub,
)
from temporalio.contrib.crewai._activities import CrewAIActivities
from temporalio.contrib.crewai._models import LLMCallInput, LLMCallOutput
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

# =============================================================================
# Test Fixtures
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
        """Simulate CrewAI's LLM.acall behavior."""
        return self._response


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
def activity_config(mock_llm_factory):
    """Create activity configuration for tests."""
    return CrewAIActivityConfig(llm_factory=mock_llm_factory)


# =============================================================================
# Model Tests
# =============================================================================


def test_llm_call_input_serialization():
    """Test LLMCallInput can be serialized to dict."""
    input_data = LLMCallInput(
        model="gpt-4",
        messages=[{"role": "user", "content": "Hello"}],
        tools=[{"name": "test", "description": "A test tool"}],
        llm_kwargs={"temperature": 0.7},
    )

    d = asdict(input_data)
    assert d["model"] == "gpt-4"
    assert d["messages"] == [{"role": "user", "content": "Hello"}]
    assert d["tools"] == [{"name": "test", "description": "A test tool"}]
    assert d["llm_kwargs"] == {"temperature": 0.7}


def test_llm_call_output_serialization():
    """Test LLMCallOutput can be serialized to dict."""
    # Content response
    output = LLMCallOutput(content="Hello!", tool_calls=None)
    d = asdict(output)
    assert d["content"] == "Hello!"
    assert d["tool_calls"] is None

    # Tool calls response
    output = LLMCallOutput(
        content=None, tool_calls=[{"function": {"name": "test", "arguments": {}}}]
    )
    d = asdict(output)
    assert d["content"] is None
    assert len(d["tool_calls"]) == 1


def test_llm_call_input_defaults():
    """Test LLMCallInput has correct defaults."""
    input_data = LLMCallInput(
        model="gpt-4",
        messages=[{"role": "user", "content": "Hello"}],
    )

    assert input_data.tools is None
    assert input_data.llm_kwargs == {}


def test_llm_call_output_defaults():
    """Test LLMCallOutput has correct defaults."""
    output = LLMCallOutput()

    assert output.content is None
    assert output.tool_calls is None
    assert output.usage is None


# =============================================================================
# Activity Tests
# =============================================================================


@pytest.mark.asyncio
async def test_llm_activity_text_response(activity_config, mock_llm_factory):
    """Test LLM activity returns text content."""
    mock_llm_factory("gpt-4").set_response("Hello from LLM!")

    activities = CrewAIActivities(activity_config)
    result = await activities.llm_call(
        LLMCallInput(
            model="gpt-4",
            messages=[{"role": "user", "content": "Say hello"}],
        )
    )

    assert result.content == "Hello from LLM!"
    assert result.tool_calls is None


@pytest.mark.asyncio
async def test_llm_activity_tool_calls(activity_config, mock_llm_factory):
    """Test LLM activity returns tool calls."""
    tool_calls = [{"function": {"name": "calculator", "arguments": {"x": 1}}}]
    mock_llm_factory("gpt-4").set_response(tool_calls)

    activities = CrewAIActivities(activity_config)
    result = await activities.llm_call(
        LLMCallInput(
            model="gpt-4",
            messages=[{"role": "user", "content": "Calculate 1+1"}],
            tools=[{"name": "calculator", "description": "Math"}],
        )
    )

    assert result.content is None
    assert result.tool_calls == tool_calls


@pytest.mark.asyncio
async def test_llm_activity_passes_kwargs(activity_config, mock_llm_factory):
    """Test LLM activity passes kwargs to LLM."""
    mock_llm = mock_llm_factory("gpt-4")
    mock_llm.set_response("Response")

    # Capture kwargs passed to acall
    captured_kwargs = {}
    original_acall = mock_llm.acall

    async def capturing_acall(*args, **kwargs):
        captured_kwargs.update(kwargs)
        return await original_acall(*args, **kwargs)

    mock_llm.acall = capturing_acall

    activities = CrewAIActivities(activity_config)
    await activities.llm_call(
        LLMCallInput(
            model="gpt-4",
            messages=[{"role": "user", "content": "Hello"}],
            llm_kwargs={"temperature": 0.7, "max_tokens": 100},
        )
    )

    assert captured_kwargs.get("temperature") == 0.7
    assert captured_kwargs.get("max_tokens") == 100


@pytest.mark.asyncio
async def test_llm_activity_passes_none_available_functions(
    activity_config, mock_llm_factory
):
    """Test LLM activity passes available_functions=None to LLM."""
    mock_llm = mock_llm_factory("gpt-4")
    mock_llm.set_response("Response")

    # Capture available_functions passed to acall
    captured_available_functions = "not_called"

    async def capturing_acall(*args, available_functions=None, **kwargs):
        nonlocal captured_available_functions
        captured_available_functions = available_functions
        return "Response"

    mock_llm.acall = capturing_acall

    activities = CrewAIActivities(activity_config)
    await activities.llm_call(
        LLMCallInput(
            model="gpt-4",
            messages=[{"role": "user", "content": "Hello"}],
            tools=[{"name": "calculator"}],
        )
    )

    # Key assertion: available_functions must be None so tool_calls are returned
    assert captured_available_functions is None


# =============================================================================
# Integration Tests (with Temporal)
# =============================================================================


@workflow.defn
class SimpleLLMWorkflow:
    """Test workflow that makes an LLM call."""

    @workflow.run
    async def run(self, prompt: str) -> str:
        from temporalio.contrib.crewai._llm import _LLMStub

        stub = _LLMStub(model="gpt-4")
        result = await stub.acall(
            messages=[{"role": "user", "content": prompt}],
        )
        return result if isinstance(result, str) else str(result)


@pytest.mark.asyncio
async def test_llm_stub_in_workflow(activity_config, mock_llm_factory):
    """Test LLM stub works in a Temporal workflow."""
    mock_llm_factory("gpt-4").set_response("Hello from workflow!")

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[SimpleLLMWorkflow],
            activities=crewai_activities(activity_config),
        ):
            result = await env.client.execute_workflow(
                SimpleLLMWorkflow.run,
                "Say hello",
                id="test-llm-workflow",
                task_queue="test-queue",
            )

            assert result == "Hello from workflow!"


@workflow.defn
class ToolCallWorkflow:
    """Test workflow that handles tool calls."""

    @workflow.run
    async def run(self, prompt: str) -> dict:
        from temporalio.contrib.crewai._llm import _LLMStub

        stub = _LLMStub(model="gpt-4")
        result = await stub.acall(
            messages=[{"role": "user", "content": prompt}],
            tools=[{"name": "calculator"}],
        )

        # Return whether we got tool calls or content
        if isinstance(result, list):
            return {"type": "tool_calls", "count": len(result)}
        return {"type": "content", "value": result}


@pytest.mark.asyncio
async def test_tool_calls_returned_to_workflow(activity_config, mock_llm_factory):
    """Test that tool calls are returned to workflow for handling."""
    tool_calls = [{"function": {"name": "calculator", "arguments": {}}}]
    mock_llm_factory("gpt-4").set_response(tool_calls)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[ToolCallWorkflow],
            activities=crewai_activities(activity_config),
        ):
            result = await env.client.execute_workflow(
                ToolCallWorkflow.run,
                "Use calculator",
                id="test-tool-workflow",
                task_queue="test-queue",
            )

            assert result["type"] == "tool_calls"
            assert result["count"] == 1


# =============================================================================
# Configuration Tests
# =============================================================================


def test_llm_activity_config_defaults():
    """Test LLMActivityConfig has sensible defaults."""
    config = LLMActivityConfig()

    assert config.start_to_close_timeout == timedelta(seconds=60)
    assert config.schedule_to_close_timeout is None
    assert config.retry_policy is None
    assert config.heartbeat_timeout is None
    assert config.task_queue is None


def test_crewai_activity_config_requires_llm_factory():
    """Test CrewAIActivityConfig requires llm_factory."""
    with pytest.raises(TypeError):
        CrewAIActivityConfig()  # type: ignore


def test_crewai_activities_returns_list(activity_config):
    """Test crewai_activities returns list of callables."""
    activities = crewai_activities(activity_config)

    assert isinstance(activities, list)
    assert len(activities) >= 1
    assert callable(activities[0])


# =============================================================================
# LLM Stub Tests
# =============================================================================


def test_llm_stub_creation():
    """Test llm_stub creates _LLMStub instance."""
    stub = llm_stub("gpt-4")

    assert stub.model == "gpt-4"
    assert stub.activity_config is not None


def test_llm_stub_with_config():
    """Test llm_stub accepts activity config."""
    config = LLMActivityConfig(
        start_to_close_timeout=timedelta(seconds=120),
    )
    stub = llm_stub("gpt-4", activity_config=config)

    assert stub.activity_config.start_to_close_timeout == timedelta(seconds=120)


def test_llm_stub_with_kwargs():
    """Test llm_stub accepts LLM kwargs."""
    stub = llm_stub("gpt-4", temperature=0.7, max_tokens=100)

    assert stub._llm_kwargs["temperature"] == 0.7
    assert stub._llm_kwargs["max_tokens"] == 100
