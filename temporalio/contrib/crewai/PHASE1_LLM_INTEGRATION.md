# Phase 1: Core Infrastructure and LLM Activity

This document details the implementation plan for Phase 1 of the CrewAI + Temporal integration.

## Goal

Establish the foundational architecture and implement the most critical component - LLM call routing through Temporal activities.

---

## Module Structure

```
temporalio/contrib/crewai/
├── __init__.py           # Public API exports
├── _llm.py               # LLM stub implementation
├── _activities.py        # Activity class with all activity methods
├── _models.py            # Data models for activity I/O
├── _converter.py         # Pydantic payload converter
├── _worker.py            # Activity configuration and factory
├── _utils.py             # Shared utilities
└── _spike/               # Phase 0 spike (already implemented)
```

---

## 1. Data Models (`_models.py`)

Based on verified interfaces from Phase 0:

```python
from dataclasses import dataclass, field
from typing import Any


@dataclass
class LLMCallInput:
    """Input for LLM call activity.

    Note: We do NOT include available_functions - we want tool_calls returned
    directly, not executed inside the activity.
    """
    model: str
    messages: list[dict[str, Any]]
    tools: list[dict] | None = None
    llm_kwargs: dict[str, Any] = field(default_factory=dict)


@dataclass
class LLMCallOutput:
    """Output from LLM call activity.

    Either content OR tool_calls will be populated, not both.
    - content: Text response from LLM
    - tool_calls: List of tool call requests from LLM
    """
    content: str | None = None
    tool_calls: list[dict] | None = None
    usage: dict[str, int] | None = None
```

**Design Decisions**:
1. Using `@dataclass` for simplicity and JSON serialization compatibility
2. `messages` uses `list[dict]` instead of `list[LLMMessage]` for serialization
3. `tools` serialized as plain dicts for cross-boundary compatibility
4. No `available_functions` - tools are executed via `activity_as_tool` in Phase 2

---

## 2. Activity Configuration (`_worker.py`)

Following the OpenAI agents pattern:

```python
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Callable

from temporalio.common import Priority, RetryPolicy
from temporalio.workflow import ActivityCancellationType, VersioningIntent


@dataclass
class LLMActivityConfig:
    """Configuration for LLM activity execution."""

    start_to_close_timeout: timedelta = timedelta(seconds=60)
    schedule_to_close_timeout: timedelta | None = None
    retry_policy: RetryPolicy | None = None
    heartbeat_timeout: timedelta | None = None
    task_queue: str | None = None
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL


@dataclass
class CrewAIActivityConfig:
    """Configuration for CrewAI activities.

    Provides factory functions and configuration for all CrewAI activities.
    """

    # LLM configuration
    llm_factory: Callable[[str], Any]
    """Factory function that creates an LLM instance from a model name.
    Example: lambda model: LLM(model=model)
    """

    llm_activity_config: LLMActivityConfig = field(default_factory=LLMActivityConfig)
    """Activity execution configuration for LLM calls."""

    # Future phases will add:
    # - storage_factory for memory operations (Phase 3)
    # - knowledge_storage_factory for knowledge queries (Phase 4)
```

---

## 3. Activities (`_activities.py`)

Class-based activities with injected configuration:

```python
from temporalio import activity

from ._models import LLMCallInput, LLMCallOutput
from ._worker import CrewAIActivityConfig


class CrewAIActivities:
    """All CrewAI activities with injected configuration."""

    def __init__(self, config: CrewAIActivityConfig):
        self._config = config

    @activity.defn(name="crewai_llm_call")
    async def llm_call(self, input: LLMCallInput) -> LLMCallOutput:
        """Execute an LLM call.

        Key behavior: We do NOT pass available_functions to the LLM.
        This causes tool_calls to be returned directly rather than executed.
        Tool execution happens in the workflow via activity_as_tool (Phase 2).
        """
        llm = self._config.llm_factory(input.model)

        # Call LLM WITHOUT available_functions
        result = await llm.acall(
            messages=input.messages,
            tools=input.tools,
            available_functions=None,  # Key: don't pass this
            **(input.llm_kwargs or {}),
        )

        # Heartbeat for long-running calls
        activity.heartbeat({"model": input.model, "status": "completed"})

        # Handle return type based on CrewAI's LLM behavior:
        # - str: simple text response
        # - list: tool_calls to be executed by workflow
        if isinstance(result, list):
            return LLMCallOutput(content=None, tool_calls=result)
        elif isinstance(result, str):
            return LLMCallOutput(content=result, tool_calls=None)
        else:
            # Fallback for other response types
            return LLMCallOutput(
                content=str(result) if result else None,
                tool_calls=getattr(result, "tool_calls", None),
            )


def crewai_activities(config: CrewAIActivityConfig) -> list[Callable]:
    """Get all CrewAI activities configured with the given config.

    Usage:
        config = CrewAIActivityConfig(llm_factory=lambda m: LLM(model=m))
        worker = Worker(
            client,
            task_queue="crewai",
            workflows=[MyWorkflow],
            activities=crewai_activities(config),
        )
    """
    instance = CrewAIActivities(config)
    return [
        instance.llm_call,
        # Future phases will add more activities
    ]
```

---

## 4. LLM Stub (`_llm.py`)

The stub that workflows use to make LLM calls:

```python
from typing import Any

from temporalio import workflow

from ._models import LLMCallInput, LLMCallOutput
from ._worker import LLMActivityConfig
from ._utils import _serialize_tools


class _LLMStub:
    """Stub that routes LLM calls through Temporal activities.

    This class mimics CrewAI's BaseLLM interface but executes calls
    as Temporal activities for durability and visibility.
    """

    def __init__(
        self,
        model: str,
        activity_config: LLMActivityConfig | None = None,
        **llm_kwargs: Any,
    ):
        self.model = model
        self.activity_config = activity_config or LLMActivityConfig()
        self._llm_kwargs = llm_kwargs

    async def acall(
        self,
        messages: str | list[dict],
        tools: list[dict] | None = None,
        callbacks: Any | None = None,
        available_functions: dict | None = None,  # Ignored
        from_task: Any | None = None,
        from_agent: Any | None = None,
        **kwargs: Any,
    ) -> str | list | Any:
        """Execute LLM call via Temporal activity.

        The activity does NOT receive available_functions, so if the LLM
        returns tool_calls, they are returned directly to the caller (CrewAI's
        agent executor) for execution via activity_as_tool wrappers.

        Args:
            messages: Input messages (string or list of message dicts)
            tools: Tool definitions for the LLM
            callbacks: Ignored (not supported in activity context)
            available_functions: Ignored (we want tool_calls returned, not executed)
            from_task: Task context (passed through in kwargs)
            from_agent: Agent context (passed through in kwargs)
            **kwargs: Additional LLM kwargs

        Returns:
            str: Text content if LLM returns text
            list: Tool calls if LLM requests tool execution
        """
        # Normalize messages
        if isinstance(messages, str):
            normalized_messages = [{"role": "user", "content": messages}]
        else:
            normalized_messages = messages

        # Serialize tools for activity input
        serialized_tools = _serialize_tools(tools) if tools else None

        input_data = LLMCallInput(
            model=self.model,
            messages=normalized_messages,
            tools=serialized_tools,
            llm_kwargs={**self._llm_kwargs, **kwargs},
        )

        # Execute as activity
        result = await workflow.execute_activity(
            "crewai_llm_call",
            input_data,
            start_to_close_timeout=self.activity_config.start_to_close_timeout,
            schedule_to_close_timeout=self.activity_config.schedule_to_close_timeout,
            retry_policy=self.activity_config.retry_policy,
            heartbeat_timeout=self.activity_config.heartbeat_timeout,
        )

        # Result is dict (JSON-deserialized), not dataclass
        tool_calls = result.get("tool_calls") if isinstance(result, dict) else result.tool_calls
        content = result.get("content") if isinstance(result, dict) else result.content

        # Return tool_calls if present, otherwise content
        if tool_calls:
            return tool_calls
        return content or ""


def llm_stub(
    model: str,
    activity_config: LLMActivityConfig | None = None,
    **llm_kwargs: Any,
) -> _LLMStub:
    """Create an LLM stub that routes calls through Temporal activities.

    This should be used instead of crewai.LLM when running crews in
    Temporal workflows.

    Args:
        model: The model name (e.g., "gpt-4", "gpt-4o-mini")
        activity_config: Optional activity execution configuration
        **llm_kwargs: Additional kwargs passed to the LLM

    Returns:
        An LLM stub compatible with CrewAI's LLM interface

    Example:
        @workflow.defn
        class MyCrewWorkflow:
            @workflow.run
            async def run(self):
                from temporalio.contrib.crewai import llm_stub

                agent = Agent(
                    role="Researcher",
                    llm=llm_stub("gpt-4"),  # Use stub instead of LLM
                    ...
                )
                crew = Crew(agents=[agent], tasks=[...])
                return await crew.akickoff()
    """
    return _LLMStub(model=model, activity_config=activity_config, **llm_kwargs)
```

---

## 5. Utilities (`_utils.py`)

```python
from typing import Any


def _serialize_tools(tools: list[Any] | None) -> list[dict] | None:
    """Serialize tool definitions for activity input.

    Converts CrewAI tool objects to plain dicts for JSON serialization.
    """
    if tools is None:
        return None

    serialized = []
    for tool in tools:
        if isinstance(tool, dict):
            serialized.append(tool)
        elif hasattr(tool, "model_dump"):
            # Pydantic model
            serialized.append(tool.model_dump())
        elif hasattr(tool, "__dict__"):
            serialized.append({
                k: v for k, v in tool.__dict__.items()
                if not k.startswith("_")
            })
        else:
            # Fallback
            serialized.append({"name": str(tool)})

    return serialized


def _to_dict(obj: Any) -> dict[str, Any]:
    """Convert an object to a dict for serialization."""
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if hasattr(obj, "__dict__"):
        return {k: v for k, v in obj.__dict__.items() if not k.startswith("_")}
    raise TypeError(f"Cannot convert {type(obj)} to dict")
```

---

## 6. Payload Converter (`_converter.py`)

```python
from temporalio.contrib.pydantic import PydanticPayloadConverter, ToJsonOptions
from temporalio.converter import DataConverter, DefaultPayloadConverter


class CrewAIPayloadConverter(PydanticPayloadConverter):
    """Payload converter for CrewAI activity I/O.

    Uses Pydantic for serialization with exclude_unset=True to minimize
    payload size.
    """

    def __init__(self) -> None:
        super().__init__(ToJsonOptions(exclude_unset=True))


def crewai_data_converter(
    existing: DataConverter | None = None
) -> DataConverter:
    """Create or update a DataConverter for CrewAI.

    Args:
        existing: Optional existing DataConverter to update

    Returns:
        DataConverter configured with CrewAIPayloadConverter
    """
    if existing is None:
        return DataConverter(payload_converter_class=CrewAIPayloadConverter)

    if existing.payload_converter_class is DefaultPayloadConverter:
        return DataConverter(
            payload_converter_class=CrewAIPayloadConverter,
            failure_converter_class=existing.failure_converter_class,
        )

    # If already custom, verify it's compatible
    if not issubclass(existing.payload_converter_class, PydanticPayloadConverter):
        raise ValueError(
            "CrewAI requires PydanticPayloadConverter. "
            f"Got: {existing.payload_converter_class.__name__}"
        )

    return existing
```

---

## 7. Public API (`__init__.py`)

```python
"""Support for running CrewAI crews as part of Temporal workflows.

This module provides integration between CrewAI and Temporal workflows,
enabling durable execution of AI agent crews with full observability.

.. warning::
    This module is experimental and may change in future versions.
"""

from ._converter import CrewAIPayloadConverter, crewai_data_converter
from ._llm import _LLMStub, llm_stub
from ._worker import (
    CrewAIActivityConfig,
    LLMActivityConfig,
    crewai_activities,
)

__all__ = [
    # LLM
    "llm_stub",
    # Configuration
    "CrewAIActivityConfig",
    "LLMActivityConfig",
    # Worker setup
    "crewai_activities",
    "crewai_data_converter",
    "CrewAIPayloadConverter",
]
```

---

## 8. Test Plan

### Unit Tests (`tests/contrib/crewai/test_phase1.py`)

```python
"""Phase 1 Tests: LLM Integration"""

import pytest
from dataclasses import asdict
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

from temporalio import workflow
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from temporalio.contrib.crewai import (
    llm_stub,
    crewai_activities,
    CrewAIActivityConfig,
    LLMActivityConfig,
)
from temporalio.contrib.crewai._models import LLMCallInput, LLMCallOutput
from temporalio.contrib.crewai._activities import CrewAIActivities


# =============================================================================
# Test Fixtures
# =============================================================================

class MockLLM:
    """Mock LLM that simulates CrewAI's LLM behavior."""

    def __init__(self, model: str):
        self.model = model
        self._response: str | list = "Mock response"

    def set_response(self, response: str | list):
        self._response = response

    async def acall(self, messages, tools=None, available_functions=None, **kwargs):
        return self._response


@pytest.fixture
def mock_llm_factory():
    """Factory that creates mock LLMs."""
    llms: dict[str, MockLLM] = {}

    def factory(model: str) -> MockLLM:
        if model not in llms:
            llms[model] = MockLLM(model)
        return llms[model]

    factory.llms = llms
    return factory


@pytest.fixture
def activity_config(mock_llm_factory):
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
        content=None,
        tool_calls=[{"function": {"name": "test", "arguments": {}}}]
    )
    d = asdict(output)
    assert d["content"] is None
    assert len(d["tool_calls"]) == 1


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
```

---

## 9. Success Criteria

1. **LLM calls work through activities**
   - `llm_stub("gpt-4")` can be used in place of `LLM(model="gpt-4")`
   - Activity visible in Temporal UI with model name
   - Heartbeats work for long-running calls

2. **Tool calls returned correctly**
   - When LLM requests tools, `tool_calls` are returned to workflow
   - Tool calls are NOT executed inside the activity
   - Workflow receives `list[dict]` format matching CrewAI's expectations

3. **Serialization works**
   - `LLMCallInput` and `LLMCallOutput` serialize/deserialize correctly
   - Pydantic converter handles dataclasses properly
   - No serialization errors in integration tests

4. **Retries work**
   - Activity failures trigger Temporal retries
   - Retry policy from `LLMActivityConfig` is respected

5. **All tests pass**
   - Unit tests for models, activities
   - Integration tests with WorkflowEnvironment
   - Tests run in < 30 seconds

---

## 10. Implementation Order

1. Create `_models.py` with data classes
2. Create `_utils.py` with serialization helpers
3. Create `_worker.py` with config classes
4. Create `_activities.py` with CrewAIActivities class
5. Create `_llm.py` with LLM stub
6. Create `_converter.py` with payload converter
7. Create `__init__.py` with public exports
8. Write and run tests
9. Update DESIGN.md with any changes

---

## 11. Dependencies

- Phase 0 complete (verified interfaces)
- `temporalio` SDK
- `pydantic` 2.0+
- `crewai` (for type hints and testing)

---

## 12. Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `_models.py` | Create | Data models for activity I/O |
| `_utils.py` | Create | Serialization utilities |
| `_worker.py` | Create | Activity configuration |
| `_activities.py` | Create | Activity implementations |
| `_llm.py` | Create | LLM stub |
| `_converter.py` | Create | Pydantic converter |
| `__init__.py` | Create | Public API exports |
| `tests/.../test_phase1.py` | Create | Unit and integration tests |
| `DESIGN.md` | Update | Reflect any changes |
