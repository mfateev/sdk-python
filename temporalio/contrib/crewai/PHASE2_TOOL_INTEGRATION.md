# Phase 2: Tool Integration

This document details the implementation plan for Phase 2 of the CrewAI + Temporal integration.

## Goal

Enable CrewAI agents to use Temporal activities as tools, providing durability, visibility, and retry handling for tool executions.

---

## Background: How CrewAI Tools Work

CrewAI tools are based on `CrewStructuredTool` which has:
- `name`: Tool identifier
- `description`: Used by LLM to understand when to use the tool
- `args_schema`: Pydantic model defining input arguments
- `func`: The callable to execute
- `ainvoke(input: dict)`: Async invocation method

When an LLM decides to use a tool, CrewAI:
1. Parses the tool call from LLM response
2. Validates arguments against `args_schema`
3. Calls `ainvoke()` with the arguments
4. Returns result to LLM for next step

---

## Design: `activity_as_tool()`

### Overview

The `activity_as_tool()` function wraps a Temporal activity as a CrewAI tool. When the tool is invoked by an agent, it executes the activity via `workflow.execute_activity()`.

```python
from temporalio.contrib.crewai import activity_as_tool

@activity.defn
async def search_database(query: str, limit: int = 10) -> str:
    """Search the database for matching records."""
    # Implementation...
    return results

# In workflow:
agent = Agent(
    role="Researcher",
    tools=[
        activity_as_tool(
            search_database,
            start_to_close_timeout=timedelta(seconds=30),
        )
    ],
    llm=llm_stub("gpt-4"),
)
```

### Key Design Decisions

1. **Workflow-side execution**: The tool wrapper calls `workflow.execute_activity()`, so it must be used inside a workflow.

2. **Schema inference**: Arguments schema is inferred from the activity function signature using `inspect` and Pydantic's `create_model()`.

3. **Activity name from decorator**: Uses the activity's registered name from `@activity.defn`.

4. **Marker attribute**: Sets `_is_temporal_activity_tool = True` for validation.

5. **String serialization**: Activity inputs/outputs must be JSON-serializable. The activity should return a string result.

---

## Module Structure

```
temporalio/contrib/crewai/
├── _tools.py             # NEW: activity_as_tool() implementation
├── _worker.py            # UPDATED: Add ToolActivityConfig
├── _utils.py             # UPDATED: Add _build_args_schema()
├── __init__.py           # UPDATED: Export new APIs
└── ...
```

---

## 1. Tool Configuration (`_worker.py`)

```python
@dataclass
class ToolActivityConfig:
    """Configuration for tool activity execution."""

    start_to_close_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=60))
    """Maximum time for the tool to complete."""

    schedule_to_close_timeout: timedelta | None = None
    """Maximum time from scheduling to completion."""

    retry_policy: RetryPolicy | None = None
    """Retry policy for failed tool calls."""

    heartbeat_timeout: timedelta | None = None
    """Heartbeat timeout for long-running tools."""

    task_queue: str | None = None
    """Task queue for tool activities. If None, uses workflow's queue."""

    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL
    """How activity handles workflow cancellation."""
```

---

## 2. Schema Builder (`_utils.py`)

```python
def _build_args_schema(func: Callable) -> type[BaseModel]:
    """Build a Pydantic args schema from a function signature.

    This is used to generate the args_schema for CrewStructuredTool
    from an activity function's signature.

    Args:
        func: The function to build schema from

    Returns:
        A Pydantic model class for the function's arguments
    """
    import inspect
    from typing import Any, get_type_hints
    from pydantic import Field, create_model

    sig = inspect.signature(func)
    type_hints = get_type_hints(func)

    fields = {}
    for param_name, param in sig.parameters.items():
        # Skip self/cls
        if param_name in ("self", "cls"):
            continue
        # Skip *args, **kwargs
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            continue

        annotation = type_hints.get(param_name, Any)
        default = ... if param.default is param.empty else param.default
        fields[param_name] = (annotation, Field(default=default))

    func_name = getattr(func, "__name__", "Activity")
    schema_name = f"{func_name.title().replace('_', '')}Schema"
    return create_model(schema_name, **fields)
```

---

## 3. Activity as Tool (`_tools.py`)

```python
"""Tool wrapper for executing activities as CrewAI tools."""

from collections.abc import Callable
from datetime import timedelta
from typing import Any

from temporalio import workflow

from ._utils import _build_args_schema
from ._worker import ToolActivityConfig


def activity_as_tool(
    activity_fn: Callable,
    *,
    start_to_close_timeout: timedelta | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    retry_policy: Any = None,
    heartbeat_timeout: timedelta | None = None,
    task_queue: str | None = None,
    cancellation_type: Any = None,
    name: str | None = None,
    description: str | None = None,
) -> Any:
    """Wrap a Temporal activity as a CrewAI tool.

    This creates a CrewStructuredTool that executes the activity via
    workflow.execute_activity() when invoked by an agent.

    Args:
        activity_fn: The activity function (decorated with @activity.defn)
        start_to_close_timeout: Timeout for activity execution
        schedule_to_close_timeout: Timeout from scheduling to completion
        retry_policy: Retry policy for failures
        heartbeat_timeout: Heartbeat timeout for long-running activities
        task_queue: Task queue for the activity
        cancellation_type: How to handle workflow cancellation
        name: Tool name (defaults to activity name)
        description: Tool description (defaults to activity docstring)

    Returns:
        A CrewStructuredTool that executes the activity

    Example:
        @activity.defn
        async def search_web(query: str) -> str:
            '''Search the web for information.'''
            return await do_search(query)

        agent = Agent(
            tools=[activity_as_tool(search_web, start_to_close_timeout=timedelta(seconds=30))],
            ...
        )
    """
    from crewai.tools.structured_tool import CrewStructuredTool

    # Get activity name from the decorated function
    activity_name = getattr(activity_fn, "__temporal_activity_definition", {}).get(
        "name", activity_fn.__name__
    )

    # Build configuration
    config = ToolActivityConfig(
        start_to_close_timeout=start_to_close_timeout or timedelta(seconds=60),
        schedule_to_close_timeout=schedule_to_close_timeout,
        retry_policy=retry_policy,
        heartbeat_timeout=heartbeat_timeout,
        task_queue=task_queue,
        cancellation_type=cancellation_type,
    )

    # Build args schema from function signature
    args_schema = _build_args_schema(activity_fn)

    # Tool name and description
    tool_name = name or activity_name
    tool_description = description or activity_fn.__doc__ or f"Execute {tool_name}"

    # Create the wrapper function that executes the activity
    async def execute_activity(**kwargs: Any) -> str:
        """Execute the activity and return result."""
        # Build activity options
        activity_options: dict[str, Any] = {
            "start_to_close_timeout": config.start_to_close_timeout,
        }
        if config.schedule_to_close_timeout:
            activity_options["schedule_to_close_timeout"] = config.schedule_to_close_timeout
        if config.retry_policy:
            activity_options["retry_policy"] = config.retry_policy
        if config.heartbeat_timeout:
            activity_options["heartbeat_timeout"] = config.heartbeat_timeout
        if config.task_queue:
            activity_options["task_queue"] = config.task_queue
        if config.cancellation_type:
            activity_options["cancellation_type"] = config.cancellation_type

        # Execute activity
        result = await workflow.execute_activity(
            activity_name,
            kwargs,  # Pass kwargs as single dict argument
            **activity_options,
        )

        # Convert result to string for LLM
        return str(result) if result is not None else ""

    # Preserve function metadata for schema generation
    execute_activity.__name__ = activity_fn.__name__
    execute_activity.__doc__ = tool_description

    # Create the tool
    tool = CrewStructuredTool(
        name=tool_name,
        description=tool_description,
        args_schema=args_schema,
        func=execute_activity,
    )

    # Mark as Temporal activity tool for validation
    tool._is_temporal_activity_tool = True  # type: ignore

    return tool
```

---

## 4. Updated Public API (`__init__.py`)

```python
from ._tools import activity_as_tool
from ._worker import ToolActivityConfig

__all__ = [
    # Phase 1
    "llm_stub",
    "CrewAIActivityConfig",
    "LLMActivityConfig",
    "crewai_activities",
    "crewai_data_converter",
    # Phase 2
    "activity_as_tool",
    "ToolActivityConfig",
]
```

---

## 5. Test Plan

### Unit Tests

```python
def test_activity_as_tool_creation():
    """Test that activity_as_tool creates a valid CrewStructuredTool."""
    @activity.defn
    async def my_activity(query: str, limit: int = 10) -> str:
        """Search for something."""
        return "result"

    tool = activity_as_tool(my_activity, start_to_close_timeout=timedelta(seconds=30))

    assert tool.name == "my_activity"
    assert "Search for something" in tool.description
    assert hasattr(tool, "_is_temporal_activity_tool")
    assert tool._is_temporal_activity_tool is True


def test_args_schema_inference():
    """Test that args schema is correctly inferred from function signature."""
    @activity.defn
    async def my_activity(name: str, count: int = 5, flag: bool = False) -> str:
        """Do something."""
        return "done"

    tool = activity_as_tool(my_activity)

    schema = tool.args_schema
    assert "name" in schema.model_fields
    assert "count" in schema.model_fields
    assert "flag" in schema.model_fields

    # Check defaults
    assert schema.model_fields["count"].default == 5
    assert schema.model_fields["flag"].default is False


def test_is_temporal_tool():
    """Test _is_temporal_tool() utility."""
    @activity.defn
    async def my_activity(x: str) -> str:
        return x

    tool = activity_as_tool(my_activity)
    assert _is_temporal_tool(tool) is True

    # Non-temporal tool
    from crewai.tools.structured_tool import CrewStructuredTool
    regular_tool = CrewStructuredTool.from_function(lambda x: x, name="test")
    assert _is_temporal_tool(regular_tool) is False
```

### Integration Tests

```python
@workflow.defn
class ToolWorkflow:
    """Test workflow that uses a tool."""

    @workflow.run
    async def run(self, query: str) -> str:
        # Create tool inside workflow
        tool = activity_as_tool(
            search_activity,
            start_to_close_timeout=timedelta(seconds=30),
        )

        # Invoke tool directly
        result = await tool.ainvoke({"query": query})
        return result


@pytest.mark.asyncio
async def test_tool_executes_activity(activity_config):
    """Test that tool invocation executes the underlying activity."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[ToolWorkflow],
            activities=[search_activity],
        ):
            result = await env.client.execute_workflow(
                ToolWorkflow.run,
                "test query",
                id="test-tool-workflow",
                task_queue="test-queue",
            )

            assert "search result" in result.lower()
```

---

## 6. Success Criteria

1. **Tool creation works**: `activity_as_tool()` creates valid `CrewStructuredTool`
2. **Schema inference**: Args schema correctly inferred from activity signature
3. **Activity execution**: Tool invocation executes activity via `workflow.execute_activity()`
4. **Timeout/retry**: Activity options (timeout, retry) are passed correctly
5. **Validation**: `_is_temporal_tool()` correctly identifies wrapped tools
6. **All tests pass**: Unit and integration tests complete successfully

---

## 7. Implementation Order

1. Add `ToolActivityConfig` to `_worker.py`
2. Add `_build_args_schema()` to `_utils.py`
3. Create `_tools.py` with `activity_as_tool()`
4. Update `__init__.py` with new exports
5. Write and run tests
6. Commit

---

## 8. Dependencies

- Phase 1 complete (LLM activity integration)
- `crewai` package with `CrewStructuredTool`
- `pydantic` for schema generation
