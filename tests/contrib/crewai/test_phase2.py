"""Phase 2 Tests: Tool Integration.

These tests verify the activity_as_tool() functionality:
- Tool creation from activities
- Argument schema inference
- Tool execution via workflow activities
- _is_temporal_tool validation
"""

from datetime import timedelta

import pytest

from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.contrib.crewai import (
    ToolActivityConfig,
    activity_as_tool,
)
from temporalio.contrib.crewai._utils import _build_args_schema, _is_temporal_tool
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

# =============================================================================
# Test Activities (with individual parameters for unit tests)
# =============================================================================


@activity.defn
async def search_activity(query: str, limit: int = 10) -> str:
    """Search for information.

    Args:
        query: The search query
        limit: Maximum number of results
    """
    return f"Found results for '{query}' (limit={limit})"


@activity.defn
async def calculator_activity(expression: str) -> str:
    """Calculate a mathematical expression."""
    # Simple eval for testing (don't do this in production!)
    try:
        result = eval(expression)  # noqa: S307
        return f"Result: {result}"
    except Exception as e:
        return f"Error: {e}"


@activity.defn(name="custom_name_activity")
async def activity_with_custom_name(data: dict) -> str:
    """Activity with custom name."""
    return f"Processed: {data}"


@activity.defn
async def no_args_activity() -> str:
    """Activity with no arguments."""
    return "No args needed"


@activity.defn
async def complex_args_activity(
    name: str,
    count: int,
    enabled: bool = True,
    tags: list[str] | None = None,
) -> str:
    """Activity with complex arguments."""
    return f"name={name}, count={count}, enabled={enabled}, tags={tags}"


# =============================================================================
# Test Activities (with dict input for integration tests)
# These accept a single dict argument, which is how activity_as_tool() calls them
# =============================================================================


@activity.defn(name="search_dict_activity")
async def search_dict_activity(args: dict) -> str:
    """Search activity accepting dict input."""
    query = args.get("query", "")
    limit = args.get("limit", 10)
    return f"Found results for '{query}' (limit={limit})"


@activity.defn(name="calculator_dict_activity")
async def calculator_dict_activity(args: dict) -> str:
    """Calculator activity accepting dict input."""
    expression = args.get("expression", "0")
    try:
        result = eval(expression)  # noqa: S307
        return f"Result: {result}"
    except Exception as e:
        return f"Error: {e}"


@activity.defn(name="no_args_dict_activity")
async def no_args_dict_activity(args: dict) -> str:
    """No-args activity accepting dict input."""
    return "No args needed"


@activity.defn(name="complex_dict_activity")
async def complex_dict_activity(args: dict) -> str:
    """Complex args activity accepting dict input."""
    name = args.get("name", "")
    count = args.get("count", 0)
    enabled = args.get("enabled", True)
    tags = args.get("tags")
    return f"name={name}, count={count}, enabled={enabled}, tags={tags}"


# =============================================================================
# Unit Tests: _build_args_schema
# =============================================================================


def test_build_args_schema_basic():
    """Test _build_args_schema with basic parameters."""
    schema = _build_args_schema(search_activity)

    assert "query" in schema.model_fields
    assert "limit" in schema.model_fields

    # Check types
    assert schema.model_fields["query"].annotation is str
    assert schema.model_fields["limit"].annotation is int

    # Check defaults
    assert schema.model_fields["limit"].default == 10


def test_build_args_schema_no_args():
    """Test _build_args_schema with no arguments."""
    schema = _build_args_schema(no_args_activity)

    # Should have no fields
    assert len(schema.model_fields) == 0


def test_build_args_schema_complex_types():
    """Test _build_args_schema with complex types."""
    schema = _build_args_schema(complex_args_activity)

    assert "name" in schema.model_fields
    assert "count" in schema.model_fields
    assert "enabled" in schema.model_fields
    assert "tags" in schema.model_fields

    # Check defaults
    assert schema.model_fields["enabled"].default is True
    assert schema.model_fields["tags"].default is None


def test_build_args_schema_generates_schema_name():
    """Test that schema name is generated from function name."""
    schema = _build_args_schema(search_activity)
    assert "Search" in schema.__name__


# =============================================================================
# Unit Tests: activity_as_tool
# =============================================================================


def test_activity_as_tool_creation():
    """Test that activity_as_tool creates a valid tool.

    Note: BaseTool transforms description to include tool name and args schema.
    """
    tool = activity_as_tool(
        search_activity, start_to_close_timeout=timedelta(seconds=30)
    )

    assert tool.name == "search_activity"
    # BaseTool wraps description - original docstring is preserved inside
    assert "Search for information" in tool.description
    assert hasattr(tool, "_is_temporal_activity_tool")
    assert tool._is_temporal_activity_tool is True


def test_activity_as_tool_inherits_from_basetool():
    """Test that activity_as_tool returns a BaseTool instance.

    CrewAI 1.7.2+ requires tools to be BaseTool instances.
    """
    from crewai.tools.base_tool import BaseTool

    tool = activity_as_tool(search_activity)
    assert isinstance(tool, BaseTool)


def test_activity_as_tool_with_custom_name():
    """Test activity_as_tool with custom name parameter."""
    tool = activity_as_tool(search_activity, name="web_search")

    assert tool.name == "web_search"


def test_activity_as_tool_with_custom_description():
    """Test activity_as_tool with custom description parameter.

    Note: BaseTool transforms description to include tool name and args schema.
    """
    tool = activity_as_tool(
        search_activity,
        description="Search the web for relevant information.",
    )

    # BaseTool wraps description with tool name and args schema
    assert "Search the web for relevant information." in tool.description


def test_activity_as_tool_uses_activity_defn_name():
    """Test that activity_as_tool uses name from @activity.defn decorator."""
    tool = activity_as_tool(activity_with_custom_name)

    assert tool.name == "custom_name_activity"


def test_activity_as_tool_args_schema():
    """Test that args schema is correctly inferred."""
    tool = activity_as_tool(search_activity)

    schema = tool.args_schema
    assert "query" in schema.model_fields
    assert "limit" in schema.model_fields


def test_activity_as_tool_timeout_config():
    """Test activity_as_tool with timeout configuration."""
    tool = activity_as_tool(
        search_activity,
        start_to_close_timeout=timedelta(seconds=120),
        schedule_to_close_timeout=timedelta(minutes=5),
    )

    # Tool should be created successfully
    assert tool is not None
    assert tool._is_temporal_activity_tool is True


def test_activity_as_tool_retry_policy():
    """Test activity_as_tool with retry policy."""
    retry = RetryPolicy(
        initial_interval=timedelta(seconds=1),
        maximum_interval=timedelta(seconds=30),
        maximum_attempts=3,
    )

    tool = activity_as_tool(search_activity, retry_policy=retry)

    assert tool is not None


# =============================================================================
# Unit Tests: _is_temporal_tool
# =============================================================================


def test_is_temporal_tool_returns_true_for_activity_tool():
    """Test _is_temporal_tool returns True for activity-backed tools."""
    tool = activity_as_tool(search_activity)
    assert _is_temporal_tool(tool) is True


def test_is_temporal_tool_returns_false_for_regular_tool():
    """Test _is_temporal_tool returns False for regular CrewAI tools."""
    from crewai.tools.structured_tool import CrewStructuredTool

    def regular_func(x: str) -> str:
        """Regular function."""
        return x

    regular_tool = CrewStructuredTool.from_function(
        regular_func, name="regular", description="A regular tool"
    )
    assert _is_temporal_tool(regular_tool) is False


def test_is_temporal_tool_returns_false_for_non_tool():
    """Test _is_temporal_tool returns False for non-tool objects."""
    assert _is_temporal_tool("not a tool") is False
    assert _is_temporal_tool(123) is False
    assert _is_temporal_tool(None) is False
    assert _is_temporal_tool({}) is False


# =============================================================================
# Unit Tests: ToolActivityConfig
# =============================================================================


def test_tool_activity_config_defaults():
    """Test ToolActivityConfig has sensible defaults."""
    config = ToolActivityConfig()

    assert config.start_to_close_timeout == timedelta(seconds=60)
    assert config.schedule_to_close_timeout is None
    assert config.retry_policy is None
    assert config.heartbeat_timeout is None
    assert config.task_queue is None


def test_tool_activity_config_custom_values():
    """Test ToolActivityConfig with custom values."""
    retry = RetryPolicy(maximum_attempts=5)
    config = ToolActivityConfig(
        start_to_close_timeout=timedelta(seconds=120),
        retry_policy=retry,
        task_queue="tools-queue",
    )

    assert config.start_to_close_timeout == timedelta(seconds=120)
    assert config.retry_policy is retry
    assert config.task_queue == "tools-queue"


# =============================================================================
# Integration Tests
# =============================================================================

# Note: activity_as_tool() imports CrewAI which causes issues when called inside
# the workflow sandbox. For integration tests, we test the underlying mechanism
# directly using workflow.execute_activity() with dict args.
#
# IMPORTANT: Activities used with activity_as_tool() must accept a single dict
# argument, since that's how they receive the tool's parsed input.


@workflow.defn
class DirectActivityWorkflow:
    """Test workflow that directly calls activity with dict args."""

    @workflow.run
    async def run(self, query: str) -> str:
        # This mimics what activity_as_tool does internally
        result = await workflow.execute_activity(
            "search_dict_activity",
            {"query": query, "limit": 5},
            start_to_close_timeout=timedelta(seconds=30),
        )
        return str(result) if result else ""


@pytest.mark.asyncio
async def test_activity_with_dict_args():
    """Test that activities can be called with dict arguments."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[DirectActivityWorkflow],
            activities=[search_dict_activity],
        ):
            result = await env.client.execute_workflow(
                DirectActivityWorkflow.run,
                "test query",
                id="test-dict-args",
                task_queue="test-queue",
            )

            assert "test query" in result
            assert "limit=5" in result


@workflow.defn
class CalculatorDirectWorkflow:
    """Test workflow calling calculator activity directly."""

    @workflow.run
    async def run(self, expression: str) -> str:
        result = await workflow.execute_activity(
            "calculator_dict_activity",
            {"expression": expression},
            start_to_close_timeout=timedelta(seconds=10),
        )
        return str(result) if result else ""


@pytest.mark.asyncio
async def test_calculator_activity_direct():
    """Test calculator activity with dict args."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[CalculatorDirectWorkflow],
            activities=[calculator_dict_activity],
        ):
            result = await env.client.execute_workflow(
                CalculatorDirectWorkflow.run,
                "2 + 3 * 4",
                id="test-calculator-direct",
                task_queue="test-queue",
            )

            assert "14" in result  # 2 + 3 * 4 = 14


@workflow.defn
class NoArgsDirectWorkflow:
    """Test workflow calling no-args activity."""

    @workflow.run
    async def run(self) -> str:
        result = await workflow.execute_activity(
            "no_args_dict_activity",
            {},
            start_to_close_timeout=timedelta(seconds=10),
        )
        return str(result) if result else ""


@pytest.mark.asyncio
async def test_no_args_activity_direct():
    """Test activity with no arguments."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[NoArgsDirectWorkflow],
            activities=[no_args_dict_activity],
        ):
            result = await env.client.execute_workflow(
                NoArgsDirectWorkflow.run,
                id="test-no-args-direct",
                task_queue="test-queue",
            )

            assert "No args needed" in result


@workflow.defn
class ComplexArgsDirectWorkflow:
    """Test workflow calling complex args activity."""

    @workflow.run
    async def run(self) -> str:
        result = await workflow.execute_activity(
            "complex_dict_activity",
            {"name": "test", "count": 42, "enabled": False, "tags": ["a", "b"]},
            start_to_close_timeout=timedelta(seconds=10),
        )
        return str(result) if result else ""


@pytest.mark.asyncio
async def test_complex_args_activity_direct():
    """Test activity with complex arguments."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[ComplexArgsDirectWorkflow],
            activities=[complex_dict_activity],
        ):
            result = await env.client.execute_workflow(
                ComplexArgsDirectWorkflow.run,
                id="test-complex-args-direct",
                task_queue="test-queue",
            )

            assert "name=test" in result
            assert "count=42" in result
            assert "enabled=False" in result
