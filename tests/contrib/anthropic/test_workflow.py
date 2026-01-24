"""Unit tests for workflow module (activity_as_tool)."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock, patch

import pytest

from temporalio import activity
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError


class TestActivityAsTool:
    """Tests for activity_as_tool function."""

    def test_basic_conversion(self):
        """activity_as_tool should convert activity to SdkMcpTool."""
        from temporalio.contrib.anthropic.workflow import activity_as_tool

        @activity.defn
        async def get_weather(city: str) -> str:
            """Get the weather for a city."""
            return f"Weather in {city}: Sunny"

        tool = activity_as_tool(
            get_weather,
            start_to_close_timeout=timedelta(seconds=30),
        )

        assert tool.name == "get_weather"
        assert tool.description == "Get the weather for a city."
        assert "properties" in tool.input_schema
        assert "city" in tool.input_schema["properties"]
        assert tool.input_schema["properties"]["city"]["type"] == "string"
        assert "city" in tool.input_schema["required"]

    def test_custom_description(self):
        """activity_as_tool should use custom description if provided."""
        from temporalio.contrib.anthropic.workflow import activity_as_tool

        @activity.defn
        async def my_activity(x: int) -> int:
            return x * 2

        tool = activity_as_tool(
            my_activity,
            start_to_close_timeout=timedelta(seconds=10),
            description="Custom description for the tool",
        )

        assert tool.description == "Custom description for the tool"

    def test_multiple_parameters(self):
        """activity_as_tool should handle multiple parameters."""
        from temporalio.contrib.anthropic.workflow import activity_as_tool

        @activity.defn
        async def add_numbers(a: int, b: float) -> float:
            """Add two numbers together."""
            return a + b

        tool = activity_as_tool(
            add_numbers,
            start_to_close_timeout=timedelta(seconds=10),
        )

        assert tool.name == "add_numbers"
        assert "a" in tool.input_schema["properties"]
        assert "b" in tool.input_schema["properties"]
        assert tool.input_schema["properties"]["a"]["type"] == "integer"
        assert tool.input_schema["properties"]["b"]["type"] == "number"
        assert set(tool.input_schema["required"]) == {"a", "b"}

    def test_optional_parameter(self):
        """activity_as_tool should handle optional parameters."""
        from temporalio.contrib.anthropic.workflow import activity_as_tool

        @activity.defn
        async def greet(name: str, greeting: str = "Hello") -> str:
            """Greet someone."""
            return f"{greeting}, {name}!"

        tool = activity_as_tool(
            greet,
            start_to_close_timeout=timedelta(seconds=10),
        )

        # name is required, greeting is optional
        assert "name" in tool.input_schema["required"]
        assert "greeting" not in tool.input_schema["required"]

    def test_no_docstring(self):
        """activity_as_tool should handle activities without docstrings."""
        from temporalio.contrib.anthropic.workflow import activity_as_tool

        @activity.defn
        async def no_docs(x: str) -> str:
            return x

        tool = activity_as_tool(
            no_docs,
            start_to_close_timeout=timedelta(seconds=10),
        )

        assert tool.description == "Execute no_docs"

    def test_rejects_non_activity(self):
        """activity_as_tool should reject functions without @activity.defn."""
        from temporalio.contrib.anthropic.workflow import activity_as_tool

        async def not_an_activity(x: str) -> str:
            return x

        with pytest.raises(ApplicationError) as exc_info:
            activity_as_tool(
                not_an_activity,
                start_to_close_timeout=timedelta(seconds=10),
            )

        assert "must be decorated with @activity.defn" in str(exc_info.value)

    def test_various_types(self):
        """activity_as_tool should handle various Python types."""
        from temporalio.contrib.anthropic.workflow import activity_as_tool

        @activity.defn
        async def process_data(
            text: str,
            count: int,
            ratio: float,
            enabled: bool,
            items: list,
            config: dict,
        ) -> str:
            return "done"

        tool = activity_as_tool(
            process_data,
            start_to_close_timeout=timedelta(seconds=10),
        )

        props = tool.input_schema["properties"]
        assert props["text"]["type"] == "string"
        assert props["count"]["type"] == "integer"
        assert props["ratio"]["type"] == "number"
        assert props["enabled"]["type"] == "boolean"
        assert props["items"]["type"] == "array"
        assert props["config"]["type"] == "object"


class TestActivityAsToolHandler:
    """Tests for the handler created by activity_as_tool."""

    @pytest.mark.asyncio
    async def test_handler_calls_activity(self):
        """Handler should call workflow.execute_activity."""
        from temporalio.contrib.anthropic.workflow import activity_as_tool

        @activity.defn
        async def echo(message: str) -> str:
            """Echo a message."""
            return message

        tool = activity_as_tool(
            echo,
            start_to_close_timeout=timedelta(seconds=10),
        )

        with patch(
            "temporalio.contrib.anthropic.workflow.temporal_workflow.execute_activity",
            new_callable=AsyncMock,
        ) as mock_execute:
            mock_execute.return_value = "Hello World"

            result = await tool.handler({"message": "Hello World"})

            # Verify activity was called
            mock_execute.assert_called_once()

            # Verify result format
            assert "content" in result
            assert result["content"][0]["type"] == "text"
            assert "Hello World" in result["content"][0]["text"]

    @pytest.mark.asyncio
    async def test_handler_with_multiple_args(self):
        """Handler should pass multiple args to activity."""
        from temporalio.contrib.anthropic.workflow import activity_as_tool

        @activity.defn
        async def add(a: int, b: int) -> int:
            """Add two numbers."""
            return a + b

        tool = activity_as_tool(
            add,
            start_to_close_timeout=timedelta(seconds=10),
        )

        with patch(
            "temporalio.contrib.anthropic.workflow.temporal_workflow.execute_activity",
            new_callable=AsyncMock,
        ) as mock_execute:
            mock_execute.return_value = 5

            result = await tool.handler({"a": 2, "b": 3})

            # Verify activity was called with args
            mock_execute.assert_called_once()
            call_kwargs = mock_execute.call_args.kwargs
            assert call_kwargs["args"] == [2, 3]

    @pytest.mark.asyncio
    async def test_handler_error_returns_mcp_error(self):
        """Handler should return MCP error format on exception."""
        from temporalio.contrib.anthropic.workflow import activity_as_tool

        @activity.defn
        async def failing_activity(x: str) -> str:
            """This will fail."""
            return x

        tool = activity_as_tool(
            failing_activity,
            start_to_close_timeout=timedelta(seconds=10),
        )

        with patch(
            "temporalio.contrib.anthropic.workflow.temporal_workflow.execute_activity",
            new_callable=AsyncMock,
        ) as mock_execute:
            mock_execute.side_effect = Exception("Activity failed")

            result = await tool.handler({"x": "test"})

            # Verify error format
            assert "content" in result
            assert "is_error" in result
            assert result["is_error"] is True
            assert "Error:" in result["content"][0]["text"]

    @pytest.mark.asyncio
    async def test_handler_passes_activity_options(self):
        """Handler should pass activity options to execute_activity."""
        from temporalio.contrib.anthropic.workflow import activity_as_tool

        @activity.defn
        async def my_activity(x: str) -> str:
            return x

        retry_policy = RetryPolicy(maximum_attempts=3)
        tool = activity_as_tool(
            my_activity,
            start_to_close_timeout=timedelta(seconds=30),
            heartbeat_timeout=timedelta(seconds=5),
            retry_policy=retry_policy,
            task_queue="custom-queue",
        )

        with patch(
            "temporalio.contrib.anthropic.workflow.temporal_workflow.execute_activity",
            new_callable=AsyncMock,
        ) as mock_execute:
            mock_execute.return_value = "result"

            await tool.handler({"x": "test"})

            call_kwargs = mock_execute.call_args.kwargs
            assert call_kwargs["start_to_close_timeout"] == timedelta(seconds=30)
            assert call_kwargs["heartbeat_timeout"] == timedelta(seconds=5)
            assert call_kwargs["retry_policy"] == retry_policy
            assert call_kwargs["task_queue"] == "custom-queue"

    @pytest.mark.asyncio
    async def test_handler_handles_none_result(self):
        """Handler should handle None result from activity."""
        from temporalio.contrib.anthropic.workflow import activity_as_tool

        @activity.defn
        async def void_activity(x: str) -> None:
            """Does nothing."""
            pass

        tool = activity_as_tool(
            void_activity,
            start_to_close_timeout=timedelta(seconds=10),
        )

        with patch(
            "temporalio.contrib.anthropic.workflow.temporal_workflow.execute_activity",
            new_callable=AsyncMock,
        ) as mock_execute:
            mock_execute.return_value = None

            result = await tool.handler({"x": "test"})

            assert "content" in result
            assert result["content"][0]["text"] == "Success"
