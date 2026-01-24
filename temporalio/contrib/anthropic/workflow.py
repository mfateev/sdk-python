"""Workflow-specific primitives for working with Claude Agent SDK in a workflow context.

.. warning::
    This API is experimental and may change in future versions.
    Use with caution in production environments.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Generic, TypeVar, get_type_hints

from temporalio import activity
from temporalio import workflow as temporal_workflow
from temporalio.common import Priority, RetryPolicy
from temporalio.exceptions import ApplicationError, TemporalError
from temporalio.workflow import (
    ActivityCancellationType,
    VersioningIntent,
)

T = TypeVar("T")


@dataclass
class SdkMcpTool(Generic[T]):
    """MCP tool definition compatible with Claude Agent SDK.

    This dataclass mirrors claude_agent_sdk.SdkMcpTool for use in Temporal workflows.
    It can be used directly with claude_agent_sdk.create_sdk_mcp_server().

    Attributes:
        name: Unique identifier for the tool
        description: Human-readable description of what the tool does
        input_schema: JSON Schema or type defining input parameters
        handler: Async function that executes the tool
    """

    name: str
    description: str
    input_schema: type[T] | dict[str, Any]
    handler: Callable[[T], Awaitable[dict[str, Any]]]


def activity_as_tool(
    fn: Callable,
    *,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: Priority = Priority.default,
    description: str | None = None,
) -> SdkMcpTool[Any]:
    """Convert a Temporal activity function to a Claude Agent SDK MCP tool.

    .. warning::
        This API is experimental and may change in future versions.
        Use with caution in production environments.

    This function takes a Temporal activity function and converts it into a
    Claude Agent SDK MCP tool that can be used with create_sdk_mcp_server().
    The tool will automatically handle the conversion of inputs and outputs
    between the agent and the activity.

    For undocumented arguments, refer to :py:mod:`workflow` and :py:meth:`start_activity`

    Args:
        fn: A Temporal activity function to convert to a tool.
        description: Optional description for the tool. If not provided,
            uses the activity's docstring.

    Returns:
        A Claude Agent SDK MCP tool that wraps the provided activity.

    Raises:
        ApplicationError: If the function is not properly decorated as a Temporal activity.

    Example:
        >>> @activity.defn
        >>> async def get_weather(city: str) -> str:
        ...     '''Get the weather for a given city.'''
        ...     return f"Weather in {city}: Sunny, 72F"
        >>>
        >>> # Create tool with custom activity options
        >>> tool = activity_as_tool(
        ...     get_weather,
        ...     start_to_close_timeout=timedelta(seconds=30),
        ...     retry_policy=RetryPolicy(maximum_attempts=3),
        ... )
        >>>
        >>> # Use with Claude Agent SDK
        >>> server = create_sdk_mcp_server("weather", tools=[tool])
    """

    # Validate that fn is a Temporal activity
    activity_def = activity._Definition.from_callable(fn)
    if not activity_def:
        raise ApplicationError(
            "Function must be decorated with @activity.defn to be used as a tool",
            "invalid_tool",
        )
    if activity_def.name is None:
        raise ApplicationError(
            "Activity must have a name to be converted to a tool",
            "invalid_tool",
        )

    activity_name = activity_def.name

    # Extract function metadata
    tool_name = fn.__name__
    tool_description = description or fn.__doc__ or f"Execute {tool_name}"

    # Build input schema from function signature
    input_schema = _build_input_schema(fn)

    # Create the handler that calls the activity
    async def handler(args: dict[str, Any]) -> dict[str, Any]:
        """Execute the activity and return result in MCP format."""
        # Convert args dict to positional/keyword args based on signature
        sig = inspect.signature(fn)
        params = list(sig.parameters.keys())

        # Handle 'self' parameter for class-based activities
        if params and params[0] == "self":
            params = params[1:]

        # Build args list in parameter order
        call_args = [args.get(p) for p in params if p in args]

        # Activity execution options
        activity_options = dict(
            task_queue=task_queue,
            schedule_to_close_timeout=schedule_to_close_timeout,
            schedule_to_start_timeout=schedule_to_start_timeout,
            start_to_close_timeout=start_to_close_timeout,
            heartbeat_timeout=heartbeat_timeout,
            retry_policy=retry_policy,
            cancellation_type=cancellation_type,
            activity_id=activity_id,
            versioning_intent=versioning_intent,
            summary=summary or tool_description,
            priority=priority,
        )

        try:
            # Use positional arg for single-arg activities, args list for multiple
            if len(call_args) == 1:
                result = await temporal_workflow.execute_activity(
                    activity_name,
                    call_args[0],
                    **activity_options,
                )
            else:
                result = await temporal_workflow.execute_activity(
                    activity_name,
                    args=call_args,
                    **activity_options,
                )

            # Convert result to MCP format
            result_text = str(result) if result is not None else "Success"
            return {"content": [{"type": "text", "text": result_text}]}

        except Exception as e:
            # Return error in MCP format
            return {
                "content": [{"type": "text", "text": f"Error: {e}"}],
                "is_error": True,
            }

    return SdkMcpTool(
        name=tool_name,
        description=tool_description,
        input_schema=input_schema,
        handler=handler,
    )


def _build_input_schema(fn: Callable) -> dict[str, Any]:
    """Build JSON Schema from function signature.

    Args:
        fn: Function to extract schema from

    Returns:
        JSON Schema dict for the function's parameters
    """
    sig = inspect.signature(fn)
    params = list(sig.parameters.items())

    # Skip 'self' parameter for class-based activities
    if params and params[0][0] == "self":
        params = params[1:]

    # Try to get type hints
    try:
        hints = get_type_hints(fn)
    except Exception:
        hints = {}

    properties: dict[str, Any] = {}
    required: list[str] = []

    for name, param in params:
        # Get type from hints or default to string
        param_type = hints.get(name)
        json_type = _python_type_to_json_schema(param_type)

        properties[name] = json_type

        # Check if required (no default value)
        if param.default is inspect.Parameter.empty:
            required.append(name)

    return {
        "type": "object",
        "properties": properties,
        "required": required,
    }


def _python_type_to_json_schema(python_type: Any) -> dict[str, Any]:
    """Convert Python type to JSON Schema type.

    Args:
        python_type: Python type annotation

    Returns:
        JSON Schema type definition
    """
    if python_type is None:
        return {"type": "string"}

    # Handle basic types
    if python_type is str:
        return {"type": "string"}
    elif python_type is int:
        return {"type": "integer"}
    elif python_type is float:
        return {"type": "number"}
    elif python_type is bool:
        return {"type": "boolean"}
    elif python_type is list or (
        hasattr(python_type, "__origin__") and python_type.__origin__ is list
    ):
        return {"type": "array"}
    elif python_type is dict or (
        hasattr(python_type, "__origin__") and python_type.__origin__ is dict
    ):
        return {"type": "object"}

    # Default to string for unknown types
    return {"type": "string"}


class ToolSerializationError(TemporalError):
    """Error that occurs when a tool output could not be serialized.

    .. warning::
        This exception is experimental and may change in future versions.
        Use with caution in production environments.

    This exception is raised when a tool returns a value that cannot be
    properly serialized for use by the Claude agent. All tool outputs must
    be convertible to strings for the agent to process them.
    """
