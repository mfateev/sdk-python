"""Tool wrapper for executing activities as CrewAI tools.

This module provides the activity_as_tool() function which wraps a Temporal
activity as a CrewAI tool. When the tool is invoked by an agent, it executes
the activity via workflow.execute_activity().
"""

from collections.abc import Callable
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.workflow import ActivityCancellationType

from ._utils import _build_args_schema
from ._worker import ToolActivityConfig


def activity_as_tool(
    activity_fn: Callable,
    *,
    start_to_close_timeout: timedelta | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    retry_policy: RetryPolicy | None = None,
    heartbeat_timeout: timedelta | None = None,
    task_queue: str | None = None,
    cancellation_type: ActivityCancellationType | None = None,
    name: str | None = None,
    description: str | None = None,
) -> Any:
    """Wrap a Temporal activity as a CrewAI tool.

    This creates a CrewStructuredTool that executes the activity via
    workflow.execute_activity() when invoked by an agent. The tool must
    be created and used inside a Temporal workflow.

    Args:
        activity_fn: The activity function (decorated with @activity.defn)
        start_to_close_timeout: Timeout for activity execution (default: 60s)
        schedule_to_close_timeout: Timeout from scheduling to completion
        retry_policy: Retry policy for failures
        heartbeat_timeout: Heartbeat timeout for long-running activities
        task_queue: Task queue for the activity (default: workflow's queue)
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

        @workflow.defn
        class MyWorkflow:
            @workflow.run
            async def run(self):
                agent = Agent(
                    tools=[
                        activity_as_tool(
                            search_web,
                            start_to_close_timeout=timedelta(seconds=30),
                        )
                    ],
                    llm=llm_stub("gpt-4"),
                )
                ...
    """
    # Import CrewAI here to avoid import errors when crewai not installed
    from crewai.tools.structured_tool import CrewStructuredTool

    # Get activity name from the decorated function
    # The @activity.defn decorator stores the definition in __temporal_activity_definition
    activity_def = getattr(activity_fn, "__temporal_activity_definition", None)
    if activity_def:
        activity_name = activity_def.name
    else:
        # Fallback to function name
        activity_name = getattr(activity_fn, "__name__", "unknown_activity")

    # Build configuration with defaults
    config = ToolActivityConfig(
        start_to_close_timeout=start_to_close_timeout or timedelta(seconds=60),
        schedule_to_close_timeout=schedule_to_close_timeout,
        retry_policy=retry_policy,
        heartbeat_timeout=heartbeat_timeout,
        task_queue=task_queue,
        cancellation_type=cancellation_type or ActivityCancellationType.TRY_CANCEL,
    )

    # Build args schema from function signature
    args_schema = _build_args_schema(activity_fn)

    # Tool name and description
    tool_name = name or activity_name
    tool_description = description or activity_fn.__doc__ or f"Execute {tool_name}"
    # Clean up description (remove extra whitespace)
    tool_description = " ".join(tool_description.split())

    # Create the wrapper function that executes the activity
    async def execute_activity(**kwargs: Any) -> str:
        """Execute the activity and return result as string."""
        # Build activity options
        activity_options: dict[str, Any] = {
            "start_to_close_timeout": config.start_to_close_timeout,
        }
        if config.schedule_to_close_timeout:
            activity_options[
                "schedule_to_close_timeout"
            ] = config.schedule_to_close_timeout
        if config.retry_policy:
            activity_options["retry_policy"] = config.retry_policy
        if config.heartbeat_timeout:
            activity_options["heartbeat_timeout"] = config.heartbeat_timeout
        if config.task_queue:
            activity_options["task_queue"] = config.task_queue
        if config.cancellation_type:
            activity_options["cancellation_type"] = config.cancellation_type

        # Execute activity with kwargs as the argument
        # Activities receive a single dict argument when called this way
        result = await workflow.execute_activity(
            activity_name,
            kwargs,
            **activity_options,
        )

        # Convert result to string for LLM consumption
        return str(result) if result is not None else ""

    # Preserve function metadata
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
    tool._is_temporal_activity_tool = True  # type: ignore[attr-defined]

    return tool
