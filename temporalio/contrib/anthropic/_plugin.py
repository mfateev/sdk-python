"""Temporal plugin for Anthropic Claude Agent SDK integration."""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Sequence
from datetime import timedelta

from temporalio.contrib.anthropic._activities import invoke_llm_activity
from temporalio.contrib.pydantic import PydanticPayloadConverter, ToJsonOptions
from temporalio.converter import DataConverter, DefaultPayloadConverter
from temporalio.plugin import SimplePlugin
from temporalio.worker import WorkflowRunner
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner


@dataclasses.dataclass
class TransportActivityParameters:
    """Configuration parameters for LLM transport activity execution.

    .. warning::
        This API is experimental and may change in future versions.
        Use with caution in production environments.

    These parameters control how the invoke_llm_activity executes within Temporal.
    They map directly to Temporal activity execution options.

    Args:
        task_queue: Optional task queue override for LLM activities.
        schedule_to_close_timeout: Total timeout from scheduling to completion.
        schedule_to_start_timeout: Timeout from scheduling to worker pickup.
        start_to_close_timeout: Timeout from worker pickup to completion (default: 5 minutes).
        heartbeat_timeout: Maximum time between heartbeats.
        retry_policy: Custom retry policy for activity failures.
        cancellation_type: How to handle activity cancellation.
        versioning_intent: Temporal versioning intent.
        priority: Activity execution priority.
        use_local_activity: Execute as local activity (for testing).
    """

    task_queue: str | None = None
    schedule_to_close_timeout: timedelta | None = None
    schedule_to_start_timeout: timedelta | None = None
    start_to_close_timeout: timedelta = timedelta(minutes=5)
    heartbeat_timeout: timedelta | None = None
    retry_policy: object | None = None  # RetryPolicy type
    cancellation_type: object = None  # ActivityCancellationType
    versioning_intent: object | None = None  # VersioningIntent
    priority: object = None  # Priority
    use_local_activity: bool = False


class AnthropicPayloadConverter(PydanticPayloadConverter):
    """PayloadConverter for Anthropic Claude Agent SDK.

    .. warning::
        This API is experimental and may change in future versions.
        Use with caution in production environments.

    This converter ensures proper serialization of Pydantic models and dataclasses
    used in the Claude Agent SDK integration.
    """

    def __init__(self) -> None:
        """Initialize the payload converter."""
        super().__init__(ToJsonOptions(exclude_unset=True))


def _data_converter(converter: DataConverter | None) -> DataConverter:
    """Configure data converter for Anthropic integration.

    Args:
        converter: Existing data converter or None

    Returns:
        Data converter configured for Anthropic integration
    """
    if converter is None:
        return DataConverter(payload_converter_class=AnthropicPayloadConverter)
    elif converter.payload_converter_class is DefaultPayloadConverter:
        return dataclasses.replace(
            converter, payload_converter_class=AnthropicPayloadConverter
        )
    elif not isinstance(converter.payload_converter, AnthropicPayloadConverter):
        raise ValueError(
            "The payload converter must be of type AnthropicPayloadConverter."
        )
    return converter


class AnthropicAgentsPlugin(SimplePlugin):
    """Temporal plugin for integrating Anthropic Claude Agent SDK with workflows.

    .. warning::
        This class is experimental and may change in future versions.
        Use with caution in production environments.

    This plugin provides integration between the Anthropic Claude Agent SDK and
    Temporal workflows. It automatically configures the necessary activities and
    data converters to enable Claude agents to run within Temporal workflows.

    Unlike the OpenAI plugin, this plugin does not use global hooks since the
    Claude Agent SDK does not provide them. Instead, users explicitly pass a
    TemporalTransport to the SDK's query() function.

    The plugin:
    1. Configures the Pydantic data converter for type-safe serialization
    2. Registers LLM execution activities (invoke_llm_activity)
    3. Sets up sandbox passthrough for claude_agent_sdk module
    4. (Future) Manages MCP server activities and lifecycles

    Args:
        transport_params: Configuration parameters for LLM transport activity
            execution. If None, default parameters will be used.
        register_activities: Whether to register activities during worker execution.
            This can be disabled on some workers to allow separation of workflows
            and activities, but should not be disabled on all workers.

    Example:
        >>> from temporalio.client import Client
        >>> from temporalio.worker import Worker
        >>> from temporalio.contrib.anthropic import (
        ...     AnthropicAgentsPlugin,
        ...     TemporalTransport,
        ...     TransportActivityParameters,
        ... )
        >>> from claude_agent_sdk import query, ClaudeAgentOptions
        >>> from datetime import timedelta
        >>>
        >>> # Configure transport parameters
        >>> transport_params = TransportActivityParameters(
        ...     start_to_close_timeout=timedelta(minutes=5),
        ... )
        >>>
        >>> # Create plugin
        >>> plugin = AnthropicAgentsPlugin(
        ...     transport_params=transport_params,
        ... )
        >>>
        >>> # Use with client and worker
        >>> client = await Client.connect(
        ...     "localhost:7233",
        ...     plugins=[plugin],
        ... )
        >>> worker = Worker(
        ...     client,
        ...     task_queue="my-task-queue",
        ...     workflows=[MyWorkflow],
        ... )
        >>>
        >>> # In workflow: use explicit transport
        >>> @workflow.defn
        >>> class MyWorkflow:
        ...     @workflow.run
        ...     async def run(self, prompt: str) -> str:
        ...         transport = TemporalTransport()
        ...         options = ClaudeAgentOptions(model="claude-sonnet-4-5")
        ...         async for message in query(prompt, options, transport):
        ...             return str(message)
    """

    def __init__(
        self,
        transport_params: TransportActivityParameters | None = None,
        register_activities: bool = True,
    ) -> None:
        """Initialize the Anthropic agents plugin.

        Args:
            transport_params: Configuration parameters for LLM transport activity
                execution. If None, default parameters will be used.
            register_activities: Whether to register activities during worker
                execution. This can be disabled on some workers to allow separation
                of workflows and activities.
        """
        if transport_params is None:
            transport_params = TransportActivityParameters()

        # Ensure timeout is set
        if (
            transport_params.start_to_close_timeout is None
            and transport_params.schedule_to_close_timeout is None
        ):
            transport_params.start_to_close_timeout = timedelta(minutes=5)

        # Activity registration function
        def add_activities(
            activities: Sequence[Callable] | None,
        ) -> Sequence[Callable]:
            if not register_activities:
                return activities or []

            # Register LLM activity
            new_activities = [invoke_llm_activity]

            # TODO: Add MCP server activities when implemented
            # for mcp_server in mcp_server_providers:
            #     new_activities.extend(mcp_server._get_activities())

            return list(activities or []) + new_activities

        # Workflow runner configuration for sandbox
        def workflow_runner(runner: WorkflowRunner | None) -> WorkflowRunner:
            if not runner:
                raise ValueError("No WorkflowRunner provided to the Anthropic plugin.")

            # Add passthrough for claude_agent_sdk and anthropic modules
            if isinstance(runner, SandboxedWorkflowRunner):
                return dataclasses.replace(
                    runner,
                    restrictions=runner.restrictions.with_passthrough_modules(
                        "claude_agent_sdk", "anthropic"
                    ),
                )
            return runner

        # Initialize SimplePlugin
        super().__init__(
            name="AnthropicAgentsPlugin",
            data_converter=_data_converter,
            activities=add_activities,
            workflow_runner=workflow_runner,
        )
