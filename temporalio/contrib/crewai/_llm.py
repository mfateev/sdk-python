"""LLM stub for workflow-side LLM calls.

This module provides an LLM stub that routes calls through Temporal activities.
Use this instead of crewai.LLM when running crews in Temporal workflows.
"""

from typing import Any

from crewai.llms.base_llm import BaseLLM

from temporalio import workflow

from ._models import LLMCallInput
from ._utils import _serialize_tools
from ._worker import LLMActivityConfig


class _LLMStub(BaseLLM):
    """Stub that routes LLM calls through Temporal activities.

    This class inherits from CrewAI's BaseLLM to ensure it is preserved
    by CrewAI's Agent during initialization (not converted to crewai.LLM).

    The stub is designed to be used as a drop-in replacement for crewai.LLM
    in workflow code. When the workflow calls acall(), the stub executes
    a Temporal activity that makes the actual LLM call.
    """

    # Store activity config separately (not in BaseLLM)
    _activity_config: LLMActivityConfig
    _extra_llm_kwargs: dict[str, Any]

    def __init__(
        self,
        model: str,
        activity_config: LLMActivityConfig | None = None,
        **llm_kwargs: Any,
    ):
        """Initialize the LLM stub.

        Args:
            model: The model name (e.g., "gpt-4", "gpt-4o-mini")
            activity_config: Optional activity execution configuration
            **llm_kwargs: Additional kwargs passed to the LLM
        """
        # Initialize BaseLLM with model
        super().__init__(model=model)
        # Store our custom attributes
        object.__setattr__(
            self, "_activity_config", activity_config or LLMActivityConfig()
        )
        object.__setattr__(self, "_extra_llm_kwargs", llm_kwargs)

    @property
    def activity_config(self) -> LLMActivityConfig:
        """Get the activity configuration."""
        return self._activity_config

    def call(
        self,
        messages: str | list[dict[str, str]],
        tools: list[dict] | None = None,
        callbacks: list[Any] | None = None,
        available_functions: dict[str, Any] | None = None,
        from_task: Any | None = None,
        from_agent: Any | None = None,
    ) -> str | Any:
        """Not supported in Temporal workflows - use async method instead."""
        raise RuntimeError(
            "Synchronous LLM calls are not supported in Temporal workflows. "
            "Use acall() instead, which is called automatically by CrewAI's "
            "async execution methods (akickoff)."
        )

    async def acall(
        self,
        messages: str | list[dict],
        tools: list[Any] | None = None,
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
            from_task: Task context (ignored, not serializable)
            from_agent: Agent context (ignored, not serializable)
            **kwargs: Additional LLM kwargs

        Returns:
            str: Text content if LLM returns text
            list: Tool calls if LLM requests tool execution
        """
        # Normalize messages to list format
        if isinstance(messages, str):
            normalized_messages = [{"role": "user", "content": messages}]
        else:
            normalized_messages = messages

        # Serialize tools for activity input
        serialized_tools = _serialize_tools(tools) if tools else None

        # Prepare activity input
        input_data = LLMCallInput(
            model=self.model,
            messages=normalized_messages,
            tools=serialized_tools,
            llm_kwargs={**self._extra_llm_kwargs, **kwargs},
        )

        # Build activity options
        activity_options: dict[str, Any] = {
            "start_to_close_timeout": self.activity_config.start_to_close_timeout,
        }

        # Add optional parameters if configured
        if self.activity_config.schedule_to_close_timeout:
            activity_options["schedule_to_close_timeout"] = (
                self.activity_config.schedule_to_close_timeout
            )

        if self.activity_config.retry_policy:
            activity_options["retry_policy"] = self.activity_config.retry_policy

        if self.activity_config.heartbeat_timeout:
            activity_options["heartbeat_timeout"] = (
                self.activity_config.heartbeat_timeout
            )

        if self.activity_config.task_queue:
            activity_options["task_queue"] = self.activity_config.task_queue

        if self.activity_config.cancellation_type:
            activity_options["cancellation_type"] = (
                self.activity_config.cancellation_type
            )

        # Execute as activity
        result = await workflow.execute_activity(
            "crewai_llm_call",
            input_data,
            **activity_options,
        )

        # Result is dict (JSON-deserialized), not dataclass
        if isinstance(result, dict):
            tool_calls = result.get("tool_calls")
            content = result.get("content")
        else:
            # Handle if somehow we get a typed response
            tool_calls = getattr(result, "tool_calls", None)
            content = getattr(result, "content", None)

        # Return tool_calls if present, otherwise content
        if tool_calls:
            return tool_calls
        return content or ""


def _is_llm_stub(obj: Any) -> bool:
    """Check if an object is a Temporal LLM stub.

    Args:
        obj: Object to check

    Returns:
        True if the object is an _LLMStub instance
    """
    return isinstance(obj, _LLMStub)


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
