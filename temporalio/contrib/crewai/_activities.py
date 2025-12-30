"""CrewAI activity implementations.

This module contains the activity class that implements all CrewAI activities.
Activities are executed by Temporal workers and provide durability for
non-deterministic operations like LLM calls.
"""

from temporalio import activity

from ._models import LLMCallInput, LLMCallOutput
from ._utils import _safe_heartbeat

# Use TYPE_CHECKING to avoid circular imports
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._worker import CrewAIActivityConfig


class CrewAIActivities:
    """All CrewAI activities with injected configuration.

    This class uses constructor injection for configuration, following
    the pattern from temporalio.contrib.openai_agents.

    Example:
        config = CrewAIActivityConfig(
            llm_factory=lambda model: LLM(model=model),
        )
        activities = CrewAIActivities(config)
        # Register with worker:
        # Worker(client, ..., activities=[activities.llm_call])
    """

    def __init__(self, config: "CrewAIActivityConfig"):
        """Initialize with configuration.

        Args:
            config: Configuration containing factories and settings
        """
        self._config = config

    @activity.defn(name="crewai_llm_call")
    async def llm_call(self, input: LLMCallInput) -> LLMCallOutput:
        """Execute an LLM call.

        This activity calls the LLM via the configured factory and returns
        the result. Key behavior: we do NOT pass available_functions to the LLM.
        This causes tool_calls to be returned directly rather than executed
        inside the activity. Tool execution happens in the workflow via
        activity_as_tool (Phase 2).

        Args:
            input: LLM call input containing model, messages, and optional tools

        Returns:
            LLMCallOutput with either content or tool_calls populated
        """
        # Create LLM instance via factory
        llm = self._config.llm_factory(input.model)

        # Call LLM WITHOUT available_functions
        # This is key: by not passing available_functions, the LLM returns
        # tool_calls directly instead of trying to execute them
        result = await llm.acall(
            messages=input.messages,
            tools=input.tools,
            available_functions=None,  # Key: tools executed in workflow, not here
            **(input.llm_kwargs or {}),
        )

        # Heartbeat after completion (safe for unit tests)
        _safe_heartbeat({"model": input.model, "status": "completed"})

        # Handle return type based on CrewAI's LLM behavior:
        # - str: simple text response
        # - list: tool_calls to be executed by workflow
        if isinstance(result, list):
            return LLMCallOutput(content=None, tool_calls=result)
        elif isinstance(result, str):
            return LLMCallOutput(content=result, tool_calls=None)
        else:
            # Fallback for other response types (e.g., objects with attributes)
            return LLMCallOutput(
                content=str(result) if result else None,
                tool_calls=getattr(result, "tool_calls", None),
            )

    # Future phases will add:
    # @activity.defn(name="crewai_tool_call")
    # async def tool_call(self, input: ToolCallInput) -> ToolCallOutput: ...
    #
    # @activity.defn(name="crewai_memory_save")
    # async def memory_save(self, input: MemorySaveInput) -> None: ...
    #
    # @activity.defn(name="crewai_memory_search")
    # async def memory_search(self, input: MemorySearchInput) -> MemorySearchOutput: ...
