"""Activities for Phase 0 spike.

These activities demonstrate the core patterns for routing CrewAI operations
through Temporal activities.
"""

from dataclasses import dataclass
from typing import Any, Callable

from temporalio import activity

from .models import (
    LLMCallInput,
    LLMCallOutput,
    MemorySaveInput,
    MemorySearchInput,
    MemorySearchOutput,
    ToolCallInput,
    ToolCallOutput,
)


@dataclass
class SpikeActivityConfig:
    """Configuration for spike activities."""

    # Factory to create LLM instance from model name
    llm_factory: Callable[[str], Any]
    # Registry of tool functions
    tool_registry: dict[str, Callable[..., Any]] | None = None
    # In-memory storage for spike (real impl would use RAGStorage)
    memory_store: dict[str, list[dict]] | None = None


class SpikeActivities:
    """Activity implementations for the spike.

    These demonstrate the class-based activity pattern with injected configuration.
    """

    def __init__(self, config: SpikeActivityConfig):
        self._config = config
        # Initialize in-memory storage if not provided
        if self._config.memory_store is None:
            self._config.memory_store = {"short_term": [], "entity": []}

    @activity.defn(name="spike_llm_call")
    async def llm_call(self, input: LLMCallInput) -> LLMCallOutput:
        """Execute an LLM call.

        Key insight: We do NOT pass available_functions to the LLM.
        This causes tool_calls to be returned directly rather than executed.
        """
        llm = self._config.llm_factory(input.model)

        # Make the LLM call WITHOUT available_functions
        # This causes tool_calls to be returned, not executed
        result = await llm.acall(
            messages=input.messages,
            tools=input.tools,
            available_functions=None,  # Key: don't pass this
            **(input.llm_kwargs or {}),
        )

        # Heartbeat only if in activity context
        try:
            activity.heartbeat({"model": input.model, "status": "completed"})
        except RuntimeError:
            pass  # Not in activity context (e.g., unit tests)

        # Result handling based on CrewAI's LLM behavior:
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

    @activity.defn(name="spike_tool_call")
    async def tool_call(self, input: ToolCallInput) -> ToolCallOutput:
        """Execute a tool call.

        This demonstrates how tools are executed as separate activities,
        allowing full visibility and retry handling.
        """
        if not self._config.tool_registry:
            return ToolCallOutput(
                result="", error=f"Tool registry not configured"
            )

        tool_fn = self._config.tool_registry.get(input.tool_name)
        if not tool_fn:
            return ToolCallOutput(
                result="", error=f"Tool '{input.tool_name}' not found"
            )

        try:
            result = tool_fn(**input.tool_args)
            # Heartbeat only if in activity context
            try:
                activity.heartbeat({"tool": input.tool_name, "status": "completed"})
            except RuntimeError:
                pass  # Not in activity context
            return ToolCallOutput(result=str(result))
        except Exception as e:
            return ToolCallOutput(result="", error=str(e))

    @activity.defn(name="spike_memory_save")
    async def memory_save(self, input: MemorySaveInput) -> None:
        """Save to memory storage.

        This spike uses in-memory storage. Real implementation uses RAGStorage.
        """
        assert self._config.memory_store is not None
        store = self._config.memory_store.setdefault(input.storage_type, [])
        store.append({"value": input.value, "metadata": input.metadata})
        # Heartbeat only if in activity context
        try:
            activity.heartbeat({"storage_type": input.storage_type, "action": "save"})
        except RuntimeError:
            pass  # Not in activity context

    @activity.defn(name="spike_memory_search")
    async def memory_search(self, input: MemorySearchInput) -> MemorySearchOutput:
        """Search memory storage.

        This spike uses simple string matching. Real implementation uses RAGStorage.
        """
        assert self._config.memory_store is not None
        store = self._config.memory_store.get(input.storage_type, [])

        # Simple string matching for spike
        results = []
        for item in store:
            if input.query.lower() in str(item["value"]).lower():
                results.append(item)
                if len(results) >= input.limit:
                    break

        # Heartbeat only if in activity context
        try:
            activity.heartbeat({
                "storage_type": input.storage_type,
                "action": "search",
                "results_count": len(results),
            })
        except RuntimeError:
            pass  # Not in activity context
        return MemorySearchOutput(results=results)


def get_spike_activities(config: SpikeActivityConfig) -> list[Callable]:
    """Get all spike activities configured with the given config."""
    instance = SpikeActivities(config)
    return [
        instance.llm_call,
        instance.tool_call,
        instance.memory_save,
        instance.memory_search,
    ]
