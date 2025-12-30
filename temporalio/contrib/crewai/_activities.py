"""CrewAI activity implementations.

This module contains the activity class that implements all CrewAI activities.
Activities are executed by Temporal workers and provide durability for
non-deterministic operations like LLM calls, memory, and knowledge operations.
"""

from typing import TYPE_CHECKING

from temporalio import activity

from ._models import (
    KnowledgeResetInput,
    KnowledgeSaveInput,
    KnowledgeSearchInput,
    KnowledgeSearchOutput,
    LLMCallInput,
    LLMCallOutput,
    LTMLoadInput,
    LTMLoadOutput,
    LTMResetInput,
    LTMSaveInput,
    MemoryResetInput,
    MemorySaveInput,
    MemorySearchInput,
    MemorySearchOutput,
)
from ._utils import _safe_heartbeat

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

    # =========================================================================
    # Memory Activities (Phase 3)
    # =========================================================================

    @activity.defn(name="crewai_memory_save")
    async def memory_save(self, input: MemorySaveInput) -> None:
        """Save a value to RAG memory storage.

        Args:
            input: Contains storage_type, value, and metadata
        """
        storage = self._config.get_rag_storage(input.storage_type)
        await storage.asave(input.value, input.metadata)
        _safe_heartbeat({"storage_type": input.storage_type, "operation": "save"})

    @activity.defn(name="crewai_memory_search")
    async def memory_search(self, input: MemorySearchInput) -> MemorySearchOutput:
        """Search RAG memory storage.

        Args:
            input: Contains storage_type, query, limit, filter, score_threshold

        Returns:
            MemorySearchOutput with list of results
        """
        storage = self._config.get_rag_storage(input.storage_type)
        results = await storage.asearch(
            query=input.query,
            limit=input.limit,
            filter=input.filter,
            score_threshold=input.score_threshold,
        )
        _safe_heartbeat({"storage_type": input.storage_type, "operation": "search"})
        return MemorySearchOutput(results=results)

    @activity.defn(name="crewai_memory_reset")
    async def memory_reset(self, input: MemoryResetInput) -> None:
        """Reset RAG memory storage.

        Args:
            input: Contains storage_type to reset
        """
        storage = self._config.get_rag_storage(input.storage_type)
        # RAGStorage.reset() is synchronous in CrewAI
        storage.reset()
        _safe_heartbeat({"storage_type": input.storage_type, "operation": "reset"})

    # =========================================================================
    # Long-term Memory Activities (Phase 3)
    # =========================================================================

    @activity.defn(name="crewai_ltm_save")
    async def ltm_save(self, input: LTMSaveInput) -> None:
        """Save data to long-term memory storage.

        Args:
            input: Contains task_description, metadata, datetime, score, db_path
        """
        storage = self._config.get_ltm_storage(input.db_path)
        await storage.asave(
            task_description=input.task_description,
            metadata=input.metadata,
            datetime=input.datetime,
            score=input.score,
        )
        _safe_heartbeat({"operation": "ltm_save"})

    @activity.defn(name="crewai_ltm_load")
    async def ltm_load(self, input: LTMLoadInput) -> LTMLoadOutput:
        """Load data from long-term memory storage.

        Args:
            input: Contains task_description, latest_n, db_path

        Returns:
            LTMLoadOutput with list of results
        """
        storage = self._config.get_ltm_storage(input.db_path)
        results = await storage.aload(
            task_description=input.task_description,
            latest_n=input.latest_n,
        )
        _safe_heartbeat({"operation": "ltm_load"})
        return LTMLoadOutput(results=results or [])

    @activity.defn(name="crewai_ltm_reset")
    async def ltm_reset(self, input: LTMResetInput) -> None:
        """Reset long-term memory storage.

        Args:
            input: Contains db_path
        """
        storage = self._config.get_ltm_storage(input.db_path)
        await storage.areset()
        _safe_heartbeat({"operation": "ltm_reset"})

    # =========================================================================
    # Knowledge Activities (Phase 4)
    # =========================================================================

    @activity.defn(name="crewai_knowledge_search")
    async def knowledge_search(
        self, input: KnowledgeSearchInput
    ) -> KnowledgeSearchOutput:
        """Search the knowledge base.

        Args:
            input: Contains query, limit, metadata_filter, score_threshold, collection_name

        Returns:
            KnowledgeSearchOutput with list of results
        """
        storage = self._config.get_knowledge_storage(input.collection_name)
        results = await storage.asearch(
            query=input.query,
            limit=input.limit,
            metadata_filter=input.metadata_filter,
            score_threshold=input.score_threshold,
        )
        _safe_heartbeat({"operation": "knowledge_search"})
        # Convert SearchResult objects to dicts for serialization
        return KnowledgeSearchOutput(
            results=[
                r if isinstance(r, dict) else {"content": str(r), "score": 0.0}
                for r in results
            ]
        )

    @activity.defn(name="crewai_knowledge_save")
    async def knowledge_save(self, input: KnowledgeSaveInput) -> None:
        """Save documents to the knowledge base.

        Args:
            input: Contains documents and collection_name
        """
        storage = self._config.get_knowledge_storage(input.collection_name)
        await storage.asave(input.documents)
        _safe_heartbeat({"operation": "knowledge_save"})

    @activity.defn(name="crewai_knowledge_reset")
    async def knowledge_reset(self, input: KnowledgeResetInput) -> None:
        """Reset the knowledge base.

        Args:
            input: Contains collection_name
        """
        storage = self._config.get_knowledge_storage(input.collection_name)
        await storage.areset()
        _safe_heartbeat({"operation": "knowledge_reset"})
