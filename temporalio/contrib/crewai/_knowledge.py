"""Knowledge storage stub for CrewAI Temporal integration.

This module provides a stub implementation of CrewAI's KnowledgeStorage interface
that routes all operations through Temporal activities. This ensures that
knowledge operations are durable and survive worker restarts.
"""

from datetime import timedelta
from typing import Any

from temporalio import workflow

from ._models import (
    KnowledgeResetInput,
    KnowledgeSaveInput,
    KnowledgeSearchInput,
    KnowledgeSearchOutput,
)


class _KnowledgeStorageStub:
    """Stub implementation of KnowledgeStorage that routes to activities.

    This class implements the same interface as crewai.knowledge.storage.KnowledgeStorage
    but executes all operations via Temporal activities for durability.

    The stub must be used within a Temporal workflow context.

    Example:
        @workflow.defn
        class MyCrewWorkflow:
            @workflow.run
            async def run(self):
                storage = _KnowledgeStorageStub(
                    collection_name="my_knowledge",
                    start_to_close_timeout=timedelta(seconds=30),
                )
                # Use storage in crew/agent configuration
    """

    _is_temporal_knowledge_stub: bool = True

    def __init__(
        self,
        collection_name: str | None = None,
        *,
        start_to_close_timeout: timedelta = timedelta(seconds=30),
        task_queue: str | None = None,
    ):
        """Initialize the knowledge storage stub.

        Args:
            collection_name: Name of the knowledge collection
            start_to_close_timeout: Timeout for activity execution
            task_queue: Optional task queue for activities
        """
        self._collection_name = collection_name
        self._start_to_close_timeout = start_to_close_timeout
        self._task_queue = task_queue

    def _get_activity_options(self) -> dict[str, Any]:
        """Build activity options dict."""
        options: dict[str, Any] = {
            "start_to_close_timeout": self._start_to_close_timeout,
        }
        if self._task_queue:
            options["task_queue"] = self._task_queue
        return options

    def search(
        self,
        query: list[str],
        limit: int = 5,
        metadata_filter: dict[str, Any] | None = None,
        score_threshold: float = 0.6,
    ) -> list[Any]:
        """Synchronous search is not supported in Temporal workflows.

        Use asearch instead. KnowledgeStorage.search is synchronous but we need
        async for activities.
        """
        raise NotImplementedError(
            "Synchronous search() is not supported in Temporal workflows. "
            "Use asearch() instead."
        )

    async def asearch(
        self,
        query: list[str],
        limit: int = 5,
        metadata_filter: dict[str, Any] | None = None,
        score_threshold: float = 0.6,
    ) -> list[Any]:
        """Search for documents in the knowledge base asynchronously via activity.

        Args:
            query: List of query strings
            limit: Maximum number of results
            metadata_filter: Optional metadata filter
            score_threshold: Minimum similarity score

        Returns:
            List of search results
        """
        result: KnowledgeSearchOutput = await workflow.execute_activity(
            "crewai_knowledge_search",
            KnowledgeSearchInput(
                query=query,
                limit=limit,
                metadata_filter=metadata_filter,
                score_threshold=score_threshold,
                collection_name=self._collection_name,
            ),
            **self._get_activity_options(),
        )
        return result.results

    def save(self, documents: list[str]) -> None:
        """Synchronous save is not supported in Temporal workflows."""
        raise NotImplementedError(
            "Synchronous save() is not supported in Temporal workflows. "
            "Use asave() instead."
        )

    async def asave(self, documents: list[str]) -> None:
        """Save documents to the knowledge base asynchronously via activity.

        Args:
            documents: List of document strings to save
        """
        await workflow.execute_activity(
            "crewai_knowledge_save",
            KnowledgeSaveInput(
                documents=documents,
                collection_name=self._collection_name,
            ),
            **self._get_activity_options(),
        )

    def reset(self) -> None:
        """Synchronous reset is not supported in Temporal workflows."""
        raise NotImplementedError(
            "Synchronous reset() is not supported in Temporal workflows. "
            "Use areset() instead."
        )

    async def areset(self) -> None:
        """Reset the knowledge base asynchronously via activity."""
        await workflow.execute_activity(
            "crewai_knowledge_reset",
            KnowledgeResetInput(collection_name=self._collection_name),
            **self._get_activity_options(),
        )


def knowledge_storage_stub(
    collection_name: str | None = None,
    *,
    start_to_close_timeout: timedelta = timedelta(seconds=30),
    task_queue: str | None = None,
) -> _KnowledgeStorageStub:
    """Create a knowledge storage stub.

    This creates a storage stub for knowledge that routes operations
    through Temporal activities.

    Args:
        collection_name: Name of the knowledge collection
        start_to_close_timeout: Timeout for activity execution
        task_queue: Optional task queue for activities

    Returns:
        A KnowledgeStorage-compatible stub

    Example:
        @workflow.defn
        class MyCrewWorkflow:
            @workflow.run
            async def run(self):
                storage = knowledge_storage_stub(collection_name="docs")
                # Use with Knowledge sources
    """
    return _KnowledgeStorageStub(
        collection_name=collection_name,
        start_to_close_timeout=start_to_close_timeout,
        task_queue=task_queue,
    )


def _is_temporal_knowledge_stub(obj: Any) -> bool:
    """Check if an object is a Temporal knowledge storage stub.

    This is used by validation logic to ensure knowledge storage
    in crews is backed by Temporal activities.

    Args:
        obj: Object to check

    Returns:
        True if the object is a Temporal knowledge stub
    """
    return getattr(obj, "_is_temporal_knowledge_stub", False)
