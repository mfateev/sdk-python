"""Memory storage stubs for CrewAI Temporal integration.

This module provides stub implementations of CrewAI's storage interfaces
that route all operations through Temporal activities. This ensures that
memory operations are durable and survive worker restarts.

The stubs implement the same interface as the real storage classes but
execute operations via workflow.execute_activity().
"""

from datetime import timedelta
from typing import Any

from temporalio import workflow

from ._models import (
    LTMLoadInput,
    LTMLoadOutput,
    LTMResetInput,
    LTMSaveInput,
    MemoryResetInput,
    MemorySaveInput,
    MemorySearchInput,
    MemorySearchOutput,
)


class _RAGStorageStub:
    """Stub implementation of RAGStorage that routes to activities.

    This class implements the same interface as crewai.memory.storage.RAGStorage
    but executes all operations via Temporal activities for durability.

    The stub must be used within a Temporal workflow context.

    Example:
        @workflow.defn
        class MyCrewWorkflow:
            @workflow.run
            async def run(self):
                storage = _RAGStorageStub(
                    storage_type="short_term",
                    start_to_close_timeout=timedelta(seconds=30),
                )
                # Use storage in crew configuration
    """

    _is_temporal_memory_stub: bool = True

    def __init__(
        self,
        storage_type: str,
        *,
        start_to_close_timeout: timedelta = timedelta(seconds=30),
        task_queue: str | None = None,
    ):
        """Initialize the RAG storage stub.

        Args:
            storage_type: Type of memory ("short_term" or "entity")
            start_to_close_timeout: Timeout for activity execution
            task_queue: Optional task queue for activities
        """
        self._storage_type = storage_type
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

    def save(self, value: Any, metadata: dict[str, Any]) -> None:
        """Synchronous save is not supported in Temporal workflows.

        RAGStorage.save is synchronous but we need async for activities.
        CrewAI should use asave when memory=True.
        """
        raise NotImplementedError(
            "Synchronous save() is not supported in Temporal workflows. "
            "Use asave() instead. Ensure your crew uses async methods."
        )

    async def asave(self, value: Any, metadata: dict[str, Any]) -> None:
        """Save a value to storage asynchronously via activity.

        Args:
            value: The value to save
            metadata: Metadata associated with the value
        """
        await workflow.execute_activity(
            "crewai_memory_save",
            MemorySaveInput(
                storage_type=self._storage_type,
                value=value,
                metadata=metadata,
            ),
            **self._get_activity_options(),
        )

    def search(
        self,
        query: str,
        limit: int = 5,
        filter: dict[str, Any] | None = None,
        score_threshold: float = 0.6,
    ) -> list[Any]:
        """Synchronous search is not supported in Temporal workflows."""
        raise NotImplementedError(
            "Synchronous search() is not supported in Temporal workflows. "
            "Use asearch() instead. Ensure your crew uses async methods."
        )

    async def asearch(
        self,
        query: str,
        limit: int = 5,
        filter: dict[str, Any] | None = None,
        score_threshold: float = 0.6,
    ) -> list[Any]:
        """Search for matching entries asynchronously via activity.

        Args:
            query: The search query
            limit: Maximum number of results
            filter: Optional metadata filter
            score_threshold: Minimum similarity score

        Returns:
            List of matching entries
        """
        result: MemorySearchOutput = await workflow.execute_activity(
            "crewai_memory_search",
            MemorySearchInput(
                storage_type=self._storage_type,
                query=query,
                limit=limit,
                filter=filter,
                score_threshold=score_threshold,
            ),
            **self._get_activity_options(),
        )
        return result.results

    def reset(self) -> None:
        """Synchronous reset is not supported in Temporal workflows."""
        raise NotImplementedError(
            "Synchronous reset() is not supported in Temporal workflows. "
            "Use areset() instead."
        )

    async def areset(self) -> None:
        """Reset the storage asynchronously via activity."""
        await workflow.execute_activity(
            "crewai_memory_reset",
            MemoryResetInput(storage_type=self._storage_type),
            **self._get_activity_options(),
        )


class _LTMStorageStub:
    """Stub implementation of LTMSQLiteStorage that routes to activities.

    This class implements the same interface as crewai.memory.storage.LTMSQLiteStorage
    but executes all operations via Temporal activities for durability.

    The stub must be used within a Temporal workflow context.

    Example:
        @workflow.defn
        class MyCrewWorkflow:
            @workflow.run
            async def run(self):
                storage = _LTMStorageStub(
                    start_to_close_timeout=timedelta(seconds=30),
                )
                # Use storage in crew configuration
    """

    _is_temporal_memory_stub: bool = True

    def __init__(
        self,
        db_path: str | None = None,
        *,
        start_to_close_timeout: timedelta = timedelta(seconds=30),
        task_queue: str | None = None,
    ):
        """Initialize the LTM storage stub.

        Args:
            db_path: Optional path to the SQLite database (used by activity)
            start_to_close_timeout: Timeout for activity execution
            task_queue: Optional task queue for activities
        """
        self._db_path = db_path
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

    def save(
        self,
        task_description: str,
        metadata: dict[str, Any],
        datetime: str,
        score: int | float,
    ) -> None:
        """Synchronous save is not supported in Temporal workflows."""
        raise NotImplementedError(
            "Synchronous save() is not supported in Temporal workflows. "
            "Use asave() instead."
        )

    async def asave(
        self,
        task_description: str,
        metadata: dict[str, Any],
        datetime: str,
        score: int | float,
    ) -> None:
        """Save data to LTM asynchronously via activity.

        Args:
            task_description: Description of the task
            metadata: Metadata associated with the memory
            datetime: ISO format datetime string
            score: Quality score of the memory
        """
        await workflow.execute_activity(
            "crewai_ltm_save",
            LTMSaveInput(
                task_description=task_description,
                metadata=metadata,
                datetime=datetime,
                score=float(score),
                db_path=self._db_path,
            ),
            **self._get_activity_options(),
        )

    def load(self, task_description: str, latest_n: int) -> list[dict[str, Any]] | None:
        """Synchronous load is not supported in Temporal workflows."""
        raise NotImplementedError(
            "Synchronous load() is not supported in Temporal workflows. "
            "Use aload() instead."
        )

    async def aload(
        self, task_description: str, latest_n: int
    ) -> list[dict[str, Any]] | None:
        """Query LTM asynchronously via activity.

        Args:
            task_description: Description of the task to search for
            latest_n: Maximum number of results to return

        Returns:
            List of matching memory entries or None
        """
        result: LTMLoadOutput = await workflow.execute_activity(
            "crewai_ltm_load",
            LTMLoadInput(
                task_description=task_description,
                latest_n=latest_n,
                db_path=self._db_path,
            ),
            **self._get_activity_options(),
        )
        return result.results if result.results else None

    def reset(self) -> None:
        """Synchronous reset is not supported in Temporal workflows."""
        raise NotImplementedError(
            "Synchronous reset() is not supported in Temporal workflows. "
            "Use areset() instead."
        )

    async def areset(self) -> None:
        """Reset LTM asynchronously via activity."""
        await workflow.execute_activity(
            "crewai_ltm_reset",
            LTMResetInput(db_path=self._db_path),
            **self._get_activity_options(),
        )


def short_term_memory_stub(
    *,
    start_to_close_timeout: timedelta = timedelta(seconds=30),
    task_queue: str | None = None,
) -> _RAGStorageStub:
    """Create a short-term memory storage stub.

    This creates a storage stub for short-term memory that routes
    operations through Temporal activities.

    Args:
        start_to_close_timeout: Timeout for activity execution
        task_queue: Optional task queue for activities

    Returns:
        A RAGStorage-compatible stub for short-term memory

    Example:
        @workflow.defn
        class MyCrewWorkflow:
            @workflow.run
            async def run(self):
                from crewai.memory.short_term import ShortTermMemory

                storage = short_term_memory_stub()
                memory = ShortTermMemory(storage=storage)
                crew = Crew(memory=memory, ...)
    """
    return _RAGStorageStub(
        storage_type="short_term",
        start_to_close_timeout=start_to_close_timeout,
        task_queue=task_queue,
    )


def entity_memory_stub(
    *,
    start_to_close_timeout: timedelta = timedelta(seconds=30),
    task_queue: str | None = None,
) -> _RAGStorageStub:
    """Create an entity memory storage stub.

    This creates a storage stub for entity memory that routes
    operations through Temporal activities.

    Args:
        start_to_close_timeout: Timeout for activity execution
        task_queue: Optional task queue for activities

    Returns:
        A RAGStorage-compatible stub for entity memory

    Example:
        @workflow.defn
        class MyCrewWorkflow:
            @workflow.run
            async def run(self):
                from crewai.memory.entity import EntityMemory

                storage = entity_memory_stub()
                memory = EntityMemory(storage=storage)
    """
    return _RAGStorageStub(
        storage_type="entity",
        start_to_close_timeout=start_to_close_timeout,
        task_queue=task_queue,
    )


def long_term_memory_stub(
    db_path: str | None = None,
    *,
    start_to_close_timeout: timedelta = timedelta(seconds=30),
    task_queue: str | None = None,
) -> _LTMStorageStub:
    """Create a long-term memory storage stub.

    This creates a storage stub for long-term memory that routes
    operations through Temporal activities.

    Args:
        db_path: Optional path to the SQLite database
        start_to_close_timeout: Timeout for activity execution
        task_queue: Optional task queue for activities

    Returns:
        A LTMSQLiteStorage-compatible stub for long-term memory

    Example:
        @workflow.defn
        class MyCrewWorkflow:
            @workflow.run
            async def run(self):
                from crewai.memory.long_term import LongTermMemory

                storage = long_term_memory_stub()
                memory = LongTermMemory(storage=storage)
    """
    return _LTMStorageStub(
        db_path=db_path,
        start_to_close_timeout=start_to_close_timeout,
        task_queue=task_queue,
    )


def _is_temporal_memory_stub(obj: Any) -> bool:
    """Check if an object is a Temporal memory stub.

    This is used by validation logic to ensure memory storage
    in crews is backed by Temporal activities.

    Args:
        obj: Object to check

    Returns:
        True if the object is a Temporal memory stub
    """
    return getattr(obj, "_is_temporal_memory_stub", False)
