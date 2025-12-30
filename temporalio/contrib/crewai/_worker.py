"""Worker configuration for CrewAI activities."""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from temporalio.common import RetryPolicy
from temporalio.workflow import ActivityCancellationType

if TYPE_CHECKING:
    pass


@dataclass
class LLMActivityConfig:
    """Configuration for LLM activity execution.

    This controls how LLM calls are executed as Temporal activities,
    including timeouts, retries, and task queue routing.
    """

    start_to_close_timeout: timedelta = field(
        default_factory=lambda: timedelta(seconds=60)
    )
    """Maximum time for the LLM call to complete.

    Should be set based on expected LLM response time. Consider that
    complex prompts or large context windows may take longer.
    """

    schedule_to_close_timeout: timedelta | None = None
    """Maximum time from scheduling to completion.

    If set, this overrides start_to_close_timeout for overall deadline.
    """

    retry_policy: RetryPolicy | None = None
    """Retry policy for failed LLM calls.

    Consider using exponential backoff for rate limit errors:
        RetryPolicy(
            initial_interval=timedelta(seconds=1),
            maximum_interval=timedelta(seconds=60),
            backoff_coefficient=2.0,
            maximum_attempts=3,
        )
    """

    heartbeat_timeout: timedelta | None = None
    """Timeout for heartbeats during long-running LLM calls.

    If set, the activity must heartbeat more frequently than this interval.
    Useful for detecting stuck LLM calls.
    """

    task_queue: str | None = None
    """Specific task queue for LLM activities.

    If None, uses the workflow's task queue. Set this to route LLM
    activities to specialized workers (e.g., with API keys configured).
    """

    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL
    """How activity handles workflow cancellation."""


@dataclass
class ToolActivityConfig:
    """Configuration for tool activity execution.

    This controls how tool calls are executed as Temporal activities,
    including timeouts, retries, and task queue routing.

    Example:
        tool = activity_as_tool(
            my_activity,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
    """

    start_to_close_timeout: timedelta = field(
        default_factory=lambda: timedelta(seconds=60)
    )
    """Maximum time for the tool to complete."""

    schedule_to_close_timeout: timedelta | None = None
    """Maximum time from scheduling to completion."""

    retry_policy: RetryPolicy | None = None
    """Retry policy for failed tool calls."""

    heartbeat_timeout: timedelta | None = None
    """Heartbeat timeout for long-running tools."""

    task_queue: str | None = None
    """Task queue for tool activities. If None, uses workflow's queue."""

    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL
    """How activity handles workflow cancellation."""


@dataclass
class CrewAIActivityConfig:
    """Configuration for all CrewAI activities.

    This is the main configuration object passed to crewai_activities().
    It provides factory functions and execution configuration for all
    activity types.

    Example:
        from crewai.llm import LLM
        from crewai.memory.storage.rag_storage import RAGStorage
        from crewai.memory.storage.ltm_sqlite_storage import LTMSQLiteStorage

        config = CrewAIActivityConfig(
            llm_factory=lambda model: LLM(model=model),
            rag_storage_factory=lambda storage_type: RAGStorage(type=storage_type),
            ltm_storage_factory=lambda db_path: LTMSQLiteStorage(db_path=db_path),
            llm_activity_config=LLMActivityConfig(
                start_to_close_timeout=timedelta(seconds=120),
                retry_policy=RetryPolicy(maximum_attempts=3),
            ),
        )

        worker = Worker(
            client,
            task_queue="crewai",
            workflows=[MyCrewWorkflow],
            activities=crewai_activities(config),
        )
    """

    llm_factory: Callable[[str], Any]
    """Factory function that creates an LLM instance from a model name.

    This should return an object with an async `acall()` method matching
    CrewAI's LLM interface.

    Example:
        lambda model: LLM(model=model)
    """

    llm_activity_config: LLMActivityConfig = field(default_factory=LLMActivityConfig)
    """Activity execution configuration for LLM calls."""

    # Phase 3: Memory storage factories
    rag_storage_factory: Callable[[str], Any] | None = None
    """Factory function that creates a RAGStorage instance from a storage type.

    The storage_type will be "short_term" or "entity".

    Example:
        lambda storage_type: RAGStorage(type=storage_type)
    """

    ltm_storage_factory: Callable[[str | None], Any] | None = None
    """Factory function that creates an LTMSQLiteStorage instance.

    The db_path parameter may be None to use the default path.

    Example:
        lambda db_path: LTMSQLiteStorage(db_path=db_path)
    """

    # Phase 4: Knowledge storage factory
    knowledge_storage_factory: Callable[[str | None], Any] | None = None
    """Factory function that creates a KnowledgeStorage instance.

    The collection_name parameter may be None for the default collection.

    Example:
        lambda name: KnowledgeStorage(collection_name=name)
    """

    def get_rag_storage(self, storage_type: str) -> Any:
        """Get a RAGStorage instance for the given storage type.

        Args:
            storage_type: Type of memory ("short_term" or "entity")

        Returns:
            RAGStorage instance

        Raises:
            ValueError: If rag_storage_factory is not configured
        """
        if self.rag_storage_factory is None:
            raise ValueError(
                "rag_storage_factory must be configured to use memory activities. "
                "Example: rag_storage_factory=lambda t: RAGStorage(type=t)"
            )
        return self.rag_storage_factory(storage_type)

    def get_ltm_storage(self, db_path: str | None) -> Any:
        """Get an LTMSQLiteStorage instance.

        Args:
            db_path: Optional path to the SQLite database

        Returns:
            LTMSQLiteStorage instance

        Raises:
            ValueError: If ltm_storage_factory is not configured
        """
        if self.ltm_storage_factory is None:
            raise ValueError(
                "ltm_storage_factory must be configured to use LTM activities. "
                "Example: ltm_storage_factory=lambda p: LTMSQLiteStorage(db_path=p)"
            )
        return self.ltm_storage_factory(db_path)

    def get_knowledge_storage(self, collection_name: str | None) -> Any:
        """Get a KnowledgeStorage instance.

        Args:
            collection_name: Optional name of the knowledge collection

        Returns:
            KnowledgeStorage instance

        Raises:
            ValueError: If knowledge_storage_factory is not configured
        """
        if self.knowledge_storage_factory is None:
            raise ValueError(
                "knowledge_storage_factory must be configured to use knowledge activities. "
                "Example: knowledge_storage_factory=lambda n: KnowledgeStorage(collection_name=n)"
            )
        return self.knowledge_storage_factory(collection_name)


def crewai_activities(config: CrewAIActivityConfig) -> list[Callable]:
    """Get all CrewAI activities configured with the given config.

    This function returns a list of activity callables that can be passed
    to a Temporal Worker. The activities are configured with the provided
    factories and settings.

    Args:
        config: Configuration for the activities

    Returns:
        List of activity callables for Worker registration

    Example:
        config = CrewAIActivityConfig(
            llm_factory=lambda m: LLM(model=m),
        )

        worker = Worker(
            client,
            task_queue="crewai",
            workflows=[MyWorkflow],
            activities=crewai_activities(config),
        )
    """
    # Import here to avoid circular dependency
    from ._activities import CrewAIActivities

    instance = CrewAIActivities(config)
    return [
        instance.llm_call,
        # Phase 3: Memory activities
        instance.memory_save,
        instance.memory_search,
        instance.memory_reset,
        instance.ltm_save,
        instance.ltm_load,
        instance.ltm_reset,
        # Phase 4: Knowledge activities
        instance.knowledge_search,
        instance.knowledge_save,
        instance.knowledge_reset,
    ]
