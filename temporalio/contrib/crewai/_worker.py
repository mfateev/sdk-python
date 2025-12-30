"""Worker configuration for CrewAI activities."""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from temporalio.common import RetryPolicy
from temporalio.workflow import ActivityCancellationType

if TYPE_CHECKING:
    from ._activities import CrewAIActivities


@dataclass
class LLMActivityConfig:
    """Configuration for LLM activity execution.

    This controls how LLM calls are executed as Temporal activities,
    including timeouts, retries, and task queue routing.
    """

    start_to_close_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=60))
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
class CrewAIActivityConfig:
    """Configuration for all CrewAI activities.

    This is the main configuration object passed to crewai_activities().
    It provides factory functions and execution configuration for all
    activity types.

    Example:
        from crewai.llm import LLM

        config = CrewAIActivityConfig(
            llm_factory=lambda model: LLM(model=model),
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

    # Future phases will add:
    # tool_registry: dict[str, Callable] | None = None  # Phase 2
    # storage_factory: Callable[[str], Any] | None = None  # Phase 3
    # ltm_storage_factory: Callable[[str | None], Any] | None = None  # Phase 3
    # knowledge_storage_factory: Callable[[str | None], Any] | None = None  # Phase 4


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
        # Future phases will add:
        # instance.tool_call,  # Phase 2
        # instance.memory_save,  # Phase 3
        # instance.memory_search,  # Phase 3
        # instance.memory_reset,  # Phase 3
        # instance.ltm_save,  # Phase 3
        # instance.ltm_load,  # Phase 3
        # instance.ltm_reset,  # Phase 3
        # instance.knowledge_search,  # Phase 4
        # instance.knowledge_save,  # Phase 4
        # instance.knowledge_reset,  # Phase 4
    ]
