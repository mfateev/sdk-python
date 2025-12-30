"""Crew runner with validation for Temporal workflows.

This module provides TemporalCrewRunner, a validated wrapper for running
CrewAI crews in Temporal workflows. It validates that all crew components
are properly configured for durable execution.
"""

from typing import Any

from ._knowledge import _is_temporal_knowledge_stub
from ._llm import _is_llm_stub
from ._memory import _is_temporal_memory_stub
from ._utils import _is_temporal_tool


class CrewValidationError(Exception):
    """Raised when crew configuration is invalid for Temporal workflows."""

    pass


class TemporalCrewRunner:
    """Validated runner for CrewAI crews in Temporal workflows.

    This class wraps a CrewAI Crew and validates that all components are
    properly configured for durable execution via Temporal activities.

    Validation checks:
    - All agents use llm_stub() for their LLM
    - All tools use activity_as_tool()
    - Memory uses Temporal storage stubs if enabled
    - Knowledge uses Temporal storage stubs if configured
    - Tasks don't require human input
    - Planning uses llm_stub() if enabled
    - Rate limiting is not configured (incompatible with Temporal)

    Example:
        @workflow.defn
        class MyCrewWorkflow:
            @workflow.run
            async def run(self, task_input: str):
                agent = Agent(
                    role="Researcher",
                    llm=llm_stub("gpt-4"),
                    tools=[activity_as_tool(search_web)],
                )
                task = Task(
                    description=task_input,
                    agent=agent,
                )
                crew = Crew(agents=[agent], tasks=[task])

                runner = TemporalCrewRunner(crew)
                return await runner.kickoff()
    """

    def __init__(self, crew: Any, *, skip_validation: bool = False):
        """Initialize the runner with a crew.

        Args:
            crew: The CrewAI Crew instance to run
            skip_validation: If True, skip validation (for testing only)

        Raises:
            CrewValidationError: If the crew is not properly configured
        """
        self._crew = crew
        if not skip_validation:
            self._validate()

    def _validate(self) -> None:
        """Validate the crew configuration.

        Raises:
            CrewValidationError: If validation fails
        """
        errors: list[str] = []

        # Validate agents
        for i, agent in enumerate(self._crew.agents):
            agent_errors = self._validate_agent(agent, i)
            errors.extend(agent_errors)

        # Validate tasks
        for i, task in enumerate(self._crew.tasks):
            task_errors = self._validate_task(task, i)
            errors.extend(task_errors)

        # Validate crew-level settings
        crew_errors = self._validate_crew_settings()
        errors.extend(crew_errors)

        if errors:
            error_msg = "Crew validation failed:\n" + "\n".join(
                f"  - {e}" for e in errors
            )
            raise CrewValidationError(error_msg)

    def _validate_agent(self, agent: Any, index: int) -> list[str]:
        """Validate a single agent.

        Args:
            agent: The agent to validate
            index: Agent index for error messages

        Returns:
            List of validation error messages
        """
        errors: list[str] = []
        agent_name = getattr(agent, "role", f"Agent {index}")

        # Check LLM is a stub
        llm = getattr(agent, "llm", None)
        if llm is not None and not _is_llm_stub(llm):
            errors.append(
                f"Agent '{agent_name}' must use llm_stub() instead of a direct LLM. "
                f"Example: llm=llm_stub('gpt-4')"
            )

        # Check tools are activity-backed
        tools = getattr(agent, "tools", None) or []
        for j, tool in enumerate(tools):
            if not _is_temporal_tool(tool):
                tool_name = getattr(tool, "name", f"tool {j}")
                errors.append(
                    f"Agent '{agent_name}' tool '{tool_name}' must use activity_as_tool(). "
                    f"Example: tools=[activity_as_tool(my_activity)]"
                )

        # Check max_rpm is not set
        max_rpm = getattr(agent, "max_rpm", None)
        if max_rpm is not None:
            errors.append(
                f"Agent '{agent_name}' must not set max_rpm. "
                f"Rate limiting is incompatible with Temporal workflows."
            )

        return errors

    def _validate_task(self, task: Any, index: int) -> list[str]:
        """Validate a single task.

        Args:
            task: The task to validate
            index: Task index for error messages

        Returns:
            List of validation error messages
        """
        errors: list[str] = []
        task_name = getattr(task, "description", f"Task {index}")[:50]

        # Check human_input is not enabled
        human_input = getattr(task, "human_input", False)
        if human_input:
            errors.append(
                f"Task '{task_name}...' must not set human_input=True. "
                f"Human input is not supported in Temporal workflows."
            )

        return errors

    def _validate_crew_settings(self) -> list[str]:
        """Validate crew-level settings.

        Returns:
            List of validation error messages
        """
        errors: list[str] = []

        # Check max_rpm is not set at crew level
        max_rpm = getattr(self._crew, "max_rpm", None)
        if max_rpm is not None:
            errors.append(
                "Crew must not set max_rpm. "
                "Rate limiting is incompatible with Temporal workflows."
            )

        # Check planning LLM if planning is enabled
        planning = getattr(self._crew, "planning", False)
        if planning:
            planning_llm = getattr(self._crew, "planning_llm", None)
            if planning_llm is not None and not _is_llm_stub(planning_llm):
                errors.append(
                    "Crew planning_llm must use llm_stub() if planning is enabled. "
                    "Example: planning_llm=llm_stub('gpt-4')"
                )

        # Check memory storage if memory is enabled
        memory = getattr(self._crew, "memory", False)
        if memory:
            # Check short-term memory
            short_term_memory = getattr(self._crew, "short_term_memory", None)
            if short_term_memory is not None:
                storage = getattr(short_term_memory, "storage", None)
                if storage is not None and not _is_temporal_memory_stub(storage):
                    errors.append(
                        "Crew short_term_memory storage must use short_term_memory_stub(). "
                        "Example: ShortTermMemory(storage=short_term_memory_stub())"
                    )

            # Check long-term memory
            long_term_memory = getattr(self._crew, "long_term_memory", None)
            if long_term_memory is not None:
                storage = getattr(long_term_memory, "storage", None)
                if storage is not None and not _is_temporal_memory_stub(storage):
                    errors.append(
                        "Crew long_term_memory storage must use long_term_memory_stub(). "
                        "Example: LongTermMemory(storage=long_term_memory_stub())"
                    )

            # Check entity memory
            entity_memory = getattr(self._crew, "entity_memory", None)
            if entity_memory is not None:
                storage = getattr(entity_memory, "storage", None)
                if storage is not None and not _is_temporal_memory_stub(storage):
                    errors.append(
                        "Crew entity_memory storage must use entity_memory_stub(). "
                        "Example: EntityMemory(storage=entity_memory_stub())"
                    )

        # Check knowledge storage
        knowledge_sources = getattr(self._crew, "knowledge_sources", None)
        if knowledge_sources:
            for i, source in enumerate(knowledge_sources):
                storage = getattr(source, "storage", None)
                if storage is not None and not _is_temporal_knowledge_stub(storage):
                    errors.append(
                        f"Knowledge source {i} storage must use knowledge_storage_stub(). "
                        "Example: storage=knowledge_storage_stub()"
                    )

        return errors

    async def kickoff(self, inputs: dict[str, Any] | None = None) -> Any:
        """Run the crew asynchronously.

        This is the main entry point for running the crew in a Temporal workflow.
        It calls crew.akickoff() which properly uses async execution.

        Args:
            inputs: Optional inputs to pass to the crew

        Returns:
            The crew output (CrewOutput)
        """
        if inputs:
            return await self._crew.akickoff(inputs=inputs)
        return await self._crew.akickoff()

    async def kickoff_for_each(self, inputs: list[dict[str, Any]]) -> list[Any]:
        """Run the crew for multiple input sets.

        Args:
            inputs: List of input dicts to process

        Returns:
            List of crew outputs
        """
        return await self._crew.akickoff_for_each(inputs=inputs)
