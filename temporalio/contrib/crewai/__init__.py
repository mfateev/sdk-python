"""Support for running CrewAI crews as part of Temporal workflows.

This module provides integration between CrewAI and Temporal workflows,
enabling durable execution of AI agent crews with full observability.

.. warning::
    This module is experimental and may change in future versions.

Example usage:

    from temporalio.contrib.crewai import (
        llm_stub,
        crewai_activities,
        CrewAIActivityConfig,
        crewai_data_converter,
    )

    # Worker setup
    config = CrewAIActivityConfig(
        llm_factory=lambda model: LLM(model=model),
    )

    worker = Worker(
        client,
        task_queue="crewai",
        workflows=[MyCrewWorkflow],
        activities=crewai_activities(config),
    )

    # In workflow code
    @workflow.defn
    class MyCrewWorkflow:
        @workflow.run
        async def run(self):
            agent = Agent(
                role="Researcher",
                llm=llm_stub("gpt-4"),  # Use stub instead of LLM
                ...
            )
            crew = Crew(agents=[agent], tasks=[...])
            return await crew.akickoff()
"""

from ._converter import (
    CrewAIPayloadConverter,
    crewai_data_converter,
    make_crewai_data_converter,
)
from ._llm import _LLMStub, llm_stub
from ._tools import activity_as_tool
from ._worker import (
    CrewAIActivityConfig,
    LLMActivityConfig,
    ToolActivityConfig,
    crewai_activities,
)

__all__ = [
    # LLM stub for workflow-side calls
    "llm_stub",
    "_LLMStub",
    # Tool wrapper for activities
    "activity_as_tool",
    # Configuration
    "CrewAIActivityConfig",
    "LLMActivityConfig",
    "ToolActivityConfig",
    # Worker setup
    "crewai_activities",
    # Data conversion
    "crewai_data_converter",
    "make_crewai_data_converter",
    "CrewAIPayloadConverter",
]
