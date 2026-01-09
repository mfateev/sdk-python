"""Temporal plugin for CrewAI integration.

This module provides a SimplePlugin implementation that simplifies worker
setup for CrewAI workflows.
"""

import dataclasses
from collections.abc import Callable, Sequence
from typing import Any

from temporalio.plugin import SimplePlugin
from temporalio.worker import WorkflowRunner
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

from ._converter import crewai_data_converter
from ._worker import CrewAIActivityConfig, crewai_activities


class CrewAIPlugin(SimplePlugin):
    """Temporal plugin for running CrewAI crews in workflows.

    .. warning::
        This class is experimental and may change in future versions.
        Use with caution in production environments.

    This plugin provides seamless integration between CrewAI and Temporal
    workflows. It automatically configures the necessary data converters
    and activities to enable CrewAI crews to run within Temporal workflows.

    The plugin:
    1. Configures the Pydantic data converter for type-safe serialization
    2. Registers all CrewAI activities (LLM, memory, knowledge)
    3. Configures sandbox passthrough for the crewai module

    Args:
        config: Configuration for CrewAI activities including factories
            for creating LLM instances and storage backends.
        register_activities: Whether to register activities during worker
            execution. Set to False if activities are registered on a
            separate worker.

    Example:
        >>> from temporalio.client import Client
        >>> from temporalio.worker import Worker
        >>> from temporalio.contrib.crewai import CrewAIPlugin, CrewAIActivityConfig
        >>> from crewai.llm import LLM
        >>>
        >>> # Create plugin with configuration
        >>> config = CrewAIActivityConfig(
        ...     llm_factory=lambda model: LLM(model=model),
        ... )
        >>> plugin = CrewAIPlugin(config=config)
        >>>
        >>> # Use with client and worker
        >>> client = await Client.connect(
        ...     "localhost:7233",
        ...     plugins=[plugin]
        ... )
        >>> worker = Worker(
        ...     client,
        ...     task_queue="crewai",
        ...     workflows=[MyCrewWorkflow],
        ... )
    """

    def __init__(
        self,
        config: CrewAIActivityConfig,
        *,
        register_activities: bool = True,
    ) -> None:
        """Initialize the CrewAI plugin.

        Args:
            config: Configuration for CrewAI activities.
            register_activities: Whether to register activities. Defaults to True.
        """
        self._config = config

        def add_activities(
            activities: Sequence[Callable[..., Any]] | None,
        ) -> Sequence[Callable[..., Any]]:
            if not register_activities:
                return activities or []
            return list(activities or []) + crewai_activities(config)

        def workflow_runner(runner: WorkflowRunner | None) -> WorkflowRunner:
            if not runner:
                raise ValueError("No WorkflowRunner provided to the CrewAI plugin.")

            # If in sandbox, configure for CrewAI:
            # 1. Allow open() for prompt file loading (I18N.load_prompts)
            # 2. Pass through crewai module for @lru_cache state sharing
            if isinstance(runner, SandboxedWorkflowRunner):
                restrictions = runner.restrictions
                # Unrestrict open() - CrewAI reads prompt files at Agent init
                restrictions = dataclasses.replace(
                    restrictions,
                    invalid_module_members=restrictions.invalid_module_members.with_child_unrestricted(
                        "__builtins__", "open"
                    ),
                )
                # Pass through crewai for cache sharing
                restrictions = restrictions.with_passthrough_modules("crewai")
                return dataclasses.replace(runner, restrictions=restrictions)
            return runner

        def data_converter(
            converter: Any,  # DataConverter | None
        ) -> Any:  # DataConverter
            # If no converter provided, use our crewai converter
            if converter is None:
                return crewai_data_converter
            # Otherwise return as-is (user may have custom converter)
            return converter

        super().__init__(
            name="CrewAIPlugin",
            data_converter=data_converter,
            activities=add_activities,
            workflow_runner=workflow_runner,
        )
