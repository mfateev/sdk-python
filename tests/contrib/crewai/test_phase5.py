"""Phase 5 Tests: Crew Runner and Validation.

These tests verify the TemporalCrewRunner functionality:
- Crew validation (LLM stubs, tool wrappers, memory stubs, etc.)
- Validation error messages
- Runner execution
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from temporalio.contrib.crewai import (
    knowledge_storage_stub,
    llm_stub,
    long_term_memory_stub,
    short_term_memory_stub,
)
from temporalio.contrib.crewai._llm import _is_llm_stub
from temporalio.contrib.crewai._runner import (
    CrewValidationError,
    TemporalCrewRunner,
)


# =============================================================================
# Unit Tests: _is_llm_stub helper
# =============================================================================


def test_is_llm_stub_returns_true_for_stub():
    """Test _is_llm_stub returns True for LLM stubs."""
    stub = llm_stub("gpt-4")
    assert _is_llm_stub(stub) is True


def test_is_llm_stub_returns_false_for_non_stub():
    """Test _is_llm_stub returns False for non-stub objects."""
    assert _is_llm_stub("not a stub") is False
    assert _is_llm_stub(None) is False
    assert _is_llm_stub({}) is False
    assert _is_llm_stub(MagicMock()) is False


# =============================================================================
# Unit Tests: CrewValidationError
# =============================================================================


def test_crew_validation_error_is_exception():
    """Test CrewValidationError is an Exception."""
    error = CrewValidationError("test error")
    assert isinstance(error, Exception)
    assert str(error) == "test error"


# =============================================================================
# Unit Tests: Agent Validation
# =============================================================================


def test_runner_validates_agent_llm_is_stub():
    """Test runner validates that agent LLM is a stub."""
    mock_agent = MagicMock()
    mock_agent.role = "Researcher"
    mock_agent.llm = MagicMock()  # Not a stub
    mock_agent.tools = []
    mock_agent.max_rpm = None

    mock_crew = MagicMock()
    mock_crew.agents = [mock_agent]
    mock_crew.tasks = []
    mock_crew.max_rpm = None
    mock_crew.planning = False
    mock_crew.memory = False
    mock_crew.knowledge_sources = None

    with pytest.raises(CrewValidationError) as exc_info:
        TemporalCrewRunner(mock_crew)

    assert "must use llm_stub()" in str(exc_info.value)
    assert "Researcher" in str(exc_info.value)


def test_runner_accepts_agent_with_llm_stub():
    """Test runner accepts agent with llm_stub."""
    mock_agent = MagicMock()
    mock_agent.role = "Researcher"
    mock_agent.llm = llm_stub("gpt-4")
    mock_agent.tools = []
    mock_agent.max_rpm = None

    mock_crew = MagicMock()
    mock_crew.agents = [mock_agent]
    mock_crew.tasks = []
    mock_crew.max_rpm = None
    mock_crew.planning = False
    mock_crew.memory = False
    mock_crew.knowledge_sources = None

    # Should not raise
    runner = TemporalCrewRunner(mock_crew)
    assert runner._crew is mock_crew


def test_runner_validates_agent_tools_are_temporal():
    """Test runner validates that agent tools use activity_as_tool."""
    mock_tool = MagicMock()
    mock_tool.name = "invalid_tool"
    # Explicitly set to False - MagicMock returns truthy values by default
    mock_tool._is_temporal_activity_tool = False

    mock_agent = MagicMock()
    mock_agent.role = "Researcher"
    mock_agent.llm = llm_stub("gpt-4")
    mock_agent.tools = [mock_tool]  # Invalid tool
    mock_agent.max_rpm = None

    mock_crew = MagicMock()
    mock_crew.agents = [mock_agent]
    mock_crew.tasks = []
    mock_crew.max_rpm = None
    mock_crew.planning = False
    mock_crew.memory = False
    mock_crew.knowledge_sources = None

    with pytest.raises(CrewValidationError) as exc_info:
        TemporalCrewRunner(mock_crew)

    assert "must use activity_as_tool()" in str(exc_info.value)
    assert "invalid_tool" in str(exc_info.value)


def test_runner_accepts_agent_with_temporal_tools():
    """Test runner accepts agent with activity_as_tool tools."""
    # Create a mock tool that looks like a temporal tool
    mock_tool = MagicMock()
    mock_tool.name = "search_web"
    mock_tool._is_temporal_activity_tool = True  # Mark as temporal tool

    mock_agent = MagicMock()
    mock_agent.role = "Researcher"
    mock_agent.llm = llm_stub("gpt-4")
    mock_agent.tools = [mock_tool]  # Valid temporal tool
    mock_agent.max_rpm = None

    mock_crew = MagicMock()
    mock_crew.agents = [mock_agent]
    mock_crew.tasks = []
    mock_crew.max_rpm = None
    mock_crew.planning = False
    mock_crew.memory = False
    mock_crew.knowledge_sources = None

    # Should not raise
    runner = TemporalCrewRunner(mock_crew)
    assert runner._crew is mock_crew


def test_runner_validates_agent_max_rpm_not_set():
    """Test runner validates that agent max_rpm is not set."""
    mock_agent = MagicMock()
    mock_agent.role = "Researcher"
    mock_agent.llm = llm_stub("gpt-4")
    mock_agent.tools = []
    mock_agent.max_rpm = 10  # Not allowed

    mock_crew = MagicMock()
    mock_crew.agents = [mock_agent]
    mock_crew.tasks = []
    mock_crew.max_rpm = None
    mock_crew.planning = False
    mock_crew.memory = False
    mock_crew.knowledge_sources = None

    with pytest.raises(CrewValidationError) as exc_info:
        TemporalCrewRunner(mock_crew)

    assert "must not set max_rpm" in str(exc_info.value)
    assert "Researcher" in str(exc_info.value)


# =============================================================================
# Unit Tests: Task Validation
# =============================================================================


def test_runner_validates_task_no_human_input():
    """Test runner validates that tasks don't have human_input=True."""
    mock_agent = MagicMock()
    mock_agent.role = "Researcher"
    mock_agent.llm = llm_stub("gpt-4")
    mock_agent.tools = []
    mock_agent.max_rpm = None

    mock_task = MagicMock()
    mock_task.description = "Research the topic"
    mock_task.human_input = True  # Not allowed

    mock_crew = MagicMock()
    mock_crew.agents = [mock_agent]
    mock_crew.tasks = [mock_task]
    mock_crew.max_rpm = None
    mock_crew.planning = False
    mock_crew.memory = False
    mock_crew.knowledge_sources = None

    with pytest.raises(CrewValidationError) as exc_info:
        TemporalCrewRunner(mock_crew)

    assert "must not set human_input=True" in str(exc_info.value)


def test_runner_accepts_task_without_human_input():
    """Test runner accepts tasks without human_input."""
    mock_agent = MagicMock()
    mock_agent.role = "Researcher"
    mock_agent.llm = llm_stub("gpt-4")
    mock_agent.tools = []
    mock_agent.max_rpm = None

    mock_task = MagicMock()
    mock_task.description = "Research the topic"
    mock_task.human_input = False

    mock_crew = MagicMock()
    mock_crew.agents = [mock_agent]
    mock_crew.tasks = [mock_task]
    mock_crew.max_rpm = None
    mock_crew.planning = False
    mock_crew.memory = False
    mock_crew.knowledge_sources = None

    # Should not raise
    runner = TemporalCrewRunner(mock_crew)
    assert runner._crew is mock_crew


# =============================================================================
# Unit Tests: Crew-level Validation
# =============================================================================


def test_runner_validates_crew_max_rpm_not_set():
    """Test runner validates that crew max_rpm is not set."""
    mock_agent = MagicMock()
    mock_agent.role = "Researcher"
    mock_agent.llm = llm_stub("gpt-4")
    mock_agent.tools = []
    mock_agent.max_rpm = None

    mock_crew = MagicMock()
    mock_crew.agents = [mock_agent]
    mock_crew.tasks = []
    mock_crew.max_rpm = 100  # Not allowed
    mock_crew.planning = False
    mock_crew.memory = False
    mock_crew.knowledge_sources = None

    with pytest.raises(CrewValidationError) as exc_info:
        TemporalCrewRunner(mock_crew)

    assert "Crew must not set max_rpm" in str(exc_info.value)


def test_runner_validates_planning_llm_is_stub():
    """Test runner validates that planning_llm is a stub if planning enabled."""
    mock_agent = MagicMock()
    mock_agent.role = "Researcher"
    mock_agent.llm = llm_stub("gpt-4")
    mock_agent.tools = []
    mock_agent.max_rpm = None

    mock_crew = MagicMock()
    mock_crew.agents = [mock_agent]
    mock_crew.tasks = []
    mock_crew.max_rpm = None
    mock_crew.planning = True
    mock_crew.planning_llm = MagicMock()  # Not a stub
    mock_crew.memory = False
    mock_crew.knowledge_sources = None

    with pytest.raises(CrewValidationError) as exc_info:
        TemporalCrewRunner(mock_crew)

    assert "planning_llm must use llm_stub()" in str(exc_info.value)


def test_runner_accepts_planning_with_llm_stub():
    """Test runner accepts planning with llm_stub."""
    mock_agent = MagicMock()
    mock_agent.role = "Researcher"
    mock_agent.llm = llm_stub("gpt-4")
    mock_agent.tools = []
    mock_agent.max_rpm = None

    mock_crew = MagicMock()
    mock_crew.agents = [mock_agent]
    mock_crew.tasks = []
    mock_crew.max_rpm = None
    mock_crew.planning = True
    mock_crew.planning_llm = llm_stub("gpt-4")  # Valid stub
    mock_crew.memory = False
    mock_crew.knowledge_sources = None

    # Should not raise
    runner = TemporalCrewRunner(mock_crew)
    assert runner._crew is mock_crew


# =============================================================================
# Unit Tests: Memory Validation
# =============================================================================


def test_runner_validates_short_term_memory_storage():
    """Test runner validates short_term_memory storage is a stub."""
    mock_agent = MagicMock()
    mock_agent.role = "Researcher"
    mock_agent.llm = llm_stub("gpt-4")
    mock_agent.tools = []
    mock_agent.max_rpm = None

    mock_storage = MagicMock()
    # Explicitly set to False - MagicMock returns truthy values by default
    mock_storage._is_temporal_memory_stub = False

    mock_memory = MagicMock()
    mock_memory.storage = mock_storage

    mock_crew = MagicMock()
    mock_crew.agents = [mock_agent]
    mock_crew.tasks = []
    mock_crew.max_rpm = None
    mock_crew.planning = False
    mock_crew.memory = True
    mock_crew.short_term_memory = mock_memory
    mock_crew.long_term_memory = None
    mock_crew.entity_memory = None
    mock_crew.knowledge_sources = None

    with pytest.raises(CrewValidationError) as exc_info:
        TemporalCrewRunner(mock_crew)

    assert "short_term_memory storage must use short_term_memory_stub()" in str(
        exc_info.value
    )


def test_runner_accepts_short_term_memory_with_stub():
    """Test runner accepts short_term_memory with temporal stub."""
    mock_agent = MagicMock()
    mock_agent.role = "Researcher"
    mock_agent.llm = llm_stub("gpt-4")
    mock_agent.tools = []
    mock_agent.max_rpm = None

    mock_memory = MagicMock()
    mock_memory.storage = short_term_memory_stub()

    mock_crew = MagicMock()
    mock_crew.agents = [mock_agent]
    mock_crew.tasks = []
    mock_crew.max_rpm = None
    mock_crew.planning = False
    mock_crew.memory = True
    mock_crew.short_term_memory = mock_memory
    mock_crew.long_term_memory = None
    mock_crew.entity_memory = None
    mock_crew.knowledge_sources = None

    # Should not raise
    runner = TemporalCrewRunner(mock_crew)
    assert runner._crew is mock_crew


def test_runner_validates_long_term_memory_storage():
    """Test runner validates long_term_memory storage is a stub."""
    mock_agent = MagicMock()
    mock_agent.role = "Researcher"
    mock_agent.llm = llm_stub("gpt-4")
    mock_agent.tools = []
    mock_agent.max_rpm = None

    mock_storage = MagicMock()
    # Explicitly set to False - MagicMock returns truthy values by default
    mock_storage._is_temporal_memory_stub = False

    mock_memory = MagicMock()
    mock_memory.storage = mock_storage

    mock_crew = MagicMock()
    mock_crew.agents = [mock_agent]
    mock_crew.tasks = []
    mock_crew.max_rpm = None
    mock_crew.planning = False
    mock_crew.memory = True
    mock_crew.short_term_memory = None
    mock_crew.long_term_memory = mock_memory
    mock_crew.entity_memory = None
    mock_crew.knowledge_sources = None

    with pytest.raises(CrewValidationError) as exc_info:
        TemporalCrewRunner(mock_crew)

    assert "long_term_memory storage must use long_term_memory_stub()" in str(
        exc_info.value
    )


def test_runner_validates_entity_memory_storage():
    """Test runner validates entity_memory storage is a stub."""
    mock_agent = MagicMock()
    mock_agent.role = "Researcher"
    mock_agent.llm = llm_stub("gpt-4")
    mock_agent.tools = []
    mock_agent.max_rpm = None

    mock_storage = MagicMock()
    # Explicitly set to False - MagicMock returns truthy values by default
    mock_storage._is_temporal_memory_stub = False

    mock_memory = MagicMock()
    mock_memory.storage = mock_storage

    mock_crew = MagicMock()
    mock_crew.agents = [mock_agent]
    mock_crew.tasks = []
    mock_crew.max_rpm = None
    mock_crew.planning = False
    mock_crew.memory = True
    mock_crew.short_term_memory = None
    mock_crew.long_term_memory = None
    mock_crew.entity_memory = mock_memory
    mock_crew.knowledge_sources = None

    with pytest.raises(CrewValidationError) as exc_info:
        TemporalCrewRunner(mock_crew)

    assert "entity_memory storage must use entity_memory_stub()" in str(exc_info.value)


# =============================================================================
# Unit Tests: Knowledge Validation
# =============================================================================


def test_runner_validates_knowledge_storage():
    """Test runner validates knowledge source storage is a stub."""
    mock_agent = MagicMock()
    mock_agent.role = "Researcher"
    mock_agent.llm = llm_stub("gpt-4")
    mock_agent.tools = []
    mock_agent.max_rpm = None

    mock_storage = MagicMock()
    # Explicitly set to False - MagicMock returns truthy values by default
    mock_storage._is_temporal_knowledge_stub = False

    mock_source = MagicMock()
    mock_source.storage = mock_storage

    mock_crew = MagicMock()
    mock_crew.agents = [mock_agent]
    mock_crew.tasks = []
    mock_crew.max_rpm = None
    mock_crew.planning = False
    mock_crew.memory = False
    mock_crew.knowledge_sources = [mock_source]

    with pytest.raises(CrewValidationError) as exc_info:
        TemporalCrewRunner(mock_crew)

    assert "storage must use knowledge_storage_stub()" in str(exc_info.value)


def test_runner_accepts_knowledge_with_stub():
    """Test runner accepts knowledge source with temporal stub."""
    mock_agent = MagicMock()
    mock_agent.role = "Researcher"
    mock_agent.llm = llm_stub("gpt-4")
    mock_agent.tools = []
    mock_agent.max_rpm = None

    mock_source = MagicMock()
    mock_source.storage = knowledge_storage_stub()

    mock_crew = MagicMock()
    mock_crew.agents = [mock_agent]
    mock_crew.tasks = []
    mock_crew.max_rpm = None
    mock_crew.planning = False
    mock_crew.memory = False
    mock_crew.knowledge_sources = [mock_source]

    # Should not raise
    runner = TemporalCrewRunner(mock_crew)
    assert runner._crew is mock_crew


# =============================================================================
# Unit Tests: Skip Validation
# =============================================================================


def test_runner_skip_validation():
    """Test runner with skip_validation=True skips all validation."""
    mock_crew = MagicMock()
    mock_crew.agents = []

    # Should not raise even with invalid config
    runner = TemporalCrewRunner(mock_crew, skip_validation=True)
    assert runner._crew is mock_crew


# =============================================================================
# Unit Tests: Multiple Errors
# =============================================================================


def test_runner_collects_multiple_validation_errors():
    """Test runner collects all validation errors."""
    mock_tool = MagicMock()
    mock_tool.name = "bad_tool"
    # Explicitly set to False - MagicMock returns truthy values by default
    mock_tool._is_temporal_activity_tool = False

    mock_agent = MagicMock()
    mock_agent.role = "Researcher"
    mock_agent.llm = MagicMock()  # Error 1 (not a stub)
    mock_agent.tools = [mock_tool]  # Error 2 (not a temporal tool)
    mock_agent.max_rpm = 10  # Error 3

    mock_task = MagicMock()
    mock_task.description = "Research"
    mock_task.human_input = True  # Error 4

    mock_crew = MagicMock()
    mock_crew.agents = [mock_agent]
    mock_crew.tasks = [mock_task]
    mock_crew.max_rpm = 100  # Error 5
    mock_crew.planning = False
    mock_crew.memory = False
    mock_crew.knowledge_sources = None

    with pytest.raises(CrewValidationError) as exc_info:
        TemporalCrewRunner(mock_crew)

    error_msg = str(exc_info.value)
    assert "must use llm_stub()" in error_msg
    assert "must use activity_as_tool()" in error_msg
    assert "must not set max_rpm" in error_msg
    assert "must not set human_input=True" in error_msg
    assert "Crew must not set max_rpm" in error_msg


# =============================================================================
# Unit Tests: kickoff execution
# =============================================================================


@pytest.mark.asyncio
async def test_runner_kickoff_calls_akickoff():
    """Test runner.kickoff() calls crew.akickoff()."""
    mock_crew = MagicMock()
    mock_crew.agents = []
    mock_crew.tasks = []
    mock_crew.max_rpm = None
    mock_crew.planning = False
    mock_crew.memory = False
    mock_crew.knowledge_sources = None
    mock_crew.akickoff = AsyncMock(return_value="crew output")

    runner = TemporalCrewRunner(mock_crew)
    result = await runner.kickoff()

    mock_crew.akickoff.assert_called_once_with()
    assert result == "crew output"


@pytest.mark.asyncio
async def test_runner_kickoff_with_inputs():
    """Test runner.kickoff() passes inputs to crew."""
    mock_crew = MagicMock()
    mock_crew.agents = []
    mock_crew.tasks = []
    mock_crew.max_rpm = None
    mock_crew.planning = False
    mock_crew.memory = False
    mock_crew.knowledge_sources = None
    mock_crew.akickoff = AsyncMock(return_value="output")

    runner = TemporalCrewRunner(mock_crew)
    result = await runner.kickoff(inputs={"topic": "AI"})

    mock_crew.akickoff.assert_called_once_with(inputs={"topic": "AI"})
    assert result == "output"


@pytest.mark.asyncio
async def test_runner_kickoff_for_each():
    """Test runner.kickoff_for_each() calls crew.akickoff_for_each()."""
    mock_crew = MagicMock()
    mock_crew.agents = []
    mock_crew.tasks = []
    mock_crew.max_rpm = None
    mock_crew.planning = False
    mock_crew.memory = False
    mock_crew.knowledge_sources = None
    mock_crew.akickoff_for_each = AsyncMock(return_value=["out1", "out2"])

    runner = TemporalCrewRunner(mock_crew)
    inputs = [{"topic": "AI"}, {"topic": "ML"}]
    result = await runner.kickoff_for_each(inputs)

    mock_crew.akickoff_for_each.assert_called_once_with(inputs=inputs)
    assert result == ["out1", "out2"]
