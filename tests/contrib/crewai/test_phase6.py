"""Phase 6 Tests: Plugin Architecture.

These tests verify the CrewAIPlugin functionality:
- Plugin initialization and configuration
- Data converter setup
- Activity registration
- Workflow runner configuration
"""

from unittest.mock import MagicMock

import pytest

from temporalio.contrib.crewai import CrewAIActivityConfig, CrewAIPlugin

# =============================================================================
# Unit Tests: Plugin Initialization
# =============================================================================


def test_plugin_creation():
    """Test that CrewAIPlugin can be created with config."""
    config = CrewAIActivityConfig(llm_factory=lambda m: MagicMock())

    plugin = CrewAIPlugin(config=config)

    assert plugin._config is config
    assert plugin.name() == "CrewAIPlugin"


def test_plugin_has_data_converter_hook():
    """Test that plugin has data converter configuration."""
    config = CrewAIActivityConfig(llm_factory=lambda m: MagicMock())

    plugin = CrewAIPlugin(config=config)

    # Plugin should have a data_converter hook
    assert plugin.data_converter is not None


def test_plugin_has_activities_hook():
    """Test that plugin has activities configuration."""
    config = CrewAIActivityConfig(llm_factory=lambda m: MagicMock())

    plugin = CrewAIPlugin(config=config)

    # Plugin should have an activities hook
    assert plugin.activities is not None


def test_plugin_has_workflow_runner_hook():
    """Test that plugin has workflow runner configuration."""
    config = CrewAIActivityConfig(llm_factory=lambda m: MagicMock())

    plugin = CrewAIPlugin(config=config)

    # Plugin should have a workflow_runner hook
    assert plugin.workflow_runner is not None


# =============================================================================
# Unit Tests: Activity Registration
# =============================================================================


def test_plugin_adds_activities():
    """Test that plugin adds CrewAI activities."""
    config = CrewAIActivityConfig(llm_factory=lambda m: MagicMock())

    plugin = CrewAIPlugin(config=config)

    # Get activities via the hook (it's a callable)
    assert callable(plugin.activities)
    activities = plugin.activities(None)

    # Should have all CrewAI activities (LLM, memory, knowledge)
    activity_names = [
        getattr(a, "__temporal_activity_definition").name
        for a in activities
        if hasattr(a, "__temporal_activity_definition")
    ]

    assert "crewai_llm_call" in activity_names
    assert "crewai_memory_save" in activity_names
    assert "crewai_memory_search" in activity_names
    assert "crewai_memory_reset" in activity_names
    assert "crewai_ltm_save" in activity_names
    assert "crewai_ltm_load" in activity_names
    assert "crewai_ltm_reset" in activity_names
    assert "crewai_knowledge_search" in activity_names
    assert "crewai_knowledge_save" in activity_names
    assert "crewai_knowledge_reset" in activity_names


def test_plugin_appends_to_existing_activities():
    """Test that plugin appends to existing activities list."""
    config = CrewAIActivityConfig(llm_factory=lambda m: MagicMock())

    plugin = CrewAIPlugin(config=config)

    # Create some existing activities
    existing_activity = MagicMock()
    existing = [existing_activity]

    # Get activities via the hook
    assert callable(plugin.activities)
    activities = plugin.activities(existing)

    # Should include existing plus CrewAI activities
    assert existing_activity in activities
    assert len(activities) > 1


def test_plugin_register_activities_false():
    """Test that plugin doesn't add activities when register_activities=False."""
    config = CrewAIActivityConfig(llm_factory=lambda m: MagicMock())

    plugin = CrewAIPlugin(config=config, register_activities=False)

    # Get activities via the hook
    assert callable(plugin.activities)
    activities = plugin.activities(None)

    # Should be empty
    assert len(activities) == 0


def test_plugin_register_activities_false_preserves_existing():
    """Test that register_activities=False preserves existing activities."""
    config = CrewAIActivityConfig(llm_factory=lambda m: MagicMock())

    plugin = CrewAIPlugin(config=config, register_activities=False)

    existing_activity = MagicMock()
    existing = [existing_activity]

    # Get activities via the hook
    assert callable(plugin.activities)
    activities = plugin.activities(existing)

    # Should only have existing
    assert list(activities) == [existing_activity]


# =============================================================================
# Unit Tests: Data Converter
# =============================================================================


def test_plugin_data_converter_uses_crewai_converter():
    """Test that plugin uses CrewAI data converter when none provided."""
    from temporalio.contrib.crewai import crewai_data_converter

    config = CrewAIActivityConfig(llm_factory=lambda m: MagicMock())

    plugin = CrewAIPlugin(config=config)

    # Get converter via the hook with None
    assert callable(plugin.data_converter)
    converter = plugin.data_converter(None)

    # Should return the crewai_data_converter
    assert converter is crewai_data_converter


def test_plugin_data_converter_preserves_custom():
    """Test that plugin preserves custom data converter."""
    config = CrewAIActivityConfig(llm_factory=lambda m: MagicMock())

    plugin = CrewAIPlugin(config=config)

    custom_converter = MagicMock()

    # Get converter via the hook with custom
    assert callable(plugin.data_converter)
    converter = plugin.data_converter(custom_converter)

    # Should return the custom converter
    assert converter is custom_converter


# =============================================================================
# Unit Tests: Workflow Runner
# =============================================================================


def test_plugin_workflow_runner_raises_without_runner():
    """Test that plugin raises error when no runner provided."""
    config = CrewAIActivityConfig(llm_factory=lambda m: MagicMock())

    plugin = CrewAIPlugin(config=config)

    assert callable(plugin.workflow_runner)
    with pytest.raises(ValueError) as exc_info:
        plugin.workflow_runner(None)

    assert "No WorkflowRunner provided" in str(exc_info.value)


def test_plugin_workflow_runner_returns_non_sandbox_runner():
    """Test that plugin returns non-sandbox runner unchanged."""
    config = CrewAIActivityConfig(llm_factory=lambda m: MagicMock())

    plugin = CrewAIPlugin(config=config)

    mock_runner = MagicMock()
    mock_runner.__class__.__name__ = "SomeOtherRunner"

    # Simulate not being a SandboxedWorkflowRunner
    assert callable(plugin.workflow_runner)
    result = plugin.workflow_runner(mock_runner)

    # Should return the same runner
    assert result is mock_runner


# =============================================================================
# Integration Tests: Plugin with Temporal Components
# =============================================================================


def test_plugin_is_simple_plugin():
    """Test that CrewAIPlugin is a SimplePlugin."""
    from temporalio.plugin import SimplePlugin

    config = CrewAIActivityConfig(llm_factory=lambda m: MagicMock())

    plugin = CrewAIPlugin(config=config)

    assert isinstance(plugin, SimplePlugin)
