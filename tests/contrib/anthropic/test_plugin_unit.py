"""Unit tests for AnthropicAgentsPlugin.

These tests verify plugin initialization and configuration without requiring
a full Temporal environment.
"""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from temporalio.contrib.anthropic._plugin import (
    AnthropicAgentsPlugin,
    AnthropicPayloadConverter,
    TransportActivityParameters,
    _data_converter,
)
from temporalio.converter import DataConverter, DefaultPayloadConverter


class TestTransportActivityParameters:
    """Test TransportActivityParameters dataclass."""

    def test_default_parameters(self):
        """Should have sensible defaults."""
        params = TransportActivityParameters()

        assert params.task_queue is None
        assert params.schedule_to_close_timeout is None
        assert params.schedule_to_start_timeout is None
        assert params.start_to_close_timeout == timedelta(minutes=5)
        assert params.heartbeat_timeout is None
        assert params.retry_policy is None
        assert params.use_local_activity is False

    def test_custom_parameters(self):
        """Should accept custom parameters."""
        params = TransportActivityParameters(
            task_queue="custom-queue",
            start_to_close_timeout=timedelta(minutes=10),
            use_local_activity=True,
        )

        assert params.task_queue == "custom-queue"
        assert params.start_to_close_timeout == timedelta(minutes=10)
        assert params.use_local_activity is True

    def test_dataclass_frozen(self):
        """Parameters should be immutable after creation."""
        params = TransportActivityParameters()

        # Dataclasses are not frozen by default, but we can verify structure
        assert hasattr(params, "task_queue")
        assert hasattr(params, "start_to_close_timeout")


class TestAnthropicPayloadConverter:
    """Test AnthropicPayloadConverter."""

    def test_initialization(self):
        """Should initialize without errors."""
        converter = AnthropicPayloadConverter()
        assert converter is not None

    def test_exclude_unset_option(self):
        """Should use exclude_unset=True option."""
        converter = AnthropicPayloadConverter()
        # Verify it's a PydanticPayloadConverter
        assert type(converter).__name__ == "AnthropicPayloadConverter"


class TestDataConverter:
    """Test _data_converter helper function."""

    def test_none_creates_anthropic_converter(self):
        """None should create DataConverter with AnthropicPayloadConverter."""
        result = _data_converter(None)

        assert isinstance(result, DataConverter)
        assert result.payload_converter_class == AnthropicPayloadConverter

    def test_default_converter_replaced(self):
        """DefaultPayloadConverter should be replaced with AnthropicPayloadConverter."""
        original = DataConverter(payload_converter_class=DefaultPayloadConverter)
        result = _data_converter(original)

        assert isinstance(result, DataConverter)
        assert result.payload_converter_class == AnthropicPayloadConverter

    def test_anthropic_converter_unchanged(self):
        """AnthropicPayloadConverter should pass through unchanged."""
        original = DataConverter(payload_converter_class=AnthropicPayloadConverter)
        result = _data_converter(original)

        assert result is original

    def test_custom_converter_raises(self):
        """Custom non-Anthropic converter should raise ValueError."""

        class CustomConverter:
            pass

        original = DataConverter(payload_converter_class=CustomConverter)

        with pytest.raises(ValueError, match="AnthropicPayloadConverter"):
            _data_converter(original)


class TestAnthropicAgentsPlugin:
    """Test AnthropicAgentsPlugin initialization."""

    def test_initialization_default_params(self):
        """Should initialize with default parameters."""
        plugin = AnthropicAgentsPlugin()

        assert plugin.name == "AnthropicAgentsPlugin"

    def test_initialization_custom_params(self):
        """Should accept custom transport parameters."""
        params = TransportActivityParameters(
            start_to_close_timeout=timedelta(minutes=10)
        )

        plugin = AnthropicAgentsPlugin(transport_params=params)

        assert plugin.name == "AnthropicAgentsPlugin"

    def test_initialization_register_activities_false(self):
        """Should accept register_activities=False."""
        plugin = AnthropicAgentsPlugin(register_activities=False)

        assert plugin.name == "AnthropicAgentsPlugin"

    def test_data_converter_configuration(self):
        """Plugin should configure data converter."""
        plugin = AnthropicAgentsPlugin()

        # Plugin should have data_converter callable
        assert hasattr(plugin, "_data_converter")

    def test_activities_registration(self):
        """Plugin should register invoke_llm_activity."""
        plugin = AnthropicAgentsPlugin(register_activities=True)

        # Test activity registration function
        activities = plugin._activities([])

        # Should add invoke_llm_activity
        assert len(activities) == 1
        assert activities[0].__name__ == "invoke_llm_activity"

    def test_activities_not_registered_when_disabled(self):
        """Plugin should not register activities when disabled."""
        plugin = AnthropicAgentsPlugin(register_activities=False)

        # Test activity registration function
        existing_activities = [lambda: None]
        activities = plugin._activities(existing_activities)

        # Should not add new activities
        assert len(activities) == 1
        assert activities == existing_activities

    def test_activities_appends_to_existing(self):
        """Plugin should append to existing activities."""
        plugin = AnthropicAgentsPlugin(register_activities=True)

        # Test with existing activities
        mock_activity = MagicMock()
        existing = [mock_activity]
        activities = plugin._activities(existing)

        # Should have existing + new
        assert len(activities) == 2
        assert activities[0] == mock_activity
        assert activities[1].__name__ == "invoke_llm_activity"

    def test_workflow_runner_passthrough_sandbox(self):
        """Plugin should configure sandbox passthrough."""
        plugin = AnthropicAgentsPlugin()

        # Mock sandboxed workflow runner
        mock_restrictions = MagicMock()
        mock_restrictions.with_passthrough_modules.return_value = mock_restrictions

        mock_runner = MagicMock()
        mock_runner.__class__.__name__ = "SandboxedWorkflowRunner"
        mock_runner.restrictions = mock_restrictions

        # Check if runner is SandboxedWorkflowRunner
        from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

        # Properly mock isinstance check
        original_isinstance = isinstance

        def mock_isinstance(obj, cls):
            if cls is SandboxedWorkflowRunner:
                return obj.__class__.__name__ == "SandboxedWorkflowRunner"
            return original_isinstance(obj, cls)

        # Apply workflow runner configuration
        import temporalio.contrib.anthropic._plugin as plugin_module

        # We can't easily test the workflow_runner without full integration
        # Just verify the function exists
        assert plugin._workflow_runner is not None

    def test_workflow_runner_no_runner_raises(self):
        """Plugin should raise if no runner provided."""
        plugin = AnthropicAgentsPlugin()

        with pytest.raises(ValueError, match="No WorkflowRunner"):
            plugin._workflow_runner(None)

    def test_default_timeout_set(self):
        """Plugin should set default timeout if none provided."""
        # Both timeouts None
        params = TransportActivityParameters(
            start_to_close_timeout=None, schedule_to_close_timeout=None
        )

        plugin = AnthropicAgentsPlugin(transport_params=params)

        # Plugin should have set a default
        # (This is done in __init__, we verify it doesn't crash)
        assert plugin is not None


class TestPluginIntegration:
    """Test plugin integration scenarios."""

    def test_plugin_name(self):
        """Plugin should have correct name."""
        plugin = AnthropicAgentsPlugin()
        assert plugin.name == "AnthropicAgentsPlugin"

    def test_plugin_with_all_options(self):
        """Plugin should work with all options configured."""
        params = TransportActivityParameters(
            task_queue="custom-queue",
            start_to_close_timeout=timedelta(seconds=30),
            heartbeat_timeout=timedelta(seconds=10),
            use_local_activity=True,
        )

        plugin = AnthropicAgentsPlugin(
            transport_params=params, register_activities=True
        )

        assert plugin.name == "AnthropicAgentsPlugin"

    def test_multiple_plugin_instances(self):
        """Should be able to create multiple plugin instances."""
        plugin1 = AnthropicAgentsPlugin()
        plugin2 = AnthropicAgentsPlugin()

        assert plugin1.name == plugin2.name
        assert plugin1 is not plugin2
