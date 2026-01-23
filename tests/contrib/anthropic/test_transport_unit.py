"""Unit tests for TemporalTransport.

These tests use mocks to isolate the transport logic without requiring
a Temporal environment or API calls.
"""

from __future__ import annotations

import json
from datetime import timedelta
from unittest.mock import AsyncMock, patch

import pytest

from temporalio.contrib.anthropic._activities import InvokeLLMInput
from temporalio.contrib.anthropic._transport import TemporalTransport


class TestTemporalTransportLifecycle:
    """Test transport lifecycle methods."""

    def test_init_not_ready(self):
        """Transport should not be ready after init."""
        transport = TemporalTransport()
        assert not transport.is_ready()

    @pytest.mark.asyncio
    async def test_connect_makes_ready(self):
        """connect() should make transport ready."""
        transport = TemporalTransport()
        await transport.connect()
        assert transport.is_ready()

    @pytest.mark.asyncio
    async def test_close_makes_not_ready(self):
        """close() should make transport not ready."""
        transport = TemporalTransport()
        await transport.connect()
        await transport.close()
        assert not transport.is_ready()

    @pytest.mark.asyncio
    async def test_close_clears_response_queue(self):
        """close() should clear response queue."""
        transport = TemporalTransport()
        transport._response_queue.append({"test": "data"})
        await transport.close()
        assert len(transport._response_queue) == 0

    @pytest.mark.asyncio
    async def test_end_input_is_noop(self):
        """end_input() should not raise errors."""
        transport = TemporalTransport()
        await transport.end_input()  # Should not raise


class TestTemporalTransportWrite:
    """Test transport write() method."""

    @pytest.mark.asyncio
    async def test_write_before_connect_raises(self):
        """write() before connect() should raise RuntimeError."""
        transport = TemporalTransport()

        with pytest.raises(RuntimeError, match="not connected"):
            await transport.write('{"type": "message"}')

    @pytest.mark.asyncio
    async def test_write_calls_activity(self):
        """write() should call invoke_llm_activity."""
        transport = TemporalTransport(
            model="claude-test",
            max_tokens=100,
        )
        await transport.connect()

        message = {"type": "message", "role": "user", "content": "test"}
        mock_response = {"type": "message", "role": "assistant", "content": "response"}

        with patch('temporalio.workflow.execute_activity', new_callable=AsyncMock) as mock_activity:
            mock_activity.return_value = mock_response

            await transport.write(json.dumps(message))

            # Verify activity was called
            mock_activity.assert_called_once()

            # Verify input structure
            call_args = mock_activity.call_args
            activity_input = call_args[0][1]  # Second positional arg is InvokeLLMInput
            assert isinstance(activity_input, InvokeLLMInput)
            assert activity_input.model == "claude-test"
            assert activity_input.max_tokens == 100
            assert len(activity_input.messages) == 1
            assert activity_input.messages[0] == message

    @pytest.mark.asyncio
    async def test_write_appends_to_conversation_history(self):
        """write() should append message to conversation history."""
        transport = TemporalTransport()
        await transport.connect()

        msg1 = {"type": "message", "role": "user", "content": "first"}
        msg2 = {"type": "message", "role": "user", "content": "second"}

        with patch('temporalio.workflow.execute_activity', new_callable=AsyncMock) as mock_activity:
            mock_activity.return_value = {"type": "message"}

            await transport.write(json.dumps(msg1))
            await transport.write(json.dumps(msg2))

            # Conversation history should have both messages
            assert len(transport._conversation_history) == 2
            assert transport._conversation_history[0] == msg1
            assert transport._conversation_history[1] == msg2

    @pytest.mark.asyncio
    async def test_write_queues_response(self):
        """write() should queue response for reading."""
        transport = TemporalTransport()
        await transport.connect()

        message = {"type": "message", "role": "user"}
        response = {"type": "message", "role": "assistant", "content": "reply"}

        with patch('temporalio.workflow.execute_activity', new_callable=AsyncMock) as mock_activity:
            mock_activity.return_value = response

            await transport.write(json.dumps(message))

            # Response should be queued
            assert len(transport._response_queue) == 1
            assert transport._response_queue[0] == response

    @pytest.mark.asyncio
    async def test_write_passes_timeout(self):
        """write() should pass start_to_close_timeout to activity."""
        timeout = timedelta(minutes=10)
        transport = TemporalTransport(start_to_close_timeout=timeout)
        await transport.connect()

        with patch('temporalio.workflow.execute_activity', new_callable=AsyncMock) as mock_activity:
            mock_activity.return_value = {"type": "message"}

            await transport.write('{"type": "message"}')

            # Verify timeout was passed
            call_kwargs = mock_activity.call_args[1]
            assert call_kwargs['start_to_close_timeout'] == timeout


class TestTemporalTransportRead:
    """Test transport read_messages() method."""

    @pytest.mark.asyncio
    async def test_read_messages_yields_queued_responses(self):
        """read_messages() should yield all queued responses."""
        transport = TemporalTransport()

        # Manually queue some responses
        response1 = {"id": 1}
        response2 = {"id": 2}
        transport._response_queue.append(response1)
        transport._response_queue.append(response2)

        messages = []
        async for msg in transport.read_messages():
            messages.append(msg)

        assert len(messages) == 2
        assert messages[0] == response1
        assert messages[1] == response2

    @pytest.mark.asyncio
    async def test_read_messages_empties_queue(self):
        """read_messages() should empty the response queue."""
        transport = TemporalTransport()
        transport._response_queue.append({"test": "data"})

        async for _ in transport.read_messages():
            pass

        assert len(transport._response_queue) == 0

    @pytest.mark.asyncio
    async def test_read_messages_empty_queue_yields_nothing(self):
        """read_messages() on empty queue should yield nothing."""
        transport = TemporalTransport()

        messages = []
        async for msg in transport.read_messages():
            messages.append(msg)

        assert len(messages) == 0


class TestTemporalTransportParameters:
    """Test transport initialization parameters."""

    def test_custom_model(self):
        """Should accept custom model parameter."""
        transport = TemporalTransport(model="claude-opus-4-5")
        assert transport._model == "claude-opus-4-5"

    def test_custom_max_tokens(self):
        """Should accept custom max_tokens parameter."""
        transport = TemporalTransport(max_tokens=2048)
        assert transport._max_tokens == 2048

    def test_custom_timeout(self):
        """Should accept custom timeout parameter."""
        timeout = timedelta(seconds=30)
        transport = TemporalTransport(start_to_close_timeout=timeout)
        assert transport._timeout == timeout

    def test_default_parameters(self):
        """Should have sensible defaults."""
        transport = TemporalTransport()
        assert transport._model == "claude-sonnet-4-5"
        assert transport._max_tokens == 4096
        assert transport._timeout == timedelta(minutes=5)


class TestTemporalTransportIntegration:
    """Test transport with multiple operations."""

    @pytest.mark.asyncio
    async def test_full_conversation_flow(self):
        """Test complete conversation flow."""
        transport = TemporalTransport()
        await transport.connect()

        # Simulate conversation
        with patch('temporalio.workflow.execute_activity', new_callable=AsyncMock) as mock_activity:
            mock_activity.side_effect = [
                {"id": 1, "content": "response 1"},
                {"id": 2, "content": "response 2"},
            ]

            # User message 1
            await transport.write('{"role": "user", "content": "hello"}')

            # Read response 1
            responses = []
            async for msg in transport.read_messages():
                responses.append(msg)
            assert len(responses) == 1
            assert responses[0]["id"] == 1

            # User message 2
            await transport.write('{"role": "user", "content": "follow-up"}')

            # Read response 2
            responses = []
            async for msg in transport.read_messages():
                responses.append(msg)
            assert len(responses) == 1
            assert responses[0]["id"] == 2

            # Verify conversation history contains both messages
            assert len(transport._conversation_history) == 2

    @pytest.mark.asyncio
    async def test_connect_write_read_close_cycle(self):
        """Test complete lifecycle."""
        transport = TemporalTransport()

        # Not ready initially
        assert not transport.is_ready()

        # Connect
        await transport.connect()
        assert transport.is_ready()

        # Write
        with patch('temporalio.workflow.execute_activity', new_callable=AsyncMock) as mock_activity:
            mock_activity.return_value = {"response": "ok"}
            await transport.write('{"message": "test"}')

        # Read
        messages = []
        async for msg in transport.read_messages():
            messages.append(msg)
        assert len(messages) == 1

        # Close
        await transport.close()
        assert not transport.is_ready()


class TestTemporalTransportErrorHandling:
    """Test error handling in transport."""

    @pytest.mark.asyncio
    async def test_write_with_invalid_json_passes_to_activity(self):
        """write() should pass invalid JSON to activity (let it handle errors)."""
        transport = TemporalTransport()
        await transport.connect()

        invalid_json = "not json"

        with patch('temporalio.workflow.execute_activity', new_callable=AsyncMock) as mock_activity:
            mock_activity.return_value = {"error": "handled"}

            # Should raise JSONDecodeError
            with pytest.raises(json.JSONDecodeError):
                await transport.write(invalid_json)

    @pytest.mark.asyncio
    async def test_write_propagates_activity_errors(self):
        """write() should propagate activity execution errors."""
        transport = TemporalTransport()
        await transport.connect()

        with patch('temporalio.workflow.execute_activity', new_callable=AsyncMock) as mock_activity:
            mock_activity.side_effect = RuntimeError("Activity failed")

            with pytest.raises(RuntimeError, match="Activity failed"):
                await transport.write('{"message": "test"}')
