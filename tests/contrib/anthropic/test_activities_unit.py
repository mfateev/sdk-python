"""Unit tests for activity functions and message conversion.

These tests use mocks to isolate activity logic without making real API calls.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from temporalio.exceptions import ApplicationError

from temporalio.contrib.anthropic._activities import (
    InvokeLLMInput,
    _convert_to_api_messages,
    _convert_to_sdk_message,
    invoke_llm_activity,
)


class TestMessageConversion:
    """Test message format conversion functions."""

    def test_convert_to_api_messages_passthrough(self):
        """_convert_to_api_messages should pass through messages."""
        messages = [
            {"role": "user", "content": "hello"},
            {"role": "assistant", "content": "hi"},
        ]
        result = _convert_to_api_messages(messages)
        assert result == messages

    def test_convert_to_api_messages_empty(self):
        """_convert_to_api_messages should handle empty list."""
        result = _convert_to_api_messages([])
        assert result == []

    def test_convert_to_sdk_message_text_only(self):
        """_convert_to_sdk_message should handle text-only responses."""
        # Mock Anthropic API response
        mock_response = MagicMock()
        mock_response.model = "claude-sonnet-4-5"
        mock_response.stop_reason = "end_turn"

        # Mock text content block
        text_block = MagicMock()
        text_block.type = "text"
        text_block.text = "Hello, how can I help?"
        mock_response.content = [text_block]

        # Mock usage
        mock_usage = MagicMock()
        mock_usage.input_tokens = 10
        mock_usage.output_tokens = 20
        mock_response.usage = mock_usage

        result = _convert_to_sdk_message(mock_response)

        assert result["type"] == "message"
        assert result["role"] == "assistant"
        assert result["model"] == "claude-sonnet-4-5"
        assert result["stop_reason"] == "end_turn"
        assert len(result["content"]) == 1
        assert result["content"][0]["type"] == "text"
        assert result["content"][0]["text"] == "Hello, how can I help?"
        assert result["usage"]["input_tokens"] == 10
        assert result["usage"]["output_tokens"] == 20

    def test_convert_to_sdk_message_with_tool_use(self):
        """_convert_to_sdk_message should handle tool use blocks."""
        mock_response = MagicMock()
        mock_response.model = "claude-sonnet-4-5"
        mock_response.stop_reason = "tool_use"

        # Mock tool use block
        tool_block = MagicMock()
        tool_block.type = "tool_use"
        tool_block.id = "tool_123"
        tool_block.name = "get_weather"
        tool_block.input = {"location": "San Francisco"}

        # Mock text block
        text_block = MagicMock()
        text_block.type = "text"
        text_block.text = "Let me check the weather."

        mock_response.content = [text_block, tool_block]

        mock_usage = MagicMock()
        mock_usage.input_tokens = 15
        mock_usage.output_tokens = 25
        mock_response.usage = mock_usage

        result = _convert_to_sdk_message(mock_response)

        assert len(result["content"]) == 2
        assert result["content"][0]["type"] == "text"
        assert result["content"][1]["type"] == "tool_use"
        assert result["content"][1]["id"] == "tool_123"
        assert result["content"][1]["name"] == "get_weather"
        assert result["content"][1]["input"] == {"location": "San Francisco"}

    def test_convert_to_sdk_message_multiple_text_blocks(self):
        """_convert_to_sdk_message should handle multiple text blocks."""
        mock_response = MagicMock()
        mock_response.model = "claude-sonnet-4-5"
        mock_response.stop_reason = "end_turn"

        # Multiple text blocks
        text1 = MagicMock()
        text1.type = "text"
        text1.text = "First part."

        text2 = MagicMock()
        text2.type = "text"
        text2.text = "Second part."

        mock_response.content = [text1, text2]

        mock_usage = MagicMock()
        mock_usage.input_tokens = 5
        mock_usage.output_tokens = 10
        mock_response.usage = mock_usage

        result = _convert_to_sdk_message(mock_response)

        assert len(result["content"]) == 2
        assert result["content"][0]["text"] == "First part."
        assert result["content"][1]["text"] == "Second part."


class TestInvokeLLMActivity:
    """Test invoke_llm_activity function."""

    @pytest.mark.asyncio
    async def test_invoke_llm_activity_success(self):
        """Activity should call Anthropic API and return converted message."""
        input_data = InvokeLLMInput(
            messages=[{"role": "user", "content": "test"}],
            model="claude-sonnet-4-5",
            max_tokens=100,
        )

        # Mock Anthropic client and response
        mock_response = MagicMock()
        mock_response.model = "claude-sonnet-4-5"
        mock_response.stop_reason = "end_turn"

        text_block = MagicMock()
        text_block.type = "text"
        text_block.text = "Test response"
        mock_response.content = [text_block]

        mock_usage = MagicMock()
        mock_usage.input_tokens = 5
        mock_usage.output_tokens = 10
        mock_response.usage = mock_usage

        mock_client = MagicMock()
        mock_client.messages.create = AsyncMock(return_value=mock_response)

        with patch('anthropic.AsyncAnthropic', return_value=mock_client):
            result = await invoke_llm_activity(input_data)

        # Verify API was called correctly
        mock_client.messages.create.assert_called_once_with(
            model="claude-sonnet-4-5",
            messages=[{"role": "user", "content": "test"}],
            max_tokens=100,
        )

        # Verify result structure
        assert result["type"] == "message"
        assert result["role"] == "assistant"
        assert result["model"] == "claude-sonnet-4-5"
        assert result["content"][0]["text"] == "Test response"

    @pytest.mark.asyncio
    async def test_invoke_llm_activity_retry_after_header(self):
        """Activity should respect retry-after header."""
        input_data = InvokeLLMInput(
            messages=[{"role": "user", "content": "test"}],
            model="claude-sonnet-4-5",
            max_tokens=100,
        )

        # Mock APIStatusError with retry-after header
        from anthropic import APIStatusError

        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {"retry-after": "5"}

        error = APIStatusError(
            message="Rate limit",
            response=mock_response,
            body=None,
        )

        mock_client = MagicMock()
        mock_client.messages.create = AsyncMock(side_effect=error)

        with patch('anthropic.AsyncAnthropic', return_value=mock_client):
            with pytest.raises(ApplicationError) as exc_info:
                await invoke_llm_activity(input_data)

        # Verify ApplicationError has retry delay
        assert exc_info.value.non_retryable is False
        assert exc_info.value.next_retry_delay == timedelta(seconds=5)

    @pytest.mark.asyncio
    async def test_invoke_llm_activity_retry_after_ms_header(self):
        """Activity should respect retry-after-ms header."""
        input_data = InvokeLLMInput(
            messages=[{"role": "user", "content": "test"}],
            model="claude-sonnet-4-5",
            max_tokens=100,
        )

        from anthropic import APIStatusError

        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {"retry-after-ms": "3000"}

        error = APIStatusError(
            message="Rate limit",
            response=mock_response,
            body=None,
        )

        mock_client = MagicMock()
        mock_client.messages.create = AsyncMock(side_effect=error)

        with patch('anthropic.AsyncAnthropic', return_value=mock_client):
            with pytest.raises(ApplicationError) as exc_info:
                await invoke_llm_activity(input_data)

        assert exc_info.value.next_retry_delay == timedelta(milliseconds=3000)

    @pytest.mark.asyncio
    async def test_invoke_llm_activity_should_retry_false(self):
        """Activity should mark error as non-retryable when x-should-retry is false."""
        input_data = InvokeLLMInput(
            messages=[{"role": "user", "content": "test"}],
            model="claude-sonnet-4-5",
            max_tokens=100,
        )

        from anthropic import APIStatusError

        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.headers = {"x-should-retry": "false"}

        error = APIStatusError(
            message="Bad request",
            response=mock_response,
            body=None,
        )

        mock_client = MagicMock()
        mock_client.messages.create = AsyncMock(side_effect=error)

        with patch('anthropic.AsyncAnthropic', return_value=mock_client):
            with pytest.raises(ApplicationError) as exc_info:
                await invoke_llm_activity(input_data)

        # Verify non-retryable
        assert exc_info.value.non_retryable is True

    @pytest.mark.asyncio
    async def test_invoke_llm_activity_should_retry_true(self):
        """Activity should re-raise when x-should-retry is true."""
        input_data = InvokeLLMInput(
            messages=[{"role": "user", "content": "test"}],
            model="claude-sonnet-4-5",
            max_tokens=100,
        )

        from anthropic import APIStatusError

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.headers = {"x-should-retry": "true"}

        error = APIStatusError(
            message="Server error",
            response=mock_response,
            body=None,
        )

        mock_client = MagicMock()
        mock_client.messages.create = AsyncMock(side_effect=error)

        with patch('anthropic.AsyncAnthropic', return_value=mock_client):
            # Should raise original error, not ApplicationError
            with pytest.raises(APIStatusError):
                await invoke_llm_activity(input_data)

    @pytest.mark.asyncio
    async def test_invoke_llm_activity_retryable_status_codes(self):
        """Activity should mark certain status codes as retryable."""
        input_data = InvokeLLMInput(
            messages=[{"role": "user", "content": "test"}],
            model="claude-sonnet-4-5",
            max_tokens=100,
        )

        from anthropic import APIStatusError

        # Test retryable status codes
        for status_code in [408, 409, 429, 500, 502, 503]:
            mock_response = MagicMock()
            mock_response.status_code = status_code
            mock_response.headers = {}

            error = APIStatusError(
                message=f"Error {status_code}",
                response=mock_response,
                body=None,
            )

            mock_client = MagicMock()
            mock_client.messages.create = AsyncMock(side_effect=error)

            with patch('anthropic.AsyncAnthropic', return_value=mock_client):
                with pytest.raises(ApplicationError) as exc_info:
                    await invoke_llm_activity(input_data)

                # Should be retryable
                assert exc_info.value.non_retryable is False

    @pytest.mark.asyncio
    async def test_invoke_llm_activity_non_retryable_status_codes(self):
        """Activity should mark certain status codes as non-retryable."""
        input_data = InvokeLLMInput(
            messages=[{"role": "user", "content": "test"}],
            model="claude-sonnet-4-5",
            max_tokens=100,
        )

        from anthropic import APIStatusError

        # Test non-retryable status codes
        for status_code in [400, 401, 403, 404]:
            mock_response = MagicMock()
            mock_response.status_code = status_code
            mock_response.headers = {}

            error = APIStatusError(
                message=f"Error {status_code}",
                response=mock_response,
                body=None,
            )

            mock_client = MagicMock()
            mock_client.messages.create = AsyncMock(side_effect=error)

            with patch('anthropic.AsyncAnthropic', return_value=mock_client):
                with pytest.raises(ApplicationError) as exc_info:
                    await invoke_llm_activity(input_data)

                # Should be non-retryable
                assert exc_info.value.non_retryable is True

    @pytest.mark.asyncio
    async def test_invoke_llm_activity_unexpected_error(self):
        """Activity should log and re-raise unexpected errors."""
        input_data = InvokeLLMInput(
            messages=[{"role": "user", "content": "test"}],
            model="claude-sonnet-4-5",
            max_tokens=100,
        )

        mock_client = MagicMock()
        mock_client.messages.create = AsyncMock(side_effect=ValueError("Unexpected"))

        with patch('anthropic.AsyncAnthropic', return_value=mock_client):
            with pytest.raises(ValueError, match="Unexpected"):
                await invoke_llm_activity(input_data)


class TestInvokeLLMInput:
    """Test InvokeLLMInput dataclass."""

    def test_invoke_llm_input_creation(self):
        """Should create InvokeLLMInput with required fields."""
        input_data = InvokeLLMInput(
            messages=[{"role": "user", "content": "test"}],
            model="claude-sonnet-4-5",
            max_tokens=100,
        )

        assert input_data.messages == [{"role": "user", "content": "test"}]
        assert input_data.model == "claude-sonnet-4-5"
        assert input_data.max_tokens == 100

    def test_invoke_llm_input_serialization(self):
        """InvokeLLMInput should be serializable."""
        input_data = InvokeLLMInput(
            messages=[{"role": "user", "content": "test"}],
            model="claude-sonnet-4-5",
            max_tokens=100,
        )

        # Should be able to convert to dict (for Temporal serialization)
        from dataclasses import asdict
        data_dict = asdict(input_data)

        assert data_dict["model"] == "claude-sonnet-4-5"
        assert data_dict["max_tokens"] == 100
        assert len(data_dict["messages"]) == 1
