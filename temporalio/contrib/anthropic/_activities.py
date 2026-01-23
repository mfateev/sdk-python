"""Temporal activities for Claude Agent SDK.

This module provides activities for executing LLM calls through the Anthropic API.
Activities ensure deterministic execution and automatic retry handling.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from temporalio import activity
from temporalio.exceptions import ApplicationError


@dataclass
class InvokeLLMInput:
    """Input for LLM activity.

    Args:
        messages: Conversation history in Claude SDK message format
        model: Claude model identifier (e.g., "claude-sonnet-4-5")
        max_tokens: Maximum tokens to generate
    """

    messages: list[dict[str, Any]]
    model: str
    max_tokens: int


@activity.defn
async def invoke_llm_activity(input: InvokeLLMInput) -> dict[str, Any]:
    """Execute LLM API call through Anthropic API.

    .. warning::
        This is a POC implementation. Error handling is basic.

    This activity:
    - Calls the Anthropic Messages API
    - Converts API response to Claude SDK message format
    - Returns response for deterministic replay

    Args:
        input: Conversation history and model parameters

    Returns:
        Response message in Claude SDK format

    Raises:
        ApplicationError: For retryable and non-retryable errors
    """
    import anthropic
    from anthropic import APIStatusError

    # Extract messages for Anthropic API
    # POC: Assume SDK messages are in correct format
    api_messages = _convert_to_api_messages(input.messages)

    # Call Anthropic API
    try:
        client = anthropic.AsyncAnthropic()

        response = await client.messages.create(
            model=input.model,
            messages=api_messages,
            max_tokens=input.max_tokens,
        )

        # Convert response to Claude SDK message format
        return _convert_to_sdk_message(response)

    except APIStatusError as e:
        # Follow OpenAI's error handling pattern
        activity.logger.error(f"Anthropic API error: {e}")

        # Listen to server hints for retry behavior
        retry_after = None
        retry_after_ms_header = e.response.headers.get("retry-after-ms")
        if retry_after_ms_header is not None:
            retry_after = timedelta(milliseconds=float(retry_after_ms_header))

        if retry_after is None:
            retry_after_header = e.response.headers.get("retry-after")
            if retry_after_header is not None:
                retry_after = timedelta(seconds=float(retry_after_header))

        should_retry_header = e.response.headers.get("x-should-retry")
        if should_retry_header == "true":
            raise e
        if should_retry_header == "false":
            raise ApplicationError(
                "Non-retryable Anthropic API error",
                non_retryable=True,
                next_retry_delay=retry_after,
            ) from e

        # Status code based retry logic (following OpenAI pattern)
        if (
            e.response.status_code in [408, 409, 429]
            or e.response.status_code >= 500
        ):
            raise ApplicationError(
                f"Retryable Anthropic status code: {e.response.status_code}",
                non_retryable=False,
                next_retry_delay=retry_after,
            ) from e

        raise ApplicationError(
            f"Non-retryable Anthropic status code: {e.response.status_code}",
            non_retryable=True,
            next_retry_delay=retry_after,
        ) from e

    except Exception as e:
        # Unexpected errors - log and re-raise
        activity.logger.error(f"Unexpected error in LLM activity: {e}")
        raise


def _convert_to_api_messages(sdk_messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert Claude SDK messages to Anthropic API format.

    SDK messages may include a 'type' field that is not part of the
    Anthropic API format and must be stripped.

    Args:
        sdk_messages: Messages from Claude SDK

    Returns:
        Messages in Anthropic API format
    """
    api_messages = []
    for msg in sdk_messages:
        # Create a copy and remove 'type' field if present
        # Anthropic API only accepts 'role' and 'content'
        api_msg = {"role": msg["role"], "content": msg["content"]}
        api_messages.append(api_msg)
    return api_messages


def _convert_to_sdk_message(api_response: Any) -> dict[str, Any]:
    """Convert Anthropic API response to Claude SDK message format.

    Args:
        api_response: Response from Anthropic Messages API

    Returns:
        Message in Claude SDK format
    """
    # Extract content from response
    content = []
    for block in api_response.content:
        if block.type == "text":
            content.append({
                "type": "text",
                "text": block.text,
            })
        elif block.type == "tool_use":
            # Include tool use blocks for future tool support
            content.append({
                "type": "tool_use",
                "id": block.id,
                "name": block.name,
                "input": block.input,
            })

    # Build SDK message
    return {
        "type": "message",
        "role": "assistant",
        "content": content,
        "model": api_response.model,
        "stop_reason": api_response.stop_reason,
        "usage": {
            "input_tokens": api_response.usage.input_tokens,
            "output_tokens": api_response.usage.output_tokens,
        },
    }
