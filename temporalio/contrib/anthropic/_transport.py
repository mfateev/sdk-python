"""Temporal Transport for Claude Agent SDK.

This module provides a Transport implementation that routes all LLM calls
through Temporal activities, ensuring deterministic execution and replay.
"""

from __future__ import annotations

import json
from collections import deque
from datetime import timedelta
from typing import Any, AsyncIterator

from temporalio import workflow

from temporalio.contrib.anthropic._activities import (
    InvokeLLMInput,
    invoke_llm_activity,
)


class TemporalTransport:
    """Transport that routes Claude SDK calls through Temporal activities.

    .. warning::
        This is a POC implementation. APIs are experimental and will change.

    This transport implementation replaces the default subprocess-based transport
    with Temporal activity-based execution. All LLM calls are routed through
    Temporal activities, making them deterministic and replay-safe.

    Args:
        model: Claude model to use (default: claude-sonnet-4-5)
        max_tokens: Maximum tokens in response (default: 4096)
        start_to_close_timeout: Activity timeout (default: 5 minutes)

    Example:
        >>> @workflow.defn
        >>> class MyWorkflow:
        ...     @workflow.run
        ...     async def run(self, prompt: str) -> str:
        ...         transport = TemporalTransport()
        ...         async for message in query(prompt, ClaudeAgentOptions(), transport):
        ...             return extract_text(message)
    """

    def __init__(
        self,
        *,
        model: str = "claude-sonnet-4-5",
        max_tokens: int = 4096,
        start_to_close_timeout: timedelta = timedelta(minutes=5),
    ):
        """Initialize the transport.

        Args:
            model: Claude model to use
            max_tokens: Maximum tokens in response
            start_to_close_timeout: Activity execution timeout
        """
        self._model = model
        self._max_tokens = max_tokens
        self._timeout = start_to_close_timeout
        self._conversation_history: list[dict[str, Any]] = []
        self._response_queue: deque[dict[str, Any]] = deque()
        self._ready = False

    async def connect(self) -> None:
        """Connect to transport.

        For Temporal transport, this is a no-op since we don't spawn subprocesses.
        We just mark the transport as ready.
        """
        self._ready = True

    async def write(self, data: str) -> None:
        """Send message to LLM via Temporal activity.

        This is the core of the transport - instead of spawning a subprocess
        and writing to stdin, we execute a Temporal activity that calls the
        Anthropic API.

        Args:
            data: JSON-encoded message to send

        Raises:
            RuntimeError: If transport is not connected
        """
        if not self._ready:
            raise RuntimeError("Transport not connected. Call connect() first.")

        # Parse incoming message
        message = json.loads(data)
        self._conversation_history.append(message)

        # Execute LLM call as Temporal activity
        response = await workflow.execute_activity(
            invoke_llm_activity,
            InvokeLLMInput(
                messages=self._conversation_history.copy(),
                model=self._model,
                max_tokens=self._max_tokens,
            ),
            start_to_close_timeout=self._timeout,
        )

        # Queue response for reading
        self._response_queue.append(response)

    async def read_messages(self) -> AsyncIterator[dict[str, Any]]:
        """Return messages from activity results.

        Yields:
            Messages from the response queue
        """
        while self._response_queue:
            yield self._response_queue.popleft()

    async def close(self) -> None:
        """Close the transport.

        For Temporal transport, this is a no-op since we don't have
        subprocess resources to clean up.
        """
        self._ready = False
        self._response_queue.clear()

    def is_ready(self) -> bool:
        """Check if transport is ready.

        Returns:
            True if transport is connected and ready to use
        """
        return self._ready

    async def end_input(self) -> None:
        """Signal end of input.

        For Temporal transport, this is a no-op since we don't use
        streaming input from subprocess.
        """
        pass
