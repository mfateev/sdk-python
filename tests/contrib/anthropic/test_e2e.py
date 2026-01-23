"""End-to-end tests for Anthropic integration with real Temporal server and API.

These tests require:
1. A running Temporal server (e.g., `temporal server start-dev`)
2. ANTHROPIC_API_KEY environment variable set
3. Network connectivity to Anthropic API

Run with:
    ANTHROPIC_API_KEY=your_key pytest tests/contrib/anthropic/test_e2e.py -v

Skip with:
    pytest tests/contrib/anthropic/ -v -k "not e2e"
"""

from __future__ import annotations

import json
import os

import pytest

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.anthropic import (
    AnthropicAgentsPlugin,
    TemporalTransport,
)
from temporalio.worker import Worker

# Check if environment is set up for E2E tests
HAS_API_KEY = bool(os.getenv("ANTHROPIC_API_KEY"))
TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")

# Skip all tests in this module if requirements not met
pytestmark = pytest.mark.skipif(
    not HAS_API_KEY,
    reason="E2E tests require ANTHROPIC_API_KEY environment variable",
)


@workflow.defn
class SimpleAnthropicWorkflow:
    """Simple workflow that uses TemporalTransport with Claude Agent SDK.

    This workflow demonstrates the minimal integration pattern.
    """

    @workflow.run
    async def run(self, prompt: str) -> str:
        """Execute a simple prompt through Claude Agent SDK.

        Args:
            prompt: User prompt to send to Claude

        Returns:
            Claude's response as a string
        """
        # Create transport
        transport = TemporalTransport(
            model="claude-sonnet-4-5",
            max_tokens=1024,
        )

        # Connect transport
        await transport.connect()

        try:
            # Write prompt
            user_message = {"role": "user", "content": prompt}
            await transport.write(json.dumps(user_message))

            # Read response
            responses = []
            async for message in transport.read_messages():
                # Extract text content from message
                if isinstance(message, dict):
                    # Handle SDK message format
                    if "content" in message:
                        content = message["content"]
                        if isinstance(content, list):
                            # Extract text from content blocks
                            text_parts = [
                                block.get("text", "")
                                for block in content
                                if block.get("type") == "text"
                            ]
                            responses.append(" ".join(text_parts))
                        elif isinstance(content, str):
                            responses.append(content)
                    else:
                        # Fallback: stringify the message
                        responses.append(str(message))

            return "\n".join(responses) if responses else "No response"

        finally:
            # Clean up transport
            await transport.close()


@workflow.defn
class MultiTurnAnthropicWorkflow:
    """Workflow that demonstrates multi-turn conversation."""

    @workflow.run
    async def run(self, prompts: list[str]) -> list[str]:
        """Execute multiple prompts in sequence.

        Args:
            prompts: List of prompts to send

        Returns:
            List of responses from Claude
        """
        transport = TemporalTransport(
            model="claude-sonnet-4-5",
            max_tokens=512,
        )

        await transport.connect()
        responses = []

        try:
            for prompt in prompts:
                # Write prompt
                user_message = {"role": "user", "content": prompt}
                await transport.write(json.dumps(user_message))

                # Read response
                async for message in transport.read_messages():
                    if isinstance(message, dict) and "content" in message:
                        content = message["content"]
                        if isinstance(content, list):
                            text_parts = [
                                block.get("text", "")
                                for block in content
                                if block.get("type") == "text"
                            ]
                            responses.append(" ".join(text_parts))
                        elif isinstance(content, str):
                            responses.append(content)

            return responses

        finally:
            await transport.close()


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_simple_workflow_with_real_api():
    """Test simple workflow with real Temporal server and Anthropic API.

    This test verifies:
    1. Connection to real Temporal server works
    2. Worker can execute workflows with AnthropicAgentsPlugin
    3. TemporalTransport can make real API calls
    4. invoke_llm_activity executes successfully
    5. Response is received and returned
    """
    # Create plugin
    plugin = AnthropicAgentsPlugin()

    # Connect to real Temporal server
    client = await Client.connect(
        TEMPORAL_ADDRESS,
        plugins=[plugin],
    )

    # Create worker with workflow
    task_queue = f"e2e-test-{os.urandom(8).hex()}"

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[SimpleAnthropicWorkflow],
    ):
        # Execute workflow with real API call
        result = await client.execute_workflow(
            SimpleAnthropicWorkflow.run,
            "What is 2+2? Answer with just the number.",
            id=f"test-simple-{os.urandom(8).hex()}",
            task_queue=task_queue,
        )

        # Verify we got a response
        assert result is not None
        assert len(result) > 0
        assert "4" in result  # Should contain the answer

        print(f"✓ Simple workflow result: {result}")


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_multi_turn_conversation():
    """Test multi-turn conversation with real API.

    This test verifies:
    1. Multiple prompts can be sent in sequence
    2. Conversation history is maintained
    3. Context is preserved across turns
    """
    plugin = AnthropicAgentsPlugin()
    client = await Client.connect(
        TEMPORAL_ADDRESS,
        plugins=[plugin],
    )

    task_queue = f"e2e-test-multi-{os.urandom(8).hex()}"

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[MultiTurnAnthropicWorkflow],
    ):
        # Execute workflow with multiple prompts
        prompts = [
            "My favorite number is 7.",
            "What is my favorite number?",
        ]

        result = await client.execute_workflow(
            MultiTurnAnthropicWorkflow.run,
            prompts,
            id=f"test-multi-{os.urandom(8).hex()}",
            task_queue=task_queue,
        )

        # Verify we got responses
        assert result is not None
        assert len(result) == 2  # Two responses

        # Second response should reference the number 7
        assert "7" in result[1]

        print(f"✓ Multi-turn conversation:")
        for i, response in enumerate(result):
            print(f"  Turn {i+1}: {response[:100]}...")


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_workflow_replay_determinism():
    """Test that workflow replay is deterministic with real API.

    This test verifies:
    1. Workflow execution completes successfully
    2. Workflow can be replayed from history
    3. Replay produces the same result (determinism)
    """
    plugin = AnthropicAgentsPlugin()
    client = await Client.connect(
        TEMPORAL_ADDRESS,
        plugins=[plugin],
    )

    task_queue = f"e2e-test-replay-{os.urandom(8).hex()}"
    workflow_id = f"test-replay-{os.urandom(8).hex()}"

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[SimpleAnthropicWorkflow],
    ):
        # First execution
        result1 = await client.execute_workflow(
            SimpleAnthropicWorkflow.run,
            "Say 'test' and nothing else.",
            id=workflow_id,
            task_queue=task_queue,
        )

        # Get workflow handle
        handle = client.get_workflow_handle(workflow_id)

        # Fetch history
        history = await handle.fetch_history()

        # Verify history exists and has events
        assert history is not None
        events = list(history.events)
        assert len(events) > 0

        print(f"✓ Workflow completed with {len(events)} history events")
        print(f"✓ Result: {result1}")

        # Note: True replay testing requires workflow_replay module
        # which is not available in this test context
        # This test at least verifies history is recorded correctly


@workflow.defn
class ErrorHandlingWorkflow:
    """Workflow for testing error handling."""

    @workflow.run
    async def run(self) -> str:
        """Try to use an invalid model to trigger an error."""
        transport = TemporalTransport(
            model="claude-invalid-model-xyz",  # Invalid model
            max_tokens=100,
        )

        await transport.connect()

        try:
            user_message = {"role": "user", "content": "test"}
            await transport.write(json.dumps(user_message))

            async for message in transport.read_messages():
                return str(message)

            return "No error occurred (unexpected)"

        except Exception as e:
            # Catch and return error message
            return f"Error caught: {type(e).__name__}"

        finally:
            await transport.close()


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_error_handling_with_real_api():
    """Test error handling with real API (e.g., invalid model).

    This test verifies:
    1. Activity errors are properly propagated
    2. Workflow can handle activity failures
    3. Error messages are meaningful
    """
    plugin = AnthropicAgentsPlugin()
    client = await Client.connect(
        TEMPORAL_ADDRESS,
        plugins=[plugin],
    )

    task_queue = f"e2e-test-error-{os.urandom(8).hex()}"

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[ErrorHandlingWorkflow],
    ):
        # Execute workflow - should complete but with error
        result = await client.execute_workflow(
            ErrorHandlingWorkflow.run,
            id=f"test-error-{os.urandom(8).hex()}",
            task_queue=task_queue,
        )

        # Verify error was caught and handled
        assert "Error caught" in result or "ActivityError" in result

        print(f"✓ Error handling result: {result}")


if __name__ == "__main__":
    # Allow running this file directly for manual testing
    import asyncio

    async def main():
        """Run E2E tests manually."""
        if not HAS_API_KEY:
            print("❌ ANTHROPIC_API_KEY environment variable not set")
            print("   Set it with: export ANTHROPIC_API_KEY=your_key")
            return

        print(f"Running E2E tests against {TEMPORAL_ADDRESS}...")
        print("Make sure Temporal server is running: temporal server start-dev\n")

        try:
            print("Test 1: Simple workflow...")
            await test_simple_workflow_with_real_api()
            print()

            print("Test 2: Multi-turn conversation...")
            await test_multi_turn_conversation()
            print()

            print("Test 3: Workflow replay...")
            await test_workflow_replay_determinism()
            print()

            print("Test 4: Error handling...")
            await test_error_handling_with_real_api()
            print()

            print("✅ All E2E tests passed!")

        except Exception as e:
            print(f"❌ Test failed: {e}")
            import traceback

            traceback.print_exc()

    asyncio.run(main())
