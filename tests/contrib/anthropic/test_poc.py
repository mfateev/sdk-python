"""POC test for Anthropic integration.

This test validates the core Transport replacement pattern works with Temporal.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from temporalio import workflow
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from temporalio.contrib.anthropic import TemporalTransport, invoke_llm_activity


@workflow.defn
class SimpleChatWorkflow:
    """Minimal workflow for POC testing.

    This workflow demonstrates the basic Transport replacement pattern:
    1. Create TemporalTransport
    2. Use it with Claude Agent SDK
    3. Get deterministic response through Temporal activity
    """

    @workflow.run
    async def run(self, prompt: str) -> str:
        """Run a simple chat workflow.

        Args:
            prompt: User prompt

        Returns:
            Response text from Claude
        """
        # Create Temporal transport
        transport = TemporalTransport(
            model="claude-sonnet-4-5",
            max_tokens=1024,
            start_to_close_timeout=timedelta(minutes=5),
        )

        # For POC: Direct transport testing without full SDK
        # This validates the activity execution works
        await transport.connect()

        # Simulate SDK message format
        import json

        message = {
            "type": "message",
            "role": "user",
            "content": [{"type": "text", "text": prompt}],
        }

        # Write message (triggers activity)
        await transport.write(json.dumps(message))

        # Read response
        responses = []
        async for response_msg in transport.read_messages():
            # Extract text from response
            if "content" in response_msg:
                for block in response_msg["content"]:
                    if block.get("type") == "text":
                        responses.append(block.get("text", ""))

        await transport.close()

        return "\n".join(responses) if responses else "No response"


@pytest.mark.asyncio
async def test_simple_transport():
    """Test basic Transport -> Activity -> API flow.

    This test validates:
    1. TemporalTransport can be instantiated in workflow
    2. write() calls execute_activity correctly
    3. Activity calls Anthropic API
    4. Response is returned and parseable
    5. Workflow completes successfully
    """
    # Note: This test requires ANTHROPIC_API_KEY environment variable
    async with await WorkflowEnvironment.start_time_skipping() as env:
        client = env.client

        async with Worker(
            client,
            task_queue="test-anthropic-poc",
            workflows=[SimpleChatWorkflow],
            activities=[invoke_llm_activity],
        ):
            # Execute workflow
            result = await client.execute_workflow(
                SimpleChatWorkflow.run,
                "Say 'Hello from Temporal!' and nothing else.",
                id="test-simple-transport",
                task_queue="test-anthropic-poc",
            )

            # Verify
            assert result is not None
            assert len(result) > 0
            assert "temporal" in result.lower() or "hello" in result.lower()
            print(f"✅ POC Test Passed! Response: {result}")


@workflow.defn
class LifecycleWorkflow:
    """Workflow for testing transport lifecycle."""

    @workflow.run
    async def run(self) -> bool:
        transport = TemporalTransport()

        # Should not be ready initially
        assert not transport.is_ready()

        # Connect
        await transport.connect()
        assert transport.is_ready()

        # Close
        await transport.close()
        assert not transport.is_ready()

        return True


@pytest.mark.asyncio
async def test_transport_lifecycle():
    """Test transport lifecycle methods.

    Validates:
    1. Transport starts not ready
    2. connect() makes it ready
    3. close() makes it not ready
    """
    async with await WorkflowEnvironment.start_time_skipping() as env:
        client = env.client

        async with Worker(
            client,
            task_queue="test-lifecycle",
            workflows=[LifecycleWorkflow],
            activities=[invoke_llm_activity],
        ):
            result = await client.execute_workflow(
                LifecycleWorkflow.run,
                id="test-lifecycle",
                task_queue="test-lifecycle",
            )

            assert result is True
            print("✅ Lifecycle Test Passed!")


if __name__ == "__main__":
    # Manual test for development
    import asyncio

    print("Running POC tests...")
    print("\n=== Test 1: Transport Lifecycle ===")
    asyncio.run(test_transport_lifecycle())
    print("\n=== Test 2: Simple Transport (requires ANTHROPIC_API_KEY) ===")
    asyncio.run(test_simple_transport())
    print("\n=== All POC Tests Passed! ===")
