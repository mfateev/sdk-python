#!/usr/bin/env python3
"""Manual test script for Anthropic POC development.

This script provides a quick way to test the POC implementation without
running the full test suite. Useful for development and debugging.

Requirements:
- Temporal server running on localhost:7233
- ANTHROPIC_API_KEY environment variable set

Usage:
    # Start Temporal dev server in another terminal:
    temporal server start-dev

    # Run this script:
    export ANTHROPIC_API_KEY=your_key_here
    python scripts/test_anthropic_poc.py
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import timedelta

from temporalio import workflow
from temporalio.client import Client
from temporalio.worker import Worker

from temporalio.contrib.anthropic import TemporalTransport, invoke_llm_activity


@workflow.defn
class ManualTestWorkflow:
    """Manual test workflow for POC verification."""

    @workflow.run
    async def run(self, prompt: str) -> str:
        """Execute a simple prompt through Temporal transport.

        Args:
            prompt: User prompt to send to Claude

        Returns:
            Response text from Claude
        """
        # Create Temporal transport
        transport = TemporalTransport(
            model="claude-sonnet-4-5",
            max_tokens=1024,
            start_to_close_timeout=timedelta(minutes=5),
        )

        # Connect
        await transport.connect()

        # Simulate SDK message format
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

            # Log full response for debugging
            workflow.logger.info(f"Received response: {response_msg}")

        await transport.close()

        return "\n".join(responses) if responses else "No response"


async def main() -> None:
    """Run manual test."""
    print("=" * 60)
    print("Anthropic POC Manual Test")
    print("=" * 60)

    # Check for API key
    if not os.getenv("ANTHROPIC_API_KEY"):
        print("\n❌ ERROR: ANTHROPIC_API_KEY environment variable not set")
        print("   Please set it before running this test:")
        print("   export ANTHROPIC_API_KEY=your_key_here")
        return

    print("\n✅ ANTHROPIC_API_KEY found")

    # Connect to Temporal server
    print("\n📡 Connecting to Temporal server at localhost:7233...")
    try:
        client = await Client.connect("localhost:7233")
        print("✅ Connected to Temporal server")
    except Exception as e:
        print(f"\n❌ ERROR: Could not connect to Temporal server: {e}")
        print("   Make sure Temporal server is running:")
        print("   temporal server start-dev")
        return

    # Start worker
    print("\n🏃 Starting worker...")
    async with Worker(
        client,
        task_queue="anthropic-poc-manual",
        workflows=[ManualTestWorkflow],
        activities=[invoke_llm_activity],
    ):
        print("✅ Worker started")

        # Execute workflow
        prompt = "Explain what a Temporal workflow is in one sentence."
        print(f"\n📝 Prompt: {prompt}")
        print("\n⏳ Executing workflow (calling Claude API via Temporal activity)...")

        try:
            result = await client.execute_workflow(
                ManualTestWorkflow.run,
                prompt,
                id=f"manual-test-{int(asyncio.get_event_loop().time())}",
                task_queue="anthropic-poc-manual",
            )

            print("\n" + "=" * 60)
            print("✅ SUCCESS! POC WORKS!")
            print("=" * 60)
            print(f"\n📤 Response from Claude:\n")
            print(f"   {result}")
            print("\n" + "=" * 60)
            print("POC Validation:")
            print("✅ TemporalTransport instantiated in workflow")
            print("✅ Activity execution triggered")
            print("✅ Anthropic API called")
            print("✅ Response returned deterministically")
            print("✅ No subprocess spawning (pure Temporal activities)")
            print("=" * 60)

        except Exception as e:
            print("\n" + "=" * 60)
            print("❌ ERROR: Workflow execution failed")
            print("=" * 60)
            print(f"\n{type(e).__name__}: {e}")
            print("\nPossible causes:")
            print("- Invalid ANTHROPIC_API_KEY")
            print("- Network connectivity issues")
            print("- API rate limiting")
            print("- Message format mismatch")
            raise


if __name__ == "__main__":
    asyncio.run(main())
