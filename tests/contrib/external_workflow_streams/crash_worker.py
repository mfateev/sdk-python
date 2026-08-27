"""A Worker in a process of its own, so a test can kill it outright.

A crash is not a shutdown. Every in-process way of stopping a Worker runs the
graceful path -- which, for External Workflow Streams, is exactly the path that
*finalizes the annotation and writes the marker*. Testing "the Worker went away
before its marker committed" against a Worker that politely committed its marker
on the way out would assert the opposite of the case.

So the Worker under test runs here, in a child process that is sent ``SIGKILL``
with a Workflow Task retained. Nothing gets a chance to run.

Two things live in this module rather than in the test:

- the Workflow definition, because both processes must register the *same* one;
- a provider that records every offset it reads into Redis, because a crashed
  process cannot be asked afterwards what it had read. The read log is the only
  evidence that survives the kill, and comparing it with the replacement
  Worker's is what turns "the records arrived eventually" into "the same offsets
  were read again".

The provider import is deliberately **inside** a function: this module is
re-imported by the Workflow sandbox, and importing a provider from Workflow code
is refused outright.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.client import Client
from temporalio.worker import Worker

with workflow.unsafe.imports_passed_through():
    from temporalio.contrib.external_workflow_streams._api import external_stream
    from temporalio.contrib.external_workflow_streams._output_api import (
        external_output_stream,
    )

STREAM_NAME = "tokens"

READ_LOG = "reads"
"""Key suffix under which a recording provider logs the offsets it read."""

OUTPUT_STAGE_LOG = "output-stages"
"""Key suffix used to prove an output stage survived a process kill."""


@workflow.defn
class CrashConsumeWorkflow:
    """Consumes a fixed number of records and returns them, in order.

    Returning the values is what makes loss and duplication visible: a record
    re-delivered after the crash appears twice, and one lost never appears.
    """

    @workflow.run
    async def run(self, expected: int) -> list[str]:
        tokens = external_stream.topic(STREAM_NAME, type=str)
        seen: list[str] = []
        async for token in tokens.subscribe():
            seen.append(token)
            if len(seen) >= expected:
                break
        return seen


@workflow.defn
class CrashPublishWorkflow:
    """Publishes output on a Workflow Task whose Worker will be killed."""

    @workflow.run
    async def run(self) -> str:
        topic = external_output_stream.topic("events", type=str)
        await topic.publish("accepted-after-crash")
        await topic.finish()
        return "done"


@workflow.defn
class CrashUpdatePublishWorkflow:
    """Waits for an Update that publishes while its report is withheld."""

    def __init__(self) -> None:
        self._finished = False

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self._finished)

    @workflow.update
    async def publish(self, value: str) -> str:
        await external_output_stream.topic("events", type=str).publish(value)
        return value

    @workflow.signal
    def finish(self) -> None:
        self._finished = True


def read_log_key(prefix: str, reader: str) -> str:
    return f"{prefix}:{READ_LOG}:{reader}"


def output_stage_log_key(prefix: str) -> str:
    return f"{prefix}:{OUTPUT_STAGE_LOG}"


def recording_backend(*, url: str, prefix: str, reader: str) -> Any:
    """A Redis provider that appends every offset it reads to a Redis list.

    In Redis rather than in memory because the Worker doing the reading may be
    killed: whatever it recorded has to outlive the process.
    """
    from temporalio.contrib.external_workflow_streams._redis import RedisStreamBackend

    class ReadRecordingBackend(RedisStreamBackend):
        async def read_after(  # type: ignore[override]
            self,
            key: Any,
            after: Any,
            *,
            max_records: int,
            block: timedelta | None = None,
        ) -> Any:
            records = await super().read_after(
                key, after, max_records=max_records, block=block
            )
            await self._log(records)
            return records

        async def read_range(self, key: Any, first: Any, last: Any) -> Any:  # type: ignore[override]
            records = await super().read_range(key, first, last)
            await self._log(records)
            return records

        async def _log(self, records: Any) -> None:
            offsets = [str(r.offset) for r in records if r.offset is not None]
            if offsets:
                await self._client.rpush(read_log_key(prefix, reader), *offsets)

    return ReadRecordingBackend(url=url, key_prefix=prefix)


def blocking_output_backend(*, url: str, prefix: str) -> Any:
    """A Redis provider that stops only after output is durably staged.

    Blocking inside ``stage_output`` places the process at the exact boundary
    the crash tests need: Redis has atomically stored the pending stage, while
    the Worker has not returned the activation completion to Core/server.
    """
    from temporalio.contrib.external_workflow_streams._redis import RedisStreamBackend

    class DurablyStageThenBlockBackend(RedisStreamBackend):
        async def stage_output(self, manifest: Any, records: Any) -> Any:
            stage = await super().stage_output(manifest, records)
            await self._client.rpush(
                output_stage_log_key(prefix),
                json.dumps(
                    {
                        "run_id": manifest.run_id,
                        "stage_token": manifest.stage_token,
                        "sub_batch_id": manifest.sub_batch_id,
                    },
                    sort_keys=True,
                ),
            )
            # There is deliberately no release path. The parent sends SIGKILL,
            # so neither this call nor Worker shutdown can report completion.
            await asyncio.Event().wait()
            return stage

    return DurablyStageThenBlockBackend(url=url, key_prefix=prefix)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--task-queue", required=True)
    parser.add_argument("--redis-url", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--reader", default="crashed")
    parser.add_argument(
        "--mode",
        choices=("consume", "publish", "update"),
        default="consume",
    )
    args = parser.parse_args()

    client = await Client.connect(args.address, namespace=args.namespace)
    workflows: list[type[Any]]
    if args.mode == "consume":
        backend = recording_backend(
            url=args.redis_url, prefix=args.prefix, reader=args.reader
        )
        workflows = [CrashConsumeWorkflow]
    else:
        backend = blocking_output_backend(url=args.redis_url, prefix=args.prefix)
        workflows = (
            [CrashPublishWorkflow]
            if args.mode == "publish"
            else [CrashUpdatePublishWorkflow]
        )
    async with Worker(
        client,
        task_queue=args.task_queue,
        workflows=workflows,
        external_stream_backend=backend,
    ):
        # Until killed. There is deliberately no shutdown path here.
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
