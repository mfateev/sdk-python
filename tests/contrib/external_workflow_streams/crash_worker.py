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
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.client import Client
from temporalio.worker import Worker

with workflow.unsafe.imports_passed_through():
    from temporalio.contrib.external_workflow_streams._api import external_stream

STREAM_NAME = "tokens"

READ_LOG = "reads"
"""Key suffix under which a recording provider logs the offsets it read."""


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


def read_log_key(prefix: str, reader: str) -> str:
    return f"{prefix}:{READ_LOG}:{reader}"


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


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--task-queue", required=True)
    parser.add_argument("--redis-url", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--reader", default="crashed")
    args = parser.parse_args()

    client = await Client.connect(args.address, namespace=args.namespace)
    backend = recording_backend(
        url=args.redis_url, prefix=args.prefix, reader=args.reader
    )
    async with Worker(
        client,
        task_queue=args.task_queue,
        workflows=[CrashConsumeWorkflow],
        external_stream_backend=backend,
    ):
        # Until killed. There is deliberately no shutdown path here.
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
