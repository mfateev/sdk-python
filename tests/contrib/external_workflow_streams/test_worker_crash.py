"""A Worker that dies before its marker commits, and the one that takes over.

"Marker recording commits the cursor. Reading or delivering a record does not."
The unit tests assert the *consequence* of that inside one manager -- a reset
cursor re-delivers -- but the property is about two Workers and a Workflow Task
that never completed, and nothing in one process can show that.

So the consuming Worker here runs in a child process and is sent ``SIGKILL``
while it holds a retained Workflow Task with records consumed on it. No
shutdown, no finalization, no marker. The server times the task out, a second
Worker picks up the retry, and the question is whether it resumes from the last
*committed* cursor -- re-reading everything consumed after it, and nothing
before it.
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from temporalio.client import Client
from temporalio.contrib.external_workflow_streams._backend import StreamKey
from temporalio.contrib.external_workflow_streams._record import BEGINNING
from temporalio.worker import Worker
from tests.contrib.external_workflow_streams.conftest import KEY_NAMESPACE, redis_url
from tests.contrib.external_workflow_streams.crash_worker import (
    BACKEND_NAME,
    STREAM_NAME,
    CrashConsumeWorkflow,
    read_log_key,
    recording_backend,
)
from tests.contrib.external_workflow_streams.test_worker_integration import publish

TASK_TIMEOUT_SECONDS = 6
"""Short, because the crash is only visible once the server gives up on the
retained task, and that wait is this test's floor."""

FEED_GAP_SECONDS = 0.3
"""Well under the idle timeout, so the retained task is never released.

An idle park would complete the task and *commit* the records fed here, which is
exactly the state this test needs not to reach.
"""

UNCOMMITTED = ["alpha", "beta", "gamma"]
"""Consumed on the retained task the Worker dies holding. Never committed."""

LATER = ["delta"]
"""Published to the replacement Worker, so the Run demonstrably continues."""

REPO_ROOT = Path(__file__).resolve().parents[3]


async def _reads(redis_client: Any, prefix: str, reader: str) -> list[str]:
    raw = await redis_client.lrange(read_log_key(prefix, reader), 0, -1)
    return [value.decode() if isinstance(value, bytes) else value for value in raw]


async def _history(handle: Any) -> list[Any]:
    return [e async for e in handle.fetch_history_events()]


def _markers(events: list[Any]) -> list[Any]:
    return [e for e in events if e.HasField("marker_recorded_event_attributes")]


async def _wait_until(predicate: Any, timeout: float, message: str) -> None:
    """Polls rather than sleeping a fixed time, so a slow start is not a flake."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if await predicate():
            return
        await asyncio.sleep(0.2)
    raise AssertionError(message)


@pytest.mark.timeout(240)
async def test_a_crash_before_the_marker_makes_the_next_worker_re_read(
    client: Client, redis_worker_id: str
) -> None:
    """The durability boundary, with a real dead Worker on one side of it.

    Three things have to hold together, and each fails differently:

    - **Nothing was committed.** The records were consumed on a Workflow Task
      that was retained -- so its annotation was still accumulating, unwritten --
      and that task died with its Worker. Asserted against History, because a
      marker written before the kill would make the rest of this vacuous.
    - **The same offsets are read again.** The replacement Worker's read log has
      to contain them. Comparing read logs rather than observing that records
      eventually arrived is the difference between proving a re-read and proving
      that Redis still had the data.
    - **Nothing is lost and nothing is doubled.** The Workflow returns its
      records in order, so a record delivered twice or dropped is visible in the
      result rather than only in a cursor.
    """
    pytest.importorskip("redis.asyncio", reason="redis is not installed")

    prefix = f"{KEY_NAMESPACE}:{redis_worker_id}:{uuid.uuid4().hex}"
    publisher = recording_backend(url=redis_url(), prefix=prefix, reader="publisher")
    try:
        await publisher._client.ping()
    except Exception as err:
        await publisher.aclose()
        pytest.skip(f"Redis is not reachable at {redis_url()}: {err}")

    task_queue = f"tq-{uuid.uuid4()}"
    successor = recording_backend(url=redis_url(), prefix=prefix, reader="successor")
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "tests.contrib.external_workflow_streams.crash_worker",
        "--address",
        client.service_client.config.target_host,
        "--namespace",
        client.namespace,
        "--task-queue",
        task_queue,
        "--redis-url",
        redis_url(),
        "--prefix",
        prefix,
        cwd=str(REPO_ROOT),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    handle = None
    try:
        expected = len(UNCOMMITTED) + len(LATER)
        handle = await client.start_workflow(
            CrashConsumeWorkflow.run,
            expected,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
            task_timeout=timedelta(seconds=TASK_TIMEOUT_SECONDS),
        )
        description = await handle.describe()
        key = StreamKey(
            client.namespace,
            handle.id,
            description.raw_description.workflow_execution_info.first_run_id,
            STREAM_NAME,
        )

        # The child has to be the one holding the task, or the records would
        # simply be waiting for the successor and nothing would be read twice.
        async def child_took_the_task() -> bool:
            return any(
                e.HasField("workflow_task_started_event_attributes")
                for e in await _history(handle)
            )

        await _wait_until(
            child_took_the_task, 60, "the child Worker never picked the task up"
        )

        # One producer session, one continuous sequence: `(session_id,
        # sequence)` is the append idempotency key, so restarting the numbering
        # would re-use a key with different content, the backend would reject
        # it, and the Workflow would appear to hang for reasons far from here.
        session = f"crash-{uuid.uuid4()}"
        for value in UNCOMMITTED:
            await publish(publisher, key, [value], session=session)
            await asyncio.sleep(FEED_GAP_SECONDS)

        async def child_read_them() -> bool:
            read = await _reads(publisher._client, prefix, "crashed")
            return len(read) >= len(UNCOMMITTED)

        await _wait_until(
            child_read_them,
            30,
            "the child Worker never read the records it is supposed to die "
            "holding, so there is nothing for the successor to re-read",
        )
        consumed = await publisher.read_after(
            key, BEGINNING, max_records=100, block=None
        )
        uncommitted_offsets = [str(r.offset) for r in consumed]

        # The kill. Nothing runs: no finalization, no marker, no wake.
        child.kill()
        await child.wait()

        assert not _markers(await _history(handle)), (
            "a marker was written before the crash, so the records the Worker "
            "died holding were committed after all and the re-read below would "
            "prove nothing -- the Workflow Task must still have been retained"
        )

        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[CrashConsumeWorkflow],
            external_stream_backends={BACKEND_NAME: successor},
        ):
            await _wait_until(
                lambda: _read_anything(publisher._client, prefix, "successor"),
                120,
                "the replacement Worker never read the stream at all",
            )
            await publish(publisher, key, LATER, session=session)
            result = await asyncio.wait_for(handle.result(), 120)

        successor_reads = await _reads(publisher._client, prefix, "successor")
        assert set(uncommitted_offsets) <= set(successor_reads), (
            "the replacement Worker did not re-read the offsets the dead one "
            "had already taken -- an uncommitted consumption was treated as "
            f"progress. re-read: {successor_reads}, owed: {uncommitted_offsets}"
        )
        assert result == UNCOMMITTED + LATER, (
            "the Run lost or repeated a record across the crash. A marker "
            "commits the cursor and only the first record's did, so everything "
            f"after it was owed again and exactly once, got {result}"
        )

        assert any(
            e.HasField("workflow_task_timed_out_event_attributes")
            for e in await _history(handle)
        ), (
            "no Workflow Task timed out, so the retained task was handed over "
            "rather than dying with its Worker -- which is not the case under "
            "test"
        )
    finally:
        if child.returncode is None:
            child.kill()
            await child.wait()
        if handle is not None:
            try:
                await handle.terminate()
            except Exception:
                pass
        keys = [k async for k in publisher._client.scan_iter(match=f"{prefix}*")]
        if keys:
            await publisher._client.delete(*keys)
        await publisher.aclose()
        await successor.aclose()


async def _read_anything(redis_client: Any, prefix: str, reader: str) -> bool:
    return bool(await _reads(redis_client, prefix, reader))
