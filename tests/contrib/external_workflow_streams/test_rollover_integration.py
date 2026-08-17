"""Workflow Task rollover, against a live server.

A retained Workflow Task is bounded by the server's Workflow Task timeout, and a
continuously fed stream whose inter-record gaps stay below the idle timeout never
reaches the idle parking path at all. Without rollover such a task does not
merely stay open too long -- it *fails*, and everything consumed on it is lost
with it.

The Core tests prove the deadline exists. What only a live server can show is
that it fires *while a stream is being fed*: that a task genuinely held open by
arriving records is handed on before the server times it out, that the
subscription survives that hand-off, and that inputs queued behind the retained
task -- Signals especially -- are released no later than the deadline.

Every test here stops short of the Workflow Task timeout on purpose. A retained
task that reaches it is not merely a failed assertion: the server schedules a
replacement, Core receives a Workflow Task for a run it still holds one for, and
the resulting ``dbg_panic`` takes the whole workflow-processing thread with it.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta
from typing import Any

import pytest

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.external_workflow_streams._backend import StreamKey
from temporalio.worker import Worker
from tests.contrib.external_workflow_streams.memory_backend import MemoryStreamBackend
from tests.contrib.external_workflow_streams.test_worker_integration import publish

with workflow.unsafe.imports_passed_through():
    from temporalio.contrib.external_workflow_streams._api import external_stream

TASK_TIMEOUT = timedelta(seconds=10)
"""The bound a retained task must stay inside, and what the deadline derives
from. Long enough that the rollover deadline is unambiguous, short enough that
these tests do not have to run for a minute."""

ROLLOVER_FRACTION = 0.8
"""Core arms the rollover deadline at this fraction of the Workflow Task
timeout, the same one the local-activity heartbeat uses."""

ROLLOVER_DEADLINE = TASK_TIMEOUT * ROLLOVER_FRACTION

FEED_GAP_SECONDS = 0.3
"""Well below the idle timeout, so the runtime never parks and the only thing
that can release the task is rollover."""

WAKE_SIGNAL_NAME = "__temporal_external_stream_wake"

BLOCKED_ON_ROLLOVER_ANCHOR = (
    "Core re-anchors the rollover deadline on every quiescence: "
    "begin_external_stream_quiescence calls start_wft_rollover_timer("
    "Instant::now(), ...), and start_wft_rollover_timer cancels the pending "
    "one first. A record therefore pushes the deadline out, a continuously fed "
    "stream re-establishes quiescence after every delivery, and the deadline "
    "never arrives -- the retained task runs until the server times it out. "
    "The deadline has to be anchored at the Workflow Task's start time, the way "
    "restart_external_stream_deadlines already anchors it"
)


@workflow.defn
class SteadyStreamWorkflow:
    """Consumes records until it has the number it was asked for.

    Nothing else it does can end a Workflow Task: no timers, no activities, and
    the gaps the test feeds are far under the idle timeout, so retention is only
    ever released by rollover.
    """

    def __init__(self) -> None:
        self._seen = 0

    @workflow.run
    async def run(self, expected: int) -> int:
        tokens = external_stream.topic("tokens", backend="tokens-memory", type=str)
        async for _ in tokens.subscribe():
            self._seen += 1
            if self._seen >= expected:
                break
        return self._seen

    @workflow.query
    def seen(self) -> int:
        return self._seen


@workflow.defn
class SignalledStreamWorkflow:
    """Consumes a stream until a Signal arrives, then reports when it did.

    The Signal is sent while the Workflow Task is retained, so the server has to
    hold it until that task completes. What it returns is *Workflow* time, taken
    the moment the handler ran, which is what makes "no later than the rollover
    deadline" measurable from the outside rather than inferred from wall clock.
    """

    def __init__(self) -> None:
        self._signalled: float | None = None
        self._started: float | None = None

    @workflow.run
    async def run(self) -> float:
        tokens = external_stream.topic("tokens", backend="tokens-memory", type=str)
        self._started = workflow.now().timestamp()
        subscription = tokens.subscribe()

        async def consume() -> None:
            async for _ in subscription:
                pass

        consumer = asyncio.ensure_future(consume())
        await workflow.wait_condition(lambda: self._signalled is not None)
        consumer.cancel()
        assert self._signalled is not None and self._started is not None
        return self._signalled - self._started

    @workflow.signal
    def poke(self) -> None:
        self._signalled = workflow.now().timestamp()


@pytest.fixture
def backend() -> MemoryStreamBackend:
    return MemoryStreamBackend()


async def stream_key_for(client: Client, handle: Any) -> StreamKey:
    description = await handle.describe()
    return StreamKey(
        client.namespace,
        handle.id,
        description.raw_description.workflow_execution_info.first_run_id,
        "tokens",
    )


async def history(handle: Any) -> list[Any]:
    return [e async for e in handle.fetch_history_events()]


def completed_tasks(events: list[Any]) -> list[Any]:
    return [e for e in events if e.HasField("workflow_task_completed_event_attributes")]


def timed_out_tasks(events: list[Any]) -> list[Any]:
    return [e for e in events if e.HasField("workflow_task_timed_out_event_attributes")]


async def feed(
    backend: MemoryStreamBackend,
    key: StreamKey,
    session: str,
    *,
    count: int,
    stop: asyncio.Event | None = None,
) -> None:
    """Appends one record at a time, with gaps well under the idle timeout.

    One session with a continuous sequence, because ``(session_id, sequence)``
    is the append idempotency key: restarting the numbering re-uses a key with
    different content, the backend rejects it, and the symptom is a Workflow
    that appears to hang far from the cause.
    """
    for i in range(count):
        if stop is not None and stop.is_set():
            return
        await publish(backend, key, [f"t{i}"], session=session)
        await asyncio.sleep(FEED_GAP_SECONDS)


# --- case 19 ------------------------------------------------------------------


@pytest.mark.xfail(strict=True, reason=BLOCKED_ON_ROLLOVER_ANCHOR)
@pytest.mark.timeout(180)
async def test_a_continuously_fed_stream_survives_a_rollover(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """The case rollover exists for, fed for longer than one task may live.

    Three things have to hold at once, and each fails differently:

    - the deadline has to fire *while records are arriving*, which is the only
      state in which the idle timer never will;
    - the replacement task has to inherit the subscription and its cursor, so
      the count the Workflow ends with is exactly what was published;
    - no task may run past the Workflow Task timeout, asserted against the
      server's own event timestamps rather than inferred from the Workflow
      finishing.

    The check is made *before* the timeout could be reached, so a broken
    rollover fails here rather than as a timed-out task and the Core panic that
    follows one.
    """
    total = 30  # 30 * 0.3s = 9s of feeding, past a deadline at 8s.
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[SteadyStreamWorkflow],
        external_stream_backends={"tokens-memory": backend},
    ):
        handle = await client.start_workflow(
            SteadyStreamWorkflow.run,
            total,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
            task_timeout=TASK_TIMEOUT,
        )
        key = await stream_key_for(client, handle)
        stop = asyncio.Event()
        feeder = asyncio.ensure_future(
            feed(backend, key, f"steady-{uuid.uuid4()}", count=total, stop=stop)
        )
        try:
            # A margin past the deadline, and still short of the timeout: by now
            # a working rollover has completed at least one task.
            await asyncio.sleep(ROLLOVER_DEADLINE.total_seconds() + 1)
            events = await history(handle)
            completed = completed_tasks(events)
            assert completed, (
                "the stream was fed continuously for longer than the rollover "
                f"deadline of {ROLLOVER_DEADLINE} and not one Workflow Task has "
                "completed, so the retained task is on its way to being timed "
                "out by the server -- which is the failure rollover exists to "
                "prevent"
            )

            result = await asyncio.wait_for(handle.result(), 90)
            assert result == total, (
                "the replacement task did not inherit the subscription's "
                f"cursor: {result} of {total} records were consumed"
            )

            events = await history(handle)
            assert not timed_out_tasks(events), (
                "a Workflow Task was held past the server's timeout, so "
                "rollover did not bound it"
            )
            started = {
                e.event_id: e
                for e in events
                if e.HasField("workflow_task_started_event_attributes")
            }
            for event in completed_tasks(events):
                start = started[
                    event.workflow_task_completed_event_attributes.started_event_id
                ]
                held = event.event_time.ToDatetime() - start.event_time.ToDatetime()
                assert held < TASK_TIMEOUT, (
                    f"a Workflow Task was held for {held}, past the "
                    f"{TASK_TIMEOUT} it must stay inside"
                )
        finally:
            stop.set()
            feeder.cancel()
            await handle.terminate()


# --- case 21 ------------------------------------------------------------------


@pytest.mark.xfail(strict=True, reason=BLOCKED_ON_ROLLOVER_ANCHOR)
@pytest.mark.timeout(180)
async def test_a_signal_into_a_retained_task_lands_by_the_rollover_deadline(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """Retention delays Signals, and rollover is the only thing bounding that.

    While a Workflow Task is retained the server cannot start another one, so
    Signals, Updates, and non-legacy Queries queue behind it -- and a stream can
    hold a task open far longer than an outstanding local activity ever would.
    Retention latency for those inputs is therefore bounded by the rollover
    deadline and by nothing else.

    Both halves are asserted, because either alone is satisfiable by accident:
    that the task really was retained when the Signal was sent -- no Workflow
    Task had completed, so the Signal had nothing to be delivered on -- and that
    the Workflow saw it within the deadline, measured in *Workflow* time from
    the Run's own start.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[SignalledStreamWorkflow],
        external_stream_backends={"tokens-memory": backend},
    ):
        handle = await client.start_workflow(
            SignalledStreamWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
            task_timeout=TASK_TIMEOUT,
        )
        started_at = asyncio.get_running_loop().time()
        key = await stream_key_for(client, handle)
        stop = asyncio.Event()
        feeder = asyncio.ensure_future(
            feed(backend, key, f"signalled-{uuid.uuid4()}", count=40, stop=stop)
        )
        try:
            # Long enough for the stream to be driving the Workflow, far short
            # of the deadline.
            await asyncio.sleep(2)
            assert not completed_tasks(await history(handle)), (
                "a Workflow Task had already completed, so the Signal below "
                "would not be sent into a retained one and this proves nothing"
            )

            await handle.signal(SignalledStreamWorkflow.poke)
            # Waited out only as far as the deadline, never as far as the
            # Workflow Task timeout: a retained task that reaches that is a
            # panicking Core, not a failed assertion.
            waited = asyncio.get_running_loop().time() - started_at
            try:
                elapsed = await asyncio.wait_for(
                    handle.result(), ROLLOVER_DEADLINE.total_seconds() + 1 - waited
                )
            except asyncio.TimeoutError:
                raise AssertionError(
                    "the Signal had still not been delivered by the rollover "
                    f"deadline of {ROLLOVER_DEADLINE}: the Workflow Task is "
                    "still retained, and nothing but that deadline bounds how "
                    "long an input queues behind it"
                ) from None

            assert elapsed <= ROLLOVER_DEADLINE.total_seconds(), (
                f"the Signal was delivered {elapsed:.1f}s into the Run, past the "
                f"rollover deadline of {ROLLOVER_DEADLINE}. Retention latency is "
                "bounded by that deadline and by nothing else"
            )
            events = await history(handle)
            assert not timed_out_tasks(events), (
                "the Signal was released by the server timing the retained task "
                "out rather than by a rollover"
            )
        finally:
            stop.set()
            feeder.cancel()
            try:
                await handle.terminate()
            except Exception:
                pass


# --- case 23 ------------------------------------------------------------------


@pytest.mark.xfail(strict=True, reason=BLOCKED_ON_ROLLOVER_ANCHOR)
@pytest.mark.timeout(180)
async def test_an_append_after_a_rollover_completion_wakes_the_subscription(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """A rollover leaves subscriptions active but unparked, like a command does.

    There is no retained task to notify locally and no park generation for a
    producer to observe, so the consumer's own watcher is what covers the
    window: it observes ``NoOpenWorkflowTask`` and sends the reserved wake
    Signal itself.

    The Signal is looked for in the Workflow's own History rather than inferred
    from the record eventually arriving -- after a rollover the server has
    already scheduled a replacement task, so a record that turned up could just
    as well have been picked up by that.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    session = f"rollover-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[SteadyStreamWorkflow],
        external_stream_backends={"tokens-memory": backend},
    ):
        handle = await client.start_workflow(
            SteadyStreamWorkflow.run,
            40,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
            task_timeout=TASK_TIMEOUT,
        )
        key = await stream_key_for(client, handle)
        stop = asyncio.Event()
        feeder = asyncio.ensure_future(feed(backend, key, session, count=30, stop=stop))
        try:
            await asyncio.sleep(ROLLOVER_DEADLINE.total_seconds() + 1)
            stop.set()
            feeder.cancel()

            events = await history(handle)
            assert completed_tasks(events), (
                "no Workflow Task completed while the stream was being fed, so "
                "no rollover happened and there is no post-rollover window to "
                "append into"
            )
            assert not timed_out_tasks(events), (
                "the task was released by the server's timeout rather than by a "
                "rollover"
            )
            before = len(
                [
                    e
                    for e in events
                    if e.HasField("workflow_execution_signaled_event_attributes")
                ]
            )

            await publish(backend, key, ["after-rollover"], session=session)

            async def a_wake_arrived() -> bool:
                signals = [
                    e
                    for e in await history(handle)
                    if e.HasField("workflow_execution_signaled_event_attributes")
                    and e.workflow_execution_signaled_event_attributes.signal_name
                    == WAKE_SIGNAL_NAME
                ]
                return len(signals) > before

            deadline = asyncio.get_running_loop().time() + 20
            while asyncio.get_running_loop().time() < deadline:
                if await a_wake_arrived():
                    return
                await asyncio.sleep(0.2)
            raise AssertionError(
                "no wake Signal followed the append: after a rollover "
                "completion there is no retained task to notify and no park "
                "generation to observe, so the watcher owes one"
            )
        finally:
            stop.set()
            feeder.cancel()
            try:
                await handle.terminate()
            except Exception:
                pass
