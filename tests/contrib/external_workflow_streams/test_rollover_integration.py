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

PYTEST_DONT_REWRITE: sandboxed fixture Workflows re-import this module, so pytest's
injected imports would make sandbox validation depend on pytest's import locks.
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
from temporalio.service import RPCError, RPCStatusCode
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

ROLLOVER_DELIVERY_TOLERANCE = timedelta(milliseconds=500)
"""How far past the deadline a released input may land and still count as bounded by it.

The deadline is what Core aims the replacement Workflow Task at, not an instant
the server can hit. What a test can measure is the gap between two
``WorkflowTaskStarted`` event times, and between those sit Core's completion
RPC, the server scheduling the replacement, and a poll picking it up. Measured
overheads here are 16-65ms, so half a second is most of an order of magnitude of
headroom.

It is deliberately far short of ``TASK_TIMEOUT - ROLLOVER_DEADLINE``, which is
two seconds: an input released because the *server* timed the retained task out
lands at about 10s and still fails the assertion, and telling that apart from a
rollover is the only thing the assertion is for.
"""

POST_ROLLOVER_PAUSE = timedelta(seconds=10)
"""How long the post-rollover Workflow Task boundary is held open for.

Long enough for an append and the wake it owes to be observed in History
without waiting on anything else, short enough that the test finishes.
"""


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


@workflow.defn
class RolloverThenPauseWorkflow:
    """Consumes across a rollover, then pauses on a Timer and consumes once more.

    The pause is what makes the post-rollover window addressable. A completion
    carrying a server-bound command cannot ask for retention, so the Workflow
    Task ends there with the subscription still active and still unparked --
    which is the state a rollover leaves behind, held still. The rollover's own
    window is not addressable from outside the process: Core completes a
    rollover with ``force_new_wft``, and the server has a replacement Workflow
    Task started within a millisecond of it.
    """

    def __init__(self) -> None:
        self._seen = 0

    @workflow.run
    async def run(self, before_pause: int) -> int:
        tokens = external_stream.topic("tokens", backend="tokens-memory", type=str)
        subscription = tokens.subscribe()
        iterator = subscription.__aiter__()

        while self._seen < before_pause:
            await iterator.__anext__()
            self._seen += 1

        # A real Timer: the completion that carries it cannot be retained.
        await asyncio.sleep(POST_ROLLOVER_PAUSE.total_seconds())

        # Whatever was appended into that window. That it arrives at all proves
        # only that it was not lost -- the Timer creates a Workflow Task of its
        # own when it fires, and that task would find a buffered record anyway.
        # What proves the wake is the Signal in History, which the test asserts.
        await iterator.__anext__()
        self._seen += 1
        return self._seen


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


async def stop_if_running(handle: Any) -> None:
    """Terminates the Workflow, unless it has already finished on its own.

    Every test here terminates in a ``finally``, because a Workflow left running
    keeps a Worker's Run alive past the test that owns it. A test whose whole
    point is that the Workflow *finishes* has nothing left to terminate, though,
    and the server answers that with ``NOT_FOUND`` -- an error about the
    cleanup, raised after the assertions have all passed, which reads exactly
    like the feature having failed. Only that one status is swallowed: anything
    else is a real failure of the teardown and is left to surface.
    """
    try:
        await handle.terminate()
    except RPCError as err:
        if err.status is not RPCStatusCode.NOT_FOUND:
            raise


def timers_started(events: list[Any]) -> list[Any]:
    return [e for e in events if e.HasField("timer_started_event_attributes")]


def held_for(events: list[Any], completed: Any) -> timedelta:
    """How long the Workflow Task that ``completed`` closes was held open."""
    started = {
        e.event_id: e
        for e in events
        if e.HasField("workflow_task_started_event_attributes")
    }
    start = started[completed.workflow_task_completed_event_attributes.started_event_id]
    return completed.event_time.ToDatetime() - start.event_time.ToDatetime()


def rollover_completion(events: list[Any]) -> Any | None:
    """The first Workflow Task completion that can only be a rollover.

    Identified by how long the task it closes was held: an idle park closes one
    an idle timeout after its last record, and a command-carrying completion
    closes one as soon as the activation returns. Only rollover holds a task
    open for the whole deadline, so a completion at the deadline is the
    mechanism itself rather than a task that happened to take a while.

    The window is the deadline itself, less :data:`ROLLOVER_DELIVERY_TOLERANCE`
    and up to the Workflow Task timeout. The same margin applies in this
    direction for the same reason: what is measured is the distance between two
    server event times, and Core arms its deadline from neither of them.
    """
    for completed in completed_tasks(events):
        held = held_for(events, completed)
        if ROLLOVER_DEADLINE - ROLLOVER_DELIVERY_TOLERANCE <= held < TASK_TIMEOUT:
            return completed
    return None


def wake_signals(events: list[Any]) -> list[Any]:
    """The reserved wake Signals in a Workflow's own History.

    The mechanism itself, rather than a record turning up -- which after a
    rollover proves nothing, since the server has already scheduled a
    replacement task that could have picked the record up on its own.
    """
    return [
        e
        for e in events
        if e.HasField("workflow_execution_signaled_event_attributes")
        and e.workflow_execution_signaled_event_attributes.signal_name
        == WAKE_SIGNAL_NAME
    ]


async def wait_for_history(
    handle: Any,
    predicate: Any,
    *,
    timeout: float,
    message: str,
) -> list[Any]:
    """Polls the Workflow's History until ``predicate`` holds, or fails saying why."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        events = await history(handle)
        if predicate(events):
            return events
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(message)
        await asyncio.sleep(0.2)


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
            for event in completed_tasks(events):
                held = held_for(events, event)
                assert held < TASK_TIMEOUT, (
                    f"a Workflow Task was held for {held}, past the "
                    f"{TASK_TIMEOUT} it must stay inside"
                )
            assert rollover_completion(events) is not None, (
                "no Workflow Task was held to the rollover deadline, so the "
                f"{total} records were consumed without one -- and this test "
                "asserts nothing about rollover unless one fired"
            )
        finally:
            stop.set()
            feeder.cancel()
            await stop_if_running(handle)


# --- case 21 ------------------------------------------------------------------


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

    The second half carries :data:`ROLLOVER_DELIVERY_TOLERANCE`, because what it
    measures is the distance between two ``WorkflowTaskStarted`` event times and
    the deadline can only be the moment Core *decides* to hand the task on. A
    completion RPC, the server scheduling the replacement, and a poll picking it
    up all land between the two, so an exact bound is unreachable by
    construction rather than merely flaky.
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
            # Waited out only as far as the deadline and its tolerance, never as
            # far as the Workflow Task timeout: a retained task that reaches that
            # is a panicking Core, not a failed assertion.
            bound = (ROLLOVER_DEADLINE + ROLLOVER_DELIVERY_TOLERANCE).total_seconds()
            waited = asyncio.get_running_loop().time() - started_at
            try:
                elapsed = await asyncio.wait_for(handle.result(), bound + 0.5 - waited)
            except asyncio.TimeoutError:
                raise AssertionError(
                    "the Signal had still not been delivered by the rollover "
                    f"deadline of {ROLLOVER_DEADLINE}: the Workflow Task is "
                    "still retained, and nothing but that deadline bounds how "
                    "long an input queues behind it"
                ) from None

            assert elapsed <= bound, (
                f"the Signal was delivered {elapsed:.3f}s into the Run, past the "
                f"rollover deadline of {ROLLOVER_DEADLINE} and the "
                f"{ROLLOVER_DELIVERY_TOLERANCE} allowed for getting the "
                "replacement task started. Retention latency is bounded by that "
                "deadline and by nothing else"
            )
            events = await history(handle)
            assert not timed_out_tasks(events), (
                "the Signal was released by the server timing the retained task "
                "out rather than by a rollover"
            )
        finally:
            stop.set()
            feeder.cancel()
            await stop_if_running(handle)


# --- case 23 ------------------------------------------------------------------


@pytest.mark.timeout(180)
async def test_an_append_after_a_rollover_completion_wakes_the_subscription(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """A subscription that has been through a rollover is still woken by a Signal.

    A rollover ends a Workflow Task with the subscription active and unparked:
    there is no retained task to notify locally and no park generation for a
    producer to observe, so the consumer's own watcher is what covers the
    window -- it observes ``NoOpenWorkflowTask`` and sends the reserved wake
    Signal itself.

    That window cannot be appended into directly. Core completes a rollover with
    ``force_new_wft``, so the server starts the replacement Workflow Task within
    a millisecond, and an append made from a test lands on the replacement task
    instead: readiness is answered ``Accepted``, the record is delivered live,
    and no wake is owed or sent -- which is correct behaviour and not the
    mechanism this case is about. The Workflow therefore *holds* the same state
    open on the far side of the rollover with a Timer, whose completion is
    server-bound and equally cannot be retained, and the append goes in there.

    Both halves are asserted, since either alone is satisfiable without the
    other: that a rollover really happened, by a Workflow Task held to the
    deadline and closed inside the timeout; and that the append was answered by
    a wake, by ``__temporal_external_stream_wake`` reaching the Workflow's own
    History *after* the completion that opened the window. The record arriving
    proves nothing on its own -- the Timer creates a Workflow Task when it
    fires, which would have picked the record up anyway.
    """
    before_pause = 30  # 30 * 0.3s = 9s of feeding, past a deadline at 8s.
    task_queue = f"tq-{uuid.uuid4()}"
    session = f"rollover-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[RolloverThenPauseWorkflow],
        external_stream_backends={"tokens-memory": backend},
    ):
        handle = await client.start_workflow(
            RolloverThenPauseWorkflow.run,
            before_pause,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
            task_timeout=TASK_TIMEOUT,
        )
        key = await stream_key_for(client, handle)
        stop = asyncio.Event()
        feeder = asyncio.ensure_future(
            feed(backend, key, session, count=before_pause, stop=stop)
        )
        try:
            # The Timer is started only once every fed record has been consumed,
            # which takes longer than the deadline -- so its appearance is also
            # the point past which the rollover has already happened.
            events = await wait_for_history(
                handle,
                timers_started,
                timeout=60,
                message=(
                    f"the Workflow never consumed the {before_pause} records fed "
                    "to it and reached its pause, so there is no unparked "
                    "Workflow Task boundary to append into"
                ),
            )
            stop.set()
            feeder.cancel()

            rollover = rollover_completion(events)
            assert rollover is not None, (
                "no Workflow Task was held to the rollover deadline of "
                f"{ROLLOVER_DEADLINE}, so the subscription this appends to has "
                "not been through a rollover and the case is untested"
            )
            assert not timed_out_tasks(events), (
                "the task was released by the server's timeout rather than by a "
                "rollover"
            )
            pause = timers_started(events)[0]
            assert pause.event_id > rollover.event_id, (
                "the pause began before the rollover, so the window appended "
                "into below is not a post-rollover one"
            )

            # Counted by name, and by the *same* name the check below uses: a
            # baseline that counted every Signal would be compared against a
            # total that counts only wake Signals, and any unrelated Signal in
            # this Workflow's History would then make the comparison meaningless
            # in whichever direction it happened to fall.
            before = len(wake_signals(events))

            await publish(backend, key, ["after-rollover"], session=session)

            def woken(events: list[Any]) -> bool:
                wakes = wake_signals(events)
                # Position as well as count: a Signal sent while a Workflow Task
                # was open reaches History only when that task completes, so
                # counting alone would accept one that was owed long before this
                # append and merely landed late.
                return len(wakes) > before and wakes[-1].event_id > pause.event_id

            await wait_for_history(
                handle,
                woken,
                timeout=POST_ROLLOVER_PAUSE.total_seconds(),
                message=(
                    "no wake Signal followed the append: the Workflow Task that "
                    "carried the pause left the subscription active and "
                    "unparked, so there was no retained task to notify and no "
                    "park generation to observe, and the watcher owes one"
                ),
            )

            assert await asyncio.wait_for(handle.result(), 60) == before_pause + 1, (
                "the record appended into the unparked window never reached the "
                "Workflow, so the wake Signal did not carry the subscription "
                "over the boundary"
            )
        finally:
            stop.set()
            feeder.cancel()
            await stop_if_running(handle)
