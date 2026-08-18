"""Handing a Run to another Worker: the shutdown sweep, and a failed finalization.

Two transitions that only exist between Workers, so neither can be shown inside
one:

- **Shutdown with no open Workflow Task.** Nothing local can create
  server-visible work, so the manager sweeps: it asks Core what state the Run is
  in through the read-only probe, sends the reserved wake Signal, and waits for
  the server to acknowledge it before the watchers go away. The point of the
  Signal is that *another* Worker picks the Run up.
- **A finalization that cannot be answered.** "A marker is never written without
  its terminal": if the Run's state is gone when Core asks for the terminal,
  Python fails the activation, Core writes no marker, and the retry replays from
  the previous marker -- losing no record and moving no cursor.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta
from typing import Any

import pytest

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.external_workflow_streams._annotation import decode_annotation
from temporalio.contrib.external_workflow_streams._backend import StreamKey
from temporalio.contrib.external_workflow_streams._manager import (
    ReadinessResult,
    StreamSubscriptionManager,
)
from temporalio.contrib.external_workflow_streams._record import (
    AFTER,
    BEGINNING,
    Cursor,
)
from temporalio.worker import Worker
from tests.contrib.external_workflow_streams.memory_backend import MemoryStreamBackend
from tests.contrib.external_workflow_streams.test_worker_integration import publish

with workflow.unsafe.imports_passed_through():
    from temporalio.contrib.external_workflow_streams._api import external_stream

WAKE_SIGNAL_NAME = "__temporal_external_stream_wake"

MARKER_DETAILS_KEY = "external_stream"
"""Where Core puts the marker's ``ExternalStreamMarkerData``."""

RETENTION_IDLE_TIMEOUT = timedelta(seconds=2)
"""How long a fed Workflow Task stays retained here -- stated, not inherited.

Case 30 needs a Workflow Task that is *open* at the moment Core asks for the
terminal, and everything the test does between the last record reaching the
Workflow and the shutdown call is bookkeeping read out of local state. The
one-second default makes that bookkeeping share a window with the idle timer,
which is not what this case is about.

Not larger, because the same timeout also bounds how long the Workflow waits for
its *first* record: a record appended before the subscription's watcher is
running is picked up by the idle timer's park recheck rather than by the
watcher, so raising this raises the test's own floor. Bounded above by the
Workflow Task timeout the test starts the Workflow with in any case -- a task
retained past that does not park, it fails (`wft-lifecycle.md`).
"""


@workflow.defn
class TimerThenStreamWorkflow:
    """Consumes one record, takes a timer, then consumes the rest.

    The timer is what puts the Run in the two states this module needs. Its
    completion is server-bound, so retention is suppressed: the marker commits
    there, and the Run is left cached with **no open Workflow Task** and a live
    subscription. Once the timer fires, the Workflow blocks on the stream again
    and the next task is retained.

    The marker and the retained task are therefore separated by the *whole*
    timer: the marker is written when the timer is **started**, and the retained
    task exists only once it has **fired** and its replacement task has reached
    this Worker. A test that treats the marker as the signal to start feeding is
    reading a clock, not a state -- see the caller.
    """

    @workflow.run
    async def run(self, expected: int) -> list[str]:
        tokens = external_stream.with_options(
            idle_timeout=RETENTION_IDLE_TIMEOUT
        ).topic("tokens", backend="tokens-memory", type=str)
        seen: list[str] = []
        iterator = tokens.subscribe().__aiter__()
        seen.append(await iterator.__anext__())
        await asyncio.sleep(0.5)
        while len(seen) < expected:
            seen.append(await iterator.__anext__())
        return seen


@workflow.defn
class LeftWithNoOpenTaskWorkflow:
    """Blocks on the stream first, then starts a long timer. The order is the point.

    The ``NoOpenWorkflowTask`` window needs two things at once -- a wait set Core
    knows about, and no Workflow Task holding it -- and only this order produces
    both:

    1. **Block before doing anything else.** A completion that carries no command
       sends ``WorkflowStreamQuiescent``, and that command is the *only* thing
       that registers a wait set with Core. A Workflow that starts a timer on the
       same completion it first blocks on never registers one at all: retention
       is suppressed, so no quiescent snapshot is ever sent, Core's wait set stays
       empty, and a wake Signal then marks nothing ready and creates a Workflow
       Task that Core completes with no activation. Such a Run cannot be resumed
       by anything -- which is what this case used to try to shut down.
    2. **Start the timer only after a record has woken it.** That command
       suppresses retention for *that* completion, so the Workflow Task ends with
       the wait set still registered and the Run sits in the window for as long as
       the test needs. The timer is also the "unrelated Workflow event" the
       shutdown sweep must not wait for.
    """

    @workflow.run
    async def run(self, expected: int) -> list[str]:
        tokens = external_stream.topic("tokens", backend="tokens-memory", type=str)
        seen: list[str] = []
        iterator = tokens.subscribe().__aiter__()
        seen.append(await iterator.__anext__())
        timer = asyncio.ensure_future(asyncio.sleep(600))
        while len(seen) < expected:
            seen.append(await iterator.__anext__())
        timer.cancel()
        return seen


@pytest.fixture
def backend() -> MemoryStreamBackend:
    return MemoryStreamBackend()


async def history(handle: Any) -> list[Any]:
    return [e async for e in handle.fetch_history_events()]


def markers(events: list[Any]) -> list[Any]:
    return [e for e in events if e.HasField("marker_recorded_event_attributes")]


def wake_signals(events: list[Any]) -> list[Any]:
    return [
        e
        for e in events
        if e.HasField("workflow_execution_signaled_event_attributes")
        and e.workflow_execution_signaled_event_attributes.signal_name
        == WAKE_SIGNAL_NAME
    ]


def committed_boundary(marker_event: Any, wait_id: int) -> Cursor:
    """Where a marker says one wait's consumption had reached.

    "The cursor did not move" is a statement about the marker; anything derived
    from what was published instead would be a statement about timing.

    The annotation's terminal, when it has one. A marker written on a
    command-producing completion currently carries no terminal frame at all --
    ``WorkflowStreamProgress`` emits segments only, and ``add_terminal()`` runs
    only on the park and finalization paths -- so the end of the last recorded
    run is used instead. It names the same position the terminal would.
    """
    from temporalio.bridge.proto.external_data import ExternalStreamMarkerData

    details = marker_event.marker_recorded_event_attributes.details
    data = ExternalStreamMarkerData()
    data.ParseFromString(details[MARKER_DETAILS_KEY].payloads[0].data)
    annotation = decode_annotation(data.replay_annotation)
    if annotation.terminal is not None:
        return annotation.terminal[wait_id]
    reached = annotation.header.streams[wait_id].start_cursor
    for segment in annotation.segments:
        for run in segment.runs:
            if run.wait_id == wait_id:
                reached = AFTER(run.last_offset)
    return reached


def marker_bytes(marker_event: Any) -> bytes:
    return (
        marker_event.marker_recorded_event_attributes.details[MARKER_DETAILS_KEY]
        .payloads[0]
        .data
    )


def wake_envelope(signal_event: Any) -> Any:
    """The wake Signal's own envelope, read the way Core reads it.

    Deliberately not through the ``DataConverter``: the envelope is defined at
    the protocol level precisely so Core -- which has no converter -- can read
    it.
    """
    from temporalio.bridge.proto.external_stream import WakeSignal

    payload = signal_event.workflow_execution_signaled_event_attributes.input.payloads[
        0
    ]
    envelope = WakeSignal()
    envelope.ParseFromString(payload.data)
    return envelope


async def stream_key_for(client: Client, handle: Any) -> StreamKey:
    description = await handle.describe()
    return StreamKey(
        client.namespace,
        handle.id,
        description.raw_description.workflow_execution_info.first_run_id,
        "tokens",
    )


async def wait_until(
    predicate: Any, timeout: float, message: str, interval: float = 0.2
) -> None:
    """Polls rather than sleeping a fixed time, so a slow start is not a flake.

    ``interval`` is worth lowering when the condition is read out of local
    process state rather than fetched from the server, and when what happens
    *after* it is satisfied is itself time-bounded: the poll gap is then part of
    the window the caller has to act in, not merely latency.
    """
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if await predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError(message)


# --- case 29: shutdown in the NoOpenWorkflowTask window -----------------------


@pytest.mark.timeout(180)
async def test_shutdown_with_no_open_task_hands_the_run_to_another_worker(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """The window the sweep exists for, with the second Worker that proves it.

    The Run is cached, has a wait set Core knows about, and holds no Workflow
    Task. **Nothing else will ever create work for it**: the only other thing
    that could produce a Workflow Task here is the Workflow's own 600-second
    timer, and waiting for an unrelated event is not a plan. That is why the
    sweep exists, and why it runs whether or not a record happens to be waiting
    -- the obligation is to hand the Run over, not to announce an append.

    The window is *asserted*, not assumed: the read-only probe is called on the
    live Worker before shutdown and must answer ``NoOpenWorkflowTask``. Getting
    there needs both of the Workflow's steps, which is what
    :class:`LeftWithNoOpenTaskWorkflow` documents.

    Four separate claims, none of which the others imply:

    - no marker is written, because nothing was accumulated to write;
    - the Run's state is resolved with the read-only probe rather than a
      readiness notification, which would assert a buffered record that the
      sweep has no business claiming;
    - a **new** wake Signal reaches the Workflow's own History, and it is an
      *unparked* one -- ``park_generation = 0``. Counted against the Signals
      already there rather than merely found, because an earlier wake in the
      same History proves nothing about this one;
    - a second Worker takes the resulting Workflow Task, reconstructs the
      subscription from the marker, and delivers a record published after the
      handover -- without the timer ever firing.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    session = f"handoff-{uuid.uuid4()}"
    worker_a = Worker(
        client,
        task_queue=task_queue,
        workflows=[LeftWithNoOpenTaskWorkflow],
        external_stream_backends={"tokens-memory": backend},
    )
    worker_a_task = asyncio.create_task(worker_a.run())
    handle = None
    try:
        handle = await client.start_workflow(
            LeftWithNoOpenTaskWorkflow.run,
            2,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
            task_timeout=timedelta(seconds=10),
        )
        key = await stream_key_for(client, handle)

        # Published only once the Run's first Workflow Task has ended, which is
        # what registers the wait set with Core. A record that arrives before
        # that lands on a Run whose wait set is still empty.
        await wait_until(
            lambda: _has_markers(handle),
            60,
            "the Run's first Workflow Task never ended, so its wait set was "
            "never registered with Core and no wake could reach it",
        )
        await publish(backend, key, ["alpha"], session=session)
        await wait_until(
            lambda: _timer_started(handle),
            60,
            "the record never reached the Workflow, so the completion that "
            "leaves the Run in the no-open-task window never happened",
        )

        manager = worker_a._workflow_worker._external_stream_manager
        assert manager is not None, "the manager should exist by now"
        real_probe, real_ready = manager._run_status, manager._notify_ready
        await wait_until(
            lambda: _run_is_registered(manager),
            30,
            "the manager holds no Run with subscriptions, so the sweep has "
            "nothing to sweep",
        )
        run_id = next(iter(manager._runs))
        await wait_until(
            lambda: _in_the_window(real_probe, run_id),
            30,
            "the Run never reached the no-open-Workflow-Task window, so this "
            "case is not testing the transition it names",
        )

        before = markers(await history(handle))
        signals_before = wake_signals(await history(handle))

        probes: list[str] = []
        readiness: list[str] = []

        async def counting_probe(probed_run_id: str) -> Any:
            probes.append(probed_run_id)
            return await real_probe(probed_run_id)

        async def counting_ready(
            ready_run_id: str, wait_id: int, generation: int
        ) -> Any:
            readiness.append(ready_run_id)
            return await real_ready(ready_run_id, wait_id, generation)

        manager._run_status = counting_probe
        manager._notify_ready = counting_ready

        await asyncio.wait_for(worker_a.shutdown(), 60)

        assert probes, (
            "the sweep never asked Core what state the Run was in; without the "
            "read-only probe it cannot know whether a wake is owed at all"
        )
        assert not readiness, (
            "the sweep used the readiness call as a probe. Readiness means 'a "
            "record is buffered', so using it here asserts something the sweep "
            "has no business claiming and manufactures a Workflow Task on the "
            "way out"
        )
        after = markers(await history(handle))
        assert len(after) == len(before), (
            "a marker was written on the way out of the no-open-task window; "
            "nothing was accumulated there, so there was nothing to write"
        )
        signals_after = wake_signals(await history(handle))
        assert len(signals_after) > len(signals_before), (
            "the sweep's wake Signal never reached the Workflow's own History, "
            f"which still holds the same {len(signals_before)} it did before "
            "shutdown. Nothing will ever create a Workflow Task for this Run"
        )
        envelope = wake_envelope(signals_after[-1])
        assert envelope.park_generation == 0, (
            "the shutdown wake must be an unparked one -- there is no confirmed "
            f"park to name -- got generation {envelope.park_generation}"
        )

        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[LeftWithNoOpenTaskWorkflow],
            external_stream_backends={"tokens-memory": backend},
        ):
            # Published after the handover, so delivering it proves the second
            # Worker rebuilt a live subscription rather than replaying one.
            await publish(backend, key, ["beta"], session=session)
            result = await asyncio.wait_for(handle.result(), 60)

        assert result == ["alpha", "beta"], (
            "the second Worker did not reconstruct the subscription from the "
            f"marker: {result}"
        )
        assert not any(
            e.HasField("timer_fired_event_attributes") for e in await history(handle)
        ), (
            "the Run was resumed by its own 600-second timer rather than by the "
            "wake Signal, which is the one thing this case forbids"
        )
    finally:
        if not worker_a_task.done():
            worker_a_task.cancel()
        if handle is not None:
            try:
                await handle.terminate()
            except Exception:
                pass


async def _has_markers(handle: Any) -> bool:
    return bool(markers(await history(handle)))


async def _timer_started(handle: Any) -> bool:
    return any(
        e.HasField("timer_started_event_attributes") for e in await history(handle)
    )


async def _run_is_registered(manager: Any) -> bool:
    return bool(manager._runs)


async def _in_the_window(probe: Any, run_id: str) -> bool:
    """Whether Core says this Run has waits registered and no task open.

    Asked through the read-only probe, which is the same call the sweep makes
    and the only thing that can tell the three states apart.
    """
    status = await probe(run_id)
    return getattr(status, "value", status) == "NoOpenWorkflowTask"


class _InTheWindow:
    """A Run left cached with a live wait set and no open Workflow Task.

    The state both tests below need, and getting there is neither quick nor
    obvious: the Workflow must block on the stream *first* so a
    ``WorkflowStreamQuiescent`` registers the wait set with Core, park, be woken
    by a record, and only then issue a server-bound command so the Workflow Task
    ends with the wait set still registered. See
    :class:`LeftWithNoOpenTaskWorkflow`.
    """

    def __init__(self, worker: Worker, task: asyncio.Task, handle: Any, key: StreamKey):
        self.worker = worker
        self.task = task
        self.handle = handle
        self.key = key
        self.manager = worker._workflow_worker._external_stream_manager
        self.run_id = next(iter(self.manager._runs))

    async def close(self) -> None:
        if not self.task.done():
            self.task.cancel()
        try:
            await self.handle.terminate()
        except Exception:
            pass


async def _leave_a_run_in_the_window(
    client: Client, backend: MemoryStreamBackend, session: str
) -> _InTheWindow:
    task_queue = f"tq-{uuid.uuid4()}"
    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[LeftWithNoOpenTaskWorkflow],
        external_stream_backends={"tokens-memory": backend},
    )
    worker_task = asyncio.create_task(worker.run())
    handle = await client.start_workflow(
        LeftWithNoOpenTaskWorkflow.run,
        2,
        id=f"wf-{uuid.uuid4()}",
        task_queue=task_queue,
        task_timeout=timedelta(seconds=10),
    )
    key = await stream_key_for(client, handle)

    # The first Workflow Task ends by parking, which is also what registers the
    # wait set with Core. A record published before that lands on a Run whose
    # wait set is still empty.
    await wait_until(
        lambda: _has_markers(handle),
        60,
        "the Run's first Workflow Task never ended, so it never parked and "
        "never registered a wait set with Core",
    )
    await wait_until(
        lambda: _park_is_installed(backend, key),
        30,
        "no park intent was installed, so this Run never confirmed a park and "
        "there is no resolved park to reason about",
    )

    await publish(backend, key, ["alpha"], session=session)
    await wait_until(
        lambda: _timer_started(handle),
        60,
        "the record never reached the Workflow, so the completion that leaves "
        "the Run in the no-open-task window never happened",
    )

    manager = worker._workflow_worker._external_stream_manager
    assert manager is not None, "the manager should exist by now"
    await wait_until(
        lambda: _run_is_registered(manager),
        30,
        "the manager holds no Run with subscriptions",
    )
    return _InTheWindow(worker, worker_task, handle, key)


async def _park_is_installed(backend: MemoryStreamBackend, key: StreamKey) -> bool:
    return bool(await backend.parked_wait_ids(key))


@pytest.mark.timeout(180)
async def test_a_wake_that_resolves_a_park_removes_its_intent(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """The invariant, end to end: an intent outlives its park nowhere.

    The park here is confirmed rather than aborted, and it is resolved the way a
    live one actually is -- a producer appends, the watcher sends the wake, and
    Core clears its ``park_generation``. None of that is visible in the backend,
    so if the manager does not take the intent back out on the resolve, nothing
    ever will.

    Asserted through ``current_park_generation`` because that is the call the
    damage comes through. Every wake path asks it: a producer, to decide what its
    Signal names, and this Worker's own shutdown sweep, to decide whether it owes
    a parked or an unparked wake. Both get a generation Core has already
    discarded, and both wakes are then discarded in turn -- the producer's as
    stale, the sweep's by the server, which sees the request ID of the wake that
    ended the park.
    """
    session = f"resolve-{uuid.uuid4()}"
    window = await _leave_a_run_in_the_window(client, backend, session)
    try:
        assert await backend.parked_wait_ids(window.key) == [], (
            "the park that the wake Signal resolved still has its intent "
            "installed in the backend"
        )
        for wait_id in (0, 1, 2):
            assert await backend.current_park_generation(window.key, wait_id) is None, (
                "a park that is over still reports a generation, so the next "
                "wake for this wait will name it instead of being the unparked "
                "wake it owes"
            )
    finally:
        await window.close()


@pytest.mark.timeout(180)
async def test_the_shutdown_probe_is_asked_while_core_still_holds_the_run(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """The sweep's probe has to be asked when its answer still describes something.

    P20 gives the probe four answers and three behaviours: ``WftOpen`` waits for
    C15b, ``Parked`` owes nothing, and ``NoOpenWorkflowTask``/``RunNotFound``
    owe the wake. Asked after every activation has been answered, Core has
    already dropped the Run and only ``RunNotFound`` is reachable -- so the
    sweep still sends its wake and still looks correct, while a parked Run gets
    a wake it does not need and a Run holding a Workflow Task gets one that
    races Core's own shutdown transition.

    This Run is in the ``NoOpenWorkflowTask`` window, which is asserted on the
    live Worker before shutdown, so a sweep asking at the right moment must see
    the same thing. The assertion is on what the *sweep* saw, not on what a
    probe outside it saw, because the ordering is the only thing under test.
    """
    session = f"probe-{uuid.uuid4()}"
    window = await _leave_a_run_in_the_window(client, backend, session)
    try:
        real_probe = window.manager._run_status
        await wait_until(
            lambda: _in_the_window(real_probe, window.run_id),
            30,
            "the Run never reached the no-open-Workflow-Task window, so this "
            "case is not testing the ordering it names",
        )

        answers: list[str] = []

        async def recording_probe(probed_run_id: str) -> Any:
            status = await real_probe(probed_run_id)
            answers.append(getattr(status, "value", status))
            return status

        window.manager._run_status = recording_probe

        await asyncio.wait_for(window.worker.shutdown(), 60)

        assert answers, "the sweep never probed at all"
        assert "RunNotFound" not in answers, (
            "the sweep probed after Core had already dropped the Run, so its "
            f"answers were {answers}. Every state P20 distinguishes collapses "
            "into RunNotFound there: the wake still goes out, so nothing looks "
            "broken, but the Parked and WftOpen branches are unreachable"
        )
        assert answers == ["NoOpenWorkflowTask"], (
            "the Run was in the no-open-Workflow-Task window immediately before "
            f"shutdown, and the sweep saw {answers}"
        )
    finally:
        await window.close()


# --- case 30: teardown racing finalization ------------------------------------


def _status_value(status: Any) -> Any:
    """The probe's answer as a plain string, matched structurally as Core is."""
    return getattr(status, "value", status)


def _delivery_boundary(workflow_worker: Any, run_id: str) -> Any:
    """Where this Run's deliveries have reached, read from the Run's own state.

    ``blocked_snapshot()`` is the same thing the finalization terminal is built
    from, which is what makes it the right thing to wait on: nothing else says
    "a Workflow Task is open here and its annotation is not written yet". The
    History cannot -- a retained task writes nothing until it ends, which is
    exactly the state this case must catch it in.
    """
    runtime = workflow_worker._external_stream_runtimes.get(run_id)
    return None if runtime is None else runtime.blocked_snapshot()


async def _delivery_moved(workflow_worker: Any, run_id: str, previous: Any) -> bool:
    """Whether the boundary has moved off ``previous``.

    Movement rather than a value: a cursor is ordered by the provider's own
    comparator, so "is it past the record just published" is a question only the
    backend can answer, while "did the record land" is a question the boundary
    answers by itself.
    """
    return _delivery_boundary(workflow_worker, run_id) != previous


@pytest.mark.timeout(180)
async def test_a_finalization_that_cannot_be_answered_writes_no_marker(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """The Run's state vanishes underneath a finalization, and nothing is lost.

    Finalization is answered from the Run's own out-of-sandbox state, and its
    only failure mode is that state being gone. The obligation then is not to do
    something approximate: **a marker is never written without its terminal**,
    because an abandoned Workflow Task commits no cursor and loses no record,
    while a truncated annotation is durable and wrong.

    So the Run's entry is removed exactly once, while Core is asking for the
    terminal, and the assertions are about what did *not* happen: no marker for
    that task, no cursor movement in the marker that already existed, and no
    record missing from what the Workflow finally received. The retry replays
    from the previous marker and re-consumes everything after it.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    session = f"finalize-{uuid.uuid4()}"
    values = ["alpha", "beta", "gamma", "delta"]
    worker_a = Worker(
        client,
        task_queue=task_queue,
        workflows=[TimerThenStreamWorkflow],
        external_stream_backends={"tokens-memory": backend},
    )
    worker_a_task = asyncio.create_task(worker_a.run())
    shutdown_task = None
    handle = None
    try:
        handle = await client.start_workflow(
            TimerThenStreamWorkflow.run,
            len(values),
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
            task_timeout=timedelta(seconds=10),
        )
        key = await stream_key_for(client, handle)

        # The first record is consumed on a task that also starts a timer, so
        # its marker commits: that marker is the "previous" one the retry must
        # replay from.
        await publish(backend, key, values[:1], session=session)
        await wait_until(
            lambda: _has_markers(handle),
            60,
            "no marker committed the first record, so there is no previous "
            "marker for the retry to replay from",
        )
        before = markers(await history(handle))
        committed_before = committed_boundary(before[-1], wait_id=1)
        marker_before = marker_bytes(before[-1])

        # Fed from here, and each record is *followed* to the Workflow rather
        # than merely published on a schedule. The precondition this case needs
        # is a Workflow Task that is open with an annotation still accumulating
        # in it, and the marker above says nothing about when that exists: it is
        # written when the timer is started, while the retained task appears
        # only once the timer has fired and its replacement task has reached
        # this Worker. Feeding on a fixed schedule from the marker races the
        # timer, and a shutdown that wins the race finds no open Workflow Task
        # at all -- Core issues no ``FinalizeExternalStreams``, so the sweep
        # runs instead and nothing this case is about ever happens.
        #
        # The condition waited on is the Run's own delivery boundary, which is
        # the snapshot the terminal would be built from. Once it has moved past
        # what the previous marker committed, the open task and its unwritten
        # annotation are both facts.
        workflow_worker = worker_a._workflow_worker
        manager = workflow_worker._external_stream_manager
        assert manager is not None, "the manager should exist by now"
        await wait_until(
            lambda: _run_is_registered(manager),
            30,
            "the manager holds no Run with subscriptions, so there is no Run "
            "state for a finalization to be answered from",
        )
        run_id = next(iter(manager._runs))
        assert _delivery_boundary(workflow_worker, run_id) is not None, (
            "the manager registered a Run the Worker holds no stream runtime "
            "for, so the sabotage below would remove nothing"
        )
        for value in values[1:3]:
            delivered = _delivery_boundary(workflow_worker, run_id)
            await publish(backend, key, [value], session=session)
            await wait_until(
                lambda: _delivery_moved(workflow_worker, run_id, delivered),
                60,
                f"{value!r} never reached the Workflow, so no Workflow Task is "
                "open here with an unwritten annotation in it and the "
                "finalization this case forces would have nothing to fail "
                "against",
                # The retained task the last delivery leaves behind is what the
                # steps after this loop have to run inside, so the poll gap is
                # spent out of that window. Short because the condition is a
                # dictionary in this process, not a fetch from the server.
                interval=0.02,
            )

        assert _status_value(await manager._run_status(run_id)) == "WftOpen", (
            "the records were delivered but the Workflow Task holding them is "
            "already gone, so Core has nothing to ask for a terminal on. The "
            "task is retained for RETENTION_IDLE_TIMEOUT, which every step "
            "since the last delivery is expected to fit inside"
        )

        # The Run's entry disappears underneath the finalization, exactly once.
        original = workflow_worker._handle_external_stream_jobs
        sabotaged: list[str] = []

        async def losing_the_run(act: Any, running: Any) -> Any:
            if not sabotaged and any(
                j.HasField("finalize_external_streams") for j in act.jobs
            ):
                sabotaged.append(act.run_id)
                workflow_worker._external_stream_runtimes.pop(act.run_id, None)
                await manager.evict_run(act.run_id)
            return await original(act, running)

        workflow_worker._handle_external_stream_jobs = losing_the_run

        # Shutdown is what makes Core ask for the terminal while the task is
        # still open. Not awaited: this Worker is deliberately being left unable
        # to answer, and the test's subject is what the *server* is left holding.
        shutdown_task = asyncio.create_task(worker_a.shutdown())

        async def the_task_failed() -> bool:
            return any(
                e.HasField("workflow_task_failed_event_attributes")
                for e in await history(handle)
            )

        await wait_until(
            the_task_failed,
            90,
            "the Workflow Task never failed, so the finalization was answered "
            "from somewhere other than the Run state that was removed",
        )
        assert sabotaged, "the finalization job never arrived, so nothing was raced"

        events = await history(handle)
        failure = next(
            e for e in events if e.HasField("workflow_task_failed_event_attributes")
        )
        message = failure.workflow_task_failed_event_attributes.failure.message
        assert "finalize_external_streams" in message, (
            "the Workflow Task failed for some other reason than the "
            f"unanswerable finalization: {message}"
        )
        after = markers(events)
        assert len(after) == len(before), (
            "Core wrote a marker for a Workflow Task whose terminal was never "
            "supplied -- a durable, truncated annotation is exactly what the "
            "abandon-and-retry rule exists to prevent"
        )
        assert committed_boundary(after[-1], wait_id=1) == committed_before, (
            "the committed cursor moved across a Workflow Task that was "
            "abandoned; nothing consumed on it was ever committed"
        )
        assert marker_bytes(after[-1]) == marker_before, (
            "the marker that was already in History was rewritten by the "
            "abandoned Workflow Task"
        )

        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[TimerThenStreamWorkflow],
            external_stream_backends={"tokens-memory": backend},
        ):
            await publish(backend, key, values[3:], session=session)
            result = await asyncio.wait_for(handle.result(), 90)

        assert result == values, (
            "the retry did not replay from the previous marker: every record "
            "consumed on the abandoned task was owed again, exactly once, and "
            f"in order, got {result}"
        )
    finally:
        # Terminated first, then the Worker given a moment to finish shutting
        # down: a Worker abandoned mid-shutdown leaves a Core run behind, and
        # this suite's later tests are timing-sensitive enough to notice.
        if handle is not None:
            try:
                await handle.terminate()
            except Exception:
                pass
        if shutdown_task is not None:
            await asyncio.wait([shutdown_task], timeout=10)
            if not shutdown_task.done():
                shutdown_task.cancel()
        if not worker_a_task.done():
            worker_a_task.cancel()
        await asyncio.sleep(0)


# --- the intent the Worker that installed it left behind -----------------------


HANDOFF_RUN_ID = "run-handed-over"


def _manager_for(backend: MemoryStreamBackend) -> StreamSubscriptionManager:
    """One Worker's manager, wired with only what the park handshake touches.

    No Core and no Signal path: what is under test here is state that lives in
    the *backend* and outlives both.
    """

    async def accepted(run_id: str, wait_id: int, generation: int) -> str:
        return ReadinessResult.ACCEPTED

    return StreamSubscriptionManager(
        backends={"tokens-memory": backend},
        notify_ready=accepted,
        watch_block=timedelta(milliseconds=10),
    )


async def _nothing_is_parked(backend: MemoryStreamBackend, key: StreamKey) -> bool:
    return await backend.parked_wait_ids(key) == []


async def test_a_park_intent_installed_by_a_previous_worker_is_removed(
    backend: MemoryStreamBackend,
) -> None:
    """The half of the intent invariant that only a hand-off can show.

    ``installed_park_generation`` is a *mirror* of backend state and lives on
    the Worker that installed the park. The intent is durable and survives
    eviction, a Workflow Task that moved to another Worker, and shutdown; the
    mirror survives none of them. So removal keyed on the mirror alone cannot
    reach exactly the intents that most need reaching -- the ones whose
    installer is gone -- and the Run is then stranded in a way no later resolve
    can repair: ``current_park_generation`` keeps answering a generation Core
    discarded, every producer wake names it and Core discards it as stale, and
    because a parked wake's request ID ignores sender identity the second such
    wake is byte-identical to the first and the server deduplicates it away.

    The ordering is the real Worker's: ``ResolveExternalStreamWaits`` is
    answered *before* user Workflow code runs, so the new Worker has no
    subscription to resolve against at that point and the reconstructed one
    arrives afterwards.
    """
    key = StreamKey("ns", "wf", "first-run", "tokens")

    worker_a = _manager_for(backend)
    worker_a.register(
        run_id=HANDOFF_RUN_ID, wait_id=1, stream_key=key, backend_name="tokens-memory"
    )
    confirmed = not await worker_a.prepare_park(HANDOFF_RUN_ID, 7, {1: BEGINNING})
    assert confirmed, "nothing was appended, so this park must have confirmed"
    await worker_a.shutdown()

    assert await backend.parked_wait_ids(key) == [1], (
        "this case is only meaningful if the intent outlived the Worker that "
        "installed it -- it is durable backend state, not Worker state"
    )

    worker_b = _manager_for(backend)
    try:
        await worker_b.resolve_park(HANDOFF_RUN_ID)
        worker_b.register(
            run_id=HANDOFF_RUN_ID,
            wait_id=1,
            stream_key=key,
            backend_name="tokens-memory",
        )

        await wait_until(
            lambda: _nothing_is_parked(backend, key),
            5,
            "the intent of a park no Worker is sitting in is still installed, "
            "so the next producer wake names a dead generation instead of the "
            "unparked wake it owes",
            interval=0.01,
        )
        assert await backend.current_park_generation(key, 1) is None, (
            "a park that ended with the Worker that confirmed it still reports "
            "a generation"
        )

        # What the producer actually asks, in the order it asks it: the record
        # lands, and then the wake is chosen from what the backend says is
        # parked. Nothing is, so it is the unparked wake -- which Core always
        # accepts as a recheck request -- rather than generation 7, which it
        # discards as stale.
        await publish(backend, key, ["beta"], session=f"handoff-{uuid.uuid4()}")
        assert await backend.parked_wait_ids(key) == []
    finally:
        await worker_b.shutdown()
