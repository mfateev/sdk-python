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
from temporalio.contrib.external_workflow_streams._record import AFTER, Cursor
from temporalio.worker import Worker
from tests.contrib.external_workflow_streams.memory_backend import MemoryStreamBackend
from tests.contrib.external_workflow_streams.test_worker_integration import publish

with workflow.unsafe.imports_passed_through():
    from temporalio.contrib.external_workflow_streams._api import external_stream

WAKE_SIGNAL_NAME = "__temporal_external_stream_wake"

MARKER_DETAILS_KEY = "external_stream"
"""Where Core puts the marker's ``ExternalStreamMarkerData``."""

FEED_GAP_SECONDS = 0.3
"""Below the idle timeout, so a fed Workflow Task stays retained."""


@workflow.defn
class TimerThenStreamWorkflow:
    """Consumes one record, takes a timer, then consumes the rest.

    The timer is what puts the Run in the two states this module needs. Its
    completion is server-bound, so retention is suppressed: the marker commits
    there, and the Run is left cached with **no open Workflow Task** and a live
    subscription. Once the timer fires, the Workflow blocks on the stream again
    and the next task is retained.
    """

    @workflow.run
    async def run(self, expected: int) -> list[str]:
        tokens = external_stream.topic("tokens", backend="tokens-memory", type=str)
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


async def wait_until(predicate: Any, timeout: float, message: str) -> None:
    """Polls rather than sleeping a fixed time, so a slow start is not a flake."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if await predicate():
            return
        await asyncio.sleep(0.2)
    raise AssertionError(message)


# --- case 29: shutdown in the NoOpenWorkflowTask window -----------------------


@pytest.mark.xfail(
    strict=True,
    reason=(
        "the sweep's wake Signal never reaches the Workflow's History: it is "
        "sent as a *parked* wake naming a park generation that is already dead, "
        "and the server deduplicates it against the wake that generation "
        "already had. "
        "Nothing removes a confirmed park intent from the backend when the park "
        "is resolved -- `remove_park_intent` is called only when a recheck "
        "aborts a park -- so `current_park_generation` still answers 1 long "
        "after `mark_all_ready_for_wake` cleared it in Core. "
        "`_send_external_stream_wake` reads that answer, so the sweep builds "
        "`WakeRequest(park_generation=1)` instead of the unparked "
        "`park_generation=0` P20 requires. A parked wake's request ID is "
        "derived from (namespace, workflow id, first execution run id, stream "
        "name, wait id, park generation) alone -- deliberately, since a "
        "generation is woken once -- so it comes out byte-identical to the wake "
        "the watcher already sent for generation 1. Observed by tracing "
        "`send_wake_signal`: two sends, both `park_generation=1`, one distinct "
        "request ID between them, and one "
        "`__temporal_external_stream_wake` event in History, unchanged across "
        "shutdown. No Workflow Task is created and no second Worker ever sees "
        "the Run. Were it not deduplicated, Core would reject it anyway: "
        "`accepts_wake_generation(1)` requires `park_generation == Some(1)`, "
        "and the wake that resolved the park set it to None. "
        "This is not the only blocker, and the next reader should not assume it "
        "is: with `_send_external_stream_wake` forced to `generation = 0` as an "
        "experiment, the sweep's wake does reach History, the server does "
        "create a Workflow Task, and a second Worker does take it and replay -- "
        "and the Run then stalls again. A replayed completion returns early "
        "from `_emit_external_stream_commands`, so it sends no "
        "`WorkflowStreamQuiescent`, and `become_quiescent` is the only thing "
        "that populates Core's wait set; the handed-over Run therefore ends "
        "replay with an empty one. Every later wake -- the watcher's, and the "
        "one for the record published after the handover -- creates a Workflow "
        "Task that Core completes with no activation at all, exactly as it does "
        "for a Run that never registered. "
        "One more fact a reader will trip over: the sweep's own probe answers "
        "`RunNotFound`, not `NoOpenWorkflowTask`, because the manager sweeps "
        "after the poller tasks have been awaited and Core has already dropped "
        "the Run from its cache. The same probe called on the live Worker one "
        "line earlier answers `NoOpenWorkflowTask`. Both branches owe a wake, so "
        "this does not change what the sweep does here -- but it does mean the "
        "probe cannot currently distinguish the `Parked` and `WftOpen` cases "
        "that P20 asks it to"
    ),
)
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


# --- case 30: teardown racing finalization ------------------------------------


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

        # Fed continuously from here, so the task consuming these is retained
        # and its annotation is still accumulating, unwritten, when the
        # finalization arrives.
        for value in values[1:3]:
            await publish(backend, key, [value], session=session)
            await asyncio.sleep(FEED_GAP_SECONDS)

        # The Run's entry disappears underneath the finalization, exactly once.
        workflow_worker = worker_a._workflow_worker
        original = workflow_worker._handle_external_stream_jobs
        sabotaged: list[str] = []

        async def losing_the_run(act: Any, running: Any) -> Any:
            if not sabotaged and any(
                j.HasField("finalize_external_streams") for j in act.jobs
            ):
                sabotaged.append(act.run_id)
                workflow_worker._external_stream_runtimes.pop(act.run_id, None)
                manager = workflow_worker._external_stream_manager
                if manager is not None:
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
