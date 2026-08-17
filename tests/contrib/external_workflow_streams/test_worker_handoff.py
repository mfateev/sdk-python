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
class ParkedByATimerWorkflow:
    """Subscribes, starts a long timer, and waits on the stream.

    The timer suppresses retention for the life of the Run, so every Workflow
    Task completes and the Run sits in the ``NoOpenWorkflowTask`` window -- with
    a live subscription -- for as long as the test needs. The timer itself is the
    "unrelated Workflow event" the shutdown sweep must not wait for.
    """

    @workflow.run
    async def run(self, expected: int) -> list[str]:
        tokens = external_stream.topic("tokens", backend="tokens-memory", type=str)
        timer = asyncio.ensure_future(asyncio.sleep(600))
        seen: list[str] = []
        async for token in tokens.subscribe():
            seen.append(token)
            if len(seen) >= expected:
                break
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
        "the sweep itself now runs on a clean shutdown -- the Run is probed, no "
        "marker is written, and an unparked wake Signal reaches History -- but "
        "the second Worker's own wake is deduplicated away. An unparked wake's "
        "request ID is derived from (sender identity, per-sender wake counter), "
        "and a fresh Worker restarts that counter at 1 while sharing this "
        "test's one Client identity, so its first wake derives byte-identical "
        "material to the wake Worker A already sent. The server keeps one, no "
        "Workflow Task is created, and the Run never resumes"
    ),
)
@pytest.mark.timeout(180)
async def test_shutdown_with_no_open_task_hands_the_run_to_another_worker(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """The window the sweep exists for, with the second Worker that proves it.

    The Run is cached, has a live subscription, and holds no Workflow Task. Its
    records are buffered in a process about to exit and **nothing else will ever
    say so**: the only other thing that could create a Workflow Task here is the
    Workflow's own 600-second timer, and waiting for an unrelated event is not a
    plan.

    Four separate claims, none of which the others imply:

    - no marker is written, because nothing was accumulated to write;
    - the Run's state is resolved with the read-only probe rather than a
      readiness notification, which would assert a buffered record that the
      sweep has no business claiming;
    - the wake Signal is an *unparked* one -- ``park_generation = 0`` -- read out
      of the Workflow's own History rather than inferred from a delivery;
    - a second Worker takes the resulting Workflow Task and reconstructs the
      subscription from the marker, delivering the record without the timer ever
      firing.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    session = f"handoff-{uuid.uuid4()}"
    worker_a = Worker(
        client,
        task_queue=task_queue,
        workflows=[ParkedByATimerWorkflow],
        external_stream_backends={"tokens-memory": backend},
    )
    worker_a_task = asyncio.create_task(worker_a.run())
    handle = None
    try:
        handle = await client.start_workflow(
            ParkedByATimerWorkflow.run,
            2,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
            task_timeout=timedelta(seconds=10),
        )
        key = await stream_key_for(client, handle)

        # One delivery first, so the Run has a marker for the second Worker to
        # reconstruct from and is demonstrably in the no-open-task window.
        await publish(backend, key, ["alpha"], session=session)
        await wait_until(
            lambda: _has_markers(handle),
            60,
            "no marker was written, so there is nothing for a second Worker to "
            "reconstruct the subscription from",
        )
        before = markers(await history(handle))

        probes: list[str] = []
        readiness: list[str] = []
        manager = worker_a._workflow_worker._external_stream_manager
        assert manager is not None, "the manager should exist by now"
        real_probe, real_ready = manager._run_status, manager._notify_ready

        async def counting_probe(run_id: str) -> Any:
            probes.append(run_id)
            return await real_probe(run_id)

        async def counting_ready(run_id: str, wait_id: int, generation: int) -> Any:
            readiness.append(run_id)
            return await real_ready(run_id, wait_id, generation)

        manager._run_status = counting_probe
        manager._notify_ready = counting_ready

        # Buffered on the Worker that is about to go away.
        await publish(backend, key, ["beta"], session=session)
        await asyncio.sleep(0.5)
        readiness.clear()

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
        signals = wake_signals(await history(handle))
        assert signals, (
            "no wake Signal reached the Workflow, so nothing will ever tell it "
            "the buffered record arrived"
        )
        envelope = wake_envelope(signals[-1])
        assert envelope.park_generation == 0, (
            "the shutdown wake must be an unparked one -- there is no confirmed "
            f"park to name -- got generation {envelope.park_generation}"
        )

        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[ParkedByATimerWorkflow],
            external_stream_backends={"tokens-memory": backend},
        ):
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
