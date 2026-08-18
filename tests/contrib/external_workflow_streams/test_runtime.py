"""P10a/P10b — the observation delta and the quiescent snapshot.

Driven directly against the runtime handle: what is under test is what gets
*recorded*, and threading it through a real Worker would obscure that behind
scheduling.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import pytest
import pytest_asyncio

import temporalio.converter
from temporalio.contrib.external_workflow_streams._annotation import (
    SegmentEndReason,
    decode_annotation,
)
from temporalio.contrib.external_workflow_streams._backend import StreamKey
from temporalio.contrib.external_workflow_streams._manager import (
    ReadinessResult,
    StreamSubscriptionManager,
)
from temporalio.contrib.external_workflow_streams._record import (
    AFTER,
    BEGINNING,
    Offset,
    RecordKind,
    StreamRecord,
)
from temporalio.contrib.external_workflow_streams._runtime import WorkflowStreamRuntime
from tests.contrib.external_workflow_streams.memory_backend import MemoryStreamBackend

RUN_ID = "run-1"


async def _notify(run_id: str, wait_id: int, generation: int) -> str:
    return ReadinessResult.ACCEPTED


@pytest.fixture
def backend() -> MemoryStreamBackend:
    return MemoryStreamBackend()


@pytest_asyncio.fixture  # type: ignore[reportUntypedFunctionDecorator]
async def runtime(backend: MemoryStreamBackend):
    manager = StreamSubscriptionManager(
        backends={"tokens": backend},
        notify_ready=_notify,
        watch_block=timedelta(milliseconds=10),
    )
    yield WorkflowStreamRuntime(
        manager=manager,
        backends={"tokens": backend},
        run_id=RUN_ID,
        namespace="ns",
        workflow_id="wf",
        first_execution_run_id=uuid.uuid4().hex,
        data_converter=temporalio.converter.DataConverter.default,
        default_idle_timeout=timedelta(seconds=1),
    )
    # Watchers outlive the runtime unless the manager is told to stop, and a
    # leaked watcher keeps polling a backend nothing is reading any more.
    await manager.shutdown()


def subscribe(
    runtime: WorkflowStreamRuntime,
    wait_id: int,
    name: str = "tokens",
    idle_timeout: timedelta | None = None,
) -> StreamKey:
    key = runtime.stream_key(name)
    runtime.register(
        wait_id=wait_id,
        stream_key=key,
        backend_name="tokens",
        idle_timeout=idle_timeout,
    )
    return key


def data(offset: str, session: str = "s", seq: int = 0) -> StreamRecord:
    return StreamRecord(RecordKind.DATA, b"x", session, seq).placed_at(Offset(offset))


def fence(offset: str, session: str = "s", seq: int = 0) -> StreamRecord:
    return StreamRecord(RecordKind.WRITE_FENCE, b"", session, seq).placed_at(
        Offset(offset)
    )


def annotation_of(runtime: WorkflowStreamRuntime) -> object:
    """The annotation Core would hold, decoded.

    Core accumulates deltas by byte concatenation, so this joins them the same
    way rather than reaching for anything the runtime keeps internally.
    """
    parts = []
    delta = runtime.take_observation_delta()
    if delta is not None:
        parts.append(delta)
    parts.append(runtime.add_terminal())
    return decode_annotation(b"".join(parts))


# --- emission is not conditional on records ---------------------------------


async def test_a_first_subscription_to_an_empty_stream_still_emits(
    runtime: WorkflowStreamRuntime,
) -> None:
    """Without this, replay of an empty stream has nowhere to begin.

    The binding carries the stream key, an explicit start cursor, and the
    backend that owns the wait -- none of which is derivable later, because a
    cursor is never re-derived from mutable backend state and a provider label
    cannot say which registered instance a wait was reading.
    """
    key = subscribe(runtime, 1)

    decoded = annotation_of(runtime)

    binding = decoded.header.streams[1]  # type: ignore[attr-defined]
    assert binding.provider_id == "memory"
    assert binding.backend_name == "tokens"
    assert decoded.header.streams[1].stream_key == key  # type: ignore[attr-defined]
    assert decoded.header.streams[1].start_cursor == BEGINNING  # type: ignore[attr-defined]
    assert decoded.terminal == {1: BEGINNING}  # type: ignore[attr-defined]


async def test_an_activation_that_drained_nothing_emits_an_empty_segment(
    runtime: WorkflowStreamRuntime,
) -> None:
    """The drain happened, so replay must reproduce it.

    Reproducing only the record order while changing how many drains occurred
    would change when ``wait_condition`` predicates fire.
    """
    subscribe(runtime, 1)

    decoded = annotation_of(runtime)

    assert len(decoded.segments) == 1  # type: ignore[attr-defined]
    assert decoded.segments[0].runs == ()  # type: ignore[attr-defined]


async def test_an_activation_that_observed_nothing_at_all_emits_nothing(
    runtime: WorkflowStreamRuntime,
) -> None:
    """The one case where a completion legitimately carries no progress."""
    assert runtime.take_observation_delta() is None


# --- runs -------------------------------------------------------------------


async def test_consecutive_deliveries_from_one_stream_collapse_into_one_run(
    runtime: WorkflowStreamRuntime,
) -> None:
    """The claim the whole design rests on: no field in a run is per-record."""
    subscribe(runtime, 1)
    for i in range(1, 1001):
        runtime.record_delivery(1, data(f"{i}-0"))

    decoded = annotation_of(runtime)

    (run,) = decoded.segments[0].runs  # type: ignore[attr-defined]
    assert run.count == 1000
    assert run.first_offset == Offset("1-0")
    assert run.last_offset == Offset("1000-0")


async def test_alternating_streams_produce_one_run_per_delivery(
    runtime: WorkflowStreamRuntime,
) -> None:
    """The honest worst case, recorded as such rather than assumed away.

    Per-stream ranges alone are insufficient: concurrent coroutines could
    otherwise observe a different order than the one that actually happened.
    """
    subscribe(runtime, 1)
    subscribe(runtime, 2, name="tool-events")
    for i, wait_id in enumerate([1, 2, 1, 2], start=1):
        runtime.record_delivery(wait_id, data(f"{i}-0"))

    decoded = annotation_of(runtime)

    runs = decoded.segments[0].runs  # type: ignore[attr-defined]
    assert [r.wait_id for r in runs] == [1, 2, 1, 2]
    assert all(r.count == 1 for r in runs)


async def test_a_run_resumes_after_the_other_stream_interrupts(
    runtime: WorkflowStreamRuntime,
) -> None:
    """Maximal, not merged: the interruption is itself part of the schedule."""
    subscribe(runtime, 1)
    subscribe(runtime, 2, name="tool-events")
    runtime.record_delivery(1, data("1-0"))
    runtime.record_delivery(1, data("2-0"))
    runtime.record_delivery(2, data("3-0"))
    runtime.record_delivery(1, data("4-0"))

    runs = annotation_of(runtime).segments[0].runs  # type: ignore[attr-defined]

    assert [(r.wait_id, r.count) for r in runs] == [(1, 2), (2, 1), (1, 1)]


# --- control records --------------------------------------------------------


async def test_control_records_are_counted_and_their_positions_recorded(
    runtime: WorkflowStreamRuntime,
) -> None:
    """They occupy offsets, so replay's range read will find them.

    Leaving them out of ``count`` would make every such range read find more
    records than the marker claims and fail as integrity loss -- for a stream
    that is perfectly intact.
    """
    subscribe(runtime, 1)
    runtime.record_delivery(1, data("1-0"))
    runtime.record_delivery(1, fence("2-0"))
    runtime.record_delivery(1, data("3-0"))

    (run,) = annotation_of(runtime).segments[0].runs  # type: ignore[attr-defined]

    assert run.count == 3
    assert run.control_positions == (1,)
    assert run.last_offset == Offset("3-0")


async def test_control_positions_are_sparse(runtime: WorkflowStreamRuntime) -> None:
    """One entry per fence, not one kind tag per record."""
    subscribe(runtime, 1)
    for i in range(1, 101):
        runtime.record_delivery(1, data(f"{i}-0"))
    runtime.record_delivery(1, fence("101-0"))

    (run,) = annotation_of(runtime).segments[0].runs  # type: ignore[attr-defined]

    assert run.count == 101
    assert run.control_positions == (100,)


# --- the terminal -----------------------------------------------------------


async def test_the_terminal_is_where_delivery_stopped(
    runtime: WorkflowStreamRuntime,
) -> None:
    subscribe(runtime, 1)
    subscribe(runtime, 2, name="tool-events")
    runtime.record_delivery(1, data("5-0"))

    decoded = annotation_of(runtime)

    assert decoded.terminal == {  # type: ignore[attr-defined]
        1: AFTER(Offset("5-0")),
        2: BEGINNING,
    }


async def test_the_terminal_reads_no_backend(
    runtime: WorkflowStreamRuntime, backend: MemoryStreamBackend
) -> None:
    """Finalization performs no backend I/O (ADR-010).

    The boundary is where this Workflow Task's deliveries stopped, already
    fixed. Refreshing it against the stream could name a position replay must
    not reproduce.
    """
    subscribe(runtime, 1)
    runtime.record_delivery(1, data("5-0"))
    reads_before = len(backend.range_reads)

    runtime.add_terminal()

    assert len(backend.range_reads) == reads_before


# --- deltas concatenate -----------------------------------------------------


async def test_deltas_across_activations_concatenate_into_one_annotation(
    runtime: WorkflowStreamRuntime,
) -> None:
    """What Core holds is the byte concatenation, so that is what is decoded."""
    subscribe(runtime, 1)
    runtime.record_delivery(1, data("1-0"))
    first = runtime.take_observation_delta()

    runtime.record_delivery(1, data("2-0"))
    second = runtime.take_observation_delta()

    terminal = runtime.add_terminal()

    assert first is not None and second is not None
    decoded = decode_annotation(first + second + terminal)
    assert len(decoded.segments) == 2, "one segment per activation, not one per task"
    assert [r.count for s in decoded.segments for r in s.runs] == [1, 1]
    assert decoded.terminal == {1: AFTER(Offset("2-0"))}


async def test_terminating_an_already_closed_annotation_adds_nothing(
    runtime: WorkflowStreamRuntime,
) -> None:
    """A second terminal for one Workflow Task must not start a second annotation.

    Core can ask for a terminal for a boundary it decided -- a rollover
    deadline, a shutdown -- on a task whose last completion had already closed
    the annotation. Creating a fresh header and terminal for that would append a
    complete second annotation to the one Core is about to write, and the marker
    would decode as far as the first terminal and fail on the frame after it.
    """
    subscribe(runtime, 1)
    runtime.record_delivery(1, data("1-0"))
    delta = runtime.take_observation_delta()
    assert delta is not None

    closed = delta + runtime.add_terminal()
    again = runtime.add_terminal()

    assert again == b"", "the annotation was already closed; there was nothing to add"
    decoded = decode_annotation(closed + again)
    assert decoded.terminal == {1: AFTER(Offset("1-0"))}


async def test_a_terminal_after_new_observations_opens_a_fresh_annotation(
    runtime: WorkflowStreamRuntime,
) -> None:
    """The suppression above is about a *closed* annotation, not about the Run.

    Anything observed after the close begins the next annotation, and that one
    needs its own header and its own terminal like any other.
    """
    subscribe(runtime, 1)
    runtime.record_delivery(1, data("1-0"))
    first = runtime.take_observation_delta()
    assert first is not None
    first += runtime.add_terminal()

    runtime.record_delivery(1, data("2-0"))
    second = runtime.take_observation_delta()
    assert second is not None
    second += runtime.add_terminal()

    assert decode_annotation(first).terminal == {1: AFTER(Offset("1-0"))}
    assert decode_annotation(second).terminal == {1: AFTER(Offset("2-0"))}


async def test_a_new_annotation_starts_from_the_current_cursors(
    runtime: WorkflowStreamRuntime,
) -> None:
    """The next Workflow Task continues where this one stopped.

    Restarting the header at the original start cursor would make replay of the
    second marker re-deliver everything the first one already recorded.
    """
    subscribe(runtime, 1)
    runtime.record_delivery(1, data("9-0"))
    runtime.take_observation_delta()
    runtime.add_terminal()

    runtime.start_new_annotation()
    runtime.record_delivery(1, data("10-0"))
    decoded = annotation_of(runtime)

    assert decoded.header.streams[1].start_cursor == AFTER(Offset("9-0"))  # type: ignore[attr-defined]


# --- quiescence (P10a) ------------------------------------------------------


async def test_the_quiescent_snapshot_is_the_complete_blocked_set(
    runtime: WorkflowStreamRuntime,
) -> None:
    """A partial set would be worse than none.

    It would let one idle stream park a Workflow Task another stream is still
    driving, which is the whole reason the timeout is a property of the set.
    """
    subscribe(runtime, 1)
    subscribe(runtime, 2, name="tool-events")
    subscribe(runtime, 3, name="more")

    snapshot = runtime.quiescent_snapshot()

    assert snapshot is not None
    assert [w.wait_id for w in snapshot] == [1, 2, 3]


async def test_nothing_blocked_asks_for_no_retention(
    runtime: WorkflowStreamRuntime,
) -> None:
    subscribe(runtime, 1)
    runtime.note_blocked(1, False)

    assert runtime.quiescent_snapshot() is None


async def test_a_fenced_subscription_reports_immediately_parkable(
    runtime: WorkflowStreamRuntime,
) -> None:
    """One fenced stream does not park the task; Core decides that from the set."""
    subscribe(runtime, 1)
    subscribe(runtime, 2, name="tool-events")
    runtime.record_delivery(1, fence("1-0"))

    snapshot = runtime.quiescent_snapshot()

    assert snapshot is not None
    assert [(w.wait_id, w.immediately_parkable) for w in snapshot] == [
        (1, True),
        (2, False),
    ]


async def test_a_later_record_clears_the_fence(runtime: WorkflowStreamRuntime) -> None:
    """A fence asserts nothing about other producers; a later record just wakes."""
    subscribe(runtime, 1)
    runtime.record_delivery(1, fence("1-0"))
    runtime.record_delivery(1, data("2-0"))

    snapshot = runtime.quiescent_snapshot()

    assert snapshot is not None
    assert snapshot[0].immediately_parkable is False


async def test_differing_idle_timeouts_reduce_to_their_min(
    runtime: WorkflowStreamRuntime,
) -> None:
    """The set shares one timer, so the configured values must reduce to one.

    ``min`` in ``wait_id`` order over the blocked set and nothing else, so the
    result is deterministic and reproduces on replay.
    """
    subscribe(runtime, 1, idle_timeout=timedelta(seconds=5))
    subscribe(runtime, 2, name="b", idle_timeout=timedelta(seconds=2))
    subscribe(runtime, 3, name="c", idle_timeout=timedelta(seconds=9))

    assert runtime.effective_idle_timeout() == timedelta(seconds=2)


async def test_only_blocked_subscriptions_contribute_to_the_reduction(
    runtime: WorkflowStreamRuntime,
) -> None:
    """The inputs are the quiescent set's values and nothing else."""
    subscribe(runtime, 1, idle_timeout=timedelta(seconds=5))
    subscribe(runtime, 2, name="b", idle_timeout=timedelta(milliseconds=1))
    runtime.note_blocked(2, False)

    assert runtime.effective_idle_timeout() == timedelta(seconds=5)


async def test_re_entering_the_blocked_state_bumps_the_generation(
    runtime: WorkflowStreamRuntime,
) -> None:
    """This is what makes a readiness notification for the old block stale."""
    subscribe(runtime, 1)
    snapshot = runtime.quiescent_snapshot()
    assert snapshot is not None and snapshot[0].generation == 0

    runtime.note_blocked(1, False)
    runtime.note_blocked(1, True)

    snapshot = runtime.quiescent_snapshot()
    assert snapshot is not None and snapshot[0].generation == 1


async def test_readiness_after_a_re_block_names_the_current_generation(
    backend: MemoryStreamBackend,
) -> None:
    """Bumping the generation is only half of it; the manager has to be told.

    Core compares the generation a readiness notification carries against the
    one lang put in the quiescent snapshot. The manager is what makes that call
    and only the runtime knows when a wait re-blocks, so a manager left to
    itself reports 0 for the life of the subscription. Every notification after
    the first block is then answered ``Stale``, which the manager treats as
    "re-probe later" -- but the watcher's prefetch cursor is already past the
    record and no second notification is coming, so an append after a confirmed
    park never wakes the Workflow at all.
    """
    reported: list[int] = []
    notified = asyncio.Event()

    async def notify(run_id: str, wait_id: int, generation: int) -> str:
        reported.append(generation)
        notified.set()
        return ReadinessResult.ACCEPTED

    manager = StreamSubscriptionManager(
        backends={"tokens": backend},
        notify_ready=notify,
        watch_block=timedelta(milliseconds=10),
    )
    runtime = WorkflowStreamRuntime(
        manager=manager,
        backends={"tokens": backend},
        run_id=RUN_ID,
        namespace="ns",
        workflow_id="wf",
        first_execution_run_id=uuid.uuid4().hex,
        data_converter=temporalio.converter.DataConverter.default,
        default_idle_timeout=timedelta(seconds=1),
    )
    try:
        key = subscribe(runtime, 1)
        # One resolved block, so the wait is on its second one -- the state
        # every subscription is in for every record after its first.
        runtime.note_blocked(1, False)
        runtime.note_blocked(1, True)

        await backend.append(key, StreamRecord(RecordKind.DATA, b"x", "s", 0))
        await asyncio.wait_for(notified.wait(), 5)

        snapshot = runtime.quiescent_snapshot()
        assert snapshot is not None
        assert reported == [snapshot[0].generation] == [1], (
            "the generation reported to Core is not the one lang put in the "
            f"quiescent snapshot, so Core answers Stale: reported={reported} "
            f"snapshot={snapshot[0].generation}"
        )
    finally:
        await manager.shutdown()


# --- the byte budget --------------------------------------------------------


async def test_rollover_is_requested_before_the_budget_is_reached(
    backend: MemoryStreamBackend,
) -> None:
    """The runtime asks Core to roll the task over rather than grow the marker."""
    manager = StreamSubscriptionManager(
        backends={"tokens": backend},
        notify_ready=_notify,
        watch_block=timedelta(milliseconds=10),
    )
    runtime = WorkflowStreamRuntime(
        manager=manager,
        backends={"tokens": backend},
        run_id=RUN_ID,
        namespace="ns",
        workflow_id="wf",
        first_execution_run_id="first",
        data_converter=temporalio.converter.DataConverter.default,
        default_idle_timeout=timedelta(seconds=1),
        max_annotation_bytes=2048,
    )
    try:
        subscribe(runtime, 1)
        subscribe(runtime, 2, name="tool-events")

        assert not runtime.request_rollover

        delivered = 0
        while not runtime.request_rollover:
            # Alternating, so every delivery is its own run -- the one workload
            # that cannot be range-compressed and therefore the one that reaches
            # the cap.
            for wait_id in (1, 2):
                delivered += 1
                runtime.record_delivery(wait_id, data(f"{delivered}-0"))
            runtime.take_observation_delta()

        assert delivered > 0
    finally:
        await manager.shutdown()


# --- the completion path closes every annotation it ends --------------------


def _progress_delta(stub: object) -> bytes | None:
    """The observation delta a completion is carrying, if any."""
    for command in stub._current_completion.successful.commands:  # type: ignore[attr-defined]
        if command.HasField("workflow_stream_progress"):
            return command.workflow_stream_progress.observation_delta
    return None


async def test_a_command_producing_completion_closes_its_annotation(
    runtime: WorkflowStreamRuntime,
) -> None:
    """Two Workflow Tasks in a row, each ended by the Workflow's own command.

    Core writes and clears a marker on each of them, so each delta has to be a
    whole annotation: its own header, and its own terminal. Carrying the header
    across the first would leave the second starting at whatever frame came
    first, and the leading byte of an annotation is read as its schema version
    -- so the failure is reported as an unsupported version rather than as
    anything to do with framing.
    """
    from temporalio.worker._workflow_instance import _WorkflowInstanceImpl
    from tests.contrib.external_workflow_streams.test_delivery_budget import (
        _CompletionStub,
    )

    subscribe(runtime, 1)
    runtime.record_delivery(1, data("1-0"))
    first = _CompletionStub(runtime)
    first._add_command()  # a timer, an activity: server-bound, so the task ends
    _WorkflowInstanceImpl._emit_external_stream_commands(first)  # type: ignore[arg-type]

    runtime.record_delivery(1, data("2-0"))
    second = _CompletionStub(runtime)
    second._add_command()
    _WorkflowInstanceImpl._emit_external_stream_commands(second)  # type: ignore[arg-type]

    deltas = {"first": _progress_delta(first), "second": _progress_delta(second)}
    # Decoded before anything is asserted about them: it is the *second* delta
    # that loses its header, and asserting per delta as each is decoded would
    # stop on the first one's own shortcoming instead.
    decoded = {}
    for name, delta in deltas.items():
        assert delta is not None, f"the {name} completion reported no progress"
        decoded[name] = decode_annotation(delta)

    for name, annotation in decoded.items():
        assert annotation.header.streams, f"the {name} marker records no stream"
        assert annotation.terminal is not None, (
            f"the {name} marker has no terminal, so nothing in it says where "
            "that Workflow Task's deliveries stopped"
        )

    assert decoded["second"].terminal == {1: AFTER(Offset("2-0"))}


async def test_a_rollover_request_closes_the_annotation_it_splits(
    backend: MemoryStreamBackend,
) -> None:
    """A budget rollover ends the task too, and needs no finalization round trip.

    Core takes ``request_rollover`` as authoritative over the retention the same
    completion asks for, writes the marker, and forces a replacement task --
    without asking for a terminal, because the progress command carrying the
    request is supposed to have carried one. A completion that asked for the
    split without closing the annotation would produce a marker with no terminal
    *and* leave the next one headerless.
    """
    from temporalio.worker._workflow_instance import _WorkflowInstanceImpl
    from tests.contrib.external_workflow_streams.test_delivery_budget import (
        _CompletionStub,
    )

    manager = StreamSubscriptionManager(
        backends={"tokens": backend},
        notify_ready=_notify,
        watch_block=timedelta(milliseconds=10),
    )
    runtime = WorkflowStreamRuntime(
        manager=manager,
        backends={"tokens": backend},
        run_id=RUN_ID,
        namespace="ns",
        workflow_id="wf",
        first_execution_run_id="first",
        data_converter=temporalio.converter.DataConverter.default,
        default_idle_timeout=timedelta(seconds=1),
        max_annotation_bytes=2048,
    )
    try:
        subscribe(runtime, 1)
        subscribe(runtime, 2, name="tool-events")

        # Accumulated the way Core accumulates, by byte append: the marker
        # carries every delta of the Workflow Task, not just its last one.
        accumulated: list[bytes] = []
        delivered = 0
        while not runtime.request_rollover:
            # Alternating, so every delivery is its own run -- the one workload
            # that cannot be range-compressed and therefore the one that reaches
            # the cap.
            for wait_id in (1, 2):
                delivered += 1
                runtime.record_delivery(wait_id, data(f"{delivered}-0"))
            earlier = runtime.take_observation_delta()
            if earlier is not None:
                accumulated.append(earlier)

        # The Workflow is still blocked on both streams, so this completion asks
        # for retention as well -- and the rollover wins over it.
        stub = _CompletionStub(runtime)
        _WorkflowInstanceImpl._emit_external_stream_commands(stub)  # type: ignore[arg-type]

        delta = _progress_delta(stub)
        assert delta is not None, "the completion carried no progress to roll over"
        accumulated.append(delta)
        commands = stub._current_completion.successful.commands
        assert commands[0].workflow_stream_progress.request_rollover, (
            "the completion did not ask Core to roll the task over"
        )
        assert decode_annotation(b"".join(accumulated)).terminal is not None, (
            "the annotation Core is about to write has no terminal, and the "
            "rollover path asks for none"
        )

        # And the next annotation begins from a header of its own.
        runtime.record_delivery(1, data("9999-0"))
        following = runtime.take_observation_delta()
        assert following is not None
        assert decode_annotation(following).header.streams
    finally:
        await manager.shutdown()


async def test_a_fence_ends_the_segment_as_fence_reached(
    runtime: WorkflowStreamRuntime,
) -> None:
    """How replay knows the batch stopped at a fence rather than running dry."""
    subscribe(runtime, 1)
    runtime.record_delivery(1, fence("1-0"))

    decoded = annotation_of(runtime)

    assert decoded.segments[0].end_reason == SegmentEndReason.FENCE_REACHED  # type: ignore[attr-defined]


async def test_running_dry_ends_the_segment_as_no_data_available(
    runtime: WorkflowStreamRuntime,
) -> None:
    subscribe(runtime, 1)
    runtime.record_delivery(1, data("1-0"))

    decoded = annotation_of(runtime)

    assert decoded.segments[0].end_reason == SegmentEndReason.NO_DATA_AVAILABLE  # type: ignore[attr-defined]


# --- registration -----------------------------------------------------------


async def test_naming_an_unregistered_backend_lists_what_is_registered(
    runtime: WorkflowStreamRuntime,
) -> None:
    with pytest.raises(KeyError, match="registered backends are: tokens"):
        runtime.register(
            wait_id=1,
            stream_key=runtime.stream_key("tokens"),
            backend_name="not-registered",
        )


async def test_the_stream_key_comes_from_the_chain_not_the_run(
    runtime: WorkflowStreamRuntime,
) -> None:
    """The stream spans the whole Continue-As-New chain."""
    key = runtime.stream_key("tokens")

    assert key.workflow_id == "wf"
    assert key.first_execution_run_id != RUN_ID
    assert key.stream_name == "tokens"
