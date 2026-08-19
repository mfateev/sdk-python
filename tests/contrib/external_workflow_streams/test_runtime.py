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
    ROLLOVER_HIGH_WATER,
    AnnotationHeader,
    SegmentEndReason,
    StreamBinding,
    decode_annotation,
    encode_header,
    encode_terminal,
)
from temporalio.contrib.external_workflow_streams._backend import StreamKey
from temporalio.contrib.external_workflow_streams._errors import (
    ExternalStreamCapacityError,
)
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
from temporalio.contrib.external_workflow_streams._runtime import (
    _RUN_COST_FLOOR,
    WorkflowStreamRuntime,
)
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


# --- a subscription created after the header went out -----------------------


async def test_a_wait_registered_after_the_first_delta_reaches_the_header(
    runtime: WorkflowStreamRuntime,
) -> None:
    """`register` accepts a subscription at any activation of a retained task.

    The header frame is emitted with the first delta and Core appends rather
    than rewrites, so a wait that joined later cannot be added to it in place.
    Without a binding of its own that wait reaches the marker as a run and a
    terminal entry with no stream key, no backend, and no start cursor, and
    replay of *unchanged* code fails as a wait "this Workflow did not create".
    """
    subscribe(runtime, 1, "tokens")
    runtime.record_delivery(1, data("1-0"))
    first = runtime.take_observation_delta()
    assert first is not None
    assert set(decode_annotation(first).header.streams) == {1}, (
        "the first delta's header is what this test is about; if it already "
        "carried wait 2 the fix under test is not being exercised"
    )

    key = runtime.stream_key("tokens")
    runtime.register(wait_id=2, stream_key=key, backend_name="tokens")
    runtime.record_delivery(2, data("2-0"))
    second = runtime.take_observation_delta()
    assert second is not None
    terminal = runtime.add_terminal()

    decoded = decode_annotation(first + second + terminal)

    assert set(decoded.header.streams) == {1, 2}, (
        "wait 2 has a run and a terminal entry but no binding, so replay has "
        "no stream key, no backend, and no start cursor for it"
    )
    assert decoded.header.streams[2].stream_key == key
    assert decoded.header.streams[2].backend_name == "tokens"
    assert decoded.terminal == {1: AFTER(Offset("1-0")), 2: AFTER(Offset("2-0"))}


async def test_a_late_wait_is_bound_before_the_segment_that_records_it(
    runtime: WorkflowStreamRuntime,
) -> None:
    """Order within the delta, not merely presence somewhere in the marker.

    A decoder that met the run first would have no binding to attach it to, and
    a marker truncated at a rollover boundary would carry the run without the
    binding at all.
    """
    subscribe(runtime, 1)
    runtime.record_delivery(1, data("1-0"))
    first = runtime.take_observation_delta()
    assert first is not None

    subscribe(runtime, 2)
    runtime.record_delivery(2, data("2-0"))
    second = runtime.take_observation_delta()
    assert second is not None

    # 0x04 is the bindings frame tag.
    assert second[0] == 0x04, (
        "the delta must open with the binding for the wait it goes on to "
        f"record a run for, got {second.hex()}"
    )


async def test_a_late_wait_starts_at_its_own_cursor(
    runtime: WorkflowStreamRuntime,
) -> None:
    """Not wherever the waits already in the header have reached.

    The annotation-wide position belongs to the waits that were there when it
    opened. Recording it for a wait that joined afterwards would start replay
    of that wait past records it in fact received.
    """
    subscribe(runtime, 1)
    runtime.record_delivery(1, data("9-0"))
    first = runtime.take_observation_delta()
    assert first is not None

    subscribe(runtime, 2)
    runtime.record_delivery(2, data("3-0"))
    second = runtime.take_observation_delta()
    assert second is not None

    decoded = decode_annotation(first + second + runtime.add_terminal())

    assert decoded.header.streams[2].start_cursor == BEGINNING
    assert decoded.header.streams[1].start_cursor == BEGINNING


async def test_a_wait_is_bound_once_per_annotation(
    runtime: WorkflowStreamRuntime,
) -> None:
    """Three activations, one binding each for the two waits that exist.

    A binding re-emitted every activation would grow the marker with the
    activation count and be rejected on decode as a wait bound twice.
    """
    subscribe(runtime, 1)
    runtime.record_delivery(1, data("1-0"))
    parts = [runtime.take_observation_delta()]

    subscribe(runtime, 2)
    runtime.record_delivery(2, data("2-0"))
    parts.append(runtime.take_observation_delta())

    runtime.record_delivery(2, data("3-0"))
    parts.append(runtime.take_observation_delta())

    joined = b"".join(p for p in parts if p is not None)

    # A re-emitted binding is not merely wasteful: the decoder refuses a wait
    # bound twice, so this decoding at all is the assertion.
    decoded = decode_annotation(joined + runtime.add_terminal())
    assert set(decoded.header.streams) == {1, 2}


async def test_the_next_annotation_binds_every_wait_in_its_own_header(
    runtime: WorkflowStreamRuntime,
) -> None:
    """A late binding belongs to the annotation it was emitted into.

    The next Workflow Task writes a header from scratch, so the wait that
    joined late must appear in *that* header rather than being remembered as
    already announced -- a second marker missing it is the same defect one
    Workflow Task later.
    """
    subscribe(runtime, 1)
    runtime.record_delivery(1, data("1-0"))
    runtime.take_observation_delta()
    subscribe(runtime, 2)
    runtime.record_delivery(2, data("2-0"))
    runtime.take_observation_delta()
    runtime.add_terminal()

    runtime.start_new_annotation()
    runtime.record_delivery(2, data("3-0"))
    decoded = annotation_of(runtime)

    assert set(decoded.header.streams) == {1, 2}  # type: ignore[attr-defined]
    assert decoded.header.streams[2].start_cursor == AFTER(Offset("2-0"))  # type: ignore[attr-defined]


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


async def test_a_closed_wait_can_never_re_enter_the_blocked_set(
    runtime: WorkflowStreamRuntime,
) -> None:
    """Closing keeps the wait's state, so the state has to refuse to block again.

    The subscription is deliberately not forgotten -- its binding is what a
    replay of unchanged code reads, and its cursor is what stops a
    Continue-As-New successor restarting the stream from the beginning -- which
    leaves it reachable by ``wait_id`` long after the coroutine that was
    awaiting it is gone. A wait that could re-enter the blocked set from there
    would be named by the quiescent snapshot, and Core would go back to
    retaining -- and eventually parking -- the Workflow Task for nobody.
    """
    subscribe(runtime, 1)
    runtime.note_blocked(1, False)
    runtime.unsubscribe(1)

    runtime.note_blocked(1, True)

    assert runtime.quiescent_snapshot() is None, (
        "a closed wait re-entered the quiescent set, so Core is asked to hold "
        "the Workflow Task open for a wait nothing is awaiting"
    )
    # And its binding is still there to be read, which is the whole reason
    # closing does not simply drop the state.
    assert set(annotation_of(runtime).header.streams) == {1}  # type: ignore[attr-defined]


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


def _budget_runtime(
    backend: MemoryStreamBackend,
    manager: StreamSubscriptionManager,
    *,
    max_annotation_bytes: int,
) -> WorkflowStreamRuntime:
    return WorkflowStreamRuntime(
        manager=manager,
        backends={"tokens": backend},
        run_id=RUN_ID,
        namespace="ns",
        workflow_id="wf",
        first_execution_run_id="first",
        data_converter=temporalio.converter.DataConverter.default,
        default_idle_timeout=timedelta(seconds=1),
        max_annotation_bytes=max_annotation_bytes,
    )


async def test_the_segment_that_crosses_the_mark_asks_for_rollover_itself(
    backend: MemoryStreamBackend,
) -> None:
    """The crossing segment's own completion carries the request, not the next one.

    ``take_observation_delta`` is what *closes* the activation's segment, so a
    rollover flag sampled before it describes the annotation as it stood one
    activation ago. The segment that crossed the high-water mark then went out
    with ``request_rollover = false``, and the activation after it was free to add
    another frame -- and to overflow -- before Core had ever been asked to roll
    over. One activation of delay is the whole margin the high-water mark exists
    to provide.
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
    runtime = _budget_runtime(backend, manager, max_annotation_bytes=2048)
    try:
        subscribe(runtime, 1)
        subscribe(runtime, 2, name="tool-events")

        # Fill to just short of the mark, one flushed activation at a time, so
        # that the *next* activation is the one that crosses it.
        accumulated: list[bytes] = []
        delivered = 0
        high_water = int(2048 * ROLLOVER_HIGH_WATER)
        while True:
            for wait_id in (1, 2):
                delivered += 1
                runtime.record_delivery(wait_id, data(f"{delivered}-0"))
            delta = runtime.take_observation_delta()
            if delta is not None:
                accumulated.append(delta)
            assert runtime._accumulator is not None
            # Stops with a gap the next activation's segment is bigger than, so
            # that segment is unambiguously the frame that crosses the mark.
            if runtime._accumulator.size >= high_water - 400:
                break
            assert not runtime.request_rollover, (
                "the mark was crossed by a flushed activation, so nothing here "
                "is testing the activation that crosses it"
            )

        # One more activation, whose closing segment is what crosses the mark --
        # and which is therefore the completion that has to carry the request.
        for _ in range(15):
            for wait_id in (1, 2):
                delivered += 1
                runtime.record_delivery(wait_id, data(f"{delivered}-0"))
        assert not runtime.request_rollover, (
            "the crossing segment is still open, so the accumulator cannot know "
            "about it yet -- which is the entire reason the flag must be read "
            "after the delta is taken and not before"
        )

        stub = _CompletionStub(runtime)
        _WorkflowInstanceImpl._emit_external_stream_commands(stub)  # type: ignore[arg-type]

        commands = stub._current_completion.successful.commands
        progress = commands[0].workflow_stream_progress
        assert progress.request_rollover, (
            "the completion that carried the crossing segment did not ask for the "
            "rollover, so an entire further activation may append to an "
            "annotation Core has not been told to close"
        )
        accumulated.append(progress.observation_delta)
        annotation = decode_annotation(b"".join(accumulated))
        assert annotation.terminal is not None, (
            "Core issues no finalization job for a rollover it was asked for, so "
            "the terminal has to ride this same delta"
        )
        assert annotation.segments[-1].end_reason in (
            SegmentEndReason.BUDGET_ROLLOVER,
            SegmentEndReason.NO_DATA_AVAILABLE,
        )
    finally:
        await manager.shutdown()


async def test_a_frame_larger_than_the_slack_rolls_over_instead_of_raising(
    backend: MemoryStreamBackend,
) -> None:
    """An indivisible frame bigger than the remaining budget is not a failure.

    The high-water mark is a *fraction* of the budget, and a segment frame can be
    larger than the fraction that is left: a run costs two provider-supplied
    offset strings, and nothing bounds their length from this side. Checking at
    encode time and raising is what ADR-007 rejects -- the encoding that
    overflowed overflows again on the retry, so the Workflow Task fails forever
    with no marker, no terminal, and no rollover ever asked for.

    The runtime instead stops handing records over while the annotation can still
    record them, and asks for the rollover on that boundary. Long offsets are what
    make one activation's segment expensive here; that is exactly the shape the
    old check-and-fail path could not survive.
    """
    manager = StreamSubscriptionManager(
        backends={"tokens": backend},
        notify_ready=_notify,
        watch_block=timedelta(milliseconds=10),
    )
    runtime = _budget_runtime(backend, manager, max_annotation_bytes=1024)
    long = "o" * 300
    try:
        subscribe(runtime, 1)
        subscribe(runtime, 2, name="tool-events")

        # Each record is its own run, and each run costs two 300-byte offsets --
        # far more than the 25% of 1024 bytes that the high-water margin leaves.
        delivered = 0
        while runtime.delivery_budget_remaining() > 0 and delivered < 50:
            wait_id = 1 + (delivered % 2)
            delivered += 1
            runtime.record_delivery(wait_id, data(f"{long}{delivered}-0"))

        assert delivered < 50, (
            "the annotation budget never stopped delivery, so this test is not "
            "exercising the boundary it exists for"
        )
        assert runtime.request_rollover, (
            "the runtime stopped delivering because the annotation could record "
            "no more, and said nothing about it -- Core is never asked to roll "
            "the task over and the next activation overflows"
        )
        assert runtime.annotation_budget_exhausted
        assert runtime.delivery_budget_exhausted(), (
            "records left buffered by the annotation budget need their readiness "
            "re-reported exactly as the record cap's do"
        )

        # And the annotation still closes, which is what raising would have
        # prevented: a marker with no terminal is durable and undecodable past
        # the frame after it.
        delta = runtime.take_observation_delta()
        assert delta is not None
        terminal = runtime.add_terminal()
        annotation = decode_annotation(delta + terminal)
        assert annotation.terminal is not None
        assert annotation.segments[-1].end_reason == SegmentEndReason.BUDGET_ROLLOVER, (
            "a segment cut by the byte budget has to say so: the batch continues "
            "in the following marker, which no other end reason implies"
        )
    finally:
        await manager.shutdown()


async def test_an_activation_the_annotation_budget_stopped_is_not_wedged_by_it(
    backend: MemoryStreamBackend,
) -> None:
    """Stopping delivery obliges the same completion to ask for the rollover.

    These two are one mechanism and each is useless alone. The annotation budget
    stops delivery so a segment can never overflow; the rollover is what gives the
    next Workflow Task a fresh annotation to deliver into. Stop without asking and
    the Workflow is **wedged**: the next activation begins against the same full
    annotation, is handed a delivery budget of zero, delivers nothing, and so
    observes nothing -- and a rollover condition that depended on having observed
    something would then never become true again.

    That is not hypothetical. The flag is read after `take_observation_delta`,
    which it must be, since that call is what closes the crossing segment -- and
    that call also clears the observed flag. The frame here is deliberately larger
    than the slack the *fractional* high-water mark leaves but small enough that
    the mark itself is not crossed, so the mark cannot cover for it.
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
    runtime = _budget_runtime(backend, manager, max_annotation_bytes=1024)
    long = "o" * 300
    try:
        subscribe(runtime, 1)
        subscribe(runtime, 2, name="tool-events")

        delivered = 0
        while runtime.delivery_budget_remaining() > 0 and delivered < 50:
            wait_id = 1 + (delivered % 2)
            delivered += 1
            runtime.record_delivery(wait_id, data(f"{long}{delivered}-0"))

        assert delivered < 50, "the annotation budget never stopped delivery"
        assert runtime._accumulator is None or not (
            runtime._accumulator.request_rollover
        ), (
            "the accumulator's own high-water mark is asking for this rollover, "
            "so it would mask the condition under test -- the frame has to be "
            "larger than the slack the fractional mark leaves without reaching "
            "the mark itself"
        )

        stub = _CompletionStub(runtime)
        _WorkflowInstanceImpl._emit_external_stream_commands(stub)  # type: ignore[arg-type]

        commands = stub._current_completion.successful.commands
        assert commands[0].workflow_stream_progress.request_rollover, (
            "delivery stopped for the annotation budget and the completion asked "
            "for no rollover, so the next Workflow Task inherits a full "
            "annotation and can never deliver again"
        )
        assert (
            decode_annotation(
                commands[0].workflow_stream_progress.observation_delta
            ).terminal
            is not None
        )

        # And the next Workflow Task can in fact deliver: the annotation was
        # closed and a fresh one begins from its own header.
        assert runtime.delivery_budget_remaining() > 0, (
            "the Workflow Task after the rollover is still unable to take a "
            "single record"
        )
        assert not runtime.annotation_budget_exhausted
    finally:
        await manager.shutdown()


async def test_the_first_record_of_an_activation_is_priced_from_a_measurement(
    backend: MemoryStreamBackend,
) -> None:
    """A per-segment maximum prices every activation's first record at the floor.

    ``close_segment`` empties the open segment at the end of each activation, so a
    price taken from *that* segment's runs is back to the bare floor every time --
    and a real run costs two provider-chosen offset strings, which can be many
    times the floor. Delivery then goes ahead on a price that is wrong by a factor,
    the closing segment no longer fits, and what surfaces is a byte-budget error
    that fails the Workflow.

    The measurement therefore belongs to the *annotation*, not the segment: the
    largest run encoded since the header went out is what the next record costs
    until a larger one appears.
    """
    manager = StreamSubscriptionManager(
        backends={"tokens": backend},
        notify_ready=_notify,
        watch_block=timedelta(milliseconds=10),
    )
    runtime = _budget_runtime(backend, manager, max_annotation_bytes=8192)
    long = "9" * 300
    try:
        subscribe(runtime, 1)
        subscribe(runtime, 2, name="tool-events")

        # One activation's worth of long-offset runs, then the flush that closes
        # its segment. The price has to survive that flush: `close_segment` empties
        # the open segment, and a price read from it is back to the floor.
        for i in range(4):
            runtime.record_delivery(1 + (i % 2), data(f"{long}{i}-0"))
        measured = runtime._max_run_bytes
        assert measured > _RUN_COST_FLOOR, (
            "the long offsets did not even cost more than the floor, so this test "
            "cannot tell a measurement from a guess"
        )

        runtime.take_observation_delta()

        assert runtime._run_sizes == [], "the open segment must have been emptied"
        assert runtime._max_run_bytes == measured, (
            "the price fell back to the floor when the segment was closed, so the "
            "first record of the next activation is priced at "
            f"{_RUN_COST_FLOOR} instead of {measured} -- and delivering on that "
            "price hands over a record the closing segment cannot record"
        )

        # And a fresh annotation *does* start from the floor again, which is right:
        # it has measured nothing, and the measurement is a property of the
        # annotation rather than of the Run.
        runtime.add_terminal()
        assert runtime._max_run_bytes == 0

        # Driven to the end, nothing escapes and every annotation closes: the
        # arithmetic never hands over a record it cannot then record.
        total = 0
        for _ in range(40):
            while runtime.delivery_budget_remaining() > 0 and total < 400:
                total += 1
                runtime.record_delivery(1 + (total % 2), data(f"{long}{total}-0"))
            delta = runtime.take_observation_delta()
            if delta is not None and runtime.request_rollover:
                assert decode_annotation(delta + runtime.add_terminal()).terminal
        assert total > 0, "nothing was ever delivered, so this asserts nothing"
    finally:
        await manager.shutdown()


async def test_a_run_too_large_for_any_annotation_says_so_and_fails_the_workflow(
    backend: MemoryStreamBackend,
) -> None:
    """The one boundary no rollover can move, reported as itself.

    A record is priced before its offsets are seen, so a provider whose offsets are
    far longer than anything measured can make one run cost more than the whole
    budget has left -- and a fresh annotation has to carry that same run, so
    rolling over changes nothing. That is a genuine capacity limit, and the two
    things that matter are how it is *reported*: not as an internal byte-budget
    error, which names nothing an operator can act on, and not as a Workflow Task
    failure, which the server retries on an encoding that cannot succeed (ADR-007).
    """
    manager = StreamSubscriptionManager(
        backends={"tokens": backend},
        notify_ready=_notify,
        watch_block=timedelta(milliseconds=10),
    )
    runtime = _budget_runtime(backend, manager, max_annotation_bytes=2048)
    try:
        subscribe(runtime, 1)
        with pytest.raises(ExternalStreamCapacityError) as caught:
            for i in range(10):
                runtime.record_delivery(1, data(f"{'9' * 5000}{i}-0"))

        assert "offsets" in str(caught.value), (
            f"the error must name what an operator can change: {caught.value}"
        )
        assert caught.value.non_retryable, (
            "a retryable capacity limit is a Workflow Task the server retries "
            "against an encoding that can never fit"
        )
    finally:
        await manager.shutdown()


async def test_a_subscription_set_too_large_to_record_is_refused_at_subscribe(
    backend: MemoryStreamBackend,
) -> None:
    """A header that cannot fit is rejected where the Workflow can still act on it.

    A header is one indivisible frame and a rollover writes a fresh one, so an
    oversized header is not a rollover problem -- every annotation would be the
    same size. Discovered while encoding a completion it fails the Workflow Task,
    the server retries Workflow Task failures regardless of cause, and the retry
    encodes the identical bytes: a Workflow stuck permanently with nothing durable
    to say why.

    Raised from the ``subscribe()`` call instead, it is deterministic, reproduces
    under replay, and names what to change.
    """
    manager = StreamSubscriptionManager(
        backends={"tokens": backend},
        notify_ready=_notify,
        watch_block=timedelta(milliseconds=10),
    )
    runtime = _budget_runtime(backend, manager, max_annotation_bytes=512)
    try:
        subscribe(runtime, 1)
        with pytest.raises(ExternalStreamCapacityError) as caught:
            subscribe(runtime, 2, name="s" * 600)

        assert "512" in str(caught.value)
        assert "2" in str(caught.value), "the error must name the refused wait"
        # Refused *before* it became replay-visible: a wait left half-registered
        # would reach the next header as a binding nothing is watching.
        assert set(runtime.subscriptions()) == {1}
        assert manager.subscription(RUN_ID, 2) is None

        # And the Workflow Task that survives the refusal still completes: the
        # annotation for the subscription it does hold encodes and closes.
        runtime.record_delivery(1, data("1-0"))
        annotation = annotation_of(runtime)
        assert set(annotation.header.streams) == {1}  # type: ignore[attr-defined]
        assert annotation.terminal is not None  # type: ignore[attr-defined]
    finally:
        await manager.shutdown()


async def test_the_capacity_floor_covers_everything_an_empty_annotation_carries(
    backend: MemoryStreamBackend,
) -> None:
    """Clearing header-plus-terminal is not clearing the floor.

    Every annotation also carries at least one **segment frame** -- an activation
    that drained and observed nothing still encodes one, and the empty segment is
    meaningful (ADR-018) -- and the **spill margin** a mispriced record overruns
    into. A check that priced only the header and the terminal accepted a
    subscription set that cleared it by a byte or two and then could not encode its
    very first completion, which relocates the failure rather than preventing it.
    """
    manager = StreamSubscriptionManager(
        backends={"tokens": backend},
        notify_ready=_notify,
        watch_block=timedelta(milliseconds=10),
    )
    try:
        # One wait whose stream name makes its header 230 bytes and its terminal 4.
        # The budget below fits both with room to spare -- and does not fit the
        # 3-byte segment frame and the 64-byte margin behind them.
        name = "s" * 200
        runtime = _budget_runtime(backend, manager, max_annotation_bytes=300)
        header_and_terminal = len(
            encode_header(
                AnnotationHeader(
                    {
                        1: StreamBinding(
                            stream_key=runtime.stream_key(name),
                            start_cursor=BEGINNING,
                            backend_name="tokens",
                            provider_id=MemoryStreamBackend.provider_id,
                            provider_format_version=(
                                MemoryStreamBackend.provider_format_version
                            ),
                        )
                    }
                )
            )
        ) + len(encode_terminal({1: BEGINNING}))
        assert header_and_terminal <= 300, (
            "this set clears header-plus-terminal, which is the whole point: a "
            "floor priced on those two alone accepts it"
        )

        with pytest.raises(ExternalStreamCapacityError) as caught:
            runtime.register(
                wait_id=1, stream_key=runtime.stream_key(name), backend_name="tokens"
            )
        assert "margin" in str(caught.value), (
            f"the error must say what the floor covers: {caught.value}"
        )
        assert set(runtime.subscriptions()) == set()
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
