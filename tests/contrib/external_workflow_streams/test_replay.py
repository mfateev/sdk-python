"""P13 — the replay read path and its four range checks.

The checks are sufficient *because* every provider guarantees a record's bytes
cannot change, so the only damage replay has to detect is a record that is no
longer there. Each test below deletes a record in a different position, and each
must fail a different one of the four -- which is what makes the set complete
rather than merely plausible.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import pytest
import pytest_asyncio

import temporalio.converter
import temporalio.workflow
from temporalio.contrib.external_workflow_streams._annotation import (
    Annotation,
    AnnotationHeader,
    decode_annotation,
    Run,
    Segment,
    SegmentEndReason,
    StreamBinding,
    encode_annotation,
)
from temporalio.contrib.external_workflow_streams._api import (
    _install_runtime,
    external_stream,
    merge,
)
from temporalio.contrib.external_workflow_streams._backend import StreamKey
from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec
from temporalio.contrib.external_workflow_streams._errors import (
    StreamDecodeError,
    StreamIntegrityError,
    StreamStorageError,
    classify_read_failure,
)
from temporalio.contrib.external_workflow_streams._manager import (
    ReadinessResult,
    StreamSubscriptionManager,
)
from temporalio.contrib.external_workflow_streams._record import (
    AFTER,
    BEGINNING,
    Cursor,
    Offset,
    RecordKind,
    StreamRecord,
)
from temporalio.contrib.external_workflow_streams._replay import (
    validate_run,
)
from tests.contrib.external_workflow_streams.memory_backend import MemoryStreamBackend
from tests.contrib.external_workflow_streams.test_replay_end_to_end import publish

RUN_ID = "run-1"


async def _notify(run_id: str, wait_id: int, generation: int) -> str:
    return ReadinessResult.ACCEPTED


@pytest.fixture
def backend() -> MemoryStreamBackend:
    return MemoryStreamBackend()


@pytest.fixture
def key() -> StreamKey:
    return StreamKey("ns", "wf", uuid.uuid4().hex, "tokens")


@pytest_asyncio.fixture  # type: ignore[reportUntypedFunctionDecorator]
async def manager(backend: MemoryStreamBackend):
    mgr = StreamSubscriptionManager(
        backends={"tokens": backend},
        notify_ready=_notify,
        watch_block=timedelta(milliseconds=10),
    )
    yield mgr
    await mgr.shutdown()


async def append_five(
    backend: MemoryStreamBackend, key: StreamKey
) -> list[StreamRecord]:
    """Five data records, so first / middle / last deletions are all distinct."""
    codec = StreamPayloadCodec(temporalio.converter.DataConverter.default, str)
    placed = []
    for i in range(5):
        placed.append(
            await backend.append(
                key,
                StreamRecord(
                    RecordKind.DATA, await codec.encode(f"v{i}"), "producer", i
                ),
            )
        )
    return placed


def binding(
    key: StreamKey,
    start_cursor: Cursor = BEGINNING,
    *,
    backend_name: str = "tokens",
    provider_id: str = MemoryStreamBackend.provider_id,
    provider_format_version: int = MemoryStreamBackend.provider_format_version,
) -> StreamBinding:
    """A binding naming the backend the manager fixture registers.

    Every binding names its own backend and carries that backend's provider
    identity. Nothing about replay may be derived from an annotation-wide
    provider label: the API lets each topic name a different backend, and two
    instances of one provider -- two Redis clusters, or two key prefixes --
    declare the same id while holding entirely different records.
    """
    return StreamBinding(
        stream_key=key,
        start_cursor=start_cursor,
        backend_name=backend_name,
        provider_id=provider_id,
        provider_format_version=provider_format_version,
    )


def annotation_for(
    key: StreamKey, placed: list[StreamRecord], control_positions: tuple[int, ...] = ()
) -> bytes:
    """One marker recording all of ``placed`` as a single run."""
    first, last = placed[0].offset, placed[-1].offset
    assert first is not None and last is not None
    return encode_annotation(
        Annotation(
            header=AnnotationHeader(streams={1: binding(key)}),
            segments=(
                Segment(
                    runs=(
                        Run(
                            wait_id=1,
                            first_offset=first,
                            last_offset=last,
                            count=len(placed),
                            control_positions=control_positions,
                        ),
                    ),
                    end_reason=SegmentEndReason.NO_DATA_AVAILABLE,
                ),
            ),
            terminal={1: AFTER(last)},
        )
    )


# --- the happy path ----------------------------------------------------------


@pytest.mark.asyncio
async def test_an_intact_range_replays_in_recorded_order(
    manager: StreamSubscriptionManager, backend: MemoryStreamBackend, key: StreamKey
) -> None:
    placed = await append_five(backend, key)

    plan = await manager.prepare_replay(RUN_ID, annotation_for(key, placed))

    assert plan.total_records == 5
    (segment,) = plan.segments
    assert [r.offset for _, r in segment.deliveries] == [r.offset for r in placed]
    assert all(wait_id == 1 for wait_id, _ in segment.deliveries)


@pytest.mark.asyncio
async def test_replay_performs_no_live_waiting(
    manager: StreamSubscriptionManager, backend: MemoryStreamBackend, key: StreamKey
) -> None:
    """It reads recorded ranges; it never asks what comes next.

    A live watch here would block on an empty stream and, worse, could deliver a
    record the original run never saw.
    """
    placed = await append_five(backend, key)

    await manager.prepare_replay(RUN_ID, annotation_for(key, placed))

    # `read_range` is the inclusive replay read; `read_after` is the exclusive
    # live watch, and it must not have been used at all.
    assert backend.range_reads, "replay must read the recorded range"
    assert len(backend.range_reads) == 1, (
        "one read per run, not one per record -- replay I/O is a function of "
        f"the consumed range, got {len(backend.range_reads)} reads for 5 records"
    )


@pytest.mark.asyncio
async def test_one_segment_per_recorded_activation(
    manager: StreamSubscriptionManager, backend: MemoryStreamBackend, key: StreamKey
) -> None:
    """Segments stay apart so replay reproduces the same number of drains.

    Collapsing them would reproduce the record order while changing when
    ``wait_condition`` predicates fire.
    """
    placed = await append_five(backend, key)
    first, second = placed[0].offset, placed[-1].offset
    assert first is not None and second is not None

    annotation = encode_annotation(
        Annotation(
            header=AnnotationHeader({1: binding(key)}),
            segments=(
                Segment(
                    (Run(1, first, placed[1].offset, 2),),  # type: ignore[arg-type]
                    SegmentEndReason.BATCH_LIMIT,
                ),
                Segment((), SegmentEndReason.NO_DATA_AVAILABLE),
                Segment(
                    (Run(1, placed[2].offset, second, 3),),  # type: ignore[arg-type]
                    SegmentEndReason.NO_DATA_AVAILABLE,
                ),
            ),
            terminal={1: AFTER(second)},
        )
    )

    plan = await manager.prepare_replay(RUN_ID, annotation)

    assert [len(s.deliveries) for s in plan.segments] == [2, 0, 3], (
        "the empty segment is a drain that happened and must survive replay"
    )


# --- integrity: a deletion in each position ----------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("position", [0, 2, 4], ids=["first", "middle", "last"])
async def test_deleting_a_record_fails_as_integrity_loss(
    manager: StreamSubscriptionManager,
    backend: MemoryStreamBackend,
    key: StreamKey,
    position: int,
) -> None:
    """And never resolves to an alternate stream result.

    Substituting a later record for a deleted one would hand Workflow code a
    different history than the one its commands were derived from, and the
    divergence would surface much later as an unrelated nondeterminism error.
    """
    placed = await append_five(backend, key)
    annotation = annotation_for(key, placed)
    target = placed[position].offset
    assert target is not None
    await backend.delete_for_test(key, target)

    with pytest.raises(StreamIntegrityError) as caught:
        await manager.prepare_replay(RUN_ID, annotation)

    assert str(target) in str(caught.value) or "contains" in str(caught.value)


@pytest.mark.asyncio
async def test_each_deletion_position_fails_a_different_check(
    backend: MemoryStreamBackend, key: StreamKey
) -> None:
    """What makes the set of four complete rather than merely plausible.

    If a middle deletion and an endpoint deletion tripped the same check, one of
    the four would be redundant and some other damage would be going undetected.
    """
    placed = await append_five(backend, key)
    run = Run(1, placed[0].offset, placed[-1].offset, 5)  # type: ignore[arg-type]

    messages = {}
    for name, position in (("first", 0), ("middle", 2), ("last", 4)):
        surviving = [r for i, r in enumerate(placed) if i != position]
        with pytest.raises(StreamIntegrityError) as caught:
            validate_run(run, surviving, backend)
        messages[name] = str(caught.value)

    assert "is missing from the stream" in messages["first"]
    assert "is missing from the stream" in messages["last"]
    assert "contains 4 record(s)" in messages["middle"]
    assert messages["first"] != messages["last"]


@pytest.mark.asyncio
async def test_a_wrong_control_position_fails_validation(
    backend: MemoryStreamBackend, key: StreamKey
) -> None:
    """A control record swapped for a data record would otherwise be yielded.

    Workflow code never saw the control record the first time, and delivering a
    data record in its place would put a value in front of it that the original
    run never produced.
    """
    placed = await append_five(backend, key)
    run = Run(1, placed[0].offset, placed[-1].offset, 5, control_positions=(2,))  # type: ignore[arg-type]

    with pytest.raises(StreamIntegrityError, match="control records at"):
        validate_run(run, placed, backend)


@pytest.mark.asyncio
async def test_out_of_order_records_fail_validation(
    backend: MemoryStreamBackend, key: StreamKey
) -> None:
    placed = await append_five(backend, key)
    reordered = [placed[0], placed[2], placed[1], placed[3], placed[4]]
    run = Run(1, placed[0].offset, placed[-1].offset, 5)  # type: ignore[arg-type]

    with pytest.raises(StreamIntegrityError, match="strictly increasing"):
        validate_run(run, reordered, backend)


@pytest.mark.asyncio
async def test_a_marker_naming_an_unknown_wait_is_nondeterminism_not_integrity_loss(
    manager: StreamSubscriptionManager, backend: MemoryStreamBackend, key: StreamKey
) -> None:
    """Row four of the taxonomy, and the row it must not be confused with.

    Nothing is wrong with the backend here: the recorded ranges are exactly
    where they were written, and the Workflow code changed underneath them.
    Reporting it as integrity loss would send an operator to repair a backend
    that is fine, when the fix is to version the Workflow code -- the same
    mistake in the opposite direction from calling an outage integrity loss.

    Reported rather than skipped either way, since skipping would silently
    deliver a different stream result.
    """
    placed = await append_five(backend, key)
    annotation = encode_annotation(
        Annotation(
            header=AnnotationHeader({}),
            segments=(
                Segment(
                    (Run(7, placed[0].offset, placed[-1].offset, 5),),  # type: ignore[arg-type]
                    SegmentEndReason.NO_DATA_AVAILABLE,
                ),
            ),
            terminal={},
        )
    )

    with pytest.raises(
        temporalio.workflow.NondeterminismError, match="did not create"
    ) as caught:
        await manager.prepare_replay(RUN_ID, annotation)

    assert not isinstance(caught.value, StreamIntegrityError), (
        "this row must not be reachable through the storage-failure taxonomy"
    )
    assert "workflow.patched" in str(caught.value), (
        "the message must name the remedy, which is versioning the Workflow "
        "code rather than touching the backend"
    )


# --- storage failure is not integrity loss -----------------------------------


class UnreachableBackend(MemoryStreamBackend):
    """Stands in for a backend that is down rather than damaged."""

    async def read_range(self, key, first, last):  # type: ignore[no-untyped-def]
        raise ConnectionError("connection refused")


@pytest.mark.asyncio
async def test_an_unreachable_backend_fails_as_transient_storage(
    key: StreamKey,
) -> None:
    """Distinguished from integrity loss by error type, never by retry behavior.

    The server retries a Workflow Task failure regardless of cause. An outage
    clears on its own; integrity loss does not -- and an operator sent to repair
    a backend that was merely unreachable would find nothing wrong with it.
    """
    reachable = MemoryStreamBackend()
    placed = await append_five(reachable, key)
    annotation = annotation_for(key, placed)

    unreachable = UnreachableBackend()
    manager = StreamSubscriptionManager(
        backends={"tokens": unreachable},
        notify_ready=_notify,
        watch_block=timedelta(milliseconds=10),
    )
    try:
        with pytest.raises(StreamStorageError, match="connection refused"):
            await manager.prepare_replay(RUN_ID, annotation)
    finally:
        await manager.shutdown()


# --- decode failure is not integrity loss ------------------------------------


@pytest.mark.asyncio
async def test_an_intact_but_undecodable_record_is_a_decode_error(
    manager: StreamSubscriptionManager, backend: MemoryStreamBackend, key: StreamKey
) -> None:
    """The range validated, so the bytes are the bytes that were written.

    That makes this a configuration error on the *consumer* -- its converter or
    codec does not match the producer's -- and reporting it as stream integrity
    loss would send an operator to restore a backend that was never damaged.
    """
    placed = []
    for i in range(3):
        placed.append(
            await backend.append(
                key,
                # Not a serialized Payload at all: intact bytes, undecodable.
                StreamRecord(RecordKind.DATA, b"\\xff\\xfe raw", "producer", i),
            )
        )
    annotation = annotation_for(key, placed)

    # The range itself validates -- nothing is missing.
    plan = await manager.prepare_replay(RUN_ID, annotation)
    assert plan.total_records == 3

    # It is only decoding that fails, and only then is it classified.
    codec = StreamPayloadCodec(temporalio.converter.DataConverter.default, str)
    _, record = plan.segments[0].deliveries[0]
    try:
        await codec.decode(record.payload)
        raise AssertionError("expected the record to be undecodable")
    except AssertionError:
        raise
    except Exception as err:
        classified = classify_read_failure(range_validated=True, cause=err)

    assert isinstance(classified, StreamDecodeError)
    assert not isinstance(classified, StreamIntegrityError)


@pytest.mark.asyncio
async def test_a_replay_plan_is_consumed_once(
    manager: StreamSubscriptionManager, backend: MemoryStreamBackend, key: StreamKey
) -> None:
    """A second delivery would replay the same records twice."""
    placed = await append_five(backend, key)
    await manager.prepare_replay(RUN_ID, annotation_for(key, placed))

    assert manager.take_replay_plan(RUN_ID) is not None
    assert manager.take_replay_plan(RUN_ID) is None


@pytest.mark.asyncio
async def test_eviction_discards_an_unconsumed_plan(
    manager: StreamSubscriptionManager, backend: MemoryStreamBackend, key: StreamKey
) -> None:
    """A plan outliving its Run would be delivered to whatever came next."""
    placed = await append_five(backend, key)
    await manager.prepare_replay(RUN_ID, annotation_for(key, placed))

    await manager.evict_run(RUN_ID)

    assert manager.take_replay_plan(RUN_ID) is None


# --- the delivery driver (ADR-018) -------------------------------------------


class DriverStub:
    """The two things the replay driver is allowed to touch.

    Calling the real ``_apply_replay_external_streams`` against this asserts what
    the driver does rather than what a Workflow happens to observe -- a Workflow
    that never used ``wait_condition`` would pass either way.
    """

    def __init__(self, runtime) -> None:  # type: ignore[no-untyped-def]
        self._external_stream_runtime = runtime
        self.drains: list[int] = []

    def _run_once(self, *, check_conditions: bool) -> None:
        assert check_conditions, (
            "a replay drain must check conditions -- otherwise `wait_condition` "
            "predicates fire fewer times than they did live"
        )
        self.drains.append(len(self.drains))


def make_runtime(manager, backend):  # type: ignore[no-untyped-def]
    from temporalio.contrib.external_workflow_streams._runtime import (
        WorkflowStreamRuntime,
    )

    return WorkflowStreamRuntime(
        manager=manager,
        backends={"tokens": backend},
        run_id=RUN_ID,
        namespace="ns",
        workflow_id="wf",
        first_execution_run_id=uuid.uuid4().hex,
        data_converter=temporalio.converter.DataConverter.default,
        default_idle_timeout=timedelta(seconds=1),
    )


class ConsumingStub(DriverStub):
    """A stub that also takes what the segment holds, as Workflow code does.

    Every record in a run was handed to Workflow code in the activation that run
    was recorded in, so a stub that drains nothing is not a stand-in for a
    Workflow -- it is a stand-in for a Workflow that stopped subscribing, which
    the driver now reports as nondeterminism. Tests that only care about how
    many drains happened use this; tests that assert *what* each drain saw drain
    for themselves.
    """

    def __init__(self, runtime) -> None:  # type: ignore[no-untyped-def]
        super().__init__(runtime)
        self._runtime = runtime

    def _run_once(self, *, check_conditions: bool) -> None:
        super()._run_once(check_conditions=check_conditions)
        for wait_id in sorted({w for w, _ in self._runtime._replay_ready or []}):
            self._runtime.drain(wait_id)


def drive(runtime) -> DriverStub:  # type: ignore[no-untyped-def]
    from temporalio.worker._workflow_instance import _WorkflowInstanceImpl

    stub = ConsumingStub(runtime)
    _WorkflowInstanceImpl._apply_replay_external_streams(stub, object())  # type: ignore[arg-type]
    return stub


@pytest.mark.asyncio
async def test_the_driver_drains_once_per_recorded_segment(
    manager: StreamSubscriptionManager, backend: MemoryStreamBackend, key: StreamKey
) -> None:
    """Three recorded activations become three drains, not one.

    Delivering all five records in a single drain would reproduce the record
    order while changing how many times the event loop ran, and a
    ``wait_condition`` predicate would then see a different sequence of states
    than it did live -- a nondeterminism that only shows up on replay.
    """
    placed = await append_five(backend, key)
    annotation = encode_annotation(
        Annotation(
            header=AnnotationHeader({1: binding(key)}),
            segments=(
                Segment(
                    (Run(1, placed[0].offset, placed[1].offset, 2),),
                    SegmentEndReason.BATCH_LIMIT,
                ),  # type: ignore[arg-type]
                Segment(
                    (Run(1, placed[2].offset, placed[2].offset, 1),),
                    SegmentEndReason.BATCH_LIMIT,
                ),  # type: ignore[arg-type]
                Segment(
                    (Run(1, placed[3].offset, placed[4].offset, 2),),
                    SegmentEndReason.NO_DATA_AVAILABLE,
                ),  # type: ignore[arg-type]
            ),
            terminal={1: AFTER(placed[4].offset)},  # type: ignore[arg-type]
        )
    )
    await manager.prepare_replay(RUN_ID, annotation)
    runtime = make_runtime(manager, backend)

    stub = drive(runtime)

    assert len(stub.drains) == 3, (
        f"expected one drain per recorded segment, got {len(stub.drains)}"
    )


@pytest.mark.asyncio
async def test_a_drain_sees_only_its_own_segments_records(
    manager: StreamSubscriptionManager, backend: MemoryStreamBackend, key: StreamKey
) -> None:
    """Records may not run ahead of the drain they were recorded in.

    Buffering all of them and letting the first drain take everything would put
    values in front of Workflow code before the activation that produced them.
    """
    placed = await append_five(backend, key)
    annotation = encode_annotation(
        Annotation(
            header=AnnotationHeader({1: binding(key)}),
            segments=(
                Segment(
                    (Run(1, placed[0].offset, placed[1].offset, 2),),
                    SegmentEndReason.BATCH_LIMIT,
                ),  # type: ignore[arg-type]
                Segment(
                    (Run(1, placed[2].offset, placed[4].offset, 3),),
                    SegmentEndReason.NO_DATA_AVAILABLE,
                ),  # type: ignore[arg-type]
            ),
            terminal={1: AFTER(placed[4].offset)},  # type: ignore[arg-type]
        )
    )
    await manager.prepare_replay(RUN_ID, annotation)
    runtime = make_runtime(manager, backend)

    seen_per_drain: list[list[Offset]] = []

    class DrainingStub(DriverStub):
        def _run_once(self, *, check_conditions: bool) -> None:
            super()._run_once(check_conditions=check_conditions)
            seen_per_drain.append([r.offset for r in runtime.drain(1)])  # type: ignore[misc]

    from temporalio.worker._workflow_instance import _WorkflowInstanceImpl

    _WorkflowInstanceImpl._apply_replay_external_streams(  # type: ignore[arg-type]
        DrainingStub(runtime), object()
    )

    assert seen_per_drain == [
        [placed[0].offset, placed[1].offset],
        [placed[2].offset, placed[3].offset, placed[4].offset],
    ]


@pytest.mark.asyncio
async def test_the_driver_reads_nothing_and_ends_replay_mode(
    manager: StreamSubscriptionManager, backend: MemoryStreamBackend, key: StreamKey
) -> None:
    """Delivery is memory-only, and the Run goes back to live drains afterwards.

    Leaving replay mode latched would make every later live drain read from an
    empty recorded buffer, and the Workflow would block forever on records that
    had already arrived.
    """
    placed = await append_five(backend, key)
    await manager.prepare_replay(RUN_ID, annotation_for(key, placed))
    runtime = make_runtime(manager, backend)
    before = len(backend.range_reads)

    drive(runtime)

    assert len(backend.range_reads) == before, "the delivering pass must read nothing"
    assert runtime._replay_ready is None, "replay mode must not outlive the job"


@pytest.mark.asyncio
async def test_a_replayed_drain_never_reaches_the_live_buffer(
    manager: StreamSubscriptionManager, backend: MemoryStreamBackend, key: StreamKey
) -> None:
    """A watcher may have buffered past where the marker stops.

    That buffer is speculative -- reading a record is not consuming it -- so
    delivering it would hand Workflow code records this Workflow Task never saw.
    Replay would then be the thing introducing the nondeterminism it exists to
    prevent, which is why the recorded segment is the *only* thing a drain can
    see while one is being delivered.
    """
    placed = await append_five(backend, key)
    # The marker committed the first two records only.
    await manager.prepare_replay(RUN_ID, annotation_for(key, placed[:2]))
    runtime = make_runtime(manager, backend)
    runtime.register(wait_id=1, stream_key=key, backend_name="tokens")
    subscription = manager.subscription(RUN_ID, 1)
    assert subscription is not None
    subscription._append(placed[2:])

    seen: list[list[Offset]] = []

    class DrainingStub(DriverStub):
        def _run_once(self, *, check_conditions: bool) -> None:
            super()._run_once(check_conditions=check_conditions)
            seen.append([r.offset for r in runtime.drain(1)])  # type: ignore[misc]

    from temporalio.worker._workflow_instance import _WorkflowInstanceImpl

    _WorkflowInstanceImpl._apply_replay_external_streams(  # type: ignore[arg-type]
        DrainingStub(runtime), object()
    )

    assert seen == [[placed[0].offset, placed[1].offset]]
    # And the buffered-past records are still there for the live drain that
    # follows -- withheld, not discarded.
    assert [r.offset for r in runtime.drain(1)] == [r.offset for r in placed[2:]]


@pytest.mark.asyncio
async def test_two_markers_reassemble_in_workflow_task_order(
    manager: StreamSubscriptionManager, backend: MemoryStreamBackend, key: StreamKey
) -> None:
    """A batch split by a rollover replays as one continuous consumption.

    The byte-budget high-water mark rolls the Workflow Task over rather than
    growing the marker, so one logical batch ends up in *two* markers -- and
    replay reaches them as two separate ``ReplayExternalStreams`` jobs, one per
    replayed Workflow Task. Core's own test proves the two annotations
    concatenate at the byte level; what only this side can show is that feeding
    them through the manager in Workflow Task order reproduces the original
    consumption exactly.

    Two failures this rules out, both silent:

    - a seam that loses or repeats a record, which is what an exclusive read or
      an off-by-one at the split would produce -- the second marker resumes at
      the first's terminal, so the record *at* that boundary must appear exactly
      once across the pair;
    - a second marker delivered against the first one's segmentation, which
      would put records in front of Workflow code in a drain that never saw
      them.
    """
    placed = await append_five(backend, key)
    split = 1  # The first marker stops after `placed[1]`.

    first_marker = encode_annotation(
        Annotation(
            header=AnnotationHeader({1: binding(key)}),
            segments=(
                Segment(
                    (Run(1, placed[0].offset, placed[0].offset, 1),),  # type: ignore[arg-type]
                    SegmentEndReason.BATCH_LIMIT,
                ),
                Segment(
                    (Run(1, placed[1].offset, placed[1].offset, 1),),  # type: ignore[arg-type]
                    # The annotation passed its high-water mark here, so this
                    # batch continues in the following marker.
                    SegmentEndReason.BUDGET_ROLLOVER,
                ),
            ),
            terminal={1: AFTER(placed[split].offset)},  # type: ignore[arg-type]
        )
    )
    # The replacement Workflow Task's annotation begins where the first one's
    # terminal left the subscription: a rollover preserves the cursor rather
    # than restarting it.
    second_marker = encode_annotation(
        Annotation(
            header=AnnotationHeader(
                {1: binding(key, AFTER(placed[split].offset))}  # type: ignore[arg-type]
            ),
            segments=(
                Segment(
                    (Run(1, placed[2].offset, placed[4].offset, 3),),  # type: ignore[arg-type]
                    SegmentEndReason.NO_DATA_AVAILABLE,
                ),
            ),
            terminal={1: AFTER(placed[4].offset)},  # type: ignore[arg-type]
        )
    )

    runtime = make_runtime(manager, backend)
    per_marker: list[list[list[Offset]]] = []

    for annotation in (first_marker, second_marker):
        await manager.prepare_replay(RUN_ID, annotation)
        drains: list[list[Offset]] = []

        class DrainingStub(DriverStub):
            def _run_once(self, *, check_conditions: bool) -> None:
                super()._run_once(check_conditions=check_conditions)
                drains.append([r.offset for r in runtime.drain(1)])  # type: ignore[misc]

        from temporalio.worker._workflow_instance import _WorkflowInstanceImpl

        _WorkflowInstanceImpl._apply_replay_external_streams(  # type: ignore[arg-type]
            DrainingStub(runtime), object()
        )
        per_marker.append(drains)

    assert per_marker == [
        [[placed[0].offset], [placed[1].offset]],
        [[placed[2].offset, placed[3].offset, placed[4].offset]],
    ], (
        "each marker must deliver its own segments and nothing else; a marker "
        "reassembled against the wrong segmentation puts records in a drain "
        f"that never saw them, got {per_marker}"
    )

    reassembled = [
        offset for drains in per_marker for drain in drains for offset in drain
    ]
    assert reassembled == [r.offset for r in placed], (
        "the two markers must reassemble into the original consumption, in "
        f"Workflow Task order and with nothing lost or repeated at the seam: "
        f"{reassembled}"
    )


@pytest.mark.asyncio
async def test_one_segment_delivers_each_waits_own_records(
    manager: StreamSubscriptionManager, backend: MemoryStreamBackend, key: StreamKey
) -> None:
    """A segment can carry runs from several subscriptions at once.

    Two subscriptions are two independent cursors, so handing one's records to
    the other is a different stream result rather than merely a different order.
    """
    other = StreamKey(
        key.namespace, key.workflow_id, key.first_execution_run_id, "tool-events"
    )
    placed = await append_five(backend, key)
    other_placed = await append_five(backend, other)

    annotation = encode_annotation(
        Annotation(
            header=AnnotationHeader({1: binding(key), 2: binding(other)}),
            segments=(
                Segment(
                    (
                        Run(1, placed[0].offset, placed[4].offset, 5),  # type: ignore[arg-type]
                        Run(2, other_placed[0].offset, other_placed[4].offset, 5),  # type: ignore[arg-type]
                    ),
                    SegmentEndReason.NO_DATA_AVAILABLE,
                ),
            ),
            terminal={1: AFTER(placed[4].offset), 2: AFTER(other_placed[4].offset)},  # type: ignore[arg-type]
        )
    )
    await manager.prepare_replay(RUN_ID, annotation)
    runtime = make_runtime(manager, backend)

    drained: dict[int, list[Offset]] = {}

    class DrainingStub(DriverStub):
        def _run_once(self, *, check_conditions: bool) -> None:
            super()._run_once(check_conditions=check_conditions)
            for wait_id in (1, 2):
                drained[wait_id] = [r.offset for r in runtime.drain(wait_id)]  # type: ignore[misc]

    from temporalio.worker._workflow_instance import _WorkflowInstanceImpl

    _WorkflowInstanceImpl._apply_replay_external_streams(  # type: ignore[arg-type]
        DrainingStub(runtime), object()
    )

    assert drained[1] == [r.offset for r in placed]
    assert drained[2] == [r.offset for r in other_placed]


# --- what the live buffer must look like afterwards ---------------------------


async def _until(predicate, timeout: float, message: str) -> None:  # type: ignore[no-untyped-def]
    """Polls rather than sleeping a fixed time, so a slow watcher is not a flake."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError(message)


@pytest.mark.asyncio
async def test_live_delivery_after_a_replay_resumes_past_the_marker(
    manager: StreamSubscriptionManager, backend: MemoryStreamBackend, key: StreamKey
) -> None:
    """A replayed marker's records must not be handed over a second time, live.

    Replay delivers from the annotation, but the manager's watcher has been
    reading the *same* records from the subscription's start cursor into the
    live buffer the whole time -- nothing out there knows the marker exists. So
    the first live drain after a replay re-delivers everything the marker
    recorded: observed end-to-end as a Workflow that received
    ``['alpha', 'alpha', 'beta', 'gamma']``.

    Repositioning to the marker's terminal is what makes live delivery resume
    after the recorded range rather than in front of it.
    """
    placed = await append_five(backend, key)
    runtime = make_runtime(manager, backend)
    runtime.register(wait_id=1, stream_key=key, backend_name="tokens")

    subscription = manager.subscription(RUN_ID, 1)
    assert subscription is not None
    await _until(
        lambda: subscription.buffered == 5,
        5,
        "the watcher never buffered the records, so nothing would be "
        "re-delivered either way and this proves nothing",
    )

    # A marker recording only the first two of the five, so "resumed past it"
    # and "delivered nothing at all" are distinguishable.
    await manager.prepare_replay(RUN_ID, annotation_for(key, placed[:2]))
    drive(runtime)
    # The reposition is hopped onto this loop, exactly as the Workflow thread
    # would hop it.
    await asyncio.sleep(0)

    await _until(
        lambda: subscription.buffered == 3,
        5,
        "the buffer did not refill from the marker's boundary",
    )
    assert [r.offset for r in manager.drain(RUN_ID, 1)] == [
        r.offset for r in placed[2:]
    ], (
        "the live buffer still holds records the replayed marker already "
        "delivered, so the Workflow receives them twice"
    )
    assert subscription.committed_cursor == AFTER(placed[1].offset), (
        "the marker's boundary was never committed, so a later reset would "
        "send the subscription back to the start cursor"
    )


# --- the annotation must match the subscriptions the Workflow makes ----------
#
# Delivery joins a recorded run to a subscription by `wait_id`, and an integer
# is not an identity. Everything below is row four of the failure taxonomy --
# nondeterminism, remedied by versioning the Workflow -- and specifically *not*
# `StreamIntegrityError`, which would send an operator to repair a backend that
# holds exactly the bytes it was given.


async def _one_record(backend: MemoryStreamBackend, key: StreamKey) -> StreamRecord:
    await publish(backend, key, [f"{key.stream_name}-value"])
    return backend.all_records(key)[-1]


def _single_run_annotation(bindings, runs) -> bytes:  # type: ignore[no-untyped-def]
    """One segment holding one run per wait, and a terminal that closes it."""
    return encode_annotation(
        Annotation(
            header=AnnotationHeader(bindings),
            segments=(Segment(tuple(runs), SegmentEndReason.NO_DATA_AVAILABLE),),
            terminal={run.wait_id: AFTER(run.last_offset) for run in runs},
        )
    )


@pytest.mark.asyncio
async def test_a_wait_rebound_to_another_stream_is_nondeterminism(
    manager: StreamSubscriptionManager, backend: MemoryStreamBackend
) -> None:
    """Moving wait 1 from one stream to another must fail, not deliver.

    This is the silent failure the whole check exists for: with the join done on
    ``wait_id`` alone, the left stream's recorded bytes are handed to Workflow
    code through the right stream's subscription. Nothing anywhere reports it --
    the range validates, because the records really are where the marker says --
    and the Workflow proceeds on data it never received.

    The taxonomy puts "annotation does not match the subscriptions made" in the
    nondeterminism row precisely so the remedy is versioning the Workflow rather
    than repairing a healthy backend.
    """
    left = StreamKey("ns", "wf", uuid.uuid4().hex, "left")
    right = StreamKey("ns", "wf", uuid.uuid4().hex, "right")
    recorded = await _one_record(backend, left)
    await _one_record(backend, right)

    await manager.prepare_replay(
        RUN_ID,
        _single_run_annotation(
            {1: binding(left)},
            [Run(1, recorded.offset, recorded.offset, 1)],  # type: ignore[arg-type]
        ),
    )
    runtime = make_runtime(manager, backend)
    # The Workflow's `subscribe()` calls are unchanged in number and order, so
    # wait 1 is still wait 1 -- it just reads a different stream now.
    runtime.register(wait_id=1, stream_key=right, backend_name="tokens")

    with pytest.raises(temporalio.workflow.NondeterminismError) as caught:
        drive(runtime)

    assert not isinstance(caught.value, StreamIntegrityError), (
        "the backend holds exactly what it was given; this must not be "
        "reachable through the storage-failure taxonomy"
    )
    assert "workflow.patched" in str(caught.value)
    assert runtime.drain(1) == [], (
        "the recorded record must never reach the rebound subscription"
    )


@pytest.mark.asyncio
async def test_a_rebinding_made_during_the_replay_activation_is_caught_too(
    manager: StreamSubscriptionManager, backend: MemoryStreamBackend
) -> None:
    """The other order, which is the ordinary one for a Run's first marker.

    An activation carrying both ``InitializeWorkflow`` and
    ``ReplayExternalStreams`` applies every job before any Workflow code runs,
    so the ``subscribe()`` call happens *inside* the replay driver's first
    drain. Checking only what was registered beforehand would find nothing to
    check on exactly the activation where the first marker is replayed.
    """
    left = StreamKey("ns", "wf", uuid.uuid4().hex, "left")
    right = StreamKey("ns", "wf", uuid.uuid4().hex, "right")
    recorded = await _one_record(backend, left)
    await _one_record(backend, right)

    await manager.prepare_replay(
        RUN_ID,
        _single_run_annotation(
            {1: binding(left)},
            [Run(1, recorded.offset, recorded.offset, 1)],  # type: ignore[arg-type]
        ),
    )
    runtime = make_runtime(manager, backend)

    class LateRegisteringStub(DriverStub):
        def _run_once(self, *, check_conditions: bool) -> None:
            super()._run_once(check_conditions=check_conditions)
            runtime.register(wait_id=1, stream_key=right, backend_name="tokens")

    from temporalio.worker._workflow_instance import _WorkflowInstanceImpl

    with pytest.raises(temporalio.workflow.NondeterminismError, match="workflow"):
        _WorkflowInstanceImpl._apply_replay_external_streams(  # type: ignore[arg-type]
            LateRegisteringStub(runtime), object()
        )


@pytest.mark.asyncio
async def test_a_wait_rebound_to_another_backend_is_nondeterminism(
    manager: StreamSubscriptionManager, backend: MemoryStreamBackend
) -> None:
    """The backend a topic names is Workflow code, so moving it is a code change.

    Same stream name, different store: the records the marker recorded live in
    the backend that wrote them, and reading a second store because the wait
    number matched is the same silent substitution as reading a second stream.
    """
    key = StreamKey("ns", "wf", uuid.uuid4().hex, "tokens")
    recorded = await _one_record(backend, key)
    manager._backends["other"] = MemoryStreamBackend()

    await manager.prepare_replay(
        RUN_ID,
        _single_run_annotation(
            {1: binding(key)},
            [Run(1, recorded.offset, recorded.offset, 1)],  # type: ignore[arg-type]
        ),
    )
    runtime = make_runtime(manager, backend)
    runtime._backends = dict(manager._backends)  # type: ignore[attr-defined]
    runtime.register(wait_id=1, stream_key=key, backend_name="other")

    with pytest.raises(temporalio.workflow.NondeterminismError, match="backend"):
        drive(runtime)


@pytest.mark.asyncio
async def test_a_removed_subscription_leaves_recorded_deliveries_unconsumed(
    manager: StreamSubscriptionManager, backend: MemoryStreamBackend
) -> None:
    """Records the marker recorded may not be quietly dropped.

    Deleting a ``subscribe()`` call leaves its wait unregistered, so nothing
    drains it and the prepared deliveries are discarded when the segment is
    replaced. The Workflow then reaches its next command having consumed less
    than History says it consumed -- which surfaces later, somewhere else, as an
    unrelated command mismatch.
    """
    left = StreamKey("ns", "wf", uuid.uuid4().hex, "left")
    right = StreamKey("ns", "wf", uuid.uuid4().hex, "right")
    left_record = await _one_record(backend, left)
    right_record = await _one_record(backend, right)

    await manager.prepare_replay(
        RUN_ID,
        _single_run_annotation(
            {1: binding(left), 2: binding(right)},
            [
                Run(1, left_record.offset, left_record.offset, 1),  # type: ignore[arg-type]
                Run(2, right_record.offset, right_record.offset, 1),  # type: ignore[arg-type]
            ],
        ),
    )
    runtime = make_runtime(manager, backend)
    # Wait 2's `subscribe()` call is gone; wait 1 is untouched.
    runtime.register(wait_id=1, stream_key=left, backend_name="tokens")

    class OnlyWaitOneStub(DriverStub):
        def _run_once(self, *, check_conditions: bool) -> None:
            super()._run_once(check_conditions=check_conditions)
            runtime.drain(1)

    from temporalio.worker._workflow_instance import _WorkflowInstanceImpl

    with pytest.raises(temporalio.workflow.NondeterminismError) as caught:
        _WorkflowInstanceImpl._apply_replay_external_streams(  # type: ignore[arg-type]
            OnlyWaitOneStub(runtime), object()
        )

    assert "[2]" in str(caught.value), (
        f"the error must name the wait whose records went undelivered: {caught.value}"
    )


# --- one wait, one backend instance ------------------------------------------


@pytest_asyncio.fixture  # type: ignore[reportUntypedFunctionDecorator]
async def two_backend_manager():  # type: ignore[no-untyped-def]
    """Two instances of the *same* provider, registered under two names.

    The configuration a single provider label cannot describe: both declare
    provider id ``memory``, and they hold entirely different records -- exactly
    as two Redis clusters, or two key prefixes on one cluster, would.
    """
    left, right = MemoryStreamBackend(), MemoryStreamBackend()
    mgr = StreamSubscriptionManager(
        backends={"left": left, "right": right},
        notify_ready=_notify,
        watch_block=timedelta(milliseconds=10),
    )
    yield mgr, left, right
    await mgr.shutdown()


@pytest.mark.asyncio
async def test_each_waits_range_is_read_from_the_backend_that_owns_it(
    two_backend_manager,  # type: ignore[no-untyped-def]
) -> None:
    """Replay reads a wait's recorded range from the store that wrote it.

    Resolving a wait by searching for the first registered backend declaring the
    recorded provider id cannot do this: a provider id names an implementation,
    not a store. The failure is not even clean -- the range simply is not in the
    instance that was picked, and it is reported as integrity loss against a
    backend nothing is wrong with.

    Prepared before any subscription exists, which is the real ordering: on
    replay the Workflow has not run far enough to call ``subscribe()``, so the
    annotation is the only thing that can say where to read.
    """
    manager, left_backend, right_backend = two_backend_manager
    left = StreamKey("ns", "wf", uuid.uuid4().hex, "tokens")
    right = StreamKey("ns", "wf", uuid.uuid4().hex, "tokens")
    left_record = await _one_record(left_backend, left)
    right_record = await _one_record(right_backend, right)

    plan = await manager.prepare_replay(
        RUN_ID,
        _single_run_annotation(
            {
                1: binding(left, backend_name="left"),
                2: binding(right, backend_name="right"),
            },
            [
                Run(1, left_record.offset, left_record.offset, 1),  # type: ignore[arg-type]
                Run(2, right_record.offset, right_record.offset, 1),  # type: ignore[arg-type]
            ],
        ),
    )

    assert len(left_backend.range_reads) == 1, (
        f"wait 1 must be read from the instance holding it, got "
        f"{left_backend.range_reads}"
    )
    assert len(right_backend.range_reads) == 1, (
        "wait 2's range was never read from the instance that owns it; it was "
        "routed to the other instance, which declares the same provider id"
    )
    (segment,) = plan.segments
    assert [(w, r.offset) for w, r in segment.deliveries] == [
        (1, left_record.offset),
        (2, right_record.offset),
    ]


@pytest.mark.asyncio
async def test_a_backend_declaring_another_provider_is_refused_before_any_read(
    two_backend_manager,  # type: ignore[no-untyped-def]
) -> None:
    """A name rebound to a different implementation is a deployment problem.

    Neither nondeterminism -- the Workflow is unchanged -- nor integrity loss:
    the store that wrote the records is simply not the one registered under that
    name here. It is refused before the read, so an implementation that cannot
    interpret those offsets never gets to try.
    """
    manager, left_backend, _ = two_backend_manager
    left = StreamKey("ns", "wf", uuid.uuid4().hex, "tokens")
    record = await _one_record(left_backend, left)

    with pytest.raises(StreamStorageError, match="declares"):
        await manager.prepare_replay(
            RUN_ID,
            _single_run_annotation(
                {1: binding(left, backend_name="left", provider_id="redis-streams")},
                [Run(1, record.offset, record.offset, 1)],  # type: ignore[arg-type]
            ),
        )

    assert left_backend.range_reads == [], (
        "the recorded range must not be read through an implementation that "
        "did not write it"
    )


@pytest.mark.asyncio
async def test_an_unreadable_provider_format_version_is_refused_before_any_read(
    two_backend_manager,  # type: ignore[no-untyped-def]
) -> None:
    """The recorded format version is checked, not merely stored.

    A provider that keeps its id while changing how it lays records out is the
    case a matching id cannot catch: the offsets read back mean something else,
    and every one of replay's four range checks would be applied to the wrong
    interpretation of the bytes.
    """
    manager, left_backend, _ = two_backend_manager
    left = StreamKey("ns", "wf", uuid.uuid4().hex, "tokens")
    record = await _one_record(left_backend, left)

    with pytest.raises(StreamStorageError, match="format version"):
        await manager.prepare_replay(
            RUN_ID,
            _single_run_annotation(
                {
                    1: binding(
                        left,
                        backend_name="left",
                        provider_format_version=(
                            MemoryStreamBackend.provider_format_version + 1
                        ),
                    )
                },
                [Run(1, record.offset, record.offset, 1)],  # type: ignore[arg-type]
            ),
        )

    assert left_backend.range_reads == []


@pytest.mark.asyncio
async def test_a_marker_naming_an_unregistered_backend_is_nondeterminism(
    two_backend_manager,  # type: ignore[no-untyped-def]
) -> None:
    """A recorded backend name that resolves to nothing is row four as well.

    The name came from Workflow code, so the honest readings are that the
    Workflow now names a different backend or that this Worker is missing a
    registration -- neither of which is damage to a store.
    """
    manager, left_backend, _ = two_backend_manager
    left = StreamKey("ns", "wf", uuid.uuid4().hex, "tokens")
    record = await _one_record(left_backend, left)

    with pytest.raises(temporalio.workflow.NondeterminismError, match="not registered"):
        await manager.prepare_replay(
            RUN_ID,
            _single_run_annotation(
                {1: binding(left, backend_name="retired")},
                [Run(1, record.offset, record.offset, 1)],  # type: ignore[arg-type]
            ),
        )


# --- a wait registered after the header, replayed ----------------------------


class FakeInstance:
    """Stands in for the Workflow object the per-Run subscription state hangs off."""


@pytest.mark.asyncio
async def test_a_wait_registered_mid_annotation_replays(
    manager: StreamSubscriptionManager, backend: MemoryStreamBackend
) -> None:
    """The whole round trip for a subscription created after the first delta.

    `register` accepts one at any activation of a retained Workflow Task, long
    after the header frame went to Core -- and Core appends deltas rather than
    rewriting them. A wait bound nowhere reaches the marker as a run and a
    terminal entry alone, and `prepare_replay` then has no stream key and no
    backend for it, so replay of *unchanged* code fails as a wait "this
    Workflow did not create".
    """
    from temporalio.contrib.external_workflow_streams._runtime import (
        WorkflowStreamRuntime,
    )

    chain = uuid.uuid4().hex
    first_key = StreamKey("ns", "wf", chain, "tokens")
    late_key = StreamKey("ns", "wf", chain, "tool-events")
    first_record = (await append_five(backend, first_key))[0]
    late_record = (await append_five(backend, late_key))[0]

    recorder = WorkflowStreamRuntime(
        manager=manager,
        backends={"tokens": backend},
        run_id="recording-run",
        namespace="ns",
        workflow_id="wf",
        first_execution_run_id=chain,
        data_converter=temporalio.converter.DataConverter.default,
        default_idle_timeout=timedelta(seconds=1),
    )
    recorder.register(wait_id=1, stream_key=first_key, backend_name="tokens")
    recorder.record_delivery(1, first_record)
    deltas = [recorder.take_observation_delta()]
    # The header is fixed from here on: Core already holds those bytes.
    recorder.register(wait_id=2, stream_key=late_key, backend_name="tokens")
    recorder.record_delivery(2, late_record)
    deltas.append(recorder.take_observation_delta())
    annotation = b"".join(d for d in deltas if d is not None) + recorder.add_terminal()

    assert set(decode_annotation(annotation).header.streams) == {1, 2}

    plan = await manager.prepare_replay(RUN_ID, annotation)
    runtime = make_runtime(manager, backend)
    runtime.register(wait_id=1, stream_key=first_key, backend_name="tokens")
    runtime.register(wait_id=2, stream_key=late_key, backend_name="tokens")

    drained: dict[int, list[Offset]] = {1: [], 2: []}

    class DrainingStub(DriverStub):
        def _run_once(self, *, check_conditions: bool) -> None:
            super()._run_once(check_conditions=check_conditions)
            for wait_id in (1, 2):
                drained[wait_id].extend(r.offset for r in runtime.drain(wait_id))  # type: ignore[misc]

    from temporalio.worker._workflow_instance import _WorkflowInstanceImpl

    _WorkflowInstanceImpl._apply_replay_external_streams(  # type: ignore[arg-type]
        DrainingStub(runtime), object()
    )

    assert drained == {1: [first_record.offset], 2: [late_record.offset]}
    assert plan.committed_boundaries == {
        1: AFTER(first_record.offset),  # type: ignore[arg-type]
        2: AFTER(late_record.offset),  # type: ignore[arg-type]
    }


# --- a segment's global order is the order Workflow code receives in ---------


@pytest.mark.asyncio
async def test_a_segment_replays_in_its_recorded_cross_stream_order(
    manager: StreamSubscriptionManager,
    backend: MemoryStreamBackend,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A segment recorded as (wait 2, wait 1) must not replay as (wait 1, wait 2).

    The order is reachable live and `merge` is how: a pass asks in `wait_id`
    order, finds wait 1's buffer empty and wait 2's holding a record, and wait
    1's record lands from the manager's loop before the next pass. That is one
    activation, so it is one segment, and the segment records (2, 1).

    On replay `merge` asks in the same `wait_id` order -- but every record of
    the segment is already in hand. A drain that searched the segment for its
    own wait would answer the first ask with wait 1's record and reverse the
    two, which is a different sequence of values reaching Workflow code and so
    a different sequence of commands. Taking only from the front is what makes
    the empty answer replay as empty.
    """
    chain = uuid.uuid4().hex
    left = StreamKey("ns", "wf", chain, "left")
    right = StreamKey("ns", "wf", chain, "right")
    left_record = (await append_five(backend, left))[0]
    right_record = (await append_five(backend, right))[0]

    annotation = encode_annotation(
        Annotation(
            header=AnnotationHeader({1: binding(left), 2: binding(right)}),
            segments=(
                Segment(
                    (
                        Run(2, right_record.offset, right_record.offset, 1),  # type: ignore[arg-type]
                        Run(1, left_record.offset, left_record.offset, 1),  # type: ignore[arg-type]
                    ),
                    SegmentEndReason.NO_DATA_AVAILABLE,
                ),
            ),
            terminal={
                1: AFTER(left_record.offset),  # type: ignore[arg-type]
                2: AFTER(right_record.offset),  # type: ignore[arg-type]
            },
        )
    )
    plan = await manager.prepare_replay(RUN_ID, annotation)
    assert [wait_id for wait_id, _ in plan.segments[0].deliveries] == [2, 1]

    runtime = make_runtime(manager, backend)
    instance = FakeInstance()
    monkeypatch.setattr(temporalio.workflow, "instance", lambda: instance)
    _install_runtime(instance, runtime)

    runtime.begin_replay(plan.annotation.header.streams)
    try:
        first = external_stream.topic("left", backend="tokens", type=str).subscribe()
        second = external_stream.topic("right", backend="tokens", type=str).subscribe()
        assert (first.wait_id, second.wait_id) == (1, 2)

        runtime.begin_replay_segment(list(plan.segments[0].deliveries))
        merged = merge(first, second)
        seen = [await merged.__anext__() for _ in range(2)]
        await merged.aclose()
        runtime.verify_replay_consumed()
    finally:
        runtime.end_replay()

    assert [(subscription.wait_id, value) for subscription, value in seen] == [
        (2, "v0"),
        (1, "v0"),
    ], "replay reordered a segment that recorded wait 2's record before wait 1's"


@pytest.mark.asyncio
async def test_a_replayed_drain_stops_at_another_waits_record(
    manager: StreamSubscriptionManager, backend: MemoryStreamBackend
) -> None:
    """The same rule at the drain, where a wait appears twice in one segment.

    Live, the second batch was not in this wait's buffer when the first drain
    ran -- the record between them belongs to a drain that had not happened
    yet. Handing both over at once would collapse two of the segment's drains
    into one and let the values interleave differently.
    """
    chain = uuid.uuid4().hex
    left = StreamKey("ns", "wf", chain, "left")
    right = StreamKey("ns", "wf", chain, "right")
    left_records = await append_five(backend, left)
    right_records = await append_five(backend, right)

    annotation = encode_annotation(
        Annotation(
            header=AnnotationHeader({1: binding(left), 2: binding(right)}),
            segments=(
                Segment(
                    (
                        Run(1, left_records[0].offset, left_records[0].offset, 1),  # type: ignore[arg-type]
                        Run(2, right_records[0].offset, right_records[0].offset, 1),  # type: ignore[arg-type]
                        Run(1, left_records[1].offset, left_records[1].offset, 1),  # type: ignore[arg-type]
                    ),
                    SegmentEndReason.NO_DATA_AVAILABLE,
                ),
            ),
            terminal={
                1: AFTER(left_records[1].offset),  # type: ignore[arg-type]
                2: AFTER(right_records[0].offset),  # type: ignore[arg-type]
            },
        )
    )
    await manager.prepare_replay(RUN_ID, annotation)
    runtime = make_runtime(manager, backend)
    runtime.register(wait_id=1, stream_key=left, backend_name="tokens")
    runtime.register(wait_id=2, stream_key=right, backend_name="tokens")

    taken: list[tuple[int, Offset]] = []

    class InterleavingStub(DriverStub):
        def _run_once(self, *, check_conditions: bool) -> None:
            super()._run_once(check_conditions=check_conditions)
            for wait_id in (1, 2, 1):
                taken.extend((wait_id, r.offset) for r in runtime.drain(wait_id))  # type: ignore[misc]

    from temporalio.worker._workflow_instance import _WorkflowInstanceImpl

    _WorkflowInstanceImpl._apply_replay_external_streams(  # type: ignore[arg-type]
        InterleavingStub(runtime), object()
    )

    assert taken == [
        (1, left_records[0].offset),
        (2, right_records[0].offset),
        (1, left_records[1].offset),
    ]
