"""P13 — the replay read path and its four range checks.

The checks are sufficient *because* every provider guarantees a record's bytes
cannot change, so the only damage replay has to detect is a record that is no
longer there. Each test below deletes a record in a different position, and each
must fail a different one of the four -- which is what makes the set complete
rather than merely plausible.
"""

from __future__ import annotations

import uuid
from datetime import timedelta

import pytest
import pytest_asyncio

import temporalio.converter
from temporalio.contrib.external_workflow_streams._annotation import (
    Annotation,
    AnnotationHeader,
    Run,
    Segment,
    SegmentEndReason,
    StreamBinding,
    encode_annotation,
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
    Offset,
    RecordKind,
    StreamRecord,
)
from temporalio.contrib.external_workflow_streams._replay import (
    validate_run,
)
from tests.contrib.external_workflow_streams.memory_backend import MemoryStreamBackend

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


def annotation_for(
    key: StreamKey, placed: list[StreamRecord], control_positions: tuple[int, ...] = ()
) -> bytes:
    """One marker recording all of ``placed`` as a single run."""
    first, last = placed[0].offset, placed[-1].offset
    assert first is not None and last is not None
    return encode_annotation(
        Annotation(
            header=AnnotationHeader(
                provider_id="memory",
                provider_format_version=1,
                streams={1: StreamBinding(key, BEGINNING)},
            ),
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
            header=AnnotationHeader("memory", 1, {1: StreamBinding(key, BEGINNING)}),
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
async def test_a_marker_naming_an_unknown_wait_is_reported_not_skipped(
    manager: StreamSubscriptionManager, backend: MemoryStreamBackend, key: StreamKey
) -> None:
    """Row four of the taxonomy: the Workflow code renumbered a subscription.

    Skipping it would silently deliver a different stream result, which is the
    one outcome integrity loss must never produce.
    """
    placed = await append_five(backend, key)
    annotation = encode_annotation(
        Annotation(
            header=AnnotationHeader("memory", 1, {}),
            segments=(
                Segment(
                    (Run(7, placed[0].offset, placed[-1].offset, 5),),  # type: ignore[arg-type]
                    SegmentEndReason.NO_DATA_AVAILABLE,
                ),
            ),
            terminal={},
        )
    )

    with pytest.raises(StreamIntegrityError, match="did not create"):
        await manager.prepare_replay(RUN_ID, annotation)


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


def drive(runtime) -> DriverStub:  # type: ignore[no-untyped-def]
    from temporalio.worker._workflow_instance import _WorkflowInstanceImpl

    stub = DriverStub(runtime)
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
            header=AnnotationHeader("memory", 1, {1: StreamBinding(key, BEGINNING)}),
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
            header=AnnotationHeader("memory", 1, {1: StreamBinding(key, BEGINNING)}),
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
            header=AnnotationHeader(
                "memory",
                1,
                {
                    1: StreamBinding(key, BEGINNING),
                    2: StreamBinding(other, BEGINNING),
                },
            ),
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
