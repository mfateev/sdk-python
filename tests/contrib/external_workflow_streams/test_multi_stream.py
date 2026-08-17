"""P21 — several streams, ``merge``, and two subscriptions to one stream.

The idle timeout is a **Workflow-Task policy**, not a per-subscription one: it
applies to the complete set the Workflow is blocked on, so one idle stream must
not park the task while another is still delivering. Everything below is about
that set behaving as a set.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import pytest
import pytest_asyncio

import temporalio.converter
import temporalio.workflow
from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.external_workflow_streams._annotation import decode_annotation
from temporalio.contrib.external_workflow_streams._api import (
    MAX_RECORDS_PER_ACTIVATION,
    _install_runtime,
    external_stream,
    merge,
)
from temporalio.contrib.external_workflow_streams._backend import StreamKey
from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec
from temporalio.contrib.external_workflow_streams._manager import (
    ReadinessResult,
    StreamSubscriptionManager,
)
from temporalio.contrib.external_workflow_streams._record import (
    Offset,
    RecordKind,
    StreamRecord,
)
from temporalio.contrib.external_workflow_streams._runtime import WorkflowStreamRuntime
from temporalio.worker import Worker
from tests.contrib.external_workflow_streams.memory_backend import MemoryStreamBackend

with workflow.unsafe.imports_passed_through():
    pass

RUN_ID = "run-1"


async def _notify(run_id: str, wait_id: int, generation: int) -> str:
    return ReadinessResult.ACCEPTED


@pytest.fixture
def backend() -> MemoryStreamBackend:
    return MemoryStreamBackend()


class StubManager:
    """Records registrations and starts no watcher.

    The snapshot and annotation tests are about what the runtime decides, which
    happens before any watching. A real manager would spawn prefetch loops
    against a backend nothing is writing to, and their failures would be noise.
    """

    def register(self, *, run_id, wait_id, stream_key, backend_name, start_cursor):  # type: ignore[no-untyped-def]
        pass

    def note_wait_generation(self, run_id, wait_id, generation) -> None:  # type: ignore[no-untyped-def]
        pass


@pytest.fixture
def manager() -> StubManager:
    return StubManager()


@pytest_asyncio.fixture  # type: ignore[reportUntypedFunctionDecorator]
async def live_manager(backend: MemoryStreamBackend):
    """A real manager, for the tests that need records to actually move."""
    mgr = StreamSubscriptionManager(
        backends={"tokens": backend},
        notify_ready=_notify,
        watch_block=timedelta(milliseconds=10),
    )
    yield mgr
    await mgr.shutdown()


def make_runtime(manager, backend, idle=timedelta(seconds=1)):  # type: ignore[no-untyped-def]
    return WorkflowStreamRuntime(
        manager=manager,
        backends={"tokens": backend},
        run_id=RUN_ID,
        namespace="ns",
        workflow_id="wf",
        first_execution_run_id="first-run",
        data_converter=temporalio.converter.DataConverter.default,
        default_idle_timeout=idle,
    )


def data(seq: int, offset: str) -> StreamRecord:
    return StreamRecord(RecordKind.DATA, b"x", "s", seq).placed_at(Offset(offset))


def fence(seq: int, offset: str) -> StreamRecord:
    return StreamRecord(RecordKind.WRITE_FENCE, b"", "s", seq).placed_at(Offset(offset))


# --- the idle timeout is a property of the set --------------------------------


def test_one_idle_stream_cannot_park_while_another_is_active(
    manager: StubManager, backend: MemoryStreamBackend
) -> None:
    """The criterion the whole "complete set" rule exists for.

    Parking on the idle one alone would strand records already sitting in the
    active one, and the Workflow would wait out a timeout for a stream that was
    never quiet.
    """
    runtime = make_runtime(manager, backend)
    runtime.register(
        wait_id=1, stream_key=runtime.stream_key("idle"), backend_name="tokens"
    )
    runtime.register(
        wait_id=2, stream_key=runtime.stream_key("busy"), backend_name="tokens"
    )
    runtime.note_blocked(1, True)
    # The second is *not* blocked -- Workflow code is still working through it.
    runtime.note_blocked(2, False)

    snapshot = runtime.quiescent_snapshot()

    assert snapshot is not None
    assert [w.wait_id for w in snapshot] == [1], (
        "the quiescent set must name only the waits Workflow code is actually "
        "blocked on; naming an active one would ask Core to park it"
    )


def test_the_quiescent_set_is_complete_when_everything_is_blocked(
    manager: StubManager, backend: MemoryStreamBackend
) -> None:
    """A set missing a member would let Core park a wait that was still live."""
    runtime = make_runtime(manager, backend)
    for wait_id, name in ((1, "a"), (2, "b"), (3, "c")):
        runtime.register(
            wait_id=wait_id,
            stream_key=runtime.stream_key(name),
            backend_name="tokens",
        )
        runtime.note_blocked(wait_id, True)

    snapshot = runtime.quiescent_snapshot()

    assert snapshot is not None
    assert [w.wait_id for w in snapshot] == [1, 2, 3]


def test_differing_idle_timeouts_reduce_to_the_minimum(
    manager: StubManager, backend: MemoryStreamBackend
) -> None:
    """``min``, in ``wait_id`` order, over the quiescent set and nothing else.

    Taking the maximum would let one lax subscription hold the Workflow Task
    open past what another was configured to tolerate; taking the *configured
    values of the whole set* rather than of the blocked subset would make the
    result depend on state replay does not reproduce.
    """
    runtime = make_runtime(manager, backend)
    for wait_id, seconds in ((1, 5), (2, 2), (3, 9)):
        runtime.register(
            wait_id=wait_id,
            stream_key=runtime.stream_key(f"s{wait_id}"),
            backend_name="tokens",
            idle_timeout=timedelta(seconds=seconds),
        )
        runtime.note_blocked(wait_id, True)

    assert runtime.effective_idle_timeout() == timedelta(seconds=2)


def test_with_options_timeouts_reach_the_reduction_through_the_public_api(
    manager: StubManager,
    backend: MemoryStreamBackend,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The reduction is unreachable unless ``subscribe()`` passes the value on.

    The test above registers with an explicit ``idle_timeout``, which proves
    ``min`` reduces but says nothing about whether a configured value can ever
    get there. ``with_options`` is the only route a user has, and a
    ``subscribe()`` that dropped the option would leave every quiescent set on
    the one-second default while that test, and
    ``ExternalStreamSubscription.idle_timeout``, both kept passing -- the option
    would be silently decorative.
    """

    class Instance:
        """Stands in for the Workflow object the per-Run state hangs off."""

    instance = Instance()
    monkeypatch.setattr(temporalio.workflow, "instance", lambda: instance)
    runtime = make_runtime(manager, backend, idle=timedelta(seconds=1))
    _install_runtime(instance, runtime)  # type: ignore[arg-type]

    for name, seconds in (("slow", 30), ("quick", 4), ("slower", 60)):
        subscription = (
            external_stream.with_options(idle_timeout=timedelta(seconds=seconds))
            .topic(name, backend="tokens", type=str)
            .subscribe()
        )
        runtime.note_blocked(subscription.wait_id, True)

    assert runtime.effective_idle_timeout() == timedelta(seconds=4), (
        "no configured timeout reached the runtime, so the whole quiescent set "
        "fell back to the default and every with_options() call was ignored"
    )


def test_the_reduction_ignores_waits_that_are_not_blocked(
    manager: StubManager, backend: MemoryStreamBackend
) -> None:
    """The inputs are the quiescent set's configured values and nothing else."""
    runtime = make_runtime(manager, backend)
    runtime.register(
        wait_id=1,
        stream_key=runtime.stream_key("a"),
        backend_name="tokens",
        idle_timeout=timedelta(seconds=7),
    )
    runtime.register(
        wait_id=2,
        stream_key=runtime.stream_key("b"),
        backend_name="tokens",
        idle_timeout=timedelta(milliseconds=50),
    )
    runtime.note_blocked(1, True)
    runtime.note_blocked(2, False)

    assert runtime.effective_idle_timeout() == timedelta(seconds=7)


# --- fences are per stream, parking is per set --------------------------------


def test_a_fence_on_one_stream_alone_does_not_make_the_set_parkable(
    manager: StubManager, backend: MemoryStreamBackend
) -> None:
    """It marks *that* stream parkable and says nothing about the others.

    A fence means only that one producer session has finished writing. Treating
    it as a signal for the whole set would park a Workflow Task while another
    stream was still being written to.
    """
    runtime = make_runtime(manager, backend)
    for wait_id, name in ((1, "fenced"), (2, "open")):
        runtime.register(
            wait_id=wait_id,
            stream_key=runtime.stream_key(name),
            backend_name="tokens",
        )
        runtime.note_blocked(wait_id, True)
    runtime.record_delivery(1, fence(0, "10-0"))

    snapshot = runtime.quiescent_snapshot()

    assert snapshot is not None
    parkable = {w.wait_id: w.immediately_parkable for w in snapshot}
    assert parkable == {1: True, 2: False}, (
        "the fenced stream is immediately parkable and the open one is not; "
        "Core parks the task early only when every wait says True"
    )


def test_all_fenced_streams_make_the_whole_set_parkable(
    manager: StubManager, backend: MemoryStreamBackend
) -> None:
    runtime = make_runtime(manager, backend)
    for wait_id, name in ((1, "a"), (2, "b")):
        runtime.register(
            wait_id=wait_id,
            stream_key=runtime.stream_key(name),
            backend_name="tokens",
        )
        runtime.note_blocked(wait_id, True)
        runtime.record_delivery(wait_id, fence(0, f"1{wait_id}-0"))

    snapshot = runtime.quiescent_snapshot()

    assert snapshot is not None
    assert all(w.immediately_parkable for w in snapshot)


def test_a_record_after_a_fence_reopens_the_stream(
    manager: StubManager, backend: MemoryStreamBackend
) -> None:
    """A later record does not violate a fence; it simply resumes consumption.

    The fence spoke only for one producer session. Treating the stream as closed
    would strand every record another producer went on to write.
    """
    runtime = make_runtime(manager, backend)
    runtime.register(
        wait_id=1, stream_key=runtime.stream_key("a"), backend_name="tokens"
    )
    runtime.note_blocked(1, True)
    runtime.record_delivery(1, fence(0, "10-0"))
    assert runtime.quiescent_snapshot()[0].immediately_parkable  # type: ignore[index]

    runtime.record_delivery(1, data(1, "10-1"))

    assert not runtime.quiescent_snapshot()[0].immediately_parkable  # type: ignore[index]


# --- two subscriptions to one stream ------------------------------------------


@pytest.mark.asyncio
async def test_two_same_stream_subscriptions_install_distinct_park_intents(
    live_manager, backend: MemoryStreamBackend
) -> None:
    """Verified by reading both back, not by trusting the call.

    Intents keyed by stream alone would have the second install overwrite the
    first, after which only one of the two could ever be woken -- and the
    surviving intent would name the wrong cursor for the lost one.
    """
    runtime = make_runtime(live_manager, backend)
    key = runtime.stream_key("tokens")
    runtime.register(wait_id=1, stream_key=key, backend_name="tokens")
    runtime.register(wait_id=2, stream_key=key, backend_name="tokens")
    # Two different positions, so a collapsed intent is visible rather than
    # merely suspected.
    runtime.record_delivery(1, data(0, "10-0"))
    runtime.note_blocked(1, True)
    runtime.note_blocked(2, True)

    await live_manager.prepare_park(RUN_ID, 1, runtime.blocked_snapshot())

    first = await backend.park_intent(key, 1)
    second = await backend.park_intent(key, 2)
    assert first is not None and second is not None
    assert first.cursor != second.cursor, (
        "each subscription parks at its own position; a shared intent would "
        "resume one of them at the other's cursor"
    )


@pytest.mark.asyncio
async def test_each_same_stream_subscription_receives_every_record(
    live_manager, backend: MemoryStreamBackend
) -> None:
    """Delivery is broadcast, not competing-consumer.

    Two subscriptions to one stream are two independent readers. Splitting the
    records between them would make either one's view of the stream depend on
    the other's existence.
    """
    runtime = make_runtime(live_manager, backend)
    key = runtime.stream_key("tokens")
    runtime.register(wait_id=1, stream_key=key, backend_name="tokens")
    runtime.register(wait_id=2, stream_key=key, backend_name="tokens")
    codec = StreamPayloadCodec(temporalio.converter.DataConverter.default, str)
    for i, value in enumerate(["a", "b"]):
        await backend.append(
            key, StreamRecord(RecordKind.DATA, await codec.encode(value), "p", i)
        )

    await asyncio.sleep(0.2)

    first = live_manager.drain(RUN_ID, 1)
    second = live_manager.drain(RUN_ID, 2)
    assert len(first) == 2 and len(second) == 2, (
        f"each subscription must see both records, got {len(first)} and {len(second)}"
    )


# --- the recorded delivery schedule -------------------------------------------


def test_an_alternating_two_stream_batch_encodes_one_run_per_delivery(
    manager: StubManager, backend: MemoryStreamBackend
) -> None:
    """A run is a maximal consecutive stretch from a *single* wait.

    Collapsing the alternation into two runs -- one per stream -- would record a
    delivery order that never happened, and replay would hand Workflow code all
    of one stream before any of the other.
    """
    runtime = make_runtime(manager, backend)
    for wait_id, name in ((1, "a"), (2, "b")):
        runtime.register(
            wait_id=wait_id,
            stream_key=runtime.stream_key(name),
            backend_name="tokens",
        )
    for i in range(2):
        runtime.record_delivery(1, data(i, f"1{i}-0"))
        runtime.record_delivery(2, data(i, f"2{i}-0"))

    delta = runtime.take_observation_delta()
    assert delta is not None
    annotation = decode_annotation(delta + runtime.add_terminal())

    runs = [run for segment in annotation.segments for run in segment.runs]
    assert [run.wait_id for run in runs] == [1, 2, 1, 2], (
        f"expected one run per delivery in the observed order, got "
        f"{[(r.wait_id, r.count) for r in runs]}"
    )
    assert all(run.count == 1 for run in runs)


def test_consecutive_records_from_one_stream_are_one_run(
    manager: StubManager, backend: MemoryStreamBackend
) -> None:
    """The other half of the same rule, and what keeps the annotation compact.

    If every record became its own run the annotation would grow per item, which
    is the cost model this feature exists to avoid.
    """
    runtime = make_runtime(manager, backend)
    runtime.register(
        wait_id=1, stream_key=runtime.stream_key("a"), backend_name="tokens"
    )
    for i in range(5):
        runtime.record_delivery(1, data(i, f"10-{i}"))

    delta = runtime.take_observation_delta()
    assert delta is not None
    annotation = decode_annotation(delta + runtime.add_terminal())

    runs = [run for segment in annotation.segments for run in segment.runs]
    assert len(runs) == 1
    assert runs[0].count == 5


# --- merge --------------------------------------------------------------------


class FakeRuntime:
    """Enough of the runtime protocol for the merge iterator, and no more."""

    def __init__(self) -> None:
        self.buffers: dict[int, list[StreamRecord]] = {}
        self.blocked: list[tuple[int, bool]] = []
        self.pending: dict[int, asyncio.Future[None]] = {}
        self.deliveries: list[tuple[int, StreamRecord]] = []
        self.consumed: list[tuple[int, StreamRecord]] = []
        self.registered = {"tokens-redis"}
        self.registrations: list[tuple[int, StreamKey, str]] = []
        self.budget = MAX_RECORDS_PER_ACTIVATION

    def stream_key(self, stream_name: str) -> StreamKey:
        return StreamKey("ns", "wf", "first-run", stream_name)

    def register(
        self,
        *,
        wait_id: int,
        stream_key: StreamKey,
        backend_name: str,
        idle_timeout: timedelta,
    ):
        self.registrations.append((wait_id, stream_key, backend_name))

    def drain(self, wait_id: int, max_records: int | None = None):
        buffered = self.buffers.get(wait_id, [])
        if max_records is not None:
            buffered, self.buffers[wait_id] = (
                buffered[:max_records],
                buffered[max_records:],
            )
        else:
            self.buffers[wait_id] = []
        return buffered

    def delivery_budget_remaining(self) -> int:
        return self.budget

    def codec_for(self, value_type: type | None):
        return StreamPayloadCodec(
            temporalio.converter.DataConverter.default, value_type
        )

    def new_readiness_future(self) -> asyncio.Future[None]:
        return asyncio.get_event_loop().create_future()

    def record_delivery(self, wait_id: int, record: StreamRecord) -> None:
        self.deliveries.append((wait_id, record))

    def record_consumption(self, wait_id: int, record: StreamRecord) -> None:
        self.consumed.append((wait_id, record))
        self.budget = max(0, self.budget - 1)

    def note_blocked(self, wait_id: int, blocked: bool) -> None:
        self.blocked.append((wait_id, blocked))

    def register_pending(self, wait_id: int, future: asyncio.Future[None]) -> None:
        self.pending[wait_id] = future

    def discard_pending(self, wait_id: int) -> None:
        self.pending.pop(wait_id, None)


class FakeInstance:
    pass


@pytest.fixture
def fake_runtime(monkeypatch: pytest.MonkeyPatch) -> FakeRuntime:
    instance = FakeInstance()
    monkeypatch.setattr(temporalio.workflow, "instance", lambda: instance)
    runtime = FakeRuntime()
    _install_runtime(instance, runtime)  # type: ignore[arg-type]
    return runtime


async def encoded(*values: str) -> list[StreamRecord]:
    codec = StreamPayloadCodec(temporalio.converter.DataConverter.default, str)
    return [
        StreamRecord(RecordKind.DATA, await codec.encode(v), "p", i)
        for i, v in enumerate(values)
    ]


@pytest.mark.asyncio
async def test_merge_yields_from_every_subscription(fake_runtime: FakeRuntime) -> None:
    first = external_stream.topic("a", backend="tokens-redis", type=str).subscribe()
    second = external_stream.topic("b", backend="tokens-redis", type=str).subscribe()
    fake_runtime.buffers[first.wait_id] = await encoded("a1", "a2")
    fake_runtime.buffers[second.wait_id] = await encoded("b1")

    seen = []
    async for subscription, value in merge(first, second):
        seen.append((subscription.wait_id, value))
        if len(seen) == 3:
            break

    assert seen == [(1, "a1"), (1, "a2"), (2, "b1")]


@pytest.mark.asyncio
async def test_merge_drains_in_wait_id_order_regardless_of_argument_order(
    fake_runtime: FakeRuntime,
) -> None:
    """Records that arrived in one batch have no inherent order between them.

    An order that depended on argument order -- or on which watcher happened to
    run first -- would replay differently than it ran.
    """
    first = external_stream.topic("a", backend="tokens-redis", type=str).subscribe()
    second = external_stream.topic("b", backend="tokens-redis", type=str).subscribe()
    fake_runtime.buffers[first.wait_id] = await encoded("a1")
    fake_runtime.buffers[second.wait_id] = await encoded("b1")

    seen = []
    async for subscription, value in merge(second, first):
        seen.append(value)
        if len(seen) == 2:
            break

    assert seen == ["a1", "b1"]


@pytest.mark.asyncio
async def test_merge_marks_every_wait_blocked_when_nothing_is_ready(
    fake_runtime: FakeRuntime,
) -> None:
    """The quiescent set must name the complete set.

    Blocking on only the first would leave the second out of the snapshot, and
    Core would park a wait the Workflow was still waiting on.
    """
    first = external_stream.topic("a", backend="tokens-redis", type=str).subscribe()
    second = external_stream.topic("b", backend="tokens-redis", type=str).subscribe()

    iterator = merge(first, second)
    pending = asyncio.ensure_future(iterator.__anext__())
    await asyncio.sleep(0.05)

    assert set(fake_runtime.pending) == {1, 2}
    assert (1, True) in fake_runtime.blocked and (2, True) in fake_runtime.blocked
    pending.cancel()
    try:
        await pending
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_merge_resumes_when_any_one_wait_is_resolved(
    fake_runtime: FakeRuntime,
) -> None:
    """One record on one stream is enough; it need not wait for the others."""
    first = external_stream.topic("a", backend="tokens-redis", type=str).subscribe()
    second = external_stream.topic("b", backend="tokens-redis", type=str).subscribe()

    iterator = merge(first, second)
    pending = asyncio.ensure_future(iterator.__anext__())
    await asyncio.sleep(0.05)

    fake_runtime.buffers[second.wait_id] = await encoded("b1")
    fake_runtime.pending[second.wait_id].set_result(None)

    assert await asyncio.wait_for(pending, 1) == (second, "b1")


@pytest.mark.asyncio
async def test_merging_a_subscription_with_itself_is_refused(
    fake_runtime: FakeRuntime,
) -> None:
    """It would deliver every record to that wait twice."""
    only = external_stream.topic("a", backend="tokens-redis", type=str).subscribe()

    with pytest.raises(ValueError, match="same subscription twice"):
        await merge(only, only).__anext__()


@pytest.mark.asyncio
async def test_merging_nothing_is_refused(fake_runtime: FakeRuntime) -> None:
    with pytest.raises(ValueError, match="at least one"):
        await merge().__anext__()


@pytest.mark.asyncio
async def test_merge_never_yields_control_records(fake_runtime: FakeRuntime) -> None:
    """A fence advances the cursor but belongs to the runtime, not the Workflow."""
    first = external_stream.topic("a", backend="tokens-redis", type=str).subscribe()
    second = external_stream.topic("b", backend="tokens-redis", type=str).subscribe()
    records = await encoded("a1")
    fake_runtime.buffers[first.wait_id] = [
        records[0],
        StreamRecord(RecordKind.WRITE_FENCE, b"", "p", 1),
    ]
    fake_runtime.buffers[second.wait_id] = await encoded("b1")

    seen = []
    async for subscription, value in merge(first, second):
        seen.append(value)
        if len(seen) == 2:
            break

    assert seen == ["a1", "b1"]
    # The fence was still consumed -- it occupies an offset inside a run.
    assert any(r.is_control for _, r in fake_runtime.consumed)


# --- through a real Worker ----------------------------------------------------


@workflow.defn
class TwoStreamWorkflow:
    """Consumes from two streams as one wait set, reporting the order it saw."""

    @workflow.run
    async def run(self, expected: int) -> list[str]:
        from temporalio.contrib.external_workflow_streams._api import (
            external_stream as es,
        )
        from temporalio.contrib.external_workflow_streams._api import merge as m

        options = es.with_options(idle_timeout=timedelta(seconds=30))
        left = options.topic("left", backend="tokens-memory", type=str).subscribe()
        right = options.topic("right", backend="tokens-memory", type=str).subscribe()

        seen: list[str] = []
        async for _, value in m(left, right):
            seen.append(value)
            if len(seen) >= expected:
                break
        return seen


async def test_a_workflow_consumes_two_streams_as_one_wait_set(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """One idle stream must not stall the other.

    Only the ``right`` stream is ever written to. If the two were waited on
    independently the Workflow would block on ``left`` forever and never see a
    single record.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[TwoStreamWorkflow],
        external_stream_backends={"tokens-memory": backend},
    ):
        handle = await client.start_workflow(
            TwoStreamWorkflow.run,
            2,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        description = await handle.describe()
        first_run = description.raw_description.workflow_execution_info.first_run_id
        right = StreamKey(client.namespace, handle.id, first_run, "right")
        await asyncio.sleep(1)

        codec = StreamPayloadCodec(temporalio.converter.DataConverter.default, str)
        for i, value in enumerate(["r1", "r2"]):
            await backend.append(
                right,
                StreamRecord(RecordKind.DATA, await codec.encode(value), "p", i),
            )

        assert await asyncio.wait_for(handle.result(), 60) == ["r1", "r2"]
