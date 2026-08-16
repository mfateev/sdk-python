"""P16b — the Milestone 2 cases not already covered elsewhere.

The rest of the twelve are covered by the deliverables that produced them (P21,
P15, P10b); these are the ones that fall between deliverables, which is exactly
why a milestone gate is a separate list rather than a sum of "done when"s.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import pytest
import pytest_asyncio

import temporalio.converter
from temporalio.contrib.external_workflow_streams._annotation import (
    MAX_ANNOTATION_BYTES,
)
from temporalio.contrib.external_workflow_streams._backend import ParkIntent, StreamKey
from temporalio.contrib.external_workflow_streams._continuation import Continuation
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
async def manager(backend: MemoryStreamBackend):
    mgr = StreamSubscriptionManager(
        backends={"tokens": backend},
        notify_ready=_notify,
        watch_block=timedelta(milliseconds=10),
    )
    yield mgr
    await mgr.shutdown()


def make_runtime(manager, backend, continuation=None):  # type: ignore[no-untyped-def]
    return WorkflowStreamRuntime(
        manager=manager,
        backends={"tokens": backend},
        run_id=RUN_ID,
        namespace="ns",
        workflow_id="wf",
        first_execution_run_id=uuid.uuid4().hex,
        data_converter=temporalio.converter.DataConverter.default,
        default_idle_timeout=timedelta(seconds=1),
        continuation=continuation,
    )


def data(offset: str) -> StreamRecord:
    return StreamRecord(RecordKind.DATA, b"x", "s", 0).placed_at(Offset(offset))


# --- case 1: readiness on one stream resets global quiescence -----------------


@pytest.mark.asyncio
async def test_readiness_on_one_of_several_streams_resets_global_quiescence(
    manager, backend: MemoryStreamBackend
) -> None:
    """The idle timer covers the set, so any member's record restarts it.

    Core starts a fresh idle timer when the generations it is retaining for
    change. If a record on one stream left every generation untouched, the timer
    that was already running would keep counting down toward parking a Workflow
    that had just been given work.
    """
    runtime = make_runtime(manager, backend)
    for wait_id, name in ((1, "a"), (2, "b")):
        runtime.register(
            wait_id=wait_id,
            stream_key=runtime.stream_key(name),
            backend_name="tokens",
        )
        runtime.note_blocked(wait_id, True)
    before = {w.wait_id: w.generation for w in runtime.quiescent_snapshot()}  # type: ignore[union-attr]

    # A record arrives on the first stream: it stops being blocked, Workflow code
    # consumes it, and then blocks again.
    runtime.note_blocked(1, False)
    runtime.record_delivery(1, data("10-0"))
    runtime.note_blocked(1, True)

    after = {w.wait_id: w.generation for w in runtime.quiescent_snapshot()}  # type: ignore[union-attr]
    assert after[1] > before[1], (
        "the wait that received a record must re-enter the blocked state with a "
        "new generation, which is what makes Core restart the idle timer"
    )
    assert after[2] == before[2], (
        "the other stream did not move and must keep its generation, or Core "
        "could not tell a stale readiness for it from a live one"
    )


# --- case 5: an alternating batch rolls over rather than exceeding the budget --


@pytest.mark.asyncio
async def test_an_alternating_two_stream_batch_rolls_over_within_budget(
    manager, backend: MemoryStreamBackend
) -> None:
    """Alternation is the worst case for annotation size, and must still be bounded.

    Every delivery starts a new run, so an alternating batch grows the annotation
    per *record* rather than per batch -- the one shape where the byte budget is
    reachable in normal use. It must ask for rollover rather than grow past the
    limit, because an oversized marker cannot be written at all.
    """
    runtime = make_runtime(manager, backend)
    for wait_id, name in ((1, "a"), (2, "b")):
        runtime.register(
            wait_id=wait_id,
            stream_key=runtime.stream_key(name),
            backend_name="tokens",
        )

    delivered = 0
    while not runtime.request_rollover and delivered < 20_000:
        wait_id = 1 + (delivered % 2)
        runtime.record_delivery(wait_id, data(f"{delivered}-0"))
        runtime.take_observation_delta()
        delivered += 1

    assert runtime.request_rollover, (
        f"an alternating batch of {delivered} records never asked for rollover"
    )
    annotation = b"".join(
        [runtime.take_observation_delta() or b"", runtime.add_terminal()]
    )
    assert len(annotation) <= MAX_ANNOTATION_BYTES, (
        f"the annotation reached {len(annotation)} bytes, past the "
        f"{MAX_ANNOTATION_BYTES} budget: rollover was requested too late to be "
        "of any use"
    )


# --- case 6: simultaneously ready streams are observable together -------------


@pytest.mark.asyncio
async def test_simultaneously_ready_streams_are_drained_in_one_pass(
    manager, backend: MemoryStreamBackend
) -> None:
    """Two records that arrived together cost one activation, not two.

    Waiting for readiness between them would turn every simultaneous arrival
    into a second Workflow Task -- and the whole point of retention is that a
    batch costs one.
    """
    from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec

    codec = StreamPayloadCodec(temporalio.converter.DataConverter.default, str)
    first = StreamKey("ns", "wf", "chain", "a")
    second = StreamKey("ns", "wf", "chain", "b")
    for wait_id, key in ((1, first), (2, second)):
        manager.register(
            run_id=RUN_ID,
            wait_id=wait_id,
            stream_key=key,
            backend_name="tokens",
            start_cursor=BEGINNING,
        )
    await backend.append(
        first, StreamRecord(RecordKind.DATA, await codec.encode("a1"), "p", 0)
    )
    await backend.append(
        second, StreamRecord(RecordKind.DATA, await codec.encode("b1"), "p", 0)
    )
    await asyncio.sleep(0.2)

    # Both are buffered at the same moment, so one drain pass sees both.
    assert manager.drain(RUN_ID, 1) and manager.drain(RUN_ID, 2), (
        "both streams must be buffered together; if one had to wait for a "
        "separate readiness round trip the batch would cost two activations"
    )


# --- case 9: two same-stream subscriptions stay independent throughout --------


@pytest.mark.asyncio
async def test_two_same_stream_subscriptions_never_overwrite_each_other(
    manager, backend: MemoryStreamBackend
) -> None:
    """Park, wake, cancel, and Continue-As-New, verified against the backend.

    Every step is a chance for one subscription's state to land under the
    other's key. The park intents are read back rather than assumed, because a
    collapsed intent looks like success from the caller's side -- the second
    install simply returns.
    """
    key = StreamKey("ns", "wf", "chain", "tokens")
    for wait_id, cursor in ((1, BEGINNING), (2, AFTER(Offset("50-0")))):
        await backend.install_park_intent(
            key,
            ParkIntent(
                wait_id=wait_id, cursor=cursor, park_generation=3, run_id="run-1"
            ),
        )

    # Parked: two intents, two cursors.
    first = await backend.park_intent(key, 1)
    second = await backend.park_intent(key, 2)
    assert first is not None and second is not None
    assert first.cursor != second.cursor
    assert sorted(await backend.parked_wait_ids(key)) == [1, 2]

    # Cancelled: removing one leaves the other exactly as it was.
    await backend.remove_park_intent(key, 1)
    assert await backend.park_intent(key, 1) is None
    assert await backend.park_intent(key, 2) == second
    assert await backend.parked_wait_ids(key) == [2]

    # Continued as new: each restores its own cursor, not the other's.
    runtime = make_runtime(
        manager,
        backend,
        Continuation(
            {1: AFTER(Offset("10-0")), 2: AFTER(Offset("90-0"))},
            {1: "tokens", 2: "tokens"},
        ),
    )
    runtime.register(wait_id=1, stream_key=key, backend_name="tokens")
    runtime.register(wait_id=2, stream_key=key, backend_name="tokens")
    assert runtime._subscriptions[1].start_cursor == AFTER(Offset("10-0"))
    assert runtime._subscriptions[2].start_cursor == AFTER(Offset("90-0"))


# --- case 10: the reduction reproduces on replay ------------------------------


@pytest.mark.asyncio
async def test_the_idle_timeout_reduction_reproduces_exactly(
    manager, backend: MemoryStreamBackend
) -> None:
    """It rides on a command replay compares against History.

    The inputs are the quiescent set's configured values and nothing else, which
    is what makes the result a pure function of state replay reconstructs. A
    reduction that consulted wall-clock time, arrival order, or anything else
    live would produce a different `WorkflowStreamQuiescent` on replay and fail
    as nondeterminism.
    """

    def reduce_over(order: list[int]) -> timedelta:
        runtime = make_runtime(manager, backend)
        for wait_id in order:
            runtime.register(
                wait_id=wait_id,
                stream_key=runtime.stream_key(f"s{wait_id}"),
                backend_name="tokens",
                idle_timeout=timedelta(seconds=wait_id),
            )
            runtime.note_blocked(wait_id, True)
        return runtime.effective_idle_timeout()

    # Registration order differs; the set does not. Replay reconstructs the set,
    # so only the set may matter.
    assert reduce_over([1, 2, 3]) == reduce_over([3, 2, 1]) == timedelta(seconds=1)


# --- case 11: a wake for one stream rechecks all ------------------------------


@pytest.mark.asyncio
async def test_a_wake_for_one_stream_resolves_every_blocked_wait(
    manager, backend: MemoryStreamBackend
) -> None:
    """The Signal names one stream; the runtime rechecks all of them.

    A wake can only name the stream whose producer sent it, but by the time the
    Workflow Task exists other streams may have records too. Resolving only the
    named wait would leave those buffered until something else happened to wake
    the Workflow -- and the wake it needed had already been spent.
    """
    runtime = make_runtime(manager, backend)
    loop = asyncio.get_running_loop()
    futures = {}
    for wait_id, name in ((1, "named"), (2, "other")):
        runtime.register(
            wait_id=wait_id,
            stream_key=runtime.stream_key(name),
            backend_name="tokens",
        )
        runtime.note_blocked(wait_id, True)
        future: asyncio.Future[None] = loop.create_future()
        runtime.register_pending(wait_id, future)
        futures[wait_id] = future

    # What the resolve activation does, regardless of which wait it names.
    runtime.resolve_all_pending()

    assert all(f.done() for f in futures.values()), (
        "every blocked wait must be resolved, not only the one the Signal named"
    )


@pytest.mark.asyncio
async def test_resolving_twice_is_harmless(
    manager, backend: MemoryStreamBackend
) -> None:
    """Duplicate wake Signals are expected, and must not raise.

    Two producers racing one generation both signal; the server deduplicates the
    request but a stale Signal can still arrive alongside a live one.
    """
    runtime = make_runtime(manager, backend)
    runtime.register(
        wait_id=1, stream_key=runtime.stream_key("a"), backend_name="tokens"
    )
    future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    runtime.register_pending(1, future)

    runtime.resolve_all_pending()
    runtime.resolve_all_pending()

    assert future.done()
