"""P8 — the subscription manager, driven directly.

No Workflow API and no Core activations: neither is in this deliverable's
closure, and both would obscure what is actually under test -- that backend I/O
never reaches the thread ``_apply`` runs on, and that readiness means *buffered*.
"""

from __future__ import annotations

import asyncio
import inspect
import threading
import time
import uuid
from collections.abc import Awaitable, Callable
from datetime import timedelta

import pytest

from temporalio.contrib.external_workflow_streams._backend import ParkIntent, StreamKey
from temporalio.contrib.external_workflow_streams._errors import (
    StreamStorageError,
)
from temporalio.contrib.external_workflow_streams._manager import (
    PARK_REMOVAL_ATTEMPTS,
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
from temporalio.worker.workflow_sandbox._restrictions import SandboxRestrictions
from tests.contrib.external_workflow_streams.memory_backend import MemoryStreamBackend

RUN_ID = "run-1"


@pytest.fixture
def stream_key() -> StreamKey:
    return StreamKey("ns", "wf", uuid.uuid4().hex, "tokens")


class RecordingNotifier:
    """Stands in for Core's readiness call, recording every notification."""

    def __init__(self, answer: str = ReadinessResult.ACCEPTED) -> None:
        self.answer = answer
        self.calls: list[tuple[str, int, int]] = []
        self.notified = asyncio.Event()

    async def __call__(self, run_id: str, wait_id: int, wait_generation: int) -> str:
        self.calls.append((run_id, wait_id, wait_generation))
        self.notified.set()
        return self.answer


def make_manager(
    backend: MemoryStreamBackend,
    notifier: RecordingNotifier,
    *,
    buffer_size: int = 256,
    watch_block: timedelta = timedelta(milliseconds=20),
    send_wake: object | None = None,
) -> StreamSubscriptionManager:
    return StreamSubscriptionManager(
        backends={"tokens": backend},
        notify_ready=notifier,
        send_wake=send_wake,  # type: ignore[arg-type]
        buffer_size=buffer_size,
        watch_block=watch_block,
    )


async def append(
    backend: MemoryStreamBackend, key: StreamKey, *payloads: bytes
) -> None:
    for i, payload in enumerate(payloads):
        await backend.append(
            key, StreamRecord(RecordKind.DATA, payload, f"s{payload.decode()}", i)
        )


async def until(
    condition: Callable[[], object | Awaitable[object]],
    message: str,
    timeout: float = 2.0,
) -> None:
    """Waits for something a background task is on its way to doing.

    Retries, reconciliations and watchers all run on the manager's own loop, so
    a single ``sleep`` long enough to be reliable is also long enough to make
    every one of these tests slow. Accepts an awaitable condition because most
    of what is being waited for is a question for the backend.
    """
    deadline = time.monotonic() + timeout
    while True:
        outcome = condition()
        if inspect.isawaitable(outcome):
            outcome = await outcome
        if outcome:
            return
        if time.monotonic() >= deadline:
            pytest.fail(message)
        await asyncio.sleep(0.01)


# --- readiness means buffered ------------------------------------------------


@pytest.mark.asyncio
async def test_readiness_is_reported_only_after_a_record_is_buffered(
    stream_key: StreamKey,
) -> None:
    """The rule the whole structure rests on.

    Readiness for an unbuffered record would produce an activation whose drain
    must block -- exactly the deadlock hazard the out-of-thread buffer removes.
    """
    backend = MemoryStreamBackend()
    notifier = RecordingNotifier()
    manager = make_manager(backend, notifier)
    subscription = manager.register(
        run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
    )
    try:
        await asyncio.sleep(0.05)
        assert notifier.calls == [], "nothing is buffered, so nothing is ready"

        await append(backend, stream_key, b"a")
        await asyncio.wait_for(notifier.notified.wait(), 2)

        assert notifier.calls == [(RUN_ID, 1, 0)]
        assert subscription.buffered == 1, (
            "the record must already be in the buffer when readiness is reported"
        )
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_a_drain_returns_buffered_records_without_touching_the_backend(
    stream_key: StreamKey,
) -> None:
    backend = MemoryStreamBackend()
    notifier = RecordingNotifier()
    manager = make_manager(backend, notifier)
    manager.register(
        run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
    )
    try:
        await append(backend, stream_key, b"a", b"b")
        await asyncio.wait_for(notifier.notified.wait(), 2)
        reads_before = len(backend.range_reads)

        drained = manager.drain(RUN_ID, 1)

        assert [r.payload for r in drained] == [b"a", b"b"]
        assert len(backend.range_reads) == reads_before
        assert manager.drain(RUN_ID, 1) == []
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_draining_advances_only_the_delivery_cursor(
    stream_key: StreamKey,
) -> None:
    """Consuming is not committing. Only a marker moves ``committed``."""
    backend = MemoryStreamBackend()
    notifier = RecordingNotifier()
    manager = make_manager(backend, notifier)
    subscription = manager.register(
        run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
    )
    try:
        await append(backend, stream_key, b"a")
        await asyncio.wait_for(notifier.notified.wait(), 2)
        manager.drain(RUN_ID, 1)

        assert subscription.committed_cursor == BEGINNING
        assert subscription.delivery_cursor != BEGINNING
        assert subscription.prefetch_cursor != BEGINNING
    finally:
        await manager.shutdown()


# --- backend latency never reaches the Workflow thread -----------------------


class SlowBackend(MemoryStreamBackend):
    """Every read takes longer than the Workflow deadlock timeout."""

    delay_seconds = 2.5

    async def read_after(self, key, after, *, max_records, block=None):  # type: ignore[no-untyped-def]
        await asyncio.sleep(self.delay_seconds)
        return await super().read_after(
            key, after, max_records=max_records, block=block
        )


@pytest.mark.asyncio
async def test_a_backend_slower_than_the_deadlock_timeout_delays_readiness(
    stream_key: StreamKey,
) -> None:
    """It delays the *report*, not the caller.

    The Workflow thread runs under a 2-second deadlock timeout. A provider
    slower than that must therefore be invisible to it -- and it is, because
    the drain the Workflow thread performs only ever touches the buffer.
    """
    backend = SlowBackend()
    notifier = RecordingNotifier()
    manager = make_manager(backend, notifier, watch_block=timedelta(milliseconds=20))
    manager.register(
        run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
    )
    try:
        await append(backend, stream_key, b"a")

        # Well past the deadlock timeout: no readiness yet, because nothing is
        # buffered yet.
        await asyncio.sleep(2.2)
        assert notifier.calls == []

        # And the Workflow thread's own call returns immediately regardless.
        start = asyncio.get_running_loop().time()
        assert manager.drain(RUN_ID, 1) == []
        assert asyncio.get_running_loop().time() - start < 0.5, (
            "the drain must not wait on the provider"
        )

        await asyncio.wait_for(notifier.notified.wait(), 5)
        assert manager.drain(RUN_ID, 1) != []
    finally:
        await manager.shutdown()


# --- backpressure ------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_full_buffer_stops_prefetch_without_dropping_or_blocking(
    stream_key: StreamKey,
) -> None:
    """Backpressure *is* the buffer bound. Nothing is dropped, nobody blocks."""
    backend = MemoryStreamBackend()
    notifier = RecordingNotifier()
    manager = make_manager(backend, notifier, buffer_size=3)
    subscription = manager.register(
        run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
    )
    try:
        await append(backend, stream_key, *[bytes([i]) for i in range(10)])
        await asyncio.sleep(0.2)

        assert subscription.buffered == 3, (
            f"prefetch must stop at the bound, buffered {subscription.buffered}"
        )

        # Nothing was dropped: draining frees room and the rest arrives, in order.
        seen: list[bytes] = []
        for _ in range(10):
            seen.extend(r.payload for r in manager.drain(RUN_ID, 1))
            if len(seen) == 10:
                break
            await asyncio.sleep(0.1)

        assert seen == [bytes([i]) for i in range(10)]
    finally:
        await manager.shutdown()


# --- the manager's loop owns every provider call -----------------------------


class ThreadCheckingBackend(MemoryStreamBackend):
    """Raises if called from any thread other than the one it was made on."""

    def __init__(self) -> None:
        super().__init__()
        self.owning_thread = threading.get_ident()
        self.foreign_calls: list[str] = []

    def _check(self, name: str) -> None:
        if threading.get_ident() != self.owning_thread:
            self.foreign_calls.append(name)
            raise AssertionError(
                f"{name} was called from thread {threading.get_ident()}, not the "
                f"manager's loop thread {self.owning_thread}"
            )

    async def read_after(self, key, after, *, max_records, block=None):  # type: ignore[no-untyped-def]
        self._check("read_after")
        return await super().read_after(
            key, after, max_records=max_records, block=block
        )

    async def read_range(self, key, first, last):  # type: ignore[no-untyped-def]
        self._check("read_range")
        return await super().read_range(key, first, last)


@pytest.mark.asyncio
async def test_the_provider_is_never_called_from_the_workflow_thread(
    stream_key: StreamKey,
) -> None:
    """Driven from a real second thread, standing in for the executor.

    ``activate()`` runs on a thread-pool executor, so "the Workflow thread" is
    genuinely a different OS thread -- not merely a different task.
    """
    backend = ThreadCheckingBackend()
    notifier = RecordingNotifier()
    manager = make_manager(backend, notifier)
    manager.register(
        run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
    )
    try:
        await append(backend, stream_key, b"a", b"b")
        await asyncio.wait_for(notifier.notified.wait(), 2)

        drained: list[bytes] = []

        def workflow_thread() -> None:
            for record in manager.drain(RUN_ID, 1):
                drained.append(record.payload)

        thread = threading.Thread(target=workflow_thread)
        thread.start()
        thread.join(timeout=5)

        assert drained == [b"a", b"b"]
        assert backend.foreign_calls == [], (
            f"the provider was called off the manager's loop: {backend.foreign_calls}"
        )
    finally:
        await manager.shutdown()


# --- eviction ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_eviction_discards_prefetch_state_and_restarts_from_committed(
    stream_key: StreamKey,
) -> None:
    """Reading is not consuming, and consuming is not committing.

    Nothing prefetched or delivered was ever a claim, which is exactly why "no
    cursor advances unless the marker commits" is safe to state.
    """
    backend = MemoryStreamBackend()
    notifier = RecordingNotifier()
    manager = make_manager(backend, notifier)
    subscription = manager.register(
        run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
    )
    await append(backend, stream_key, b"a", b"b", b"c")
    await asyncio.wait_for(notifier.notified.wait(), 2)
    manager.drain(RUN_ID, 1, max_records=1)

    assert subscription.delivery_cursor != BEGINNING
    assert subscription.prefetch_cursor != BEGINNING

    await manager.evict_run(RUN_ID)

    assert subscription.buffered == 0
    assert subscription.delivery_cursor == subscription.committed_cursor == BEGINNING
    assert subscription.prefetch_cursor == BEGINNING
    assert manager.subscriptions(RUN_ID) == []


@pytest.mark.asyncio
async def test_a_committed_cursor_is_where_a_restart_resumes(
    stream_key: StreamKey,
) -> None:
    backend = MemoryStreamBackend()
    notifier = RecordingNotifier()
    manager = make_manager(backend, notifier)
    subscription = manager.register(
        run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
    )
    try:
        await append(backend, stream_key, b"a", b"b")
        await asyncio.wait_for(notifier.notified.wait(), 2)
        drained = manager.drain(RUN_ID, 1)
        assert drained[0].offset is not None

        # A marker commits the first record only.
        subscription.commit(AFTER(drained[0].offset))
        subscription.reset_to_committed()

        assert subscription.delivery_cursor == AFTER(drained[0].offset)
        assert subscription.prefetch_cursor == AFTER(drained[0].offset)
    finally:
        await manager.shutdown()


# --- what the watcher does with each readiness answer ------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("answer", "keeps_watcher"),
    [
        (ReadinessResult.PARKED, True),
        (ReadinessResult.NO_OPEN_WORKFLOW_TASK, True),
        (ReadinessResult.RUN_NOT_FOUND, False),
    ],
)
async def test_undeliverable_readiness_owes_a_wake_and_keeps_the_right_watchers(
    stream_key: StreamKey, answer: str, keeps_watcher: bool
) -> None:
    """All three send a Signal; they differ in what happens afterwards.

    Tearing a watcher down for ``NoOpenWorkflowTask`` would remove the only
    thing watching during the normal window between Workflow Tasks.
    """
    backend = MemoryStreamBackend()
    notifier = RecordingNotifier(answer)
    woken: list[int] = []

    async def send_wake(subscription) -> None:  # type: ignore[no-untyped-def]
        woken.append(subscription.wait_id)

    manager = make_manager(backend, notifier, send_wake=send_wake)
    manager.register(
        run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
    )
    try:
        await append(backend, stream_key, b"a")
        await asyncio.wait_for(notifier.notified.wait(), 2)
        await asyncio.sleep(0.1)

        assert woken == [1]
        assert bool(manager.subscription(RUN_ID, 1)) is keeps_watcher
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("answer", [ReadinessResult.ACCEPTED, ReadinessResult.STALE])
async def test_deliverable_readiness_owes_no_wake(
    stream_key: StreamKey, answer: str
) -> None:
    """A Signal sent here would be a wake nobody needed.

    ``Stale`` in particular must not signal: the wait moved on, so the right
    response is to re-probe, not to manufacture a Workflow Task.
    """
    backend = MemoryStreamBackend()
    notifier = RecordingNotifier(answer)
    woken: list[int] = []

    async def send_wake(subscription) -> None:  # type: ignore[no-untyped-def]
        woken.append(subscription.wait_id)

    manager = make_manager(backend, notifier, send_wake=send_wake)
    manager.register(
        run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
    )
    try:
        await append(backend, stream_key, b"a")
        await asyncio.wait_for(notifier.notified.wait(), 2)
        await asyncio.sleep(0.1)

        assert woken == []
        assert manager.subscription(RUN_ID, 1) is not None
    finally:
        await manager.shutdown()


# --- the blocked snapshot ----------------------------------------------------


@pytest.mark.asyncio
async def test_the_blocked_snapshot_is_read_from_manager_state_alone(
    stream_key: StreamKey,
) -> None:
    """Finalization performs no backend I/O (ADR-010).

    The boundary is not "wherever the stream is now"; it is where this Workflow
    Task's deliveries stopped, which is already fixed. Refreshing it against the
    backend could name a position replay must not reproduce.
    """
    backend = ThreadCheckingBackend()
    notifier = RecordingNotifier()
    manager = make_manager(backend, notifier)
    manager.register(
        run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
    )
    manager.register(
        run_id=RUN_ID, wait_id=2, stream_key=stream_key, backend_name="tokens"
    )
    try:
        await append(backend, stream_key, b"a")
        await asyncio.wait_for(notifier.notified.wait(), 2)
        drained = manager.drain(RUN_ID, 1)
        reads_before = len(backend.range_reads)

        snapshot = manager.blocked_snapshot(RUN_ID)

        assert set(snapshot) == {1, 2}
        assert drained[0].offset is not None
        assert snapshot[1] == AFTER(drained[0].offset)
        assert snapshot[2] == BEGINNING, (
            "a subscription that delivered nothing is blocked at its start cursor"
        )
        assert len(backend.range_reads) == reads_before
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_a_start_cursor_is_where_a_continued_chain_resumes(
    stream_key: StreamKey,
) -> None:
    """All three cursors begin at the restored boundary, not at BEGINNING."""
    backend = MemoryStreamBackend()
    notifier = RecordingNotifier()
    manager = make_manager(backend, notifier)
    restored: Cursor = AFTER(Offset("500-0"))
    subscription = manager.register(
        run_id=RUN_ID,
        wait_id=1,
        stream_key=stream_key,
        backend_name="tokens",
        start_cursor=restored,
    )
    try:
        assert subscription.committed_cursor == restored
        assert subscription.delivery_cursor == restored
        assert subscription.prefetch_cursor == restored
    finally:
        await manager.shutdown()


# --- teardown ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancelling_one_subscription_leaves_the_others(
    stream_key: StreamKey,
) -> None:
    backend = MemoryStreamBackend()
    notifier = RecordingNotifier()
    manager = make_manager(backend, notifier)
    manager.register(
        run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
    )
    manager.register(
        run_id=RUN_ID, wait_id=2, stream_key=stream_key, backend_name="tokens"
    )
    try:
        await manager.cancel(RUN_ID, 1)

        assert manager.subscription(RUN_ID, 1) is None
        assert manager.subscription(RUN_ID, 2) is not None
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_cancelling_a_wait_takes_back_its_park_intent(
    stream_key: StreamKey,
) -> None:
    """The intent has to go here, because nothing later can be asked to retry.

    The resolve path can afford to log a failure and try again, since the
    subscription it is working on stays registered. This one drops it, so an
    intent left behind is left behind for good -- and a stale intent is what
    `current_park_generation` answers to every producer that asks. A producer
    naming a generation Core has discarded sends a wake Core ignores as stale:
    the record is appended, the Signal is sent, and the Workflow is never woken.
    """
    backend = MemoryStreamBackend()
    manager = make_manager(backend, RecordingNotifier())
    manager.register(
        run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
    )
    try:
        assert not await manager.prepare_park(RUN_ID, 4, {1: BEGINNING})
        assert await backend.parked_wait_ids(stream_key) == [1]

        await manager.cancel(RUN_ID, 1)

        assert await backend.parked_wait_ids(stream_key) == [], (
            "the closed wait's intent is still in the backend, advertising a "
            "park generation Core has discarded"
        )
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_cancelling_a_wait_stops_its_watcher(stream_key: StreamKey) -> None:
    """Dropping the subscription is what makes an orphaned watcher invisible.

    Once it is out of the Run's map nothing can reach it to notice, and it goes
    on prefetching into a buffer nobody can drain -- holding a backend
    connection for the rest of the Worker's life.
    """
    backend = MemoryStreamBackend()
    manager = make_manager(backend, RecordingNotifier())
    subscription = manager.register(
        run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
    )
    try:
        await asyncio.sleep(0.05)
        assert subscription._watcher is not None and not subscription._watcher.done()

        await manager.cancel(RUN_ID, 1)

        assert subscription._cancelled, "the cancelled subscription must be marked dead"
        assert subscription._watcher.done(), (
            "its watcher outlived it, so it is still reading a stream nothing "
            "is reading back"
        )
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_a_cancel_from_the_workflow_thread_reaches_a_loop_with_nothing_to_do(
    stream_key: StreamKey,
) -> None:
    """Driven from a real second thread, standing in for the executor.

    ``close()`` runs inside the synchronous ``activate()``, and ``create_task``
    from there does not raise: the task is appended to the loop's ready queue
    and the loop is never woken, so it runs only if something else happens to
    wake the loop. Between activations nothing does -- the watchers are sitting
    in blocking reads -- which is why the watch block below is long and why the
    other thread, not this coroutine, is what waits. Awaiting anything with a
    timeout here would arm a timer, and the wake that timer produces would run
    the cancel whether or not the loop was ever told about it.
    """
    backend = MemoryStreamBackend()
    manager = make_manager(
        backend, RecordingNotifier(), watch_block=timedelta(seconds=30)
    )
    manager.register(
        run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
    )
    try:
        # Long enough for the watcher to settle into its blocking read.
        await asyncio.sleep(0.1)

        def workflow_thread() -> bool:
            # Waited out here rather than on the loop, and waited out at all:
            # a task appended while the loop is still on its way into the
            # select is picked up on the way in, which hides the missing wake.
            time.sleep(0.2)
            manager.cancel_from_workflow_thread(RUN_ID, 1)
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                if manager.subscription(RUN_ID, 1) is None:
                    return True
                time.sleep(0.01)
            return False

        cancelled = await asyncio.get_running_loop().run_in_executor(
            None, workflow_thread
        )

        assert cancelled, (
            "the cancel was queued onto a loop nothing woke, so a watcher that "
            "was supposed to stop keeps running with nothing to say so"
        )
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_shutdown_tears_down_every_run(stream_key: StreamKey) -> None:
    backend = MemoryStreamBackend()
    notifier = RecordingNotifier()
    manager = make_manager(backend, notifier)
    for run_id in ("run-a", "run-b"):
        manager.register(
            run_id=run_id, wait_id=1, stream_key=stream_key, backend_name="tokens"
        )

    await manager.shutdown()

    assert manager.runs_with_subscriptions() == []


@pytest.mark.asyncio
async def test_manager_state_is_keyed_by_run(stream_key: StreamKey) -> None:
    """So a stale Run cannot leak connections into a live one."""
    backend = MemoryStreamBackend()
    notifier = RecordingNotifier()
    manager = make_manager(backend, notifier)
    manager.register(
        run_id="run-a", wait_id=1, stream_key=stream_key, backend_name="tokens"
    )
    manager.register(
        run_id="run-b", wait_id=1, stream_key=stream_key, backend_name="tokens"
    )
    try:
        await manager.evict_run("run-a")

        assert manager.subscriptions("run-a") == []
        assert len(manager.subscriptions("run-b")) == 1
    finally:
        await manager.shutdown()


# --- the sandbox -------------------------------------------------------------


def test_the_manager_module_passes_through_the_sandbox() -> None:
    """Re-importing it inside would give the Workflow a manager watching nothing.

    The real one owns the Worker's connections, watcher tasks, and buffers; only
    an opaque handle to it may cross the boundary.
    """
    assert (
        "temporalio.contrib.external_workflow_streams._manager"
        in SandboxRestrictions.passthrough_modules_minimum
    )
    assert (
        "temporalio.contrib.external_workflow_streams._manager"
        in SandboxRestrictions.default.passthrough_modules
    )


@pytest.mark.asyncio
async def test_re_registering_a_wait_stops_the_watcher_it_replaced(
    stream_key: StreamKey,
) -> None:
    """The replaced subscription is unreachable and must not keep polling.

    Nothing else would notice: the new subscription works, records are
    delivered, and the only symptom is a backend connection that never closes
    and a task that outlives its Run -- visible, if at all, as a Worker whose
    memory grows.
    """
    backend = MemoryStreamBackend()
    notifier = RecordingNotifier()
    manager = make_manager(backend, notifier)
    try:
        first = manager.register(
            run_id="run-1",
            wait_id=1,
            stream_key=stream_key,
            backend_name="tokens",
        )
        await asyncio.sleep(0.05)
        assert first._watcher is not None and not first._watcher.done()

        second = manager.register(
            run_id="run-1",
            wait_id=1,
            stream_key=stream_key,
            backend_name="tokens",
        )
        await asyncio.sleep(0.05)

        assert first._cancelled, "the replaced subscription must be marked dead"
        assert first._watcher.done(), "its watcher must be stopped, not orphaned"
        assert second._watcher is not None and not second._watcher.done(), (
            "the replacement's own watcher must still be running"
        )
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_an_evicted_run_re_delivers_the_records_it_had_already_seen(
    stream_key: StreamKey,
) -> None:
    """Discarding prefetch state is only half of it; the records must come back.

    Cursors reset to the committed boundary is the mechanism, but what makes it
    correct is the outcome: a Run that consumed records without committing a
    marker must see those same records again, or eviction would silently drop
    everything between the last marker and the eviction.
    """
    backend = MemoryStreamBackend()
    notifier = RecordingNotifier()
    manager = make_manager(backend, notifier)
    manager.register(
        run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
    )
    await append(backend, stream_key, b"a", b"b", b"c")
    await asyncio.wait_for(notifier.notified.wait(), 2)
    first_pass = [r.offset for r in manager.drain(RUN_ID, 1)]
    assert len(first_pass) == 3

    # Evicted with no marker committed: nothing it saw was ever a claim.
    await manager.evict_run(RUN_ID)

    notifier.notified.clear()
    manager.register(
        run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
    )
    await asyncio.wait_for(notifier.notified.wait(), 2)
    second_pass = [r.offset for r in manager.drain(RUN_ID, 1)]

    try:
        assert second_pass == first_pass, (
            "the restarted subscription must re-read the same offsets; resuming "
            "past them would drop every record between the last marker and the "
            "eviction"
        )
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_a_stale_answer_is_re_reported_rather_than_dropped(
    stream_key: StreamKey,
) -> None:
    """`Stale` means Core held a newer generation, not that the record landed.

    The watcher calls back here only after a *new* non-empty read, and its
    prefetch cursor is already past the buffered record, so a `Stale` treated as
    delivered announces that record to nobody. The Workflow then blocks forever
    on data it is already holding.
    """
    backend = MemoryStreamBackend()

    class StaleThenAccepted(RecordingNotifier):
        async def __call__(self, run_id: str, wait_id: int, generation: int) -> str:
            self.calls.append((run_id, wait_id, generation))
            self.notified.set()
            return (
                ReadinessResult.STALE
                if len(self.calls) == 1
                else ReadinessResult.ACCEPTED
            )

    notifier = StaleThenAccepted()
    manager = make_manager(backend, notifier)
    try:
        manager.register(
            run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
        )
        await append(backend, stream_key, b"a")
        await asyncio.wait_for(notifier.notified.wait(), 2)
        await asyncio.sleep(0.3)

        assert len(notifier.calls) >= 2, (
            "a stale answer must be re-reported; the watcher will not announce "
            f"this record again, got {len(notifier.calls)} report(s)"
        )
        assert manager.subscriptions(RUN_ID)[0].buffered == 1, (
            "the record must still be there to deliver"
        )
    finally:
        await manager.shutdown()


@pytest.mark.parametrize("position", [1, 2, 3])
@pytest.mark.asyncio
async def test_a_stale_retry_that_finds_the_run_gone_tears_the_watcher_down(
    stream_key: StreamKey, position: int
) -> None:
    """The retries' answer is the answer, and one of them requires a teardown.

    A first report can race a wait-generation change and be answered `Stale`; the
    Run can then be evicted before the delayed retry that follows. The retry loop
    kept only whether *some* attempt was accepted and threw the rest away, so
    control returned with the original `Stale` still in hand: the owed wake went
    out (right) and the `RunNotFound` teardown never ran (wrong). What is left
    behind is a watcher, a buffer, a backend read loop and a `_runs` entry for a
    Run this Worker no longer owns -- and every later readiness report and wake
    attempt is made on its behalf.

    Parameterized over which retry discovers the eviction, because nothing makes
    the first one special.
    """
    backend = MemoryStreamBackend()

    class StaleThenGone(RecordingNotifier):
        async def __call__(self, run_id: str, wait_id: int, generation: int) -> str:
            self.calls.append((run_id, wait_id, generation))
            self.notified.set()
            if len(self.calls) < position + 1:
                return ReadinessResult.STALE
            return ReadinessResult.RUN_NOT_FOUND

    notifier = StaleThenGone()
    wake = CountingWake(failures=0)
    manager = make_manager(backend, notifier, send_wake=wake)
    try:
        subscription = manager.register(
            run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
        )
        await append(backend, stream_key, b"a")

        await until(
            lambda: manager.subscription(RUN_ID, 1) is None,
            "a `RunNotFound` reached on a stale retry left the subscription in "
            "`_runs`, so this Worker keeps a watcher, a buffer and a read loop "
            "for a Run it no longer holds",
        )
        assert subscription._cancelled, "the dropped subscription is still live"
        await until(
            lambda: subscription._watcher is not None and subscription._watcher.done(),
            "the watcher for a Run that is gone is still reading from the backend",
        )
        assert len(wake.counters) == 1, (
            "the wake `RunNotFound` requires must still be sent exactly once, "
            f"got {len(wake.counters)}"
        )
        assert len(notifier.calls) == position + 1, (
            "the retries did not stop at `RunNotFound`; a Run that is gone cannot "
            "come back, and each further attempt delays the wake the record needs"
        )
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_a_readiness_transport_failure_does_not_kill_the_watcher(
    stream_key: StreamKey,
) -> None:
    """An exception is a transport problem, not an answer.

    Letting it escape ends the watcher for good: the subscription stays
    registered, its buffer keeps its records, and nothing announces them again.
    Swallowing it would instead claim the record was announced.
    """
    backend = MemoryStreamBackend()

    class FailsOnce(RecordingNotifier):
        async def __call__(self, run_id: str, wait_id: int, generation: int) -> str:
            self.calls.append((run_id, wait_id, generation))
            self.notified.set()
            if len(self.calls) == 1:
                raise ConnectionError("core unreachable")
            return ReadinessResult.ACCEPTED

    notifier = FailsOnce()
    manager = make_manager(backend, notifier)
    try:
        subscription = manager.register(
            run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
        )
        await append(backend, stream_key, b"a")
        await asyncio.wait_for(notifier.notified.wait(), 2)
        await asyncio.sleep(0.5)

        assert len(notifier.calls) >= 2, "the failing report must be retried"
        watcher = subscription._watcher
        assert watcher is not None and not watcher.done(), (
            "the watcher must survive a readiness failure; if it ends, this "
            "subscription is never served again"
        )
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_a_wake_failure_does_not_kill_the_watcher(
    stream_key: StreamKey,
) -> None:
    """The wake sender reports failure by raising, and that must stop here.

    An owed wake that could not be sent stays owed for the shutdown sweep. An
    exception escaping instead takes the watcher with it, which loses every
    later record too.
    """
    backend = MemoryStreamBackend()
    notifier = RecordingNotifier(answer=ReadinessResult.NO_OPEN_WORKFLOW_TASK)

    async def failing_wake(subscription) -> None:  # type: ignore[no-untyped-def]
        raise ConnectionError("service unavailable")

    manager = make_manager(backend, notifier, send_wake=failing_wake)
    try:
        subscription = manager.register(
            run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
        )
        await append(backend, stream_key, b"a")
        await asyncio.wait_for(notifier.notified.wait(), 2)
        await asyncio.sleep(0.3)

        watcher = subscription._watcher
        assert watcher is not None and not watcher.done(), (
            "a failed wake must not end the watcher"
        )
        assert subscription.wakes_owed >= 1, "the wake stays owed for the sweep"
    finally:
        await manager.shutdown()


# --- the park handshake's wait set (P19) --------------------------------------
#
# One Core activation is constructed here, and only here. The set that gets
# parked is decided in two places -- the Worker's job handler turns Core's
# `PrepareExternalStreamPark.waits` into the map it hands over, and the manager
# installs and rechecks against it -- so a test on either half alone cannot show
# that the two agree on what Core asked for.


def _park_job(run_id: str, quiescence_generation: int, *wait_ids: int):  # type: ignore[no-untyped-def]
    """The activation Core sends to open a park handshake."""
    from temporalio.bridge.proto.workflow_activation import WorkflowActivation

    activation = WorkflowActivation()
    activation.run_id = run_id
    job = activation.jobs.add().prepare_external_stream_park
    job.quiescence_generation = quiescence_generation
    for wait_id in wait_ids:
        job.waits.add().wait_id = wait_id
    return activation


class _StubRuntime:
    """The runtime's half of the park path: cursors in, terminal out."""

    def __init__(self, cursors: dict[int, Cursor]) -> None:
        self._cursors = cursors
        self.terminals = 0

    def blocked_snapshot(self) -> dict[int, Cursor]:
        return dict(self._cursors)

    def add_terminal(self) -> bytes:
        self.terminals += 1
        return b"terminal"


class _StubWorker:
    """Only the two things ``_handle_external_stream_jobs`` reaches for."""

    def __init__(self, run_id: str, runtime: _StubRuntime, manager: object) -> None:
        self._external_stream_runtimes = {run_id: runtime}
        self._manager = manager

    def _stream_manager(self) -> object:
        return self._manager


@pytest.mark.asyncio
async def test_the_park_set_is_cores_wait_set_and_not_every_registration() -> None:
    """A registered subscription is not necessarily one Core is parking.

    ``PrepareExternalStreamPark.waits`` is Core's complete *blocked* snapshot.
    The runtime's own registration list is a superset of it: a subscription that
    delivered a record and was not awaited again is registered and not blocked,
    so it is absent from the quiescent snapshot Core parks.

    Parking the superset is wrong in both directions. The recheck for a wait
    Core is not parking finds that wait's records -- which is *not* news, since
    nothing is waiting on them -- and aborts a park that was entirely
    legitimate, so the Workflow Task never parks and the handshake runs again on
    the next idle timeout, and again. And an intent installed for a wait outside
    the park set is an intent with no park behind it, which is precisely the
    thing `backend-contract.md` forbids leaving in a backend.
    """
    from temporalio.worker._workflow import _WorkflowWorker

    backend = MemoryStreamBackend()
    manager = make_manager(backend, RecordingNotifier())
    parked = StreamKey("ns", "wf", "first-run", "tokens")
    driving = StreamKey("ns", "wf", "first-run", "tool-events")
    try:
        manager.register(
            run_id=RUN_ID, wait_id=1, stream_key=parked, backend_name="tokens"
        )
        manager.register(
            run_id=RUN_ID, wait_id=2, stream_key=driving, backend_name="tokens"
        )
        # Wait 2 has a record sitting in it. Workflow code is not blocked on it,
        # so it is not in the set Core asks to park.
        await append(backend, driving, b"b")

        runtime = _StubRuntime({1: BEGINNING, 2: BEGINNING})
        worker = _StubWorker(RUN_ID, runtime, manager)
        completion = await _WorkflowWorker._handle_external_stream_jobs(
            worker,  # type: ignore[arg-type]
            _park_job(RUN_ID, 4, 1),
            None,  # type: ignore[arg-type]
        )

        assert completion is not None
        result = completion.successful.commands[0].external_stream_park_result
        assert result.WhichOneof("outcome") == "confirmed", (
            "a wait Core is not parking aborted the park of the wait it is"
        )
        assert await backend.park_intent(parked, 1) is not None
        assert await backend.park_intent(driving, 2) is None, (
            "an intent was installed for a wait Core never asked to park, so a "
            "producer on that stream reads a park generation nothing is sitting in"
        )
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("failing", ["install", "recheck"])
async def test_a_failed_park_leaves_no_externally_visible_half(failing: str) -> None:
    """Parking is all-or-nothing, and a raised activation is not an exception to it.

    Intents are installed one at a time, so any failure part-way through -- the
    second install, or a recheck once every install has landed -- leaves the
    completed ones visible to producers while the activation itself fails. Core
    parks nothing, so those intents describe a park that does not exist; an
    eviction then takes the local bookkeeping away and nothing can remove them
    at all.
    """

    class Failing(MemoryStreamBackend):
        async def install_park_intent(self, key, intent):  # type: ignore[no-untyped-def]
            if failing == "install" and intent.wait_id == 2:
                raise ConnectionError("backend unavailable")
            return await super().install_park_intent(key, intent)

        async def recheck(self, key, wait_id):  # type: ignore[no-untyped-def]
            if failing == "recheck":
                raise ConnectionError("backend unavailable")
            return await super().recheck(key, wait_id)

    backend = Failing()
    manager = make_manager(backend, RecordingNotifier())
    first = StreamKey("ns", "wf", "first-run", "tokens")
    second = StreamKey("ns", "wf", "first-run", "tool-events")
    try:
        manager.register(
            run_id=RUN_ID, wait_id=1, stream_key=first, backend_name="tokens"
        )
        manager.register(
            run_id=RUN_ID, wait_id=2, stream_key=second, backend_name="tokens"
        )

        # Reported as the taxonomy's transient row -- a backend that is
        # unreachable is nothing for an operator to do anything about -- with
        # the provider's own error kept as the cause, so the log still says
        # which call failed and why.
        with pytest.raises(StreamStorageError) as failure:
            await manager.prepare_park(RUN_ID, 4, {1: BEGINNING, 2: BEGINNING})
        assert isinstance(failure.value.__cause__, ConnectionError)

        assert await backend.parked_wait_ids(first) == [], (
            "the park failed, and wait 1's intent is still in the backend "
            "advertising a park Core never confirmed"
        )
        assert await backend.parked_wait_ids(second) == []
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_a_cancelled_park_takes_back_the_intents_it_had_installed() -> None:
    """Cancellation is the *most* likely way a half-installed park is abandoned.

    Core withdrawing the activation and the Worker shutting down both arrive as
    ``CancelledError``, which derives from ``BaseException`` -- so a rollback
    reached only by ``except Exception`` is skipped for exactly the failures
    most likely to leave a park half-installed. The intents that stay describe a
    park Core never confirmed, and the eviction that follows takes the local
    mirror away while they remain.
    """

    class Blocking(MemoryStreamBackend):
        """Holds the second install open, so the first one is already visible."""

        def __init__(self) -> None:
            super().__init__()
            self.reached = asyncio.Event()
            self.release = asyncio.Event()

        async def install_park_intent(self, key, intent):  # type: ignore[no-untyped-def]
            if intent.wait_id == 2:
                self.reached.set()
                await self.release.wait()
            return await super().install_park_intent(key, intent)

    backend = Blocking()
    manager = make_manager(backend, RecordingNotifier())
    key = StreamKey("ns", "wf", "first-run", "tokens")
    try:
        manager.register(
            run_id=RUN_ID, wait_id=1, stream_key=key, backend_name="tokens"
        )
        manager.register(
            run_id=RUN_ID, wait_id=2, stream_key=key, backend_name="tokens"
        )
        parking = asyncio.get_running_loop().create_task(
            manager.prepare_park(RUN_ID, 4, {1: BEGINNING, 2: BEGINNING})
        )
        await asyncio.wait_for(backend.reached.wait(), 2)
        assert await backend.parked_wait_ids(key) == [1], (
            "this case is only meaningful once one intent is installed"
        )

        parking.cancel()
        with pytest.raises(asyncio.CancelledError):
            await parking

        assert await backend.parked_wait_ids(key) == [], (
            "the park was abandoned mid-install and wait 1's intent is still in "
            "the backend, advertising a park Core never confirmed"
        )
    finally:
        backend.release.set()
        await manager.shutdown()


# --- the removal a failure leaves owed ----------------------------------------
#
# A park intent is durable backend state; the manager's knowledge of it is not.
# `installed_park_generation` is a mirror that only the Worker which installed
# the park holds, and every removal path reaches its intent *through* the
# Subscription that carries it -- which the close, the eviction and the hand-off
# all throw away. So a failed removal recorded on the Subscription is a failed
# removal recorded on the object the next step discards, and the manager keeps a
# per-Run ledger of owed removals instead.


class FailingRemovals(MemoryStreamBackend):
    """A backend whose park-intent removals fail on demand.

    ``failures`` counts down, so a test can ask for a single blip or -- with a
    number no retry can reach -- for a window it closes itself by setting it
    back to zero.
    """

    def __init__(self, failures: int = 0) -> None:
        super().__init__()
        self.failures = failures
        self.removal_attempts = 0

    async def remove_park_intent_if_matches(
        self,
        key: StreamKey,
        wait_id: int,
        *,
        run_id: str,
        park_generation: int,
    ) -> bool:
        self.removal_attempts += 1
        if self.failures > 0:
            self.failures -= 1
            raise ConnectionError("backend unavailable")
        return await super().remove_park_intent_if_matches(
            key,
            wait_id,
            run_id=run_id,
            park_generation=park_generation,
        )


async def _nothing_parked(backend: MemoryStreamBackend, key: StreamKey) -> bool:
    return await backend.parked_wait_ids(key) == []


async def _only_wait_two_is_parked(
    backend: MemoryStreamBackend, key: StreamKey
) -> bool:
    return await backend.parked_wait_ids(key) == [2]


async def _inherit(backend: MemoryStreamBackend, key: StreamKey, run_id: str) -> None:
    """Leaves behind the intent of a park whose Worker is gone.

    Installed straight into the backend rather than through a manager, because
    that is exactly what makes it inherited: no mirror of it exists anywhere on
    the Worker that finds it.
    """
    await backend.install_park_intent(
        key,
        ParkIntent(wait_id=1, cursor=BEGINNING, park_generation=7, run_id=run_id),
    )


@pytest.mark.asyncio
async def test_one_blip_does_not_end_the_inherited_park_reconciliation(
    stream_key: StreamKey,
) -> None:
    """Registration is the only moment an inherited intent is looked for.

    It runs at most once per Subscription, so a single transient backend error
    used to leave the intent installed with nothing left to try it again. What
    that costs is the invariant's whole point: ``current_park_generation`` goes
    on answering a generation Core has discarded, and because a parked wake's
    request ID ignores sender identity the wake naming it is byte-identical to
    the one that already ended that generation -- so the server deduplicates it
    and the Workflow is never told about a record that is durably present.
    """
    backend = FailingRemovals(failures=1)
    await _inherit(backend, stream_key, RUN_ID)
    manager = make_manager(backend, RecordingNotifier())
    try:
        manager.register(
            run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
        )

        await until(
            lambda: _nothing_parked(backend, stream_key),
            "the inherited intent is still installed after one failed removal, "
            "and only another registration of this same wait would try again",
        )
        assert backend.removal_attempts >= 2, "the failed removal must be retried"
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_an_inherited_intent_is_retried_autonomously_after_recovery(
    stream_key: StreamKey,
) -> None:
    """Retries are the cheap first line; the ledger is what makes them optional.

    Bounding the retries is only survivable because giving up records the
    removal rather than forgetting it. Waiting instead for "the next time this
    wait is registered" is a coincidence of eviction, not a mechanism -- and the
    Run that most needs the removal is the one cached and blocked on something
    other than this stream, which never registers this wait again at all.
    """
    backend = FailingRemovals(failures=99)
    await _inherit(backend, stream_key, RUN_ID)
    manager = make_manager(backend, RecordingNotifier())
    try:
        manager.register(
            run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
        )
        await until(
            lambda: (
                not manager._reconciliations
                and backend.removal_attempts >= PARK_REMOVAL_ATTEMPTS
            ),
            "the reconciliation must exhaust its attempts before the ledger is "
            "the only thing left holding the removal",
        )
        backend.failures = 0

        await until(
            lambda: _nothing_parked(backend, stream_key),
            "the inherited intent stayed installed after backend recovery even "
            "though no park, resolve, registration, or eviction occurred",
        )

        assert await backend.parked_wait_ids(stream_key) == [], (
            "the reconciliation exhausted its inline attempts and its ledger "
            "had no autonomous retry"
        )
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_a_close_whose_removal_fails_leaves_the_removal_owed(
    stream_key: StreamKey,
) -> None:
    """A close drops the subscription, and used to drop the removal with it.

    Nothing could retry afterwards: the resolve path iterates *registered*
    subscriptions, eviction and the shutdown sweep remove no intents, and
    another wait's park cannot touch a per-wait key.

    And the damage is not confined to the closed wait. A stale intent keeps
    ``parked_wait_ids`` non-empty, which suppresses the unparked-wake fallback
    for the whole stream: with no live wait parked, the producer sends only the
    dead generation, Core discards it as stale, and dedup silences every later
    publish -- so wait 2 here loses its wakes to wait 1's leftover.
    """
    backend = FailingRemovals(failures=99)
    manager = make_manager(backend, RecordingNotifier())
    try:
        for wait_id in (1, 2):
            manager.register(
                run_id=RUN_ID,
                wait_id=wait_id,
                stream_key=stream_key,
                backend_name="tokens",
            )
        assert not await manager.prepare_park(RUN_ID, 4, {1: BEGINNING, 2: BEGINNING})

        await manager.cancel(RUN_ID, 1)

        assert manager.subscription(RUN_ID, 1) is None, (
            "the close must still drop the subscription -- one kept registered "
            "while a removal is retried is an orphaned watcher"
        )
        assert await backend.parked_wait_ids(stream_key) == [1, 2], (
            "this case is only meaningful while the removal is genuinely failing"
        )
        backend.failures = 0

        await manager.resolve_park(RUN_ID)

        assert await backend.parked_wait_ids(stream_key) == [], (
            "the closed wait's intent survived the only path that could still "
            "have removed it, and it suppresses the unparked wake for wait 2 too"
        )
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_a_failed_resolve_removal_retries_without_another_event(
    stream_key: StreamKey,
) -> None:
    """Backend recovery alone retires a resolved park's stale intent."""
    backend = FailingRemovals(failures=99)
    manager = make_manager(backend, RecordingNotifier())
    try:
        manager.register(
            run_id=RUN_ID,
            wait_id=1,
            stream_key=stream_key,
            backend_name="tokens",
        )
        assert not await manager.prepare_park(RUN_ID, 4, {1: BEGINNING})

        await manager.resolve_park(RUN_ID)
        assert await backend.current_park_generation(stream_key, 1) == 4
        backend.failures = 0

        await until(
            lambda: _nothing_parked(backend, stream_key),
            "the resolved intent stayed installed after backend recovery even "
            "though no park, resolve, registration, or eviction occurred",
        )
        assert manager._owed_removals == {}
    finally:
        await manager.shutdown()


class BlockingAutonomousRemoval(MemoryStreamBackend):
    """Fails the eager attempt, then blocks the manager-owned retry."""

    def __init__(self) -> None:
        super().__init__()
        self.attempts = 0
        self.retry_started = asyncio.Event()
        self.retry_cancelled = asyncio.Event()

    async def remove_park_intent_if_matches(
        self,
        key: StreamKey,
        wait_id: int,
        *,
        run_id: str,
        park_generation: int,
    ) -> bool:
        self.attempts += 1
        if self.attempts == 1:
            raise ConnectionError("backend unavailable")
        self.retry_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.retry_cancelled.set()
            raise
        return False


@pytest.mark.asyncio
async def test_shutdown_cancels_and_awaits_an_owed_removal_retry(
    stream_key: StreamKey,
) -> None:
    backend = BlockingAutonomousRemoval()
    manager = make_manager(backend, RecordingNotifier())
    manager.register(
        run_id=RUN_ID,
        wait_id=1,
        stream_key=stream_key,
        backend_name="tokens",
    )
    assert not await manager.prepare_park(RUN_ID, 4, {1: BEGINNING})
    await manager.resolve_park(RUN_ID)
    await asyncio.wait_for(backend.retry_started.wait(), 2)

    await asyncio.wait_for(manager.shutdown(grace=timedelta(milliseconds=200)), 1)

    assert backend.retry_cancelled.is_set()
    assert manager._owed_removal_retries == {}


@pytest.mark.asyncio
async def test_closing_a_wait_removes_an_intent_it_only_inherited(
    stream_key: StreamKey,
) -> None:
    """The second door, which needs no failure of its own to open.

    ``installed_park_generation`` is set only by a park *this* Worker installed,
    so for an inherited intent it is ``None`` and the removal short-circuits
    silently -- the close attempts nothing at all. That is why the close is not
    a backstop for a reconciliation that has already failed: it is disabled by
    exactly the condition that made the reconciliation necessary.
    """
    backend = FailingRemovals(failures=99)
    await _inherit(backend, stream_key, RUN_ID)
    manager = make_manager(backend, RecordingNotifier())
    try:
        manager.register(
            run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
        )
        await until(
            lambda: backend.removal_attempts >= PARK_REMOVAL_ATTEMPTS,
            "the reconciliation must have given up before the close is asked to "
            "cover for it",
        )
        backend.failures = 0

        await manager.cancel(RUN_ID, 1)

        assert await backend.parked_wait_ids(stream_key) == [], (
            "the close attempted no removal, because the intent it inherited is "
            "mirrored in no generation of its own"
        )
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_an_owed_removal_outlives_eviction_and_keeps_retrying(
    stream_key: StreamKey,
) -> None:
    """Cache ownership ending does not end backend-cleanup ownership.

    Installed intents are deliberately untouched by the same path: an eviction
    is not the end of a park, and the intents of one that really is outstanding
    have to survive the Worker losing the Run. Only a removal already decided on
    is retried after it.
    """
    backend = FailingRemovals(failures=99)
    manager = make_manager(backend, RecordingNotifier())
    try:
        for wait_id in (1, 2):
            manager.register(
                run_id=RUN_ID,
                wait_id=wait_id,
                stream_key=stream_key,
                backend_name="tokens",
            )
        assert not await manager.prepare_park(RUN_ID, 4, {1: BEGINNING, 2: BEGINNING})
        await manager.cancel(RUN_ID, 1)

        await manager.evict_run(RUN_ID)
        assert await backend.parked_wait_ids(stream_key) == [1, 2]
        backend.failures = 0

        await until(
            lambda: _only_wait_two_is_parked(backend, stream_key),
            "wait 1's owed removal stopped retrying when eviction dropped the "
            "Run, while wait 2's outstanding park still had to survive",
        )

        assert await backend.parked_wait_ids(stream_key) == [2], (
            "the owed removal did not outlive the cached Run"
        )
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_a_drain_never_removes_the_intent_of_a_park_that_replaced_it(
    stream_key: StreamKey,
) -> None:
    """An entry records a removal, not the claim that some intent exists.

    A Continue-As-New successor re-uses this stream key with wait ids that start
    again at 1, so an entry a predecessor Run left behind can name a live park's
    key. Retiring it on the key alone would take the intent out from under a
    park that really is outstanding -- manufacturing exactly the unwakeable Run
    the ledger exists to prevent.
    """
    backend = FailingRemovals(failures=99)
    manager = make_manager(backend, RecordingNotifier())
    try:
        manager.register(
            run_id="run-a", wait_id=1, stream_key=stream_key, backend_name="tokens"
        )
        assert not await manager.prepare_park("run-a", 4, {1: BEGINNING})
        await manager.cancel("run-a", 1)
        backend.failures = 0

        # The successor's park, installed directly: what matters is that it is a
        # different Run's intent sitting at the key run-a still owes a removal
        # for, not how it got there.
        await backend.install_park_intent(
            stream_key,
            ParkIntent(wait_id=1, cursor=BEGINNING, park_generation=1, run_id="run-b"),
        )

        await manager.resolve_park("run-a")

        installed = await backend.park_intent(stream_key, 1)
        assert installed is not None and installed.run_id == "run-b", (
            "run-a's owed removal was retired against whatever it found, taking "
            "out the intent of a park run-b is still sitting in"
        )
    finally:
        await manager.shutdown()


# --- a wake owed on the live path ---------------------------------------------


class CountingWake:
    """Records the counter each attempt would derive its request ID from.

    A retry that re-counts is not a retry: the request ID moves with the
    counter, so the server deduplicates nothing and answers with a second, empty
    Workflow Task instead of resolving the wake that may in fact have arrived.
    """

    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.counters: list[int] = []
        #: Asked at each attempt, for tests about what has happened to the
        #: subscription by the time an attempt is made. Assigned after the
        #: manager exists, since that is what it usually asks about.
        self.observe: Callable[[], object] | None = None
        self.observed: list[object] = []

    async def __call__(self, subscription) -> None:  # type: ignore[no-untyped-def]
        self.counters.append(subscription.wake_counter)
        if self.observe is not None:
            self.observed.append(self.observe())
        if self.failures > 0:
            self.failures -= 1
            raise ConnectionError("service unavailable")


@pytest.mark.asyncio
async def test_a_failed_live_wake_is_retried_as_the_same_wake(
    stream_key: StreamKey,
) -> None:
    """One attempt on the live path is one attempt with nothing behind it.

    The watcher has already moved ``prefetch_cursor`` past the buffered record
    and returns on an empty read, so it never comes back here without a *new*
    append. ``rearm_ready`` needs the activation the lost wake was supposed to
    cause. And the idle timer only runs while a Workflow Task is retained, which
    ``NoOpenWorkflowTask`` says there is not -- so no park handshake happens
    either. The shutdown sweep already does this correctly, which is why the
    retry belongs to both and not just to it.
    """
    backend = MemoryStreamBackend()
    notifier = RecordingNotifier(answer=ReadinessResult.NO_OPEN_WORKFLOW_TASK)
    wake = CountingWake(failures=1)
    manager = make_manager(backend, notifier, send_wake=wake)
    try:
        manager.register(
            run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
        )
        await append(backend, stream_key, b"a")

        await until(
            lambda: len(wake.counters) >= 2,
            "the failed wake was logged and forgotten; nothing on this path "
            "ever attempts it again, so the buffered record is never announced",
        )
        assert wake.counters == [1, 1], (
            "the retry derived a different request ID, so it asks for a second "
            "empty Workflow Task rather than re-sending the wake that was owed"
        )
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_a_run_that_comes_back_does_not_reuse_its_last_wake_counter(
    stream_key: StreamKey,
) -> None:
    """An evicted Run's replacement subscription owes a *new* wake, not the old one.

    The unparked request ID is derived from the sender's identity and its
    counter, and the identity is fixed for the Worker's lifetime -- so the
    counter is the only thing keeping two of this Worker's wakes apart. A
    counter held on the `Subscription` restarts at one every time an evicted Run
    is rebuilt, which re-derives the request ID the previous incarnation already
    used: the server deduplicates it, no Workflow Task is created, and a Run
    holding buffered records waits for a wake that was thrown away as a
    duplicate. The sequence therefore belongs to the manager, which outlives the
    Run.
    """
    backend = MemoryStreamBackend()
    notifier = RecordingNotifier(answer=ReadinessResult.NO_OPEN_WORKFLOW_TASK)
    wake = CountingWake(failures=0)
    manager = make_manager(backend, notifier, send_wake=wake)
    try:
        manager.register(
            run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
        )
        await append(backend, stream_key, b"a")
        await until(
            lambda: len(wake.counters) >= 1,
            "the first incarnation never owed its wake, so there is nothing for "
            "the second one to collide with",
        )

        # The Run is evicted and comes back: same Worker, same wait, a brand new
        # `Subscription` -- and a record already waiting for it, exactly as after
        # the replay this guards.
        await manager.evict_run(RUN_ID)
        manager.register(
            run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
        )
        await append(backend, stream_key, b"b")
        await until(
            lambda: len(wake.counters) >= 2,
            "the rebuilt subscription never owed a wake for the record waiting for it",
        )

        assert wake.counters[0] != wake.counters[1], (
            "the rebuilt Run drew the counter its predecessor had already used, "
            "so both wakes derive one request ID and the server deduplicates "
            f"the second away: {wake.counters}"
        )
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_a_wake_owed_by_a_vanished_run_is_retried_before_it_is_dropped(
    stream_key: StreamKey,
) -> None:
    """The sharpest sub-case: after the pop there is no backstop at all.

    ``RunNotFound`` takes the subscription out of ``_runs``, and the shutdown
    sweep is driven by ``_runs`` -- so a wake given up on before that pop is not
    merely delayed, it is unreachable by every remaining mechanism.
    """
    backend = MemoryStreamBackend()
    notifier = RecordingNotifier(answer=ReadinessResult.RUN_NOT_FOUND)
    wake = CountingWake(failures=1)
    manager = make_manager(backend, notifier, send_wake=wake)
    wake.observe = lambda: manager.subscription(RUN_ID, 1) is not None
    try:
        manager.register(
            run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
        )
        await append(backend, stream_key, b"a")

        await until(
            lambda: len(wake.counters) >= 2,
            "the wake failed once and the subscription was dropped on top of "
            "it, putting it out of reach of the sweep that would have retried it",
        )
        assert wake.counters == [1, 1]
        assert wake.observed == [True, True], (
            "an attempt was made after the subscription had already left "
            "`_runs`, where the sweep that is supposed to back it up cannot "
            "see it either"
        )
        await until(
            lambda: manager.subscription(RUN_ID, 1) is None,
            "a Run this Worker no longer holds must still be dropped once the "
            "wake it owed has been attempted",
        )
    finally:
        await manager.shutdown()
