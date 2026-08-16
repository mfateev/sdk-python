"""P8 — the subscription manager, driven directly.

No Workflow API and no Core activations: neither is in this deliverable's
closure, and both would obscure what is actually under test -- that backend I/O
never reaches the thread ``_apply`` runs on, and that readiness means *buffered*.
"""

from __future__ import annotations

import asyncio
import threading
import uuid
from datetime import timedelta

import pytest

from temporalio.contrib.external_workflow_streams._backend import StreamKey
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
