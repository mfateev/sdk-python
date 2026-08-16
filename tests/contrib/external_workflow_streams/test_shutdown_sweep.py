"""P20 — the Worker shutdown wake sweep.

Two obligations Core cannot discharge. Teardown ordering, so a finalization in
flight is always answered before the Run's state disappears; and the sweep
itself, because an idle cached Run gets no eviction activation at shutdown at
all -- and that is exactly the Run that most needs one, since its records are
buffered in a process about to exit.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
import pytest_asyncio

from temporalio.contrib.external_workflow_streams._backend import StreamKey
from temporalio.contrib.external_workflow_streams._manager import (
    BEGINNING,
    ReadinessResult,
    RunStatus,
    StreamSubscriptionManager,
)
from temporalio.contrib.external_workflow_streams._record import (
    RecordKind,
    StreamRecord,
)
from tests.contrib.external_workflow_streams.memory_backend import MemoryStreamBackend

RUN_ID = "run-1"


class Harness:
    """A manager wired to recording stand-ins for Core and the Signal path."""

    def __init__(
        self,
        backend: MemoryStreamBackend,
        status: str,
        *,
        wake_fails: bool = False,
        readiness: str = ReadinessResult.ACCEPTED,
    ) -> None:
        self.backend = backend
        self.status = status
        self.wake_fails = wake_fails
        self.readiness = readiness
        self.probes: list[str] = []
        self.wakes: list[tuple[str, int]] = []
        self.metric: list[str] = []
        self.manager = StreamSubscriptionManager(
            backends={"tokens": backend},
            notify_ready=self._notify,
            send_wake=self._wake,
            run_status=self._probe,
            shutdown_wake_failed_metric=self._on_metric,
            watch_block=timedelta(milliseconds=10),
        )

    async def _notify(self, run_id: str, wait_id: int, generation: int) -> str:
        return self.readiness

    async def _probe(self, run_id: str) -> str:
        self.probes.append(run_id)
        return self.status

    async def _wake(self, subscription) -> None:  # type: ignore[no-untyped-def]
        if self.wake_fails:
            raise ConnectionError("service unavailable")
        self.wakes.append((subscription.run_id, subscription.wait_id))

    def _on_metric(self, subscription) -> None:  # type: ignore[no-untyped-def]
        self.metric.append(subscription.stream_key.stream_name)

    def register(self, wait_id: int = 1, stream_name: str = "tokens") -> StreamKey:
        key = StreamKey("ns", "wf", "first-run", stream_name)
        self.manager.register(
            run_id=RUN_ID,
            wait_id=wait_id,
            stream_key=key,
            backend_name="tokens",
            start_cursor=BEGINNING,
        )
        return key


@pytest.fixture
def backend() -> MemoryStreamBackend:
    return MemoryStreamBackend()


@pytest_asyncio.fixture  # type: ignore[reportUntypedFunctionDecorator]
async def harness_factory(backend: MemoryStreamBackend):
    made: list[Harness] = []

    def make(status: str, **kwargs) -> Harness:  # type: ignore[no-untyped-def]
        harness = Harness(backend, status, **kwargs)
        made.append(harness)
        return harness

    yield make
    for harness in made:
        if not harness.manager._shutting_down:
            await harness.manager.shutdown()


# --- the probe ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_sweep_probes_rather_than_asserting_readiness(
    harness_factory,
) -> None:
    """Readiness means "a record is buffered" and would be a lie here.

    Probing with it would manufacture a spurious Workflow Task on the way out of
    a Worker that is shutting down -- for a Run that may have nothing waiting at
    all.
    """
    harness = harness_factory(RunStatus.NO_OPEN_WORKFLOW_TASK)
    harness.register()

    await harness.manager.shutdown()

    assert harness.probes == [RUN_ID], "the sweep must use the read-only probe"


@pytest.mark.asyncio
async def test_a_run_with_no_subscriptions_is_not_probed(harness_factory) -> None:
    """Nothing is owed for a Run holding nothing."""
    harness = harness_factory(RunStatus.NO_OPEN_WORKFLOW_TASK)

    await harness.manager.shutdown()

    assert harness.probes == []


# --- the four states ----------------------------------------------------------


@pytest.mark.asyncio
async def test_shutdown_in_the_no_open_task_window_sends_an_unparked_wake(
    harness_factory,
) -> None:
    """The window the whole sweep exists for.

    The Run is cached with no open Workflow Task, its records are buffered in a
    process about to exit, and nothing else will ever tell the Workflow they
    arrived. Waiting for an unrelated Workflow event is not a plan.
    """
    harness = harness_factory(RunStatus.NO_OPEN_WORKFLOW_TASK)
    harness.register()

    await harness.manager.shutdown()

    assert harness.wakes == [(RUN_ID, 1)]
    assert harness.metric == []


@pytest.mark.asyncio
async def test_shutdown_with_a_workflow_task_open_sends_nothing(
    harness_factory,
) -> None:
    """Core owns that transition; a wake here would race it.

    The result would be a second Workflow Task for a Run already being attended
    to, which is waste at best and a duplicate resolve at worst.
    """
    harness = harness_factory(RunStatus.WFT_OPEN)
    harness.register()

    await harness.manager.shutdown()

    assert harness.wakes == [], "a Run with an open task must be left to Core"


@pytest.mark.asyncio
async def test_shutdown_on_a_parked_run_sends_nothing(harness_factory) -> None:
    """A producer's next append wakes it through the ordinary path."""
    harness = harness_factory(RunStatus.PARKED)
    harness.register()

    await harness.manager.shutdown()

    assert harness.wakes == []


@pytest.mark.asyncio
async def test_a_run_core_no_longer_knows_still_gets_its_wake(
    harness_factory,
) -> None:
    """Evicted and cached-with-no-task differ in what happens next, not in what
    is owed now.

    The Run is gone from *this* Worker, but the Workflow still exists and its
    records are still in the stream; another Worker will pick it up from the
    marker.
    """
    harness = harness_factory(RunStatus.RUN_NOT_FOUND)
    harness.register()

    await harness.manager.shutdown()

    assert harness.wakes == [(RUN_ID, 1)]


@pytest.mark.asyncio
async def test_every_subscription_of_a_swept_run_is_woken(harness_factory) -> None:
    """Each is an independent wait with its own park intent.

    Waking one and not the other would leave the second waiting on records that
    were already in its buffer.
    """
    harness = harness_factory(RunStatus.NO_OPEN_WORKFLOW_TASK)
    harness.register(wait_id=1, stream_name="a")
    harness.register(wait_id=2, stream_name="b")

    await harness.manager.shutdown()

    assert sorted(harness.wakes) == [(RUN_ID, 1), (RUN_ID, 2)]


# --- failure is reported, never dropped ---------------------------------------


@pytest.mark.asyncio
async def test_an_unacknowledged_wake_surfaces_on_the_metric(
    harness_factory,
) -> None:
    """A dropped wake is silent by nature.

    The Workflow simply waits, and nothing distinguishes that from a producer
    having nothing to say -- so it must be counted, not merely logged.
    """
    harness = harness_factory(RunStatus.NO_OPEN_WORKFLOW_TASK, wake_fails=True)
    harness.register()

    await harness.manager.shutdown()

    assert harness.metric == ["tokens"]
    assert harness.manager.shutdown_wake_failures == 1
    assert harness.wakes == [], "a failed wake must never be counted as delivered"


@pytest.mark.asyncio
async def test_a_wake_with_no_sender_configured_is_reported_not_ignored(
    backend: MemoryStreamBackend,
) -> None:
    """Silently skipping it would report a clean shutdown that lost a record."""
    manager = StreamSubscriptionManager(
        backends={"tokens": backend},
        notify_ready=lambda *_: _accepted(),
        run_status=lambda _: _no_open_task(),
        watch_block=timedelta(milliseconds=10),
    )
    manager.register(
        run_id=RUN_ID,
        wait_id=1,
        stream_key=StreamKey("ns", "wf", "first-run", "tokens"),
        backend_name="tokens",
        start_cursor=BEGINNING,
    )

    await manager.shutdown()

    assert manager.shutdown_wake_failures == 1


async def _accepted() -> str:
    return ReadinessResult.ACCEPTED


async def _no_open_task() -> str:
    return RunStatus.NO_OPEN_WORKFLOW_TASK


@pytest.mark.asyncio
async def test_a_probe_failure_does_not_stop_the_shutdown(
    backend: MemoryStreamBackend,
) -> None:
    """Shutdown is never blocked by a server that has stopped answering."""

    async def failing_probe(run_id: str) -> str:
        raise ConnectionError("service unavailable")

    manager = StreamSubscriptionManager(
        backends={"tokens": backend},
        notify_ready=lambda *_: _accepted(),
        run_status=failing_probe,
        watch_block=timedelta(milliseconds=10),
    )
    manager.register(
        run_id=RUN_ID,
        wait_id=1,
        stream_key=StreamKey("ns", "wf", "first-run", "tokens"),
        backend_name="tokens",
        start_cursor=BEGINNING,
    )

    await asyncio.wait_for(manager.shutdown(), 5)

    assert manager._runs == {}


@pytest.mark.asyncio
async def test_shutdown_is_never_blocked_past_the_grace_period(
    backend: MemoryStreamBackend,
) -> None:
    """A wake that has not been acknowledged by now is better reported than
    waited on indefinitely.

    Without the bound, a Worker that could not reach the server would hang on
    the way out -- turning a recoverable delivery problem into an unrecoverable
    process one.
    """

    async def hanging_probe(run_id: str) -> str:
        await asyncio.sleep(60)
        return RunStatus.NO_OPEN_WORKFLOW_TASK

    manager = StreamSubscriptionManager(
        backends={"tokens": backend},
        notify_ready=lambda *_: _accepted(),
        run_status=hanging_probe,
        watch_block=timedelta(milliseconds=10),
    )
    manager.register(
        run_id=RUN_ID,
        wait_id=1,
        stream_key=StreamKey("ns", "wf", "first-run", "tokens"),
        backend_name="tokens",
        start_cursor=BEGINNING,
    )

    started = asyncio.get_running_loop().time()
    await manager.shutdown(grace=timedelta(milliseconds=200))
    elapsed = asyncio.get_running_loop().time() - started

    assert elapsed < 5, f"shutdown waited {elapsed:.1f}s past its grace period"
    assert manager._runs == {}


# --- teardown ordering --------------------------------------------------------


@pytest.mark.asyncio
async def test_the_sweep_runs_before_any_subscription_is_torn_down(
    harness_factory,
) -> None:
    """A subscription torn down first has nothing left to name in its wake.

    Teardown resets cursors to the committed boundary and cancels the watcher;
    sweeping afterwards would send a wake for a wait whose buffered records had
    already been discarded.
    """
    harness = harness_factory(RunStatus.NO_OPEN_WORKFLOW_TASK)
    key = harness.register()
    await harness.backend.append(key, StreamRecord(RecordKind.DATA, b"x", "p", 0))
    await asyncio.sleep(0.1)

    await harness.manager.shutdown()

    assert harness.wakes == [(RUN_ID, 1)]
    assert harness.manager._runs == {}, "teardown still happens, just afterwards"


@pytest.mark.asyncio
async def test_teardown_removes_every_run(harness_factory) -> None:
    """The backstop half of the obligation: no Run outlives the manager.

    A Run whose watchers survived shutdown would hold a backend connection open
    for the life of the process.
    """
    harness = harness_factory(RunStatus.PARKED)
    harness.register(wait_id=1, stream_name="a")
    harness.register(wait_id=2, stream_name="b")

    await harness.manager.shutdown()

    assert harness.manager._runs == {}


@pytest.mark.asyncio
async def test_eviction_remains_the_normal_teardown_path(harness_factory) -> None:
    """The sweep is a backstop, not a replacement.

    Per-Run teardown is driven by ``RemoveFromCache``, which is what guarantees a
    finalization in flight is answered before the Run's state disappears. A
    shutdown hook that tore Runs down itself would have no such ordering.
    """
    harness = harness_factory(RunStatus.NO_OPEN_WORKFLOW_TASK)
    harness.register()

    await harness.manager.evict_run(RUN_ID)
    await harness.manager.shutdown()

    assert harness.probes == [], (
        "a Run already evicted has nothing left to sweep, and probing it would "
        "ask Core about a Run this Worker has finished with"
    )
    assert harness.wakes == []
