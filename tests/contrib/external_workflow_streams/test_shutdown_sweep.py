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
    SHUTDOWN_WAKE_ATTEMPTS,
    ReadinessResult,
    RunStatus,
    StreamSubscriptionManager,
)
from temporalio.contrib.external_workflow_streams._wake import (
    WakeRequest,
    wake_request_id,
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


def _swept_request_id(harness: Harness, subscription) -> str:  # type: ignore[no-untyped-def]
    """What the Worker's real sender derives for one swept subscription.

    Built from the manager's own sender identity rather than a literal, because
    that identity is exactly what has to be fixed across one sweep's retries and
    distinct between two Workers.
    """
    key = subscription.stream_key
    return wake_request_id(
        WakeRequest(
            namespace=key.namespace,
            workflow_id=key.workflow_id,
            first_execution_run_id=key.first_execution_run_id,
            stream_name=key.stream_name,
            wait_id=subscription.wait_id,
            park_generation=0,
            sender_identity=harness.manager.wake_sender_identity,
            wake_counter=subscription.wakes_owed,
        )
    )


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


@pytest.mark.asyncio
async def test_a_grace_period_expiry_counts_every_wake_it_abandons(
    backend: MemoryStreamBackend,
) -> None:
    """A Worker may give up on a wake; it may not do so quietly.

    The grace period cancels the sweep wherever it happens to be, and
    ``_send_owed_wake`` re-raises ``CancelledError`` by design -- so the failure
    accounting that sits after it was never reached, and neither was any
    subscription later in the serial loop. Shutdown then reported
    ``shutdown_wake_failures == 0`` for a Worker that had just abandoned every
    one of its handoffs, which is precisely the silence this counter exists to
    break: a dropped wake looks exactly like a producer with nothing to say.
    """
    hanging = asyncio.Event()

    class HangingWakeHarness(Harness):
        async def _wake(self, subscription) -> None:  # type: ignore[no-untyped-def]
            hanging.set()
            await asyncio.sleep(60)

    harness = HangingWakeHarness(backend, RunStatus.NO_OPEN_WORKFLOW_TASK)
    for wait_id in (1, 2):
        harness.manager.register(
            run_id=RUN_ID,
            wait_id=wait_id,
            stream_key=StreamKey("ns", "wf", "first-run", f"tokens-{wait_id}"),
            backend_name="tokens",
            start_cursor=BEGINNING,
        )

    started = asyncio.get_running_loop().time()
    await harness.manager.shutdown(grace=timedelta(milliseconds=200))
    elapsed = asyncio.get_running_loop().time() - started

    assert elapsed < 5, f"shutdown waited {elapsed:.1f}s past its grace period"
    assert hanging.is_set(), "the wake never got as far as hanging"
    assert harness.wakes == [], "no wake was acknowledged, so none may be reported"
    assert harness.manager.shutdown_wake_failures == 2, (
        "both handoffs were abandoned -- the one the cancellation landed inside "
        f"and the one never reached -- and {harness.manager.shutdown_wake_failures} "
        "were counted"
    )
    assert sorted(harness.metric) == ["tokens-1", "tokens-2"], (
        f"the metric must fire once per abandoned subscription: {harness.metric}"
    )


@pytest.mark.asyncio
async def test_a_probe_that_cannot_answer_is_not_reported_as_nothing_owed(
    backend: MemoryStreamBackend,
) -> None:
    """ "We could not tell" is not "nothing was owed".

    A Run whose status cannot be read may be holding a buffered record with
    nowhere to announce it. Sending a wake anyway would race a Workflow Task that
    might be open, so the sweep sends nothing -- but passing over the Run without
    counting it tears the Run down and reports a clean shutdown, which is the same
    silent loss by a different route.
    """

    class FailingProbeHarness(Harness):
        async def _probe(self, run_id: str) -> str:
            self.probes.append(run_id)
            raise ConnectionError("service unavailable")

    harness = FailingProbeHarness(backend, RunStatus.NO_OPEN_WORKFLOW_TASK)
    for wait_id in (1, 2):
        harness.manager.register(
            run_id=RUN_ID,
            wait_id=wait_id,
            stream_key=StreamKey("ns", "wf", "first-run", f"tokens-{wait_id}"),
            backend_name="tokens",
            start_cursor=BEGINNING,
        )

    await asyncio.wait_for(harness.manager.shutdown(grace=timedelta(seconds=2)), 5)

    assert harness.wakes == []
    assert harness.manager.shutdown_wake_failures == 2, (
        "a Run this Worker could say nothing about was torn down reporting a "
        "clean shutdown"
    )
    assert sorted(harness.metric) == ["tokens-1", "tokens-2"]
    assert harness.manager._runs == {}, "shutdown must still complete"


@pytest.mark.asyncio
async def test_a_hanging_probe_counts_the_runs_it_never_answered_for(
    backend: MemoryStreamBackend,
) -> None:
    """The grace period expiring inside the probe is the same loss as inside the wake."""

    class HangingProbeHarness(Harness):
        async def _probe(self, run_id: str) -> str:
            self.probes.append(run_id)
            await asyncio.sleep(60)
            return RunStatus.NO_OPEN_WORKFLOW_TASK

    harness = HangingProbeHarness(backend, RunStatus.NO_OPEN_WORKFLOW_TASK)
    harness.manager.register(
        run_id=RUN_ID,
        wait_id=1,
        stream_key=StreamKey("ns", "wf", "first-run", "tokens-1"),
        backend_name="tokens",
        start_cursor=BEGINNING,
    )

    await harness.manager.shutdown(grace=timedelta(milliseconds=200))

    assert harness.wakes == []
    assert harness.manager.shutdown_wake_failures == 1
    assert harness.metric == ["tokens-1"]


@pytest.mark.asyncio
async def test_a_run_with_nothing_owed_is_not_counted_as_a_failure(
    backend: MemoryStreamBackend,
) -> None:
    """The counter has to stay quiet where it should, or it says nothing at all.

    A parked Run is woken by a producer's append through the ordinary path, and a
    Run with an open Workflow Task is Core's to finish. Counting either as an
    abandoned handoff would make the metric fire on every clean shutdown and
    become unalertable.
    """
    for status in (RunStatus.PARKED, RunStatus.WFT_OPEN):
        harness = Harness(backend, status)
        harness.manager.register(
            run_id=RUN_ID,
            wait_id=1,
            stream_key=StreamKey("ns", "wf", "first-run", "tokens"),
            backend_name="tokens",
            start_cursor=BEGINNING,
        )

        await asyncio.wait_for(harness.manager.shutdown(grace=timedelta(seconds=2)), 5)

        assert harness.manager.shutdown_wake_failures == 0, (
            f"a Run reported as {status} owes no wake, so nothing was abandoned"
        )
        assert harness.metric == []


@pytest.mark.asyncio
async def test_a_wake_the_live_path_delivered_is_not_counted_as_abandoned(
    backend: MemoryStreamBackend,
) -> None:
    """The sweep's set is a snapshot, and the live path empties it underneath.

    Readiness that comes back `RunNotFound` owes a wake, sends it, and *then* drops
    the subscription -- and `RunNotFound` is a likely answer during shutdown, while
    watchers keep running for the whole grace window and the sweep awaits inside
    itself for them to interleave with. A subscription that leaves the Run that way
    has had its handoff made, so counting it reports a loss that did not happen on
    the counter operators are told to alert on.
    """
    other_run = "run-2"
    harness = Harness(backend, RunStatus.PARKED)
    for run_id in (RUN_ID, other_run):
        harness.manager.register(
            run_id=run_id,
            wait_id=1,
            stream_key=StreamKey("ns", "wf", run_id, "tokens"),
            backend_name="tokens",
            start_cursor=BEGINNING,
        )

    # While the sweep is busy with the first Run, the live path takes the second
    # Run's subscription away -- which is what it does on `RunNotFound`, and only
    # once its own owed wake has been acknowledged. The sweep then reaches that Run,
    # finds no subscriptions, and moves on without saying anything about it.
    #
    # Both halves of that drop are reproduced, in the order the production path
    # uses them: the flag first, then the pop. Popping alone takes the
    # subscription out of the sweep's reach while leaving its watcher looping on
    # `while not subscription._cancelled`, so the watcher outlives the test's
    # event loop and reads from the backend after it closes -- an unraisable
    # `RuntimeError: Event loop is closed` that this test's own shortcut, not the
    # manager, is responsible for.
    async def drop_the_other(run_id: str) -> str:
        if run_id == RUN_ID:
            dropped = harness.manager._runs.get(other_run, {}).pop(1, None)
            if dropped is not None:
                dropped._cancelled = True
        return RunStatus.PARKED

    harness.manager._run_status = drop_the_other  # type: ignore[assignment]

    await asyncio.wait_for(harness.manager.shutdown(grace=timedelta(seconds=2)), 5)

    assert harness.manager.shutdown_wake_failures == 0, (
        "a subscription whose wake the live path had already delivered was counted "
        "as an abandoned handoff, on the counter operators are told to alert on"
    )
    assert harness.metric == []


@pytest.mark.asyncio
async def test_a_manager_with_no_probe_owes_nothing_and_reports_nothing(
    backend: MemoryStreamBackend,
) -> None:
    """No probe wired means no sweep, which means nothing to have failed at.

    The sweep is defined entirely in terms of what Core answers, so a manager with
    no run-status probe has no handoff obligation at all. Counting its
    subscriptions as abandoned wakes would make the metric fire wherever the
    mechanism simply is not configured -- which is noise in exactly the series an
    operator is expected to alert on.
    """
    manager = StreamSubscriptionManager(
        backends={"tokens": backend},
        notify_ready=lambda *_: _accepted(),
        watch_block=timedelta(milliseconds=10),
    )
    manager.register(
        run_id=RUN_ID,
        wait_id=1,
        stream_key=StreamKey("ns", "wf", "first-run", "tokens"),
        backend_name="tokens",
        start_cursor=BEGINNING,
    )

    await asyncio.wait_for(manager.shutdown(grace=timedelta(seconds=2)), 5)

    assert manager.shutdown_wake_failures == 0
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


# --- the retry within the grace period ----------------------------------------


class FlakyWakeHarness(Harness):
    """Fails a fixed number of attempts, then succeeds."""

    def __init__(self, backend: MemoryStreamBackend, failures: int) -> None:
        super().__init__(backend, RunStatus.NO_OPEN_WORKFLOW_TASK)
        self.remaining_failures = failures
        self.attempts = 0

    async def _wake(self, subscription) -> None:  # type: ignore[no-untyped-def]
        self.attempts += 1
        if self.remaining_failures > 0:
            self.remaining_failures -= 1
            raise ConnectionError("service unavailable")
        self.wakes.append((subscription.run_id, subscription.wait_id))


@pytest.mark.asyncio
async def test_a_momentarily_failing_wake_is_retried_and_succeeds(
    backend: MemoryStreamBackend,
) -> None:
    """A Worker shutting down is often shutting down because something is unhealthy.

    That makes the first attempt the one most likely to land in the middle of
    it, and giving up there would report a clean shutdown that lost a record.
    """
    harness = FlakyWakeHarness(backend, failures=1)
    harness.register()

    await harness.manager.shutdown()

    assert harness.attempts == 2, f"expected one retry, got {harness.attempts} attempts"
    assert harness.wakes == [(RUN_ID, 1)]
    assert harness.metric == [], "a wake that eventually succeeded is not a failure"


@pytest.mark.asyncio
async def test_the_retry_is_bounded_and_then_reported(
    backend: MemoryStreamBackend,
) -> None:
    """Retrying forever would trade a lost record for a Worker that never exits.

    The metric is what makes giving up visible, which is the only reason giving
    up is acceptable.
    """
    harness = FlakyWakeHarness(backend, failures=99)
    harness.register()

    await harness.manager.shutdown()

    assert harness.attempts == SHUTDOWN_WAKE_ATTEMPTS
    assert harness.metric == ["tokens"]
    assert harness.wakes == []


@pytest.mark.asyncio
async def test_the_retry_is_the_same_wake_not_a_second_one(
    backend: MemoryStreamBackend,
) -> None:
    """Derived from the wake's identity, so the server deduplicates it.

    This is what makes retrying safe at all: without a stable request ID the
    retry would ask for a second Workflow Task, and an attempt that had in fact
    arrived would be duplicated rather than resolved.
    """
    harness = FlakyWakeHarness(backend, failures=1)
    subscription = harness.register()
    del subscription

    seen: list[str] = []

    async def recording_wake(sub) -> None:  # type: ignore[no-untyped-def]
        seen.append(_swept_request_id(harness, sub))
        if len(seen) == 1:
            raise ConnectionError("service unavailable")

    harness.manager._send_wake = recording_wake

    await harness.manager.shutdown()

    assert len(seen) == 2
    assert seen[0] == seen[1], (
        "the retry must derive the same request ID; a fresh one would ask for a "
        "second Workflow Task rather than resolve the first attempt"
    )


@pytest.mark.asyncio
async def test_two_workers_sweeps_do_not_deduplicate_each_other(
    backend: MemoryStreamBackend,
) -> None:
    """The opposite half of the same requirement, and the reason it is per instance.

    Two Workers shutting down at different times are two separate asks, and they
    share a ``Client`` -- so a sender identity taken from the client identity
    would give both first unparked wakes the same request ID. The server would
    deduplicate the second, and the Run the surviving Worker picked up would
    never get its Workflow Task.
    """
    harnesses = [Harness(backend, RunStatus.NO_OPEN_WORKFLOW_TASK) for _ in range(2)]
    seen: list[str] = []

    for harness in harnesses:
        harness.register()

        async def recording_wake(sub, harness=harness) -> None:  # type: ignore[no-untyped-def]
            seen.append(_swept_request_id(harness, sub))

        harness.manager._send_wake = recording_wake

    for harness in harnesses:
        await harness.manager.shutdown()

    assert len(seen) == 2
    assert seen[0] != seen[1], (
        "both Workers' sweeps derived the same request ID, so the second "
        "Worker's wake would be deduplicated away and its Run would stall"
    )


# --- the sweep driven through the Worker's real wake callback -----------------


def _worker_wake_callback(manager):  # type: ignore[no-untyped-def]
    """The Worker's own sender, bound to this manager and nothing else.

    The retry and the metric are the manager's, but whether they can ever run is
    the callback's: a callback that returns normally after a failed Signal makes
    an unacknowledged wake indistinguishable from a delivered one, and the loop
    below exits after its first attempt. Driving the real method is the only way
    that shows.
    """
    from temporalio.worker._workflow import _WorkflowWorker

    worker = object.__new__(_WorkflowWorker)
    worker._client = object()  # only ever handed to the patched sender
    worker._external_stream_manager = manager
    return worker._send_external_stream_wake


@pytest.fixture
def failing_signals(monkeypatch):  # type: ignore[no-untyped-def]
    """Makes the raw Signal call fail a fixed number of times, and counts it."""
    import temporalio.contrib.external_workflow_streams._wake as wake_module

    attempts: list[str] = []

    def install(failures: int) -> list[str]:
        remaining = [failures]

        async def fake_send(
            client, wake_request, *, producer_session_id: str = ""
        ) -> str:
            request_id = wake_request_id(wake_request)
            attempts.append(request_id)
            if remaining[0] > 0:
                remaining[0] -= 1
                raise ConnectionError("service unavailable")
            return request_id

        monkeypatch.setattr(wake_module, "send_wake_signal", fake_send)
        return attempts

    return install


@pytest.mark.asyncio
async def test_a_signal_failure_reaches_the_sweeps_retry(
    backend: MemoryStreamBackend, failing_signals
) -> None:
    """The retry is only real if the callback tells the sweep it failed.

    A callback that logs and returns reports success, the three-attempt loop
    ends after one call, and the record the Worker was holding is never
    announced -- while shutdown reports itself clean.
    """
    attempts = failing_signals(failures=2)
    harness = Harness(backend, RunStatus.NO_OPEN_WORKFLOW_TASK)
    harness.register()
    harness.manager._send_wake = _worker_wake_callback(harness.manager)

    await harness.manager.shutdown()

    assert len(attempts) == 3, (
        f"the sweep made {len(attempts)} Signal attempts; a failure the callback "
        "swallows leaves the retry loop with nothing to retry"
    )
    assert len(set(attempts)) == 1, (
        "the retries must be the same wake, not three separate asks"
    )
    assert harness.metric == [], "a wake that eventually succeeded is not a failure"
    assert harness.manager.shutdown_wake_failures == 0


@pytest.mark.asyncio
async def test_a_wake_that_never_lands_is_retried_then_counted(
    backend: MemoryStreamBackend, failing_signals
) -> None:
    """And the metric fires, which is the only thing that makes giving up acceptable."""
    attempts = failing_signals(failures=99)
    harness = Harness(backend, RunStatus.NO_OPEN_WORKFLOW_TASK)
    harness.register()
    harness.manager._send_wake = _worker_wake_callback(harness.manager)

    await harness.manager.shutdown()

    assert len(attempts) == SHUTDOWN_WAKE_ATTEMPTS
    assert harness.metric == ["tokens"], (
        "the wake was never acknowledged and shutdown reported nothing"
    )
    assert harness.manager.shutdown_wake_failures == 1


# --- the park intent's lifetime -----------------------------------------------


@pytest.mark.asyncio
async def test_a_resolved_park_leaves_no_intent_behind(harness_factory) -> None:
    """A park intent exists only while that park is actually outstanding.

    An aborted park was never the only park that ends. A **confirmed** one ends
    too -- when a wake Signal or a fresh quiescent snapshot clears Core's
    ``park_generation`` -- and nothing about that is visible in the backend, so
    only the manager can take the intent back out.

    What a left-behind intent costs is not tidiness. It *is* the answer
    ``current_park_generation`` gives everyone who asks: a producer choosing what
    its wake Signal names gets a generation Core has already discarded and its
    wake is ignored as stale, and the shutdown sweep gets the same answer and
    sends a parked wake whose request ID -- deliberately independent of the
    sender -- is byte-identical to the wake that resolved the park in the first
    place, so the server deduplicates it and no Workflow Task is ever created.
    """
    harness = harness_factory(RunStatus.NO_OPEN_WORKFLOW_TASK)
    key = harness.register()

    confirmed = not await harness.manager.prepare_park(RUN_ID, 1, {1: BEGINNING})
    assert confirmed, "nothing was appended, so this park must have confirmed"
    assert await harness.backend.current_park_generation(key, 1) == 1

    await harness.manager.resolve_park(RUN_ID)

    assert await harness.backend.park_intent(key, 1) is None, (
        "the intent of a park that is over is still installed"
    )
    assert await harness.backend.current_park_generation(key, 1) is None, (
        "a resolved park still reports a generation, so the next wake -- a "
        "producer's or the shutdown sweep's -- will name a park Core no longer "
        "recognises instead of the unparked wake it owes"
    )


@pytest.mark.asyncio
async def test_a_resolve_with_no_park_installed_touches_nothing(
    harness_factory,
) -> None:
    """A resolve is the ordinary delivery path, not a rare one.

    Every record delivered live arrives on a resolve activation, so removing
    intents there unconditionally would put a backend write on the hot path for
    a park that never existed. The manager mirrors what it installed and removes
    only that.
    """
    harness = harness_factory(RunStatus.NO_OPEN_WORKFLOW_TASK)
    key = harness.register()
    removals: list[int] = []

    original = harness.backend.remove_park_intent

    async def recording_remove(removed_key, wait_id: int) -> None:  # type: ignore[no-untyped-def]
        removals.append(wait_id)
        await original(removed_key, wait_id)

    harness.backend.remove_park_intent = recording_remove  # type: ignore[method-assign]

    await harness.manager.resolve_park(RUN_ID)
    assert removals == [], "a Run that never parked owes the backend nothing"

    await harness.manager.prepare_park(RUN_ID, 1, {1: BEGINNING})
    await harness.manager.resolve_park(RUN_ID)
    await harness.manager.resolve_park(RUN_ID)

    assert removals == [1], (
        "the intent must come out exactly once: once for the park that was "
        "installed, and never again for resolves with nothing outstanding"
    )
    assert await harness.backend.park_intent(key, 1) is None


@pytest.mark.asyncio
async def test_a_backend_failure_during_removal_leaves_it_owed(
    harness_factory,
) -> None:
    """Cleanup does not get to fail a Workflow Task, and does not give up either.

    A resolve is a delivery activation; raising here would trade a stale intent
    for a repeated Workflow Task on the healthy path. Swallowing the failure
    *and* forgetting the intent would be the original defect back again, so the
    removal stays owed and the next resolve retries it.
    """
    harness = harness_factory(RunStatus.NO_OPEN_WORKFLOW_TASK)
    key = harness.register()
    await harness.manager.prepare_park(RUN_ID, 1, {1: BEGINNING})

    original = harness.backend.remove_park_intent
    failures = [True]

    async def flaky_remove(removed_key, wait_id: int) -> None:  # type: ignore[no-untyped-def]
        if failures:
            failures.pop()
            raise ConnectionError("backend unavailable")
        await original(removed_key, wait_id)

    harness.backend.remove_park_intent = flaky_remove  # type: ignore[method-assign]

    await harness.manager.resolve_park(RUN_ID)
    assert await harness.backend.current_park_generation(key, 1) == 1

    await harness.manager.resolve_park(RUN_ID)
    assert await harness.backend.current_park_generation(key, 1) is None, (
        "a removal that failed once was never retried, so the intent is stale "
        "for the rest of the Run's life"
    )


# --- when the probe is asked ---------------------------------------------------


@pytest.mark.asyncio
async def test_the_sweep_acts_on_what_the_probe_heard(harness_factory) -> None:
    """The probe and the wakes belong at different moments, so they are separate.

    Core keeps the state the probe reports only until the Worker's shutdown is
    initiated: an idle cached Run has no pending work, so ``shutdown_done`` is
    satisfied by the first input after the shutdown token is cancelled and the
    workflow-state lane ends there. Everything afterwards answers
    ``RunNotFound``.

    That answer owes a wake, which is what makes the degradation invisible: a
    sweep that asks too late still sends its wake and still looks correct, while
    the two answers that mean *do not* send one stop occurring. This Run is
    parked -- a producer's append reaches it through the ordinary path and it
    needs nothing from the sweep -- and the sweep must still know that after
    Core has forgotten.
    """
    harness = harness_factory(RunStatus.PARKED)
    harness.register()

    await harness.manager.probe_runs()
    harness.status = RunStatus.RUN_NOT_FOUND

    await harness.manager.shutdown()

    assert harness.wakes == [], (
        "a parked Run was woken on the way out. Its state was read after Core "
        "had dropped it, so the Parked branch could not be taken"
    )


@pytest.mark.asyncio
async def test_a_run_the_probe_never_reached_is_still_swept(harness_factory) -> None:
    """Probing early must not become a way to miss a Run.

    A Workflow Task already in flight when the probe ran can still cache a Run
    afterwards. Nothing is known about it, so it is asked -- and both answers
    Core has left to give owe a wake.
    """
    harness = harness_factory(RunStatus.NO_OPEN_WORKFLOW_TASK)

    await harness.manager.probe_runs()
    harness.register()

    await harness.manager.shutdown()

    assert harness.wakes == [(RUN_ID, 1)]
    assert harness.probes == [RUN_ID], (
        "a Run the probe phase never saw must still be asked about"
    )
