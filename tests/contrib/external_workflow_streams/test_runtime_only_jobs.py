"""P19 — the jobs answered outside the synchronous Workflow thread.

Two of the four stream activation jobs are *themselves* backend operations, and
a third has to be prepared by one. Routing them through ``activate()`` would put
a multi-second transaction inside a call running under a 2-second deadlock
timeout: it would fail the Workflow Task for a perfectly healthy backend, and get
worse the more records replay had to validate.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import pytest

import temporalio.converter
from temporalio.contrib.external_workflow_streams._backend import StreamKey
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
from tests.contrib.external_workflow_streams.memory_backend import MemoryStreamBackend
from tests.contrib.external_workflow_streams.test_replay import (
    annotation_for,
    append_five,
)

RUN_ID = "run-1"
DEADLOCK_TIMEOUT_SECONDS = 2
"""What `_WorkflowWorker` gives an activation before declaring a deadlock."""


async def _notify(
    run_id: str,  # pyright: ignore[reportUnusedParameter]
    wait_id: int,  # pyright: ignore[reportUnusedParameter]
    generation: int,  # pyright: ignore[reportUnusedParameter]
) -> str:
    return ReadinessResult.ACCEPTED


class HostileBackend(MemoryStreamBackend):
    """Raises on every provider method.

    Registering one of these is how "this path performs no backend I/O" is
    asserted rather than trusted: if any layer reached for the provider, the
    call would explode instead of quietly succeeding.
    """

    def _refuse(self, name: str):  # type: ignore[no-untyped-def]
        raise AssertionError(f"{name} must not be called on this path")

    async def append(self, key, record):  # type: ignore[no-untyped-def]
        self._refuse("append")

    async def read_range(self, key, first, last):  # type: ignore[no-untyped-def]
        self._refuse("read_range")

    async def read_after(self, key, after, *, max_records, block=None):  # type: ignore[no-untyped-def]
        self._refuse("read_after")

    async def install_park_intent(self, key, intent):  # type: ignore[no-untyped-def]
        self._refuse("install_park_intent")

    async def recheck(self, key, wait_id):  # type: ignore[no-untyped-def]
        self._refuse("recheck")


class SlowParkBackend(MemoryStreamBackend):
    """A park handshake that takes longer than the deadlock timeout."""

    async def install_park_intent(self, key, intent):  # type: ignore[no-untyped-def]
        await asyncio.sleep(DEADLOCK_TIMEOUT_SECONDS + 0.5)
        return await super().install_park_intent(key, intent)


class SlowRangeBackend(MemoryStreamBackend):
    """A recorded-range read that takes longer than the deadlock timeout.

    Not a contrived latency: an inclusive read over a whole recorded batch is
    the operation that gets *slower* the more records the marker committed, so
    this is the shape a large, healthy replay actually has.
    """

    async def read_range(self, key, first, last):  # type: ignore[no-untyped-def]
        await asyncio.sleep(DEADLOCK_TIMEOUT_SECONDS + 0.5)
        return await super().read_range(key, first, last)


def make_runtime(backend, manager):  # type: ignore[no-untyped-def]
    return WorkflowStreamRuntime(
        manager=manager,
        backend=backend,
        run_id=RUN_ID,
        namespace="ns",
        workflow_id="wf",
        first_execution_run_id=uuid.uuid4().hex,
        data_converter=temporalio.converter.DataConverter.default,
        default_idle_timeout=timedelta(seconds=1),
    )


def make_manager(backend):  # type: ignore[no-untyped-def]
    return StreamSubscriptionManager(
        backend=backend,
        notify_ready=_notify,
        watch_block=timedelta(milliseconds=10),
    )


# --- finalization performs no backend I/O (ADR-010) --------------------------


@pytest.mark.asyncio
async def test_finalization_touches_no_provider_at_all() -> None:
    """Asserted against a provider that raises, rather than trusted.

    The boundary is not "wherever the stream is now"; it is where this Workflow
    Task's deliveries stopped, which was fixed the moment the last activation
    returned. Refreshing it against the backend would be actively wrong -- it
    could name a position replay must not reproduce -- so the only correct
    number of provider calls here is zero.
    """
    backend = HostileBackend()
    manager = make_manager(backend)
    runtime = make_runtime(backend, manager)
    try:
        # Register without going through the manager's watcher, which would call
        # `read_after` -- what is under test is the finalization path only.
        runtime._subscriptions.clear()
        runtime.register(
            wait_id=1,
            stream_key=runtime.stream_key("tokens"),
        )

        terminal = runtime.add_terminal()

        assert terminal, "finalization must produce a terminal"
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_finalization_reports_where_delivery_stopped_not_where_the_stream_is() -> (
    None
):
    """A record arriving mid-finalization changes nothing about the terminal.

    It belongs to the *next* Workflow Task and reaches Core through the normal
    readiness path -- or, if none is open by then, through the wake Signal.
    """
    backend = MemoryStreamBackend()
    manager = make_manager(backend)
    runtime = make_runtime(backend, manager)
    try:
        key = runtime.stream_key("tokens")
        runtime.register(wait_id=1, stream_key=key)
        runtime.record_delivery(
            1,
            StreamRecord(RecordKind.DATA, b"a", "s", 0).placed_at(Offset("5-0")),
        )

        # Something lands in the stream while finalization is being answered.
        await backend.append(key, StreamRecord(RecordKind.DATA, b"later", "s", 9))

        assert runtime.blocked_snapshot()[1].offset == Offset("5-0"), (
            "the terminal must name where deliveries stopped, not where the "
            "stream has since got to"
        )
    finally:
        await manager.shutdown()


# --- the park handshake outlives the deadlock timeout ------------------------


@pytest.mark.asyncio
async def test_a_park_slower_than_the_deadlock_timeout_is_still_answered() -> None:
    """It is answered off the Workflow thread, so its duration is irrelevant.

    Were this routed through `activate()`, a healthy-but-slow backend would fail
    the Workflow Task -- and it would get worse under exactly the conditions
    parking exists for.
    """
    backend = SlowParkBackend()
    manager = make_manager(backend)
    runtime = make_runtime(backend, manager)
    try:
        key = runtime.stream_key("tokens")
        runtime.register(wait_id=1, stream_key=key)

        started = asyncio.get_running_loop().time()
        became_ready = await asyncio.wait_for(
            manager.prepare_park(RUN_ID, 1, runtime.blocked_snapshot()), 30
        )
        elapsed = asyncio.get_running_loop().time() - started

        assert elapsed > DEADLOCK_TIMEOUT_SECONDS, (
            "this test is only meaningful if the handshake really did outlast "
            f"the deadlock timeout, took {elapsed:.2f}s"
        )
        assert became_ready is False, "nothing was appended, so nothing became ready"
        assert await backend.park_intent(key, 1) is not None
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_a_recheck_that_finds_records_abandons_the_whole_park() -> None:
    """All-or-nothing across the set, and every intent installed comes back out.

    A park confirmed for a set with a ready member would lose that member's
    record until a producer happened to signal it awake.
    """
    backend = MemoryStreamBackend()
    manager = make_manager(backend)
    runtime = make_runtime(backend, manager)
    try:
        first = runtime.stream_key("tokens")
        second = runtime.stream_key("tool-events")
        runtime.register(wait_id=1, stream_key=first)
        runtime.register(wait_id=2, stream_key=second)

        # A record on *one* of the two streams.
        await backend.append(first, StreamRecord(RecordKind.DATA, b"a", "s", 0))

        became_ready = await manager.prepare_park(RUN_ID, 1, runtime.blocked_snapshot())

        assert became_ready is True
        assert await backend.park_intent(first, 1) is None
        assert await backend.park_intent(second, 2) is None, (
            "the other subscription's intent must be removed too -- parking is "
            "all-or-nothing across the complete set"
        )
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_intents_are_installed_before_anything_is_rechecked() -> None:
    """This ordering is what closes the append/park race.

    A producer appends *before* it observes the park generation, so an append is
    either seen by the recheck or paired with a wake Signal. Rechecking before
    every intent was installed would leave a window where it is neither.
    """
    backend = MemoryStreamBackend()
    order: list[str] = []

    class RecordingBackend(MemoryStreamBackend):
        async def install_park_intent(self, key, intent):  # type: ignore[no-untyped-def]
            order.append(f"install:{intent.wait_id}")
            return await super().install_park_intent(key, intent)

        async def recheck(self, key, wait_id):  # type: ignore[no-untyped-def]
            order.append(f"recheck:{wait_id}")
            return await super().recheck(key, wait_id)

    backend = RecordingBackend()
    manager = make_manager(backend)
    runtime = make_runtime(backend, manager)
    try:
        for wait_id, name in ((1, "tokens"), (2, "tool-events")):
            runtime.register(
                wait_id=wait_id,
                stream_key=runtime.stream_key(name),
            )

        await manager.prepare_park(RUN_ID, 1, runtime.blocked_snapshot())

        installs = [i for i, step in enumerate(order) if step.startswith("install")]
        rechecks = [i for i, step in enumerate(order) if step.startswith("recheck")]
        assert installs and rechecks
        assert max(installs) < min(rechecks), (
            f"every intent must be installed before any recheck, got {order}"
        )
    finally:
        await manager.shutdown()


# --- replay's recorded reads outlive the deadlock timeout too ----------------


@pytest.mark.asyncio
async def test_a_replay_read_slower_than_the_deadlock_timeout_is_still_answered() -> (
    None
):
    """The reason the replay job is partitioned rather than answered in `_apply`.

    Routing it through ``activate()`` would put the recorded range reads inside
    a call running under a 2-second deadlock timeout -- failing the Workflow
    Task for a perfectly healthy backend, and getting worse the more records
    replay had to validate. Out here the duration is simply irrelevant.
    """
    backend = SlowRangeBackend()
    manager = make_manager(backend)
    key = StreamKey("ns", "wf", uuid.uuid4().hex, "tokens")
    try:
        placed = await append_five(backend, key)

        started = asyncio.get_running_loop().time()
        plan = await asyncio.wait_for(
            manager.prepare_replay(RUN_ID, annotation_for(key, placed)), 30
        )
        elapsed = asyncio.get_running_loop().time() - started

        assert elapsed > DEADLOCK_TIMEOUT_SECONDS, (
            "this test is only meaningful if the read really did outlast the "
            f"deadlock timeout, took {elapsed:.2f}s"
        )
        assert plan.total_records == len(placed)
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_the_delivering_activation_reads_nothing_itself() -> None:
    """Everything a replay activation needs is in memory before it starts.

    Preparation and delivery are two different moments, and the second one must
    perform no I/O at all -- which is asserted here by taking the prepared plan
    from a provider that refuses every call.
    """
    backend = MemoryStreamBackend()
    manager = make_manager(backend)
    key = StreamKey("ns", "wf", uuid.uuid4().hex, "tokens")
    try:
        placed = await append_five(backend, key)
        await manager.prepare_replay(RUN_ID, annotation_for(key, placed))

        # From here on the provider is hostile. Delivery still has to work.
        manager._backend = HostileBackend()

        plan = manager.take_replay_plan(RUN_ID)

        assert plan is not None
        assert plan.total_records == len(placed)
    finally:
        await manager.shutdown()
