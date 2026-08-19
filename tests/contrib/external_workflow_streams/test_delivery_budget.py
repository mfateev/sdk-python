"""The per-activation delivery budget.

A producer that never lets a subscription's buffer run dry makes one
``activate()`` call never return: the iterator re-fills before every record and
always finds one. Activations run on a thread-pool executor under a **2-second
deadlock timeout**, so the Workflow Task fails -- and every retry meets the same
producer, so the Workflow is stuck permanently rather than merely slow. Driving
the real iterator against a runtime whose ``drain`` always returned a full batch
consumed 316,086 records in two seconds and blocked zero times.

Everything here asserts the *mechanism*: that the iterator gives control back
after a fixed number of records, that the count spans a merged set rather than
each member of it, that the records the budget left behind get their readiness
re-reported, and that replay -- whose boundaries are already recorded -- is not
subject to any of it.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import Counter
from datetime import timedelta
from typing import Any

import pytest

import temporalio.converter
import temporalio.workflow
from temporalio.contrib.external_workflow_streams import _runtime as _runtime_module
from temporalio.contrib.external_workflow_streams._annotation import (
    SegmentEndReason,
    decode_annotation,
)
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
from temporalio.worker._workflow_instance import _WorkflowInstanceImpl
from tests.contrib.external_workflow_streams.memory_backend import MemoryStreamBackend

RUN_ID = "run-1"

#: How far past the budget an unbounded fake is allowed to run before the test
#: gives up. Large enough that a missing cap is unmistakable, small enough that
#: the failure arrives quickly.
_UNBOUNDED_BATCH = 1000


# --- fixtures -----------------------------------------------------------------


class FakeInstance:
    """Stands in for the user's Workflow object, which holds the per-Run state."""


@pytest.fixture
def workflow_instance(monkeypatch: pytest.MonkeyPatch) -> FakeInstance:
    instance = FakeInstance()
    monkeypatch.setattr(temporalio.workflow, "instance", lambda: instance)
    return instance


@pytest.fixture
def backend() -> MemoryStreamBackend:
    return MemoryStreamBackend()


def make_runtime(manager: Any, backend: MemoryStreamBackend) -> WorkflowStreamRuntime:
    return WorkflowStreamRuntime(
        manager=manager,
        backends={"tokens": backend},
        run_id=RUN_ID,
        namespace="ns",
        workflow_id="wf",
        first_execution_run_id="first-run",
        data_converter=temporalio.converter.DataConverter.default,
        default_idle_timeout=timedelta(seconds=1),
    )


async def encoded(value: str) -> bytes:
    codec: StreamPayloadCodec[str] = StreamPayloadCodec(
        temporalio.converter.DataConverter.default, str
    )
    return await codec.encode(value)


class NeverEmptyManager:
    """A manager whose buffers never run dry.

    The exact shape that makes an activation never return. It honours
    ``max_records`` because a real manager does; when asked for an unbounded
    drain it hands back a large batch, so a budget that failed to reach the drain
    shows up as an iterator that runs past the cap rather than as a fake that
    quietly stops.
    """

    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.max_records_seen: list[int | None] = []
        #: Every wait the runtime actually asked about. A wait that never
        #: appears was not merely served nothing -- the Worker was never
        #: enquired of on its behalf at all.
        self.drained_waits: list[int] = []
        self.rearmed: list[str] = []
        self._next: dict[int, int] = {}

    def register(self, *, run_id, wait_id, stream_key, backend_name, start_cursor):  # type: ignore[no-untyped-def]
        self._next.setdefault(wait_id, 0)

    def note_wait_generation(self, run_id, wait_id, generation) -> None:  # type: ignore[no-untyped-def]
        pass

    def drain(
        self, run_id: str, wait_id: int, max_records: int | None = None
    ) -> list[StreamRecord]:
        self.max_records_seen.append(max_records)
        self.drained_waits.append(wait_id)
        count = _UNBOUNDED_BATCH if max_records is None else max_records
        start = self._next.get(wait_id, 0)
        self._next[wait_id] = start + count
        return [
            StreamRecord(RecordKind.DATA, self.payload, f"p{wait_id}", i).placed_at(
                Offset(f"{wait_id:02d}-{i:08d}")
            )
            for i in range(start, start + count)
        ]

    def rearm_ready(self, run_id: str) -> None:
        self.rearmed.append(run_id)


async def consume(iterator: Any, seen: list[Any]) -> None:
    async for value in iterator:
        seen.append(value)


async def settle(task: asyncio.Task[None], seen: list[Any], expected: int) -> None:
    """Runs the consumer until it stops making progress, then a little longer.

    The extra window is what turns "it has not finished yet" into "it blocked":
    an iterator that ignored the budget would keep going right through it.
    """
    deadline = time.monotonic() + 5
    while len(seen) < expected and not task.done() and time.monotonic() < deadline:
        await asyncio.sleep(0.005)
    await asyncio.sleep(0.1)


async def cancel(task: asyncio.Task[None]) -> None:
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


# --- one subscription ---------------------------------------------------------


@pytest.mark.asyncio
async def test_a_never_empty_subscription_yields_control_after_the_budget(
    workflow_instance: FakeInstance, backend: MemoryStreamBackend
) -> None:
    """The measured failure, and the cap that ends it.

    Without a bound this loop never returns and the Workflow Task dies on the
    deadlock timeout -- on every retry, because the producer is still there.
    """
    manager = NeverEmptyManager(await encoded("x"))
    runtime = make_runtime(manager, backend)
    _install_runtime(workflow_instance, runtime)
    runtime.begin_activation()
    subscription = external_stream.topic(
        "tokens", backend="tokens", type=str
    ).subscribe()

    seen: list[str] = []
    task = asyncio.ensure_future(consume(subscription.__aiter__(), seen))
    await settle(task, seen, MAX_RECORDS_PER_ACTIVATION)

    assert not task.done(), (
        "the iterator ran past the budget against a buffer that never empties; "
        "this activation would never return and the Workflow Task would fail on "
        "the 2-second deadlock timeout"
    )
    assert len(seen) == MAX_RECORDS_PER_ACTIVATION
    assert runtime.delivery_budget_exhausted()
    assert manager.max_records_seen and all(
        limit is not None for limit in manager.max_records_seen
    ), "every drain must carry the remaining budget"
    assert 0 not in manager.max_records_seen, (
        "a spent budget must stop the drain from being attempted at all, rather "
        "than reaching the manager and asking it for nothing"
    )
    await cancel(task)


@pytest.mark.asyncio
async def test_the_budget_blocks_even_though_records_are_buffered(
    workflow_instance: FakeInstance, backend: MemoryStreamBackend
) -> None:
    """Blocking is the whole point: it is what ends the activation.

    The double-check refill inside ``_await_readiness`` is the hazard here. It
    exists to catch a record buffered while the wait was being registered, and if
    it ignored the budget it would hand this iterator a record immediately and
    the activation would go right back to never returning.
    """
    manager = NeverEmptyManager(await encoded("x"))
    runtime = make_runtime(manager, backend)
    _install_runtime(workflow_instance, runtime)
    runtime.begin_activation()
    subscription = external_stream.topic(
        "tokens", backend="tokens", type=str
    ).subscribe()

    seen: list[str] = []
    task = asyncio.ensure_future(consume(subscription.__aiter__(), seen))
    await settle(task, seen, MAX_RECORDS_PER_ACTIVATION)

    # Blocked on a readiness future, with the manager still holding records.
    assert runtime.resolve_all_pending() == 1, (
        "the iterator must be parked on a readiness future, which is what lets "
        "the activation return"
    )
    await cancel(task)


@pytest.mark.asyncio
async def test_the_next_activation_gets_a_fresh_budget(
    workflow_instance: FakeInstance, backend: MemoryStreamBackend
) -> None:
    """The cap bounds one activation, not the Run.

    A budget that were never reset would deliver 256 records and then never
    another one, which is a different way of hanging the same Workflow.
    """
    manager = NeverEmptyManager(await encoded("x"))
    runtime = make_runtime(manager, backend)
    _install_runtime(workflow_instance, runtime)
    runtime.begin_activation()
    subscription = external_stream.topic(
        "tokens", backend="tokens", type=str
    ).subscribe()

    seen: list[str] = []
    task = asyncio.ensure_future(consume(subscription.__aiter__(), seen))
    await settle(task, seen, MAX_RECORDS_PER_ACTIVATION)
    assert len(seen) == MAX_RECORDS_PER_ACTIVATION

    # The next activation begins: budget reset, readiness resolved.
    runtime.begin_activation()
    assert runtime.delivery_budget_remaining() == MAX_RECORDS_PER_ACTIVATION
    runtime.resolve_all_pending()
    await settle(task, seen, 2 * MAX_RECORDS_PER_ACTIVATION)

    assert len(seen) == 2 * MAX_RECORDS_PER_ACTIVATION
    assert not task.done()
    await cancel(task)


# --- the budget spans subscriptions -------------------------------------------


@pytest.mark.asyncio
async def test_merge_spends_one_budget_across_every_subscription(
    workflow_instance: FakeInstance, backend: MemoryStreamBackend
) -> None:
    """A per-subscription budget would let n streams run n times as long.

    That is the same deadlock with a larger constant in front of it, which is
    why the counter lives on the runtime rather than on a subscription.
    """
    manager = NeverEmptyManager(await encoded("x"))
    runtime = make_runtime(manager, backend)
    _install_runtime(workflow_instance, runtime)
    runtime.begin_activation()
    first = external_stream.topic("a", backend="tokens", type=str).subscribe()
    second = external_stream.topic("b", backend="tokens", type=str).subscribe()

    seen: list[tuple[int, str]] = []

    async def consume_merged() -> None:
        async for subscription, value in merge(first, second):
            seen.append((subscription.wait_id, value))

    task = asyncio.ensure_future(consume_merged())
    await settle(task, seen, MAX_RECORDS_PER_ACTIVATION)  # type: ignore[arg-type]

    assert not task.done()
    assert len(seen) == MAX_RECORDS_PER_ACTIVATION, (
        "the budget covers the whole merged set; two never-empty streams must "
        "not buy twice the records"
    )
    await cancel(task)


@pytest.mark.asyncio
async def test_merge_blocks_on_every_wait_once_the_budget_is_spent(
    workflow_instance: FakeInstance, backend: MemoryStreamBackend
) -> None:
    """``_await_any_readiness`` takes the same last look, under the same cap.

    A refill there that ignored the budget would resume the merge immediately and
    the activation would never end.
    """
    manager = NeverEmptyManager(await encoded("x"))
    runtime = make_runtime(manager, backend)
    _install_runtime(workflow_instance, runtime)
    runtime.begin_activation()
    first = external_stream.topic("a", backend="tokens", type=str).subscribe()
    second = external_stream.topic("b", backend="tokens", type=str).subscribe()

    seen: list[tuple[int, str]] = []

    async def consume_merged() -> None:
        async for subscription, value in merge(first, second):
            seen.append((subscription.wait_id, value))

    task = asyncio.ensure_future(consume_merged())
    await settle(task, seen, MAX_RECORDS_PER_ACTIVATION)  # type: ignore[arg-type]

    assert runtime.resolve_all_pending() == 2, (
        "the complete set must be blocked, not just the stream that happened to "
        "spend the last of the budget"
    )
    await cancel(task)


class OneBusyOneQuietManager:
    """Wait 1 never runs dry; wait 2 holds a single record and then nothing.

    The shape a merge has to survive: one stream a producer keeps saturated, and
    one that has something to say exactly once. A merge that drains the busy
    stream to the bottom of the budget before it looks at the quiet one never
    reaches the quiet one at all, and never will -- every fresh budget starts at
    the same lowest wait id.
    """

    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.rearmed: list[str] = []
        self._next = 0
        self._quiet_pending = True

    def register(self, *, run_id, wait_id, stream_key, backend_name, start_cursor):  # type: ignore[no-untyped-def]
        pass

    def note_wait_generation(self, run_id, wait_id, generation) -> None:  # type: ignore[no-untyped-def]
        pass

    def drain(
        self, run_id: str, wait_id: int, max_records: int | None = None
    ) -> list[StreamRecord]:
        if max_records is not None and max_records <= 0:
            return []
        if wait_id == 2:
            if not self._quiet_pending:
                return []
            self._quiet_pending = False
            return [
                StreamRecord(RecordKind.DATA, self.payload, "p2", 0).placed_at(
                    Offset("02-00000000")
                )
            ]
        count = _UNBOUNDED_BATCH if max_records is None else max_records
        start = self._next
        self._next = start + count
        return [
            StreamRecord(RecordKind.DATA, self.payload, "p1", i).placed_at(
                Offset(f"01-{i:08d}")
            )
            for i in range(start, start + count)
        ]

    def rearm_ready(self, run_id: str) -> None:
        self.rearmed.append(run_id)


@pytest.mark.asyncio
async def test_merge_does_not_starve_a_stream_behind_a_saturated_one(
    workflow_instance: FakeInstance, backend: MemoryStreamBackend
) -> None:
    """A merge that is not fair is not a merge.

    Draining one subscription's whole ready list before looking at the next lets
    the lowest wait id spend the entire activation budget by itself, and the next
    activation begins the same pass in the same order. A record sitting ready on
    a later wait is then never yielded -- deterministically, but that only makes
    it reproducible rather than acceptable.

    Three budget cycles is well past any plausible "it was just slow": one pass
    over the set is enough for a fair merge.
    """
    manager = OneBusyOneQuietManager(await encoded("x"))
    runtime = make_runtime(manager, backend)
    _install_runtime(workflow_instance, runtime)
    runtime.begin_activation()
    busy = external_stream.topic("busy", backend="tokens", type=str).subscribe()
    quiet = external_stream.topic("quiet", backend="tokens", type=str).subscribe()
    assert (busy.wait_id, quiet.wait_id) == (1, 2)

    seen: list[tuple[int, str]] = []

    async def consume_merged() -> None:
        async for subscription, value in merge(busy, quiet):
            seen.append((subscription.wait_id, value))

    task = asyncio.ensure_future(consume_merged())
    for cycle in range(1, 4):
        await settle(task, seen, cycle * MAX_RECORDS_PER_ACTIVATION)  # type: ignore[arg-type]
        # What the Worker's completion path does: a fresh budget, and readiness
        # re-armed for the records the budget left buffered.
        runtime.begin_activation()
        runtime.resolve_all_pending()

    await cancel(task)
    assert quiet.wait_id in {wait_id for wait_id, _ in seen}, (
        "the quiet stream's one ready record was never yielded across three "
        "whole activation budgets; the busy stream spent every one of them"
    )
    assert len(seen) <= 3 * MAX_RECORDS_PER_ACTIVATION + 1, (
        "the budget must still bound the merged set as a whole"
    )


# --- fairness across activations ----------------------------------------------

#: The budget the two fairness tests below run against, in place of the real
#: 256. What they prove is a relationship between the *subscription count* and
#: the budget -- more ready waits than the budget can pay for, or a count that
#: does not divide it -- and the real constant only sets the scale at which that
#: relationship bites. Standing up 257 subscriptions and handing out 768 records
#: to watch it costs more than the property is worth, and destabilised the
#: end-to-end tests that share the run.
_SMALL_BUDGET = 8


@pytest.fixture
def small_budget(monkeypatch: pytest.MonkeyPatch) -> int:
    """Shrinks the delivery budget for one test, and returns what it is now.

    Patched on ``_runtime`` rather than on ``_api``, which is where the constant
    is defined: ``_runtime`` binds the name at import, so it reads its own
    module global, and a patch on the defining module would leave every check
    that actually enforces the budget looking at 256.
    """
    monkeypatch.setattr(_runtime_module, "MAX_RECORDS_PER_ACTIVATION", _SMALL_BUDGET)
    return _SMALL_BUDGET


#: A subscription count that does not divide the budget, at the shrunk scale: 8
#: records over 3 always-ready waits is two whole passes and two waits of a
#: third, so the pass is always cut in the middle -- which is the arrangement a
#: start position fixed at the lowest wait id turns into permanent, growing
#: unfairness. The real-scale case is 100 waits against 256 records: two whole
#: passes and 56 waits of a third, the same cut in the same place.
_SKEWED_STREAMS = 3

#: How many activations the skew is watched over. Whatever the scale, a pass
#: cut in the middle hands the favoured waits exactly one record more per
#: activation than the rest, so the gap is the activation count and nothing
#: bounds it: five is five times the tolerance rather than a coin toss.
_SKEW_CYCLES = 5


async def run_budget_cycles(
    runtime: WorkflowStreamRuntime,
    task: asyncio.Task[None],
    seen: list[Any],
    cycles: int,
    budget: int,
) -> None:
    """Runs `cycles` whole activations, doing what the Worker's completion does.

    A fresh budget and readiness re-armed for the records the budget left
    buffered -- which is what makes the next pass a *new activation* rather
    than a continuation of this one, and so is the only place a start position
    that never moves can show itself.

    `budget` is passed rather than read from the constant because these callers
    run against a patched one; a never-empty set delivers exactly that many
    records per activation, which is how each cycle knows it has finished.
    """
    for cycle in range(1, cycles + 1):
        await settle(task, seen, cycle * budget)
        runtime.begin_activation()
        runtime.resolve_all_pending()


@pytest.mark.asyncio
async def test_merge_reaches_a_wait_the_budget_cannot_pay_for_in_one_pass(
    workflow_instance: FakeInstance,
    backend: MemoryStreamBackend,
    small_budget: int,
) -> None:
    """More ready subscriptions than one activation has records to hand out.

    The budget is charged per record handed to Workflow code, once per
    subscription per pass, so a pass over *budget + 1* ready waits runs out
    exactly one short. A pass that began at the lowest wait id on every
    activation ran out in the same place on every one of them: the last wait was
    never yielded, and -- because ``_fill`` returns on the spent budget before it
    reaches the manager -- never even drained. The Worker was not asked about
    that stream at all, for the life of the Run.

    Run against a budget of ``_SMALL_BUDGET`` rather than the real 256, because
    "one more ready wait than the budget can pay for" is the whole property and
    the constant only says how many that is: at full scale the starved case is
    257 subscriptions, and it starves for exactly the reason nine do here.
    """
    manager = NeverEmptyManager(await encoded("x"))
    runtime = make_runtime(manager, backend)
    _install_runtime(workflow_instance, runtime)
    runtime.begin_activation()
    # The shrunk budget has to have reached the binding the runtime reads, or
    # the counts below stand in a relationship to 256 that they were not chosen
    # for and the test passes without exercising anything.
    assert runtime.delivery_budget_remaining() == small_budget
    subscriptions = [
        external_stream.topic(f"s{i}", backend="tokens", type=str).subscribe()
        for i in range(small_budget + 1)
    ]
    last = subscriptions[-1]

    seen: list[int] = []

    async def consume_merged() -> None:
        async for subscription, _ in merge(*subscriptions):
            seen.append(subscription.wait_id)

    task = asyncio.ensure_future(consume_merged())
    await run_budget_cycles(runtime, task, seen, 3, small_budget)
    await cancel(task)

    assert last.wait_id in set(seen), (
        "the wait one past the budget was never yielded across three whole "
        "activations; every one of them spent itself on the same prefix"
    )
    assert last.wait_id in manager.drained_waits, (
        "the starved wait was never even drained -- the spent budget stopped "
        "`_fill` before it reached the manager, so the Worker was never asked "
        "whether that stream had anything at all"
    )
    assert len(seen) <= 3 * small_budget, (
        "the budget must still bound the merged set as a whole"
    )


@pytest.mark.asyncio
async def test_merge_keeps_the_skew_bounded_when_the_count_splits_the_budget(
    workflow_instance: FakeInstance,
    backend: MemoryStreamBackend,
    small_budget: int,
) -> None:
    """The general defect, of which total starvation is only the extreme.

    Nothing has to be starved outright for the merge to be unfair: a count that
    does not divide the budget is enough. At full scale, 256 records over 100
    always-ready waits leaves the pass cut at the 56th, and a pass that restarted
    at the lowest wait id was cut there on every activation -- the first 56
    taking three records per activation and the other 44 taking two, forever. The
    gap between the best- and worst-served stream then grows by one per
    activation and nothing bounds it: measured at 8 after eight activations, and
    it would be 800 after eight hundred.

    Run here at ``_SMALL_BUDGET`` over ``_SKEWED_STREAMS`` waits, which is the
    same arrangement in miniature -- two whole passes and a cut in the middle of
    a third -- because what makes the skew unbounded is the remainder, not its
    size. The gap still grows by exactly one per activation, so
    ``_SKEW_CYCLES`` activations separate a merge that resumes where the last
    pass stopped from one that does not.

    Taking at most one record per turn bounds the skew *within* a pass. Only
    resuming after the wait that last took one bounds it *across* passes, which
    is what the single-record bound was always claimed to be.
    """
    manager = NeverEmptyManager(await encoded("x"))
    runtime = make_runtime(manager, backend)
    _install_runtime(workflow_instance, runtime)
    runtime.begin_activation()
    # The shrunk budget has to have reached the binding the runtime reads, or
    # the counts below stand in a relationship to 256 that they were not chosen
    # for and the test passes without exercising anything.
    assert runtime.delivery_budget_remaining() == small_budget
    subscriptions = [
        external_stream.topic(f"s{i}", backend="tokens", type=str).subscribe()
        for i in range(_SKEWED_STREAMS)
    ]

    seen: list[int] = []

    async def consume_merged() -> None:
        async for subscription, _ in merge(*subscriptions):
            seen.append(subscription.wait_id)

    task = asyncio.ensure_future(consume_merged())
    await run_budget_cycles(runtime, task, seen, _SKEW_CYCLES, small_budget)
    await cancel(task)

    per_wait = Counter(seen)
    counts = [per_wait[s.wait_id] for s in subscriptions]
    assert max(counts) - min(counts) <= 1, (
        f"after {_SKEW_CYCLES} activations the best-served stream had "
        f"{max(counts)} records and the worst-served {min(counts)}; the skew "
        "grows by one per activation and is bounded by nothing, so a long-"
        "running merge reads its later streams arbitrarily far behind its "
        "earlier ones"
    )
    assert min(counts) > 0
    assert len(seen) <= _SKEW_CYCLES * small_budget, (
        "the budget must still bound the merged set as a whole"
    )


# --- re-arming readiness ------------------------------------------------------


class RecordingNotifier:
    """Stands in for Core's readiness call, recording every notification."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, int, int]] = []
        self.notified = asyncio.Event()

    async def __call__(self, run_id: str, wait_id: int, wait_generation: int) -> str:
        self.calls.append((run_id, wait_id, wait_generation))
        self.notified.set()
        return ReadinessResult.ACCEPTED


@pytest.mark.asyncio
async def test_rearming_re_reports_readiness_for_a_non_empty_buffer() -> None:
    """Without this the Workflow waits forever on records already in front of it.

    Readiness is reported once, when the watcher buffers a record, and that
    watcher has long since moved its prefetch cursor past whatever the budget
    left behind. Nothing else will ever announce those records.
    """
    stream_key = StreamKey("ns", "wf", uuid.uuid4().hex, "tokens")
    backend = MemoryStreamBackend()
    notifier = RecordingNotifier()
    manager = StreamSubscriptionManager(
        backends={"tokens": backend},
        notify_ready=notifier,
        watch_block=timedelta(milliseconds=20),
    )
    try:
        subscription = manager.register(
            run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
        )
        await backend.append(
            stream_key, StreamRecord(RecordKind.DATA, b"a", "session", 0)
        )
        await asyncio.wait_for(notifier.notified.wait(), 2)
        # The activation drained nothing: the budget was already spent elsewhere.
        assert subscription.buffered == 1
        first_round = len(notifier.calls)

        manager.rearm_ready(RUN_ID)
        await asyncio.sleep(0.1)

        assert len(notifier.calls) > first_round, (
            "the records the budget left buffered got no second notification, so "
            "the Workflow would block on data that is already at the Worker"
        )
        assert notifier.calls[-1] == (RUN_ID, 1, 0)
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_rearming_says_nothing_about_an_empty_buffer() -> None:
    """Readiness means *buffered*, and re-arming may not weaken that.

    A notification for an empty buffer produces an activation whose drain finds
    nothing, which is the spurious Workflow Task the "only after buffered" rule
    exists to prevent.
    """
    stream_key = StreamKey("ns", "wf", uuid.uuid4().hex, "tokens")
    backend = MemoryStreamBackend()
    notifier = RecordingNotifier()
    manager = StreamSubscriptionManager(
        backends={"tokens": backend},
        notify_ready=notifier,
        watch_block=timedelta(milliseconds=20),
    )
    try:
        manager.register(
            run_id=RUN_ID, wait_id=1, stream_key=stream_key, backend_name="tokens"
        )
        await asyncio.sleep(0.05)
        assert notifier.calls == []

        manager.rearm_ready(RUN_ID)
        await asyncio.sleep(0.1)

        assert notifier.calls == []
    finally:
        await manager.shutdown()


class _CompletionStub:
    """Just enough of the Workflow instance to drive the completion path.

    The real method is reached unbound, so what is under test is the code that
    ships rather than a copy of it.
    """

    def __init__(self, runtime: Any, *, replaying: bool = False) -> None:
        self._external_stream_runtime = runtime
        self._deleting = False
        self._is_replaying = replaying
        import temporalio.bridge.proto.workflow_completion

        self._current_completion = (
            temporalio.bridge.proto.workflow_completion.WorkflowActivationCompletion()
        )
        self._current_completion.successful.SetInParent()

    def _add_command(self) -> Any:
        return self._current_completion.successful.commands.add()


class _RecordingRuntime:
    """Answers the two questions the completion path asks, and records the call."""

    def __init__(self, *, exhausted: bool) -> None:
        self._exhausted = exhausted
        self.rearms = 0

    def delivery_budget_exhausted(self) -> bool:
        return self._exhausted

    def rearm_readiness(self) -> None:
        self.rearms += 1

    def take_observation_delta(self) -> bytes | None:
        return None

    def quiescent_snapshot(self) -> None:
        return None

    def start_new_annotation(self) -> None:
        pass

    @property
    def annotation_started(self) -> bool:
        # Nothing was ever accumulated, so there is no annotation to terminate.
        return False

    @property
    def request_rollover(self) -> bool:
        return False


@pytest.mark.parametrize("exhausted", [True, False])
def test_the_completion_rearms_exactly_when_the_budget_was_hit(
    exhausted: bool,
) -> None:
    """The window between a budget stop and the next activation must be closed.

    It has to happen on the completion path, unconditionally: the waits the
    budget stopped are marked blocked, so they enter the quiescent snapshot, and
    a snapshot is what lets Core start the idle timer and eventually park. A
    Workflow Task parked with records in the local buffer would wait out an idle
    timeout for data that had already arrived.
    """
    runtime = _RecordingRuntime(exhausted=exhausted)
    stub = _CompletionStub(runtime)

    _WorkflowInstanceImpl._emit_external_stream_commands(stub)  # type: ignore[arg-type]

    assert runtime.rearms == (1 if exhausted else 0)


def test_the_completion_rearms_even_when_it_emits_nothing_else() -> None:
    """The early returns below it must not swallow the re-arm.

    A completion carrying the Workflow's own commands returns before asking for
    retention. If the re-arm sat behind that return, a Workflow that hit the
    budget while also starting a timer would never hear about its records again.
    """
    runtime = _RecordingRuntime(exhausted=True)
    stub = _CompletionStub(runtime)
    stub._add_command()  # something server-bound rides along

    _WorkflowInstanceImpl._emit_external_stream_commands(stub)  # type: ignore[arg-type]

    assert runtime.rearms == 1


def test_a_wait_the_budget_stopped_is_not_immediately_parkable(
    backend: MemoryStreamBackend,
) -> None:
    """Blocked is not quiescent when the budget is what stopped it.

    ``immediately_parkable`` invites Core to park without waiting out the idle
    timer. A subscription the budget stopped still has records at the Worker, so
    parking it would strand them.
    """

    class StubManager:
        def cancel_from_workflow_thread(self, run_id, wait_id):  # type: ignore[no-untyped-def]
            pass

        def register(self, **kwargs: Any) -> None:
            pass

        def note_wait_generation(self, run_id, wait_id, generation) -> None:  # type: ignore[no-untyped-def]
            pass

    runtime = make_runtime(StubManager(), backend)
    runtime.begin_activation()
    runtime.register(
        wait_id=1, stream_key=runtime.stream_key("tokens"), backend_name="tokens"
    )
    record = StreamRecord(RecordKind.WRITE_FENCE, b"", "s", 0).placed_at(Offset("1"))
    runtime.record_delivery(1, record)
    runtime.note_blocked(1, True)

    fenced = runtime.quiescent_snapshot()
    assert fenced is not None and fenced[0].immediately_parkable

    for i in range(MAX_RECORDS_PER_ACTIVATION):
        runtime.record_consumption(1, record)

    stopped = runtime.quiescent_snapshot()
    assert stopped is not None and not stopped[0].immediately_parkable


# --- the double-check refill still works --------------------------------------


class LateArrivalManager:
    """Empty on the first look, holding a record on the second.

    Exactly the window the double-check inside ``_await_readiness`` exists for:
    the record landed after ``_iterate`` looked and before the wait was
    registered, so its readiness was reported to nobody and none is coming.
    """

    def __init__(self, record: StreamRecord) -> None:
        self.record: StreamRecord | None = record
        self.calls = 0

    def register(self, **kwargs: Any) -> None:
        pass

    def note_wait_generation(self, run_id, wait_id, generation) -> None:  # type: ignore[no-untyped-def]
        pass

    def drain(
        self, run_id: str, wait_id: int, max_records: int | None = None
    ) -> list[StreamRecord]:
        self.calls += 1
        if self.calls == 1 or self.record is None:
            return []
        record, self.record = self.record, None
        return [record]


@pytest.mark.asyncio
async def test_the_double_check_refill_still_delivers_a_late_record(
    workflow_instance: FakeInstance, backend: MemoryStreamBackend
) -> None:
    """Budgeting the double-check must not disable it.

    It fixes a separate stall: a record buffered while Workflow code was
    elsewhere has already had its readiness reported and consumed, so blocking
    without looking again strands the Workflow on a record sitting in front of
    it.
    """
    record = StreamRecord(RecordKind.DATA, await encoded("late"), "s", 0).placed_at(
        Offset("1")
    )
    manager = LateArrivalManager(record)
    runtime = make_runtime(manager, backend)
    _install_runtime(workflow_instance, runtime)
    runtime.begin_activation()
    subscription = external_stream.topic(
        "tokens", backend="tokens", type=str
    ).subscribe()

    iterator = subscription.__aiter__()

    assert await asyncio.wait_for(iterator.__anext__(), 1) == "late"
    assert manager.calls >= 2, (
        "the record must have been found by the look taken *after* the wait was "
        "registered, which is the window that double-check closes"
    )


# --- replay is unaffected -----------------------------------------------------


@pytest.mark.asyncio
async def test_a_replay_segment_larger_than_the_budget_is_delivered_in_full(
    workflow_instance: FakeInstance, backend: MemoryStreamBackend
) -> None:
    """The recorded boundaries already say how many records each activation got.

    Re-cutting them at the budget would deliver a different schedule than the
    live run recorded, which is divergence rather than protection: replay reads
    from a finite recorded segment, not from a producer that can outrun it.
    """
    payload = await encoded("r")
    count = MAX_RECORDS_PER_ACTIVATION + 44

    class StubManager:
        def cancel_from_workflow_thread(self, run_id, wait_id):  # type: ignore[no-untyped-def]
            pass

        def register(self, **kwargs: Any) -> None:
            pass

        def note_wait_generation(self, run_id, wait_id, generation) -> None:  # type: ignore[no-untyped-def]
            pass

        def drain(self, run_id: str, wait_id: int, max_records: int | None = None):  # type: ignore[no-untyped-def]
            return []

    runtime = make_runtime(StubManager(), backend)
    _install_runtime(workflow_instance, runtime)
    runtime.begin_activation()
    subscription = external_stream.topic(
        "tokens", backend="tokens", type=str
    ).subscribe()
    runtime.begin_replay_segment(
        [
            (
                subscription.wait_id,
                StreamRecord(RecordKind.DATA, payload, "s", i).placed_at(
                    Offset(f"{i:08d}")
                ),
            )
            for i in range(count)
        ]
    )

    seen: list[str] = []
    task = asyncio.ensure_future(consume(subscription.__aiter__(), seen))
    await settle(task, seen, count)

    assert len(seen) == count, (
        "the recorded segment was cut short by the live delivery budget; replay "
        "would then diverge from the run it is replaying"
    )

    # Back on the live buffer, which is where the budget applies again.
    runtime.end_replay()
    assert runtime.delivery_budget_remaining() == MAX_RECORDS_PER_ACTIVATION, (
        "replayed records must not spend the live budget either -- a later live "
        "delivery in the same activation would start with none left, and the "
        "completion would re-arm readiness for a purely replayed Workflow Task"
    )
    assert not runtime.delivery_budget_exhausted()
    await cancel(task)


@pytest.mark.asyncio
async def test_replay_delivers_in_full_even_when_the_live_budget_is_spent(
    workflow_instance: FakeInstance, backend: MemoryStreamBackend
) -> None:
    """The bypass has to be the replay state, not an empty counter.

    A budget that merely happened to be untouched would work by accident. What
    must hold is that a recorded segment is delivered whole no matter what the
    live counter says, because its boundaries are already in History and cutting
    them somewhere else is divergence.
    """
    payload = await encoded("r")
    count = MAX_RECORDS_PER_ACTIVATION + 44

    class StubManager:
        def cancel_from_workflow_thread(self, run_id, wait_id):  # type: ignore[no-untyped-def]
            pass

        def register(self, **kwargs: Any) -> None:
            pass

        def note_wait_generation(self, run_id, wait_id, generation) -> None:  # type: ignore[no-untyped-def]
            pass

        def drain(self, run_id: str, wait_id: int, max_records: int | None = None):  # type: ignore[no-untyped-def]
            return []

    runtime = make_runtime(StubManager(), backend)
    _install_runtime(workflow_instance, runtime)
    runtime.begin_activation()
    subscription = external_stream.topic(
        "tokens", backend="tokens", type=str
    ).subscribe()

    # Spend the whole live budget first.
    spent = StreamRecord(RecordKind.DATA, payload, "s", 0).placed_at(Offset("live"))
    for _ in range(MAX_RECORDS_PER_ACTIVATION):
        runtime.record_consumption(subscription.wait_id, spent)
    assert runtime.delivery_budget_remaining() == 0

    runtime.begin_replay_segment(
        [
            (
                subscription.wait_id,
                StreamRecord(RecordKind.DATA, payload, "s", i).placed_at(
                    Offset(f"{i:08d}")
                ),
            )
            for i in range(count)
        ]
    )

    seen: list[str] = []
    task = asyncio.ensure_future(consume(subscription.__aiter__(), seen))
    await settle(task, seen, count)

    assert len(seen) == count, (
        "a spent live budget stopped a recorded segment, so replay delivered "
        "fewer records than the marker says the live run received"
    )
    await cancel(task)


# --- the recorded reason ------------------------------------------------------


def _annotation_reasons(runtime: WorkflowStreamRuntime) -> list[SegmentEndReason]:
    delta = runtime.take_observation_delta()
    assert delta is not None
    annotation = decode_annotation(delta + runtime.add_terminal())
    return [segment.end_reason for segment in annotation.segments]


def test_a_budget_stop_records_batch_limit(backend: MemoryStreamBackend) -> None:
    """``NO_DATA_AVAILABLE`` would be a false statement, and a durable one.

    The end reason is the only thing in the annotation that says why an
    activation stopped. Recording "the stream ran dry" for an activation that
    stopped with records still buffered puts a claim in History that was never
    true.
    """

    class StubManager:
        def cancel_from_workflow_thread(self, run_id, wait_id):  # type: ignore[no-untyped-def]
            pass

        def register(self, **kwargs: Any) -> None:
            pass

        def note_wait_generation(self, run_id, wait_id, generation) -> None:  # type: ignore[no-untyped-def]
            pass

    runtime = make_runtime(StubManager(), backend)
    runtime.begin_activation()
    runtime.register(
        wait_id=1, stream_key=runtime.stream_key("tokens"), backend_name="tokens"
    )
    for i in range(MAX_RECORDS_PER_ACTIVATION):
        record = StreamRecord(RecordKind.DATA, b"x", "s", i).placed_at(
            Offset(f"{i:08d}")
        )
        runtime.record_delivery(1, record)
        runtime.record_consumption(1, record)

    assert _annotation_reasons(runtime)[0] is SegmentEndReason.BATCH_LIMIT


def test_an_activation_that_ran_out_of_records_still_records_no_data(
    backend: MemoryStreamBackend,
) -> None:
    """The other direction, without which "always BATCH_LIMIT" would pass."""

    class StubManager:
        def cancel_from_workflow_thread(self, run_id, wait_id):  # type: ignore[no-untyped-def]
            pass

        def register(self, **kwargs: Any) -> None:
            pass

        def note_wait_generation(self, run_id, wait_id, generation) -> None:  # type: ignore[no-untyped-def]
            pass

    runtime = make_runtime(StubManager(), backend)
    runtime.begin_activation()
    runtime.register(
        wait_id=1, stream_key=runtime.stream_key("tokens"), backend_name="tokens"
    )
    for i in range(5):
        record = StreamRecord(RecordKind.DATA, b"x", "s", i).placed_at(
            Offset(f"{i:08d}")
        )
        runtime.record_delivery(1, record)
        runtime.record_consumption(1, record)

    assert _annotation_reasons(runtime)[0] is SegmentEndReason.NO_DATA_AVAILABLE
