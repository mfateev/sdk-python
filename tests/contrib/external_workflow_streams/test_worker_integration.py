"""P10a/P10b/P11/P19 — the wiring, exercised through a real Worker.

Everything below runs against a live server with a real Workflow Task loop. The
unit tests elsewhere prove each piece in isolation; these prove the pieces are
actually connected to each other, which is the only thing isolation cannot show.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import pytest

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.external_workflow_streams._backend import StreamKey
from temporalio.contrib.external_workflow_streams._record import (
    RecordKind,
    StreamRecord,
)
from temporalio.worker import Worker
from tests.contrib.external_workflow_streams.memory_backend import MemoryStreamBackend

with workflow.unsafe.imports_passed_through():
    from temporalio.contrib.external_workflow_streams._api import external_stream


@workflow.defn
class ConsumeTokensWorkflow:
    """Consumes a fixed number of records, then returns them."""

    def __init__(self) -> None:
        self._seen: list[str] = []

    @workflow.run
    async def run(self, expected: int) -> list[str]:
        tokens = external_stream.with_options(idle_timeout=timedelta(seconds=30)).topic(
            "tokens", backend="tokens-memory", type=str
        )

        async for token in tokens.subscribe():
            self._seen.append(token)
            if len(self._seen) >= expected:
                break
        return self._seen

    @workflow.query
    def seen(self) -> list[str]:
        return self._seen


@workflow.defn
class CountTokensWorkflow:
    """Consumes records and returns only how many, never the values.

    Deliberately distinct from :py:class:`ConsumeTokensWorkflow`: a Workflow that
    returns its records puts them in its own result, and History would then
    contain them for a reason that has nothing to do with streams.
    """

    @workflow.run
    async def run(self, expected: int) -> int:
        tokens = external_stream.with_options(idle_timeout=timedelta(seconds=30)).topic(
            "tokens", backend="tokens-memory", type=str
        )

        seen = 0
        async for _ in tokens.subscribe():
            seen += 1
            if seen >= expected:
                break
        return seen


@workflow.defn
class SubscribeOnlyWorkflow:
    """Subscribes and blocks forever, so the task is retained."""

    @workflow.run
    async def run(self) -> None:
        tokens = external_stream.topic("tokens", backend="tokens-memory", type=str)
        async for _ in tokens.subscribe():
            pass


@workflow.defn
class TimerSuppressedSubscriptionWorkflow:
    """Subscribes and waits, with a long timer stopping the task being retained.

    Deliberately distinct from :py:class:`SubscribeOnlyWorkflow`: a Workflow
    blocked on the stream *alone* leaves a retained Workflow Task, which is a
    different shutdown transition. The timer makes every task complete, so the
    Run sits cached with a live subscription and no open Workflow Task -- the
    state that receives no eviction activation at shutdown at all.
    """

    @workflow.run
    async def run(self) -> None:
        tokens = external_stream.topic("tokens", backend="tokens-memory", type=str)
        timer = asyncio.ensure_future(asyncio.sleep(600))
        try:
            async for _ in tokens.subscribe():
                pass
        finally:
            timer.cancel()


@pytest.fixture
def backend() -> MemoryStreamBackend:
    return MemoryStreamBackend()


async def publish(
    backend: MemoryStreamBackend,
    key: StreamKey,
    values: list[str],
    *,
    session: str = "producer",
) -> None:
    """Appends records the way a producer would, encoded for the topic's type.

    The sequence continues across calls within one session, because
    ``(session_id, sequence)`` is the idempotency key: restarting it would
    re-use a key with different content, which the contract rejects outright.
    """
    import temporalio.converter
    from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec

    codec = StreamPayloadCodec(temporalio.converter.DataConverter.default, str)
    start = _sequences.get((id(backend), session), 0)
    for i, value in enumerate(values, start=start):
        await backend.append(
            key,
            StreamRecord(RecordKind.DATA, await codec.encode(value), session, i),
        )
    _sequences[(id(backend), session)] = start + len(values)


#: Per-producer-session sequence, so successive publishes do not collide.
_sequences: dict[tuple[int, str], int] = {}


async def test_a_workflow_consumes_records_it_never_read_itself(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """The vertical slice: subscribe, retain, notify, resolve, drain, deliver.

    Nothing in the Workflow ever touches the backend. The Worker's manager reads
    it on its own loop, buffers, and tells Core; Core turns that into an
    activation; and the Workflow's drain is a buffer pop that cannot block.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[ConsumeTokensWorkflow],
        external_stream_backends={"tokens-memory": backend},
    ):
        handle = await client.start_workflow(
            ConsumeTokensWorkflow.run,
            3,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        description = await handle.describe()
        key = StreamKey(
            client.namespace,
            handle.id,
            description.raw_description.workflow_execution_info.first_run_id,
            "tokens",
        )

        # Let the Workflow reach its subscription before anything is published,
        # so this exercises the wake path rather than a buffer that was already
        # full when the first activation ran.
        await asyncio.sleep(1)
        await publish(backend, key, ["alpha", "beta", "gamma"])

        assert await asyncio.wait_for(handle.result(), 30) == [
            "alpha",
            "beta",
            "gamma",
        ]


async def test_no_stream_payload_reaches_history(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """The entire point of the feature, asserted against real History.

    History cost scales with consumption batches, not with item count -- so a
    record's bytes must appear nowhere in it, and the marker that *is* there
    must be bounded metadata rather than the payloads.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[CountTokensWorkflow],
        external_stream_backends={"tokens-memory": backend},
    ):
        handle = await client.start_workflow(
            CountTokensWorkflow.run,
            2,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        description = await handle.describe()
        key = StreamKey(
            client.namespace,
            handle.id,
            description.raw_description.workflow_execution_info.first_run_id,
            "tokens",
        )
        await asyncio.sleep(1)
        await publish(backend, key, ["secret-payload-one", "secret-payload-two"])
        assert await asyncio.wait_for(handle.result(), 30) == 2

        events = [e async for e in handle.fetch_history_events()]
        raw = b"".join(e.SerializeToString() for e in events)

        assert b"secret-payload-one" not in raw, (
            "a stream payload reached Temporal History, which is the one thing "
            "this feature exists to prevent"
        )
        assert b"secret-payload-two" not in raw

        # And History did not grow an event per record either: cost scales with
        # consumption batches, not with item count.
        markers = [e for e in events if e.HasField("marker_recorded_event_attributes")]
        assert len(markers) <= 2, (
            f"expected marker cost to be per Workflow Task, got {len(markers)} "
            "markers for 2 records"
        )


async def test_a_blocked_workflow_retains_its_task_rather_than_completing_it(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """Retention is what lets the next record arrive on the *same* task.

    Without it every record would cost a Workflow Task round trip, which is the
    cost model the feature exists to avoid.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[SubscribeOnlyWorkflow],
        external_stream_backends={"tokens-memory": backend},
    ):
        handle = await client.start_workflow(
            SubscribeOnlyWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        await asyncio.sleep(2)

        try:
            history = [e async for e in handle.fetch_history_events()]
            completed = [
                e
                for e in history
                if e.HasField("workflow_task_completed_event_attributes")
            ]
            # Exactly one task has completed -- the one that ran up to the
            # subscription. The one holding the subscription open has not, and
            # will not until a record arrives or the idle timeout fires.
            assert len(completed) <= 1, (
                f"expected the subscription's task to be retained, got "
                f"{len(completed)} completed tasks"
            )
        finally:
            await handle.terminate()


async def test_a_workflow_without_registered_backends_says_so(
    client: Client,
) -> None:
    """The failure names the Worker option rather than an attribute error."""
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(client, task_queue=task_queue, workflows=[SubscribeOnlyWorkflow]):
        handle = await client.start_workflow(
            SubscribeOnlyWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        await asyncio.sleep(2)
        try:
            history = [e async for e in handle.fetch_history_events()]
            failures = [
                e
                for e in history
                if e.HasField("workflow_task_failed_event_attributes")
            ]
            assert failures, "expected the Workflow Task to fail"
            message = failures[0].workflow_task_failed_event_attributes.failure.message
            assert "external_stream_backends" in message, (
                f"the failure should name the Worker option, got: {message}"
            )
        finally:
            await handle.terminate()


@workflow.defn
class TimerThenConsumeWorkflow:
    """Consumes one record, sleeps, then consumes another.

    The sleep is the point: a completion carrying a timer is server-bound, so
    retention is suppressed and the Workflow Task ends. The second record
    therefore arrives with **no open Workflow Task**, which is the window only
    the wake Signal covers.
    """

    @workflow.run
    async def run(self) -> list[str]:
        tokens = external_stream.with_options(idle_timeout=timedelta(seconds=30)).topic(
            "tokens", backend="tokens-memory", type=str
        )
        seen: list[str] = []
        subscription = tokens.subscribe()
        iterator = subscription.__aiter__()

        seen.append(await iterator.__anext__())
        # A real timer: the completion that carries it cannot ask for retention.
        await asyncio.sleep(2)
        seen.append(await iterator.__anext__())
        return seen


async def test_an_append_with_no_open_task_wakes_the_workflow(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """The wake Signal path, end to end, with nothing else able to explain it.

    The second record is published while the Workflow is sleeping, so no
    Workflow Task is open to accept local readiness. Nothing else will create
    one -- the sleep's own timer fires on its own schedule and the Workflow
    would then find the record only by luck of timing. The watcher observing
    `NoOpenWorkflowTask` and sending the Signal is what makes the delivery
    reliable rather than incidental.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[TimerThenConsumeWorkflow],
        external_stream_backends={"tokens-memory": backend},
    ):
        handle = await client.start_workflow(
            TimerThenConsumeWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        description = await handle.describe()
        key = StreamKey(
            client.namespace,
            handle.id,
            description.raw_description.workflow_execution_info.first_run_id,
            "tokens",
        )
        await asyncio.sleep(1)
        await publish(backend, key, ["first"])

        # Let the Workflow consume it and settle into its sleep, so the second
        # append lands squarely in the no-open-task window.
        await asyncio.sleep(1)
        await publish(backend, key, ["second"])

        assert await asyncio.wait_for(handle.result(), 60) == ["first", "second"]

        events = [e async for e in handle.fetch_history_events()]
        signals = [
            e
            for e in events
            if e.HasField("workflow_execution_signaled_event_attributes")
            and e.workflow_execution_signaled_event_attributes.signal_name
            == "__temporal_external_stream_wake"
        ]
        assert signals, (
            "the record was delivered without a wake Signal, so this passed by "
            "timing rather than by the mechanism under test"
        )


@workflow.defn
class FloodedCountWorkflow:
    """Consumes far more records than one activation is allowed to deliver."""

    def __init__(self) -> None:
        self._seen = 0

    @workflow.run
    async def run(self, expected: int) -> int:
        tokens = external_stream.with_options(idle_timeout=timedelta(seconds=30)).topic(
            "tokens", backend="tokens-memory", type=str
        )
        async for _ in tokens.subscribe():
            self._seen += 1
            if self._seen >= expected:
                break
        return self._seen

    @workflow.query
    def seen(self) -> int:
        return self._seen


async def test_a_flood_larger_than_one_activations_budget_is_delivered_in_full(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """The whole budget mechanism, end to end, on a real Workflow Task loop.

    Three things have to be connected for this to finish, and each of them hangs
    the Workflow permanently on its own:

    - the per-activation budget, or the first ``activate()`` never returns and
      the Workflow Task dies on the 2-second deadlock timeout -- on every retry,
      because the records are still there;
    - the reset at the start of each activation, or delivery stops for good once
      the first budget is spent;
    - the readiness re-arm, or the records the budget left buffered are never
      announced again, because the watcher moved its prefetch cursor past them
      when it buffered them.

    So a plain "did it finish" assertion is not a weak one here: nothing about
    this Workflow completes if any part of the mechanism is missing.
    """
    from temporalio.contrib.external_workflow_streams._api import (
        MAX_RECORDS_PER_ACTIVATION,
    )

    total = 3 * MAX_RECORDS_PER_ACTIVATION
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[FloodedCountWorkflow],
        external_stream_backends={"tokens-memory": backend},
    ):
        handle = await client.start_workflow(
            FloodedCountWorkflow.run,
            total,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        description = await handle.describe()
        key = StreamKey(
            client.namespace,
            handle.id,
            description.raw_description.workflow_execution_info.first_run_id,
            "tokens",
        )
        await asyncio.sleep(1)
        # One session, one continuous sequence: `(session_id, sequence)` is the
        # idempotency key, so restarting the numbering would re-use a key with
        # different content and the backend would reject the append.
        await publish(backend, key, [f"t{i}" for i in range(total)])

        assert await asyncio.wait_for(handle.result(), 60) == total


async def test_a_clean_shutdown_sweeps_and_tears_down_the_manager(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """P20's sweep has to run on the path users actually take.

    A normal ``Worker.shutdown()`` completes with no worker task raising, so
    nothing is replaced by ``drain_poll_queue()``. Wiring the sweep there alone
    means the whole mechanism is dead on the ordinary path: the manager never
    enters its shutting-down state, every Run stays registered, watchers go on
    polling, and the buffers and backend connections go out with the process --
    while the wake Signal that would hand the Run to another Worker is never
    even considered.

    The Workflow here keeps a long timer running alongside its subscription, so
    every Workflow Task completes and the Run sits *cached, subscribed, and
    holding no Workflow Task* when shutdown begins. That is the state the sweep
    exists for, and the one that gets no eviction activation of its own.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[TimerSuppressedSubscriptionWorkflow],
        external_stream_backends={"tokens-memory": backend},
    )
    worker_task = asyncio.create_task(worker.run())
    handle = None
    try:
        handle = await client.start_workflow(
            TimerSuppressedSubscriptionWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        # Wait for the subscription to actually reach the manager rather than
        # sleeping: with no Run registered there is nothing to sweep and the
        # assertions below would hold for the wrong reason.
        deadline = asyncio.get_running_loop().time() + 30
        while asyncio.get_running_loop().time() < deadline:
            manager = worker._workflow_worker._external_stream_manager  # type: ignore[union-attr]
            if manager is not None and manager.runs_with_subscriptions():
                break
            await asyncio.sleep(0.2)
        manager = worker._workflow_worker._external_stream_manager  # type: ignore[union-attr]
        assert manager is not None and manager.runs_with_subscriptions(), (
            "the Workflow never registered a subscription, so there is nothing "
            "for a shutdown sweep to find"
        )

        await asyncio.wait_for(worker.shutdown(), 60)

        assert manager._shutting_down, (
            "the manager was never told the Worker is shutting down, so no Run "
            "was probed and no owed wake was sent"
        )
        assert manager.runs_with_subscriptions() == [], (
            "subscriptions survived shutdown: their watchers, buffers, and "
            "backend connections leak with the process"
        )
    finally:
        if handle is not None:
            try:
                await handle.terminate()
            except Exception:
                pass
        if not worker_task.done():
            worker_task.cancel()
