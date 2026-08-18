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


async def test_every_marker_a_run_writes_is_a_complete_annotation(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """Each marker carries its own header and ends with its own terminal.

    A Run that writes two markers is where this can go wrong, and
    :py:class:`TimerThenConsumeWorkflow` writes exactly two: the completion that
    carries its timer ends one Workflow Task, and the completion that returns
    ends the next. Core writes and clears the accumulated annotation on both, so
    an accumulator that carried its header across the first of them would begin
    the second annotation at whatever frame came next -- decoded as a schema
    version, since that is what leads an annotation.

    Decoding every marker independently is the assertion: the annotation is
    opaque to Core, so nothing between here and History would notice one that
    cannot be read back, and the failure would surface only on the replay that
    needed it.
    """
    from temporalio.bridge.proto.external_data import ExternalStreamMarkerData
    from temporalio.contrib.external_workflow_streams._annotation import (
        decode_annotation,
    )

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
        await asyncio.sleep(1)
        await publish(backend, key, ["second"])

        assert await asyncio.wait_for(handle.result(), 60) == ["first", "second"]

        events = [e async for e in handle.fetch_history_events()]
        markers = [
            e
            for e in events
            if e.HasField("marker_recorded_event_attributes")
            and e.marker_recorded_event_attributes.marker_name == "core_external_stream"
        ]
        assert len(markers) >= 2, (
            "this Run should write one marker per Workflow Task it ended, and "
            f"it ended at least two; got {len(markers)}"
        )
        # Decoded first, all of them: a marker that lost its header fails here,
        # and it is the *second* one that loses it, so a per-marker assertion
        # interleaved with the decoding would stop on the first marker's own
        # shortcoming and never reach the one under test.
        annotations = []
        for index, event in enumerate(markers):
            data = ExternalStreamMarkerData()
            data.ParseFromString(
                event.marker_recorded_event_attributes.details["external_stream"]
                .payloads[0]
                .data
            )
            annotations.append(decode_annotation(data.replay_annotation))

        for index, annotation in enumerate(annotations):
            assert annotation.header.streams, (
                f"marker {index} records no stream in its header, so replay of "
                "it has nothing to start from"
            )
            assert annotation.terminal is not None, (
                f"marker {index} has no terminal, so nothing in it says where "
                "the Workflow Task's deliveries stopped (ADR-008)"
            )


@workflow.defn
class TimerThenFirstRecordWorkflow:
    """Starts a timer and blocks on the stream in the **same** activation.

    Deliberately distinct from :py:class:`TimerThenConsumeWorkflow`, which
    consumes a record *before* its timer: that one is quiescent once before the
    server-bound command ever appears, so its wait set is already registered
    with Core and only has to survive. Here the very first block rides a
    completion that carries a timer, so nothing has registered the wait yet --
    which is the only difference between the two, and the whole case.
    """

    @workflow.run
    async def run(self) -> list[str]:
        tokens = external_stream.with_options(idle_timeout=timedelta(seconds=30)).topic(
            "tokens", backend="tokens-memory", type=str
        )
        iterator = tokens.subscribe().__aiter__()
        # Long enough that it cannot be what resumes this Workflow. If a record
        # is ever returned, the stream delivered it.
        timer = asyncio.ensure_future(asyncio.sleep(600))
        try:
            return [await iterator.__anext__()]
        finally:
            timer.cancel()


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Core registers a wait set only from a WorkflowStreamQuiescent it also "
        "retains for: `will_retain` gates `begin_external_stream_quiescence`, and "
        "it is false whenever server-bound commands ride along. Python has no "
        "other way to register -- the quiescent command is the only channel, and "
        "an idle timeout of zero is rejected as malformed rather than meaning "
        "'register these, retain nothing'. Sending the command anyway was tried "
        "and changes nothing. Fixing this needs Core to register the waits "
        "independently of the retention decision, which is not a Python change"
    ),
)
async def test_a_first_block_that_rides_a_server_bound_command_is_still_wakeable(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """The wait set has to be registered even by a completion that is reported.

    A completion carrying a timer must not ask for retention -- the server has
    to be told about the timer -- but the subscriptions it leaves behind are
    still active, and the wake Signal that covers that window can only resume a
    Run whose waits Core knows about. Without the registration every append
    produces a wake Signal, every wake Signal produces a Workflow Task with no
    jobs in it, and the Workflow is unresumable for the life of the timer.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[TimerThenFirstRecordWorkflow],
        external_stream_backends={"tokens-memory": backend},
    ):
        handle = await client.start_workflow(
            TimerThenFirstRecordWorkflow.run,
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

        # Published after the Workflow has settled, so the record arrives with
        # no open Workflow Task and the wake Signal is the only way in.
        await asyncio.sleep(1)
        await publish(backend, key, ["first"])

        try:
            # Short on purpose: the timer is 600 seconds, so nothing but the
            # stream can finish this Workflow and a longer wait would only make
            # a known gap slower to report.
            assert await asyncio.wait_for(handle.result(), 15) == ["first"]
        finally:
            await handle.terminate()


def _stream_markers(events: list) -> list:  # type: ignore[type-arg]
    return [
        e
        for e in events
        if e.HasField("marker_recorded_event_attributes")
        and e.marker_recorded_event_attributes.marker_name == "core_external_stream"
    ]


async def _wait_for_markers(
    handle, count: int, message: str, timeout: float = 30
) -> None:  # type: ignore[no-untyped-def]
    """Polls until History holds ``count`` stream markers.

    Polled rather than slept: a marker is written when the Workflow Task ends,
    and waiting a fixed time instead would make every assertion after it a
    statement about how fast this machine is.
    """
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if (
            len(_stream_markers([e async for e in handle.fetch_history_events()]))
            >= count
        ):
            return
        await asyncio.sleep(0.3)
    raise AssertionError(message)


@workflow.defn
class ParkedAcrossEvictionWorkflow:
    """Consumes records with nothing else to do, so every task ends in a park.

    No timer, no other command: each Workflow Task is retained until the idle
    timeout parks it, which is what puts a marker in History and leaves the Run
    parked rather than merely cached. Both are needed here -- the marker is what
    makes the Run *replay* after eviction, and the park is what makes the wake
    Signal the only thing that can bring a Workflow Task back.
    """

    @workflow.run
    async def run(self, expected: int) -> list[str]:
        tokens = external_stream.topic("tokens", backend="tokens-memory", type=str)
        seen: list[str] = []
        async for token in tokens.subscribe():
            seen.append(token)
            if len(seen) >= expected:
                break
        return seen


@workflow.defn
class FillerWorkflow:
    """Occupies the one cache slot, which is what evicts the Run under test."""

    @workflow.run
    async def run(self) -> str:
        return "done"


async def test_a_replayed_run_re_registers_its_wait_set(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """A Run that comes back through replay has to end replay registered.

    Core's wait set is per-Worker runtime state: it is not in History and it
    does not survive eviction, so a replayed Run rebuilds it or has none. The
    only thing that builds it is ``WorkflowStreamQuiescent``, and a completion
    that reports no progress because it is replaying must still report the
    snapshot -- what is already in History is the *annotation*, not the
    registration.

    The Run is deliberately left **parked** before it is evicted, so nothing
    else can explain the resume: there is no timer to fire, no watcher left
    alive on this Worker, and no open Workflow Task. A wake Signal creates the
    replacement task, and everything after that depends on the replayed Run
    knowing what it is subscribed to. Without it every later readiness is
    answered as though the Run had no subscriptions at all, each wake produces a
    Workflow Task with no activation in it, and the record sits in the stream
    while the Workflow waits.
    """
    from temporalio.contrib.external_workflow_streams._wake import (
        WakeRequest,
        send_wake_signal,
    )

    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[ParkedAcrossEvictionWorkflow, FillerWorkflow],
        external_stream_backends={"tokens-memory": backend},
        # One slot, so running anything else evicts the Run under test. Eviction
        # is what discards the wait set, the watchers, and the buffers, leaving
        # replay to rebuild all three.
        max_cached_workflows=1,
        max_concurrent_workflow_tasks=2,
    ):
        handle = await client.start_workflow(
            ParkedAcrossEvictionWorkflow.run,
            2,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        description = await handle.describe()
        first_run_id = description.raw_description.workflow_execution_info.first_run_id
        key = StreamKey(client.namespace, handle.id, first_run_id, "tokens")

        # Each wait is for a *park*, not for a duration: the record is published
        # only once the Workflow has actually parked, so it is delivered by a
        # wake rather than into a Workflow Task that happened to still be open.
        await _wait_for_markers(handle, 1, "the Run never parked at all")

        # Consumed live, so the Run has a marker to replay from and a cursor
        # that must be honoured -- a replay that re-delivered "alpha" would show
        # up in the result rather than in a timeout.
        await publish(backend, key, ["alpha"])
        await _wait_for_markers(
            handle,
            2,
            "the Run never parked again after consuming its first record, so it "
            "was never in the state this case evicts from",
        )

        # Evicts the Run: its wait set, watchers, and buffers all go with it.
        await client.execute_workflow(
            FillerWorkflow.run, id=f"filler-{uuid.uuid4()}", task_queue=task_queue
        )

        # Appended with nothing on this Worker watching for it, then woken the
        # way a durable producer wakes a parked Run. The Signal is the only
        # thing that creates a Workflow Task here, and it carries no records --
        # so if the replayed Run does not know its own subscription, the
        # Workflow Task it creates has nothing in it.
        await publish(backend, key, ["beta"])
        await send_wake_signal(
            client,
            WakeRequest(
                namespace=client.namespace,
                workflow_id=handle.id,
                first_execution_run_id=first_run_id,
                stream_name="tokens",
                wait_id=1,
                park_generation=0,
                sender_identity=f"test-{uuid.uuid4()}",
                wake_counter=1,
            ),
        )

        try:
            assert await asyncio.wait_for(handle.result(), 30) == ["alpha", "beta"]
        finally:
            try:
                await handle.terminate()
            except Exception:
                pass


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
