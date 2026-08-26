"""P10a/P10b/P11/P19 — the wiring, exercised through a real Worker.

Everything below runs against a live server with a real Workflow Task loop. The
unit tests elsewhere prove each piece in isolation; these prove the pieces are
actually connected to each other, which is the only thing isolation cannot show.

PYTEST_DONT_REWRITE: sandboxed fixture Workflows re-import this module, so pytest's
injected imports would make sandbox validation depend on pytest's import locks.
"""

# pyright: reportMissingParameterType=false
from __future__ import annotations

import asyncio
import threading
import time
import uuid
from collections.abc import Sequence
from datetime import timedelta
from pathlib import Path

import pytest

import temporalio.api.common.v1
import temporalio.api.enums.v1
import temporalio.converter
from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.external_workflow_streams._backend import StreamKey
from temporalio.contrib.external_workflow_streams._errors import (
    METRIC_DECODE,
    METRIC_INTEGRITY,
    METRIC_STORAGE,
)
from temporalio.contrib.external_workflow_streams._record import (
    Offset,
    RecordKind,
    StreamRecord,
)
from temporalio.contrib.external_workflow_streams._wake import (
    WakeRequest,
    send_wake_signal,
    wake_request_id,
)
from temporalio.runtime import MetricBuffer, Runtime, TelemetryConfig
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import WorkflowEnvironment
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
            "tokens", type=str
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
            "tokens", type=str
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
        tokens = external_stream.topic("tokens", type=str)
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
        tokens = external_stream.topic("tokens", type=str)
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
        external_stream_backend=backend,
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
        external_stream_backend=backend,
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
        external_stream_backend=backend,
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


async def test_a_workflow_without_a_configured_backend_says_so(
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
            assert "external_stream_backend" in message, (
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
            "tokens", type=str
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
        external_stream_backend=backend,
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
        external_stream_backend=backend,
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
            "tokens", type=str
        )
        iterator = tokens.subscribe().__aiter__()
        # Long enough that it cannot be what resumes this Workflow. If a record
        # is ever returned, the stream delivered it.
        timer = asyncio.ensure_future(asyncio.sleep(600))
        try:
            return [await iterator.__anext__()]
        finally:
            timer.cancel()


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
        external_stream_backend=backend,
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
            # The Workflow reaching its own end is the success case here, so
            # terminating unconditionally turns a pass into a NOT_FOUND error --
            # which is how the same mistake presented in the rollover tests.
            try:
                await handle.terminate()
            except RPCError as err:
                if err.status is not RPCStatusCode.NOT_FOUND:
                    raise


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


class ReplayFailureTrace:
    """Correlates the manager events that distinguish issues 2 and 7."""

    def __init__(self) -> None:
        self._started = time.monotonic()
        self.events: list[dict[str, object]] = []
        self.managers: list[object] = []

    def record(self, event: str, **details: object) -> None:
        entry: dict[str, object] = {
            "at_seconds": round(time.monotonic() - self._started, 6),
            "event": event,
        }
        entry.update(details)
        self.events.append(entry)

    def record_explicit_wake(self, request: WakeRequest) -> None:
        self.record(
            "explicit_wake",
            request_id=wake_request_id(request),
            workflow_id=request.workflow_id,
            run_id=request.first_execution_run_id,
            stream_name=request.stream_name,
            wait_id=request.wait_id,
            park_generation=request.park_generation,
            sender_identity=request.sender_identity,
            wake_counter=request.wake_counter,
        )

    def record_explicit_wake_acknowledged(self, request: WakeRequest) -> None:
        self.record("explicit_wake_acknowledged", request_id=wake_request_id(request))

    def manager_wake_bursts_after_explicit_ack(self) -> list[int]:
        """Counts wakes between each Core-accepted task completion."""
        acknowledged = next(
            (
                index
                for index, event in enumerate(self.events)
                if event["event"] == "explicit_wake_acknowledged"
            ),
            None,
        )
        if acknowledged is None:
            raise AssertionError("the explicit wake was never acknowledged")
        bursts: list[int] = []
        current: set[object] = set()
        for event in self.events[acknowledged + 1 :]:
            if event["event"] == "manager_wake_started":
                current.add(event["request_id"])
            elif event["event"] == "workflow_task_completion_accepted":
                bursts.append(len(current))
                current = set()
        bursts.append(len(current))
        return bursts


@pytest.fixture
def replay_failure_trace(monkeypatch: pytest.MonkeyPatch) -> ReplayFailureTrace:
    """Captures diagnostics without changing readiness or wake behavior."""
    import temporalio.contrib.external_workflow_streams._wake as wake_module
    from temporalio.contrib.external_workflow_streams._manager import (
        StreamSubscriptionManager,
    )

    trace = ReplayFailureTrace()
    original_init = StreamSubscriptionManager.__init__
    original_notify = StreamSubscriptionManager._notify_ready_with_retries
    original_evict = StreamSubscriptionManager.evict_run
    original_started = StreamSubscriptionManager.note_workflow_task_started
    original_completed = StreamSubscriptionManager.note_workflow_task_completed
    original_send_wake = wake_module.send_wake_signal

    def traced_init(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        original_init(self, *args, **kwargs)
        trace.managers.append(self)
        trace.record("manager_created", sender_identity=self.wake_sender_identity)

    async def traced_notify(self, subscription):  # type: ignore[no-untyped-def]
        generation = subscription.current_wait_generation()
        result = await original_notify(self, subscription)
        trace.record(
            "readiness",
            run_id=subscription.run_id,
            wait_id=subscription.wait_id,
            generation=generation,
            result=result,
            buffered=subscription.buffered,
            wakes_owed=subscription.wakes_owed,
        )
        return result

    async def traced_evict(self, run_id: str) -> None:  # type: ignore[no-untyped-def]
        trace.record("evict_started", run_id=run_id)
        await original_evict(self, run_id)
        trace.record("evict_finished", run_id=run_id)

    def traced_completed(self, run_id: str, *, terminal: bool) -> None:  # type: ignore[no-untyped-def]
        trace.record(
            "workflow_task_completion_accepted",
            run_id=run_id,
            terminal=terminal,
        )
        original_completed(self, run_id, terminal=terminal)

    def traced_started(self, run_id: str) -> None:  # type: ignore[no-untyped-def]
        trace.record("workflow_task_started", run_id=run_id)
        original_started(self, run_id)

    async def traced_send_wake(
        client: Client,
        request: WakeRequest,
        *,
        producer_session_id: str = "",
    ) -> str:
        request_id = wake_request_id(request)
        trace.record(
            "manager_wake_started",
            request_id=request_id,
            workflow_id=request.workflow_id,
            run_id=request.first_execution_run_id,
            stream_name=request.stream_name,
            wait_id=request.wait_id,
            park_generation=request.park_generation,
            sender_identity=request.sender_identity,
            wake_counter=request.wake_counter,
        )
        try:
            result = await original_send_wake(
                client, request, producer_session_id=producer_session_id
            )
        except BaseException as err:
            trace.record(
                "manager_wake_failed",
                request_id=request_id,
                error=repr(err),
            )
            raise
        trace.record("manager_wake_acknowledged", request_id=request_id)
        return result

    monkeypatch.setattr(StreamSubscriptionManager, "__init__", traced_init)
    monkeypatch.setattr(
        StreamSubscriptionManager, "_notify_ready_with_retries", traced_notify
    )
    monkeypatch.setattr(StreamSubscriptionManager, "evict_run", traced_evict)
    monkeypatch.setattr(
        StreamSubscriptionManager,
        "note_workflow_task_started",
        traced_started,
    )
    monkeypatch.setattr(
        StreamSubscriptionManager,
        "note_workflow_task_completed",
        traced_completed,
    )
    monkeypatch.setattr(wake_module, "send_wake_signal", traced_send_wake)
    return trace


async def _replay_failure_diagnosis(
    handle,  # type: ignore[no-untyped-def]
    backend: MemoryStreamBackend,
    key: StreamKey,
    trace: ReplayFailureTrace,
    artifact_dir: Path,
    phase: str,
) -> str:
    """Preserves the evidence a timeout cleanup used to destroy."""
    lines = [f"diagnostic phase: {phase}"]
    try:
        history = await handle.fetch_history()
        artifact = artifact_dir / f"{handle.id}-{phase}-history.json"
        artifact.write_text(history.to_json())
        lines.append(f"complete History: {artifact}")
        workflow_task_timeline = []
        for event in history.events:
            attributes = event.WhichOneof("attributes") or "?"
            if attributes.startswith("workflow_task_"):
                item: dict[str, object] = {
                    "event_id": event.event_id,
                    "attributes": attributes,
                }
                if event.HasField("workflow_task_failed_event_attributes"):
                    failed = event.workflow_task_failed_event_attributes
                    item["cause"] = failed.cause
                    item["message"] = failed.failure.message[:300]
                workflow_task_timeline.append(item)
        lines.append(f"Workflow Task timeline: {workflow_task_timeline}")
    except Exception as err:  # noqa: BLE001 -- diagnostics must not mask timeout
        lines.append(f"History unavailable: {err!r}")
    try:
        description = await handle.describe()
        lines.append(f"workflow status: {description.status}")
    except Exception as err:  # noqa: BLE001
        lines.append(f"describe unavailable: {err!r}")
    try:
        lines.append(
            "stream state: "
            f"records={backend.all_records(key)} "
            f"parked_wait_ids={await backend.parked_wait_ids(key)}"
        )
    except Exception as err:  # noqa: BLE001
        lines.append(f"stream state unavailable: {err!r}")
    for manager in trace.managers:
        subscriptions = getattr(manager, "_runs", {})
        for run_id, waits in subscriptions.items():
            for wait_id, subscription in waits.items():
                lines.append(
                    f"subscription {run_id}/{wait_id}: "
                    f"buffered={subscription.buffered} "
                    f"committed={subscription.committed_cursor} "
                    f"delivery={subscription.delivery_cursor} "
                    f"prefetch={subscription.prefetch_cursor} "
                    f"generation={subscription.current_wait_generation()} "
                    f"wakes_owed={subscription.wakes_owed} "
                    f"cancelled={subscription._cancelled} "
                    f"watcher_done="
                    f"{None if subscription._watcher is None else subscription._watcher.done()}"
                )
    lines.append(f"correlated manager trace: {trace.events}")
    return "\n".join(lines)


async def _await_replayed_result(
    handle,  # type: ignore[no-untyped-def]
    expected: list[str],
    backend: MemoryStreamBackend,
    key: StreamKey,
    trace: ReplayFailureTrace,
    artifact_dir: Path,
) -> None:
    """Waits for the result and retains both timeout and late-observation state."""
    try:
        result = await asyncio.wait_for(handle.result(), 30)
    except asyncio.TimeoutError:
        at_timeout = await _replay_failure_diagnosis(
            handle, backend, key, trace, artifact_dir, "at-timeout"
        )
        try:
            late_result = await asyncio.wait_for(handle.result(), 10)
        except asyncio.TimeoutError:
            after_observation = await _replay_failure_diagnosis(
                handle, backend, key, trace, artifact_dir, "after-observation"
            )
            raise AssertionError(
                "the replayed Run did not complete within 30 seconds and remained "
                "non-terminal for the 10-second observation window\n\n"
                f"{at_timeout}\n\n{after_observation}"
            ) from None
        raise AssertionError(
            "the replayed Run missed the 30-second bound but completed during "
            f"the observation window with {late_result!r}\n\n{at_timeout}"
        ) from None
    assert result == expected


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
        tokens = external_stream.topic("tokens", type=str)
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
    client: Client,
    backend: MemoryStreamBackend,
    replay_failure_trace: ReplayFailureTrace,
    tmp_path: Path,
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
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[ParkedAcrossEvictionWorkflow, FillerWorkflow],
        external_stream_backend=backend,
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
        request = WakeRequest(
            namespace=client.namespace,
            workflow_id=handle.id,
            first_execution_run_id=first_run_id,
            stream_name="tokens",
            wait_id=1,
            park_generation=0,
            sender_identity=f"test-{uuid.uuid4()}",
            wake_counter=1,
        )
        replay_failure_trace.record_explicit_wake(request)
        await send_wake_signal(client, request)
        replay_failure_trace.record_explicit_wake_acknowledged(request)

        try:
            await _await_replayed_result(
                handle,
                ["alpha", "beta"],
                backend,
                key,
                replay_failure_trace,
                tmp_path,
            )
            bursts = replay_failure_trace.manager_wake_bursts_after_explicit_ack()
            assert max(bursts, default=0) <= 1, (
                "readiness produced multiple wakes before Core accepted the "
                f"task completion that one wake caused: {replay_failure_trace.events}"
            )
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
            "tokens", type=str
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
        external_stream_backend=backend,
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
        external_stream_backend=backend,
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
        await asyncio.gather(worker_task, return_exceptions=True)


# --- P19 / ADR-011: decoding is Worker-side work, not Workflow-thread work ---

#: What the probe codec saw each time it decoded a stream record: the thread it
#: ran on, and the module the running event loop's class comes from. Both are
#: recorded because either one alone is arguable -- a Workflow's deterministic
#: loop is identified by its class, and the executor thread by its name.
_decode_sites: list[tuple[str, str]] = []

#: Marks the one payload this probe reacts to, so ordinary activation payloads
#: -- arguments, results, headers -- are passed through untouched and cannot be
#: mistaken for a stream record's decode.
_STREAM_SENTINEL = b"external-stream-decode-probe"

#: Longer than the Worker's 2-second deadlock timeout. A codec is user code and
#: may legitimately take this long: it can be fetching an external payload or
#: talking to a KMS. Awaited on the Worker's loop it costs nothing but latency;
#: awaited on the Workflow thread it is either a deadlocked Workflow Task or a
#: Workflow command synthesized out of a codec's internals.
_SLOW_DECODE = timedelta(seconds=2.5)


class ProbeCodec(temporalio.converter.PayloadCodec):
    """A pass-through codec that reports where the stream record was decoded.

    Only the record carrying :py:data:`_STREAM_SENTINEL` is treated specially,
    so the Worker's own ``decode_activation`` work -- which is *supposed* to run
    on the Worker's loop -- adds no observations of its own.
    """

    async def encode(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> list[temporalio.api.common.v1.Payload]:
        return list(payloads)

    def __init__(self, delay: float | None = None) -> None:
        self.delay = _SLOW_DECODE.total_seconds() if delay is None else delay

    async def decode(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> list[temporalio.api.common.v1.Payload]:
        for payload in payloads:
            if _STREAM_SENTINEL not in payload.data:
                continue
            loop = asyncio.get_running_loop()
            _decode_sites.append(
                (threading.current_thread().name, type(loop).__module__)
            )
            # Real asynchronous work, of the length a codec is allowed to
            # take: an external-payload fetch or a KMS round trip is I/O, and
            # the Worker's loop is where the Worker awaits every other
            # payload's codec.
            await asyncio.sleep(self.delay)
        return list(payloads)


async def test_a_slow_codec_decodes_off_the_workflow_thread(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """A stream record's payload codec must not run inside ``activate()``.

    ``DataConverter.decode`` is three things: an external-payload retrieval, a
    user ``PayloadCodec``, and a payload *converter*. The first two are
    arbitrary asynchronous work -- a network fetch, a KMS round trip -- and the
    Worker awaits them for every ordinary activation payload *before* handing
    the activation to the Workflow executor, precisely so that no Workflow Task
    can be failed by them. Only the third, a synchronous conversion, belongs on
    the Workflow thread.

    A stream record is a payload like any other, so the same split has to hold
    for it: by the time a record reaches ``activate()`` its codec has already
    run on the Worker's loop, and the Workflow thread does nothing but convert
    already-prepared bytes into a value.

    Running the codec on the Workflow thread instead is wrong three ways at
    once, and the assertions below name them: the awaited work happens inside a
    deterministic event loop that cannot perform I/O, it can exceed the
    2-second deadlock timeout and fail the Workflow Task for a perfectly
    healthy codec, and anything the codec awaits is synthesized as Workflow
    commands.
    """
    _decode_sites.clear()
    config = client.config()
    config["data_converter"] = temporalio.converter.DataConverter(
        payload_codec=ProbeCodec()
    )
    codec_client = Client(**config)

    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        codec_client,
        task_queue=task_queue,
        workflows=[CountTokensWorkflow],
        external_stream_backend=backend,
    ):
        handle = await codec_client.start_workflow(
            CountTokensWorkflow.run,
            1,
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

        # Published after the Workflow is already blocked on its subscription,
        # so the record arrives through the live wake path rather than sitting
        # in a buffer the first activation happens to find.
        await asyncio.sleep(1)
        from temporalio.contrib.external_workflow_streams._codec import (
            StreamPayloadCodec,
        )

        producer_codec = StreamPayloadCodec(codec_client.data_converter, str)
        await backend.append(
            key,
            StreamRecord(
                RecordKind.DATA,
                await producer_codec.encode(_STREAM_SENTINEL.decode()),
                "producer",
                0,
            ),
        )

        # A Workflow Task failed by a deadlock is *retried*, and every retry
        # re-decodes the same record and deadlocks again, so a Workflow that
        # never returns is the visible form of that failure.
        assert await asyncio.wait_for(handle.result(), 30) == 1

    assert _decode_sites, (
        "the probe codec never saw the stream record, so this test proved "
        "nothing about where decoding runs"
    )
    for thread_name, loop_module in _decode_sites:
        assert not thread_name.startswith("temporal_workflow_"), (
            f"the stream record's codec ran on the Workflow executor thread "
            f"({thread_name}): arbitrary async work inside activate(), under "
            "the 2-second deadlock timeout"
        )
        assert loop_module != "temporalio.worker._workflow_instance", (
            "the stream record's codec was awaited on the Workflow's "
            "deterministic event loop, which performs no I/O and turns every "
            "await into a Workflow command"
        )

    # The codec's own awaits must not have become Workflow commands. A timer in
    # History here is not a slow Workflow -- it is a Workflow whose History
    # depends on what a codec did internally, and replay reproduces it only for
    # as long as the codec behaves identically.
    events = [e async for e in handle.fetch_history_events()]
    assert not [e for e in events if e.HasField("timer_started_event_attributes")], (
        "the codec's await was turned into a Workflow timer command"
    )


async def test_a_replayed_record_is_prepared_off_the_workflow_thread(
    env: WorkflowEnvironment,
    replay_failure_trace: ReplayFailureTrace,
    tmp_path: Path,
) -> None:
    """Replay delivers down the same drain, so it prepares the same way.

    A replayed record never passes a watcher: its bytes are read and validated
    when the replay job is prepared, before the Workflow thread runs at all. If
    preparation happened only in the watcher, replay would hand raw producer
    bytes to a Workflow whose converter has a codec -- and the choice would be
    between running that codec inside ``activate()`` and yielding whatever the
    undecoded bytes convert to. Preparing both paths in the same place is what
    makes replay indistinguishable from live delivery, which is the property the
    whole buffer design rests on.
    """
    _decode_sites.clear()
    backend = MemoryStreamBackend()
    client = await env.connect_client(
        data_converter=temporalio.converter.DataConverter(payload_codec=ProbeCodec(0))
    )
    task_queue = f"tq-{uuid.uuid4()}"
    handle = None
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[ParkedAcrossEvictionWorkflow, FillerWorkflow],
        external_stream_backend=backend,
        max_cached_workflows=1,
        max_concurrent_workflow_tasks=2,
    ):
        try:
            handle = await client.start_workflow(
                ParkedAcrossEvictionWorkflow.run,
                2,
                id=f"wf-{uuid.uuid4()}",
                task_queue=task_queue,
            )
            description = await handle.describe()
            first_run_id = (
                description.raw_description.workflow_execution_info.first_run_id
            )
            key = StreamKey(client.namespace, handle.id, first_run_id, "tokens")

            await _wait_for_markers(handle, 1, "the Run never parked at all")
            await publish(
                backend,
                key,
                [_STREAM_SENTINEL.decode()],
                session=f"producer-{uuid.uuid4()}",
            )
            await _wait_for_markers(
                handle,
                2,
                "the Run never committed a marker, so nothing would be replayed",
            )

            # Evicts the Run, so the next Workflow Task replays the marker --
            # and re-delivers the recorded record through the replay path.
            await client.execute_workflow(
                FillerWorkflow.run, id=f"filler-{uuid.uuid4()}", task_queue=task_queue
            )
            await publish(backend, key, ["beta"], session=f"producer-{uuid.uuid4()}")
            request = WakeRequest(
                namespace=client.namespace,
                workflow_id=handle.id,
                first_execution_run_id=first_run_id,
                stream_name="tokens",
                wait_id=1,
                park_generation=0,
                sender_identity=f"test-{uuid.uuid4()}",
                wake_counter=1,
            )
            replay_failure_trace.record_explicit_wake(request)
            await send_wake_signal(client, request)
            replay_failure_trace.record_explicit_wake_acknowledged(request)

            await _await_replayed_result(
                handle,
                [_STREAM_SENTINEL.decode(), "beta"],
                backend,
                key,
                replay_failure_trace,
                tmp_path,
            )
            bursts = replay_failure_trace.manager_wake_bursts_after_explicit_ack()
            assert max(bursts, default=0) <= 1, (
                "replayed readiness produced multiple wakes before Core accepted "
                f"the task completion one wake caused: {replay_failure_trace.events}"
            )
        finally:
            if handle is not None:
                try:
                    await handle.terminate()
                except Exception:
                    pass

    assert _decode_sites, "the probe codec never saw the replayed record"
    for thread_name, loop_module in _decode_sites:
        assert not thread_name.startswith("temporal_workflow_"), (
            f"a replayed record's codec ran on the Workflow executor thread "
            f"({thread_name})"
        )
        assert loop_module != "temporalio.worker._workflow_instance", (
            "a replayed record's codec was awaited on the Workflow's "
            "deterministic event loop"
        )


# --- P18: the failure taxonomy, connected end to end -------------------------


class TaxonomyCodec(temporalio.converter.PayloadCodec):
    """A pass-through codec that can be made to reject one known value.

    Stands in for the ordinary way row three happens: a consumer whose
    converter or codec stops matching the producer's, with the stream itself
    untouched. Scoped to the one sentinel value so the Workflow's own arguments
    and result -- which travel through this same codec -- are unaffected.
    """

    def __init__(self) -> None:
        self.fail = False

    async def encode(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> list[temporalio.api.common.v1.Payload]:
        return list(payloads)

    async def decode(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> list[temporalio.api.common.v1.Payload]:
        if self.fail and any(_TAXONOMY_SENTINEL in p.data for p in payloads):
            raise RuntimeError("this codec cannot read the producer's payloads")
        return list(payloads)


class UnreachableBackend(MemoryStreamBackend):
    """A backend whose recorded-range read can be made to fail.

    Only ``read_range`` -- the replay read -- so the live path that wrote the
    marker is unaffected and the failure lands where row one describes it:
    reading back a range the marker already committed.
    """

    def __init__(self) -> None:
        super().__init__()
        self.fail_reads = False

    async def read_range(
        self, key: StreamKey, first: Offset, last: Offset
    ) -> list[StreamRecord]:
        if self.fail_reads:
            raise ConnectionError("the backend is unreachable")
        return await super().read_range(key, first, last)


_TAXONOMY_SENTINEL = b"taxonomy-sentinel"

#: The three counters, so each case can assert that the other two stayed silent.
_TAXONOMY_METRICS = (
    METRIC_STORAGE,
    METRIC_INTEGRITY,
    METRIC_DECODE,
)


def _failure_types(failure) -> list[str]:  # type: ignore[no-untyped-def]
    """Every application failure type in a failure's cause chain."""
    types = []
    while True:
        types.append(failure.application_failure_info.type)
        if not failure.HasField("cause"):
            return types
        failure = failure.cause


async def _await_failed_task(handle, message: str, timeout: float = 30):  # type: ignore[no-untyped-def]
    """The first ``WorkflowTaskFailed`` event, or an assertion naming why not."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        for event in [e async for e in handle.fetch_history_events()]:
            if event.HasField("workflow_task_failed_event_attributes"):
                return event.workflow_task_failed_event_attributes
        await asyncio.sleep(0.3)
    raise AssertionError(message)


@pytest.mark.parametrize(
    ["row", "error_type", "metric"],
    [
        ("storage", "StreamStorageError", METRIC_STORAGE),
        ("integrity", "StreamIntegrityError", METRIC_INTEGRITY),
        ("decode", "StreamDecodeError", METRIC_DECODE),
    ],
)
async def test_each_failure_row_is_reported_as_its_own(
    env: WorkflowEnvironment, row: str, error_type: str, metric: str
) -> None:
    """Rows one to three, each reaching an operator as itself.

    All three are Workflow Task failures, and the server retries a Workflow Task
    failure whatever caused it -- so the retry says nothing about which one
    happened. What distinguishes them is exactly two things, and this asserts
    both: the completion carries the **external-storage failure cause**, which
    separates all three from an ordinary Workflow bug, and **one counter**
    increments while the other two stay silent, which is what lets an alert on
    integrity loss mean integrity loss.

    They are reached the way an operator would meet them, through a marker that
    was written live and then replayed against a backend that has since changed:

    - the recorded range cannot be read at all -- transient, clears itself;
    - the recorded range reads back short -- the record was trimmed or expired,
      and an operator has to repair the backend or terminate the Run;
    - the range reads back exactly as recorded and the consumer's codec cannot
      decode it -- the stream is undamaged and the configuration is wrong.

    The third is the one the taxonomy is most easily collapsed on, because it
    is the only one whose failure surfaces from *inside* ``activate()``: the
    record is prepared on the Worker's loop, its failure travels with it, and
    the delivery that would have yielded its value raises it.
    """
    backend = UnreachableBackend()
    codec = TaxonomyCodec()
    buffer = MetricBuffer(10000)
    runtime = Runtime(telemetry=TelemetryConfig(metrics=buffer))
    client = await env.connect_client(
        runtime=runtime,
        data_converter=temporalio.converter.DataConverter(payload_codec=codec),
    )

    task_queue = f"tq-{uuid.uuid4()}"
    handle = None
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[ParkedAcrossEvictionWorkflow, FillerWorkflow],
        external_stream_backend=backend,
        # One slot, so the filler Workflow evicts the Run under test and the
        # next Workflow Task has to replay the marker.
        max_cached_workflows=1,
        max_concurrent_workflow_tasks=2,
    ):
        try:
            handle = await client.start_workflow(
                ParkedAcrossEvictionWorkflow.run,
                2,
                id=f"wf-{uuid.uuid4()}",
                task_queue=task_queue,
            )
            description = await handle.describe()
            first_run_id = (
                description.raw_description.workflow_execution_info.first_run_id
            )
            key = StreamKey(client.namespace, handle.id, first_run_id, "tokens")

            await _wait_for_markers(handle, 1, "the Run never parked at all")
            # Consumed and committed live, so there is a recorded range to
            # replay -- and the codec is asked for this value twice, once here
            # while it still works and once on replay.
            await publish(
                backend,
                key,
                [_TAXONOMY_SENTINEL.decode()],
                session=f"producer-{uuid.uuid4()}",
            )
            await _wait_for_markers(
                handle,
                2,
                "the Run never committed a marker for the record it consumed, "
                "so there is no recorded range for replay to read",
            )

            # Evicts the Run: buffers, watchers, and wait set all go with it,
            # and the next Workflow Task replays the marker.
            await client.execute_workflow(
                FillerWorkflow.run, id=f"filler-{uuid.uuid4()}", task_queue=task_queue
            )

            if row == "storage":
                backend.fail_reads = True
            elif row == "integrity":
                data = [r for r in backend.all_records(key) if not r.is_control]
                assert data, "nothing was ever appended, so nothing can be lost"
                assert data[0].offset is not None
                await backend.delete_for_test(key, data[0].offset)
            else:
                codec.fail = True

            # The wake is what creates the Workflow Task that replays.
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

            failed = await _await_failed_task(
                handle,
                f"the {row} damage produced no Workflow Task failure at all, so "
                "the Run either never replayed or silently delivered something",
            )
        finally:
            if handle is not None:
                try:
                    await handle.terminate()
                except Exception:
                    pass

    assert error_type in _failure_types(failed.failure), (
        f"the {row} failure reached the server as "
        f"{_failure_types(failed.failure)} rather than as {error_type}, so an "
        "operator cannot tell which of the three rows happened"
    )
    assert (
        failed.cause
        == temporalio.api.enums.v1.WorkflowTaskFailedCause.WORKFLOW_TASK_FAILED_CAUSE_EXTERNAL_STORAGE_FAILURE
    ), (
        "the Workflow Task failed without the external-storage cause, so it is "
        "indistinguishable from a bug in the Workflow's own code"
    )

    counted = {
        update.metric.name
        for update in buffer.retrieve_updates()
        if update.metric.name in _TAXONOMY_METRICS
    }
    assert counted == {metric}, (
        f"the {row} failure incremented {sorted(counted)} rather than only "
        f"{metric}; an alert on one row must not be diluted by another"
    )
