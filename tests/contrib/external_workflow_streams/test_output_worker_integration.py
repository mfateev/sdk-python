"""Real Worker/Core integration for Workflow-originated external output.

PYTEST_DONT_REWRITE: the sandboxed Workflow re-imports this module, so pytest's
injected imports must not become part of sandbox validation.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Sequence
from dataclasses import replace
from datetime import timedelta
from typing import Any, cast

import pytest

import temporalio.api.enums.v1
import temporalio.bridge.worker
from temporalio import workflow
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.contrib.external_workflow_streams import (
    ExternalOutputStreamClient,
    WorkflowChainKey,
)
from temporalio.contrib.external_workflow_streams._backend import (
    ParkIntent,
    ParkIntentRemoval,
    StreamKey,
)
from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec
from temporalio.contrib.external_workflow_streams._output_backend import (
    OutputReadResult,
    OutputStage,
    OutputStageConflictError,
    OutputStageManifest,
    OutputStageNotFoundError,
    OutputStageResolutionError,
    OutputStageStatus,
    OutputStreamBackend,
    OutputStreamRecord,
    PendingOutputBarrier,
    StagedOutputRecord,
)
from temporalio.contrib.external_workflow_streams._record import (
    AFTER,
    BEGINNING,
    Cursor,
    Offset,
    RecordKind,
    StreamRecord,
)
from temporalio.worker import Worker
from tests.contrib.external_workflow_streams.memory_backend import (
    MemoryStreamBackend,
)

with workflow.unsafe.imports_passed_through():
    from temporalio.contrib.external_workflow_streams import (
        external_output_stream,
        external_stream,
    )


@workflow.defn
class PublishExternalOutputWorkflow:
    """Publishes a value without returning it through Temporal."""

    @workflow.run
    async def run(self) -> int:
        topic = external_output_stream.topic("events", type=str)
        await topic.publish("workflow-output-secret")
        await topic.finish()
        return 1


@workflow.defn
class FinishOutputAcrossContinueAsNewWorkflow:
    """Finishes in one Run and verifies the successor restores that fact."""

    @workflow.run
    async def run(self) -> str:
        topic = external_output_stream.topic("events", type=str)
        if workflow.info().continued_run_id is None:
            await topic.finish()
            workflow.continue_as_new()
        try:
            await topic.publish("must not be staged")
        except workflow.NondeterminismError:
            return "successor rejected finished topic"
        return "successor incorrectly reopened finished topic"


@workflow.defn
class PublishOutputWhileInputIsRetainedWorkflow:
    """Publishes output, then keeps the Workflow Task open on an input wait."""

    @workflow.run
    async def run(self) -> int:
        output = external_output_stream.with_options(
            max_publish_latency=timedelta(seconds=1)
        ).topic("events", type=str)
        input_topic = external_stream.with_options(
            idle_timeout=timedelta(seconds=30)
        ).topic("release", type=str)
        await output.publish("visible-before-input")
        await input_topic.subscribe().__aiter__().__anext__()
        return 1


@workflow.defn
class PublishAcrossRolloverAndContinueAsNewWorkflow:
    """Crosses output capacity rollover, then continues as new."""

    @workflow.run
    async def run(self) -> str:
        topic = external_output_stream.with_options(max_records=1).topic(
            "events", type=str
        )
        if workflow.info().continued_run_id is None:
            await topic.publish("before-rollover")
            await topic.publish("after-rollover")
            workflow.continue_as_new()
        await topic.publish("after-continue-as-new")
        return "done"


@workflow.defn
class ConcurrentTurnOutputWorkflow:
    """Returns stable turn IDs that clients correlate with output envelopes."""

    def __init__(self) -> None:
        self._done = False

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self._done)
        return "done"

    @workflow.update
    async def start_turn(self, turn_id: str) -> str:
        await external_output_stream.topic("events", type=str).publish(
            f"turn_started:{turn_id}"
        )
        return turn_id

    @workflow.update
    async def finish(self) -> None:
        await external_output_stream.topic("events", type=str).finish()
        self._done = True


@workflow.defn
class PublishWhileTwoInputsParkWorkflow:
    """Keeps two input intents outstanding while output latency wins."""

    @workflow.run
    async def run(self) -> None:
        output = external_output_stream.with_options(
            max_publish_latency=timedelta(milliseconds=500)
        ).topic("events", type=str)
        inputs = external_stream.with_options(idle_timeout=timedelta(milliseconds=50))
        first = inputs.topic("first", type=str).subscribe().__aiter__()
        second = inputs.topic("second", type=str).subscribe().__aiter__()
        await output.publish("latency-wins")
        await asyncio.gather(first.__anext__(), second.__anext__())


@workflow.defn
class ThreeRetainedOutputWindowsWorkflow:
    """Publishes twice per retained latency window for three windows."""

    @workflow.run
    async def run(self) -> int:
        output = external_output_stream.with_options(
            max_publish_latency=timedelta(milliseconds=150)
        ).topic("events", type=str)
        triggers = external_stream.with_options(
            idle_timeout=timedelta(seconds=30)
        ).topic("triggers", type=str)
        windows = 0
        async for trigger in triggers.subscribe():
            if trigger == "stop":
                return windows
            await output.publish(f"{trigger}:first")
            await output.publish(f"{trigger}:second")
            windows += 1
        return windows


class _OutputMemoryBackend(MemoryStreamBackend, OutputStreamBackend):
    """In-memory output provider used to exercise the real Worker/Core path."""

    provider_id = "output-memory"
    provider_format_version = 1

    def __init__(self) -> None:
        super().__init__()
        self.stages: dict[tuple[str, int], OutputStage] = {}
        self._stage_by_offset: dict[tuple[StreamKey, Offset], tuple[str, int]] = {}
        self.read_operations: list[str] = []
        self.commit_attempts = 0
        self.commit_failures_remaining = 0
        self.commit_failed = asyncio.Event()
        self.stage_placed = asyncio.Event()
        self.stage_release: asyncio.Event | None = None
        self.pending_read = asyncio.Event()
        self.expected_park_intents = 0
        self.all_park_intents_installed = asyncio.Event()
        self.recheck_release: asyncio.Event | None = None
        self.removed_park_intents: list[int] = []

    async def stage_output(
        self,
        manifest: OutputStageManifest,
        records: Sequence[StagedOutputRecord],
    ) -> OutputStage:
        identity = (manifest.stage_token, manifest.sub_batch_id)
        existing = self.stages.get(identity)
        if existing is not None:
            if existing.manifest != manifest:
                raise OutputStageConflictError(manifest)
            return existing
        if len(records) != manifest.record_count or any(
            record.publish_index != index for index, record in enumerate(records)
        ):
            raise ValueError("staged output indexes do not match the manifest")

        placed: list[OutputStreamRecord] = []
        for record in records:
            stored = await self.append(
                manifest.stream_key,
                StreamRecord(
                    kind=record.kind,
                    payload=record.payload,
                    producer_session_id=(
                        f"stage:{manifest.stage_token}:{manifest.sub_batch_id}"
                    ),
                    sequence=record.publish_index,
                ),
            )
            assert stored.offset is not None
            placed.append(
                OutputStreamRecord(stored.kind, stored.payload, stored.offset)
            )
            self._stage_by_offset[(manifest.stream_key, stored.offset)] = identity
        stage = OutputStage(manifest, tuple(placed), OutputStageStatus.PENDING)
        self.stages[identity] = stage
        self.stage_placed.set()
        if self.stage_release is not None:
            await self.stage_release.wait()
        return stage

    async def commit_output(self, manifest: OutputStageManifest) -> OutputStage:
        self.commit_attempts += 1
        if self.commit_failures_remaining:
            self.commit_failures_remaining -= 1
            self.commit_failed.set()
            raise OSError("injected post-report output commit failure")
        return self._resolve(manifest, OutputStageStatus.COMMITTED)

    async def abort_output(self, manifest: OutputStageManifest) -> OutputStage:
        return self._resolve(manifest, OutputStageStatus.ABORTED)

    def _resolve(
        self, manifest: OutputStageManifest, status: OutputStageStatus
    ) -> OutputStage:
        identity = (manifest.stage_token, manifest.sub_batch_id)
        existing = self.stages.get(identity)
        if existing is None:
            raise OutputStageNotFoundError(manifest)
        if existing.manifest != manifest:
            raise OutputStageConflictError(manifest)
        if existing.status is not OutputStageStatus.PENDING:
            if existing.status is status:
                return existing
            raise OutputStageResolutionError(
                manifest,
                current=existing.status,
                requested=status,
            )
        resolved = replace(existing, status=status)
        self.stages[identity] = resolved
        return resolved

    async def output_stage(self, manifest: OutputStageManifest) -> OutputStage | None:
        self.read_operations.append("output_stage")
        existing = self.stages.get((manifest.stage_token, manifest.sub_batch_id))
        if existing is not None and existing.manifest != manifest:
            raise OutputStageConflictError(manifest)
        return existing

    async def append_output(
        self, key: StreamKey, record: StreamRecord
    ) -> OutputStreamRecord:
        placed = await self.append(key, record)
        assert placed.offset is not None
        return OutputStreamRecord(placed.kind, placed.payload, placed.offset)

    async def read_output_after(
        self,
        key: StreamKey,
        after: Cursor,
        *,
        max_records: int,
        block: timedelta | None = None,
    ) -> OutputReadResult:
        del block
        self.read_operations.append("read_output_after")
        visible: list[OutputStreamRecord] = []
        for record in self._after(key, after):
            assert record.offset is not None
            identity = self._stage_by_offset.get((key, record.offset))
            if identity is not None:
                stage = self.stages[identity]
                if stage.status is OutputStageStatus.PENDING:
                    self.pending_read.set()
                    return OutputReadResult(
                        tuple(visible),
                        PendingOutputBarrier(stage.manifest, stage.records[0].offset),
                    )
                if stage.status is OutputStageStatus.ABORTED:
                    continue
            visible.append(
                OutputStreamRecord(record.kind, record.payload, record.offset)
            )
            if len(visible) >= max_records:
                break
        return OutputReadResult(tuple(visible))

    async def output_tail(self, key: StreamKey) -> Cursor:
        self.read_operations.append("output_tail")
        result = await self.read_output_after(
            key, BEGINNING, max_records=2**31 - 1, block=None
        )
        if not result.records:
            return BEGINNING
        return AFTER(result.records[-1].offset)

    async def install_park_intent(self, key: StreamKey, intent: ParkIntent) -> None:
        await super().install_park_intent(key, intent)
        if (
            self.expected_park_intents
            and len(self._intents) == self.expected_park_intents
        ):
            self.all_park_intents_installed.set()

    async def recheck(self, key: StreamKey, wait_id: int) -> bool:
        if self.recheck_release is not None:
            await self.recheck_release.wait()
        return await super().recheck(key, wait_id)

    async def remove_park_intent_if_matches(
        self,
        key: StreamKey,
        wait_id: int,
        *,
        run_id: str,
        park_generation: int,
    ) -> ParkIntentRemoval:
        result = await super().remove_park_intent_if_matches(
            key,
            wait_id,
            run_id=run_id,
            park_generation=park_generation,
        )
        if result in (ParkIntentRemoval.REMOVED, ParkIntentRemoval.ABSENT):
            self.removed_park_intents.append(wait_id)
        return result


class _UnavailableOutputMemoryBackend(_OutputMemoryBackend):
    """Fails one stage, then holds its retry until a test restores storage."""

    def __init__(self) -> None:
        super().__init__()
        self.failed_once = False
        self.failed_stage_attempted = asyncio.Event()
        self.recovery_release = asyncio.Event()

    async def stage_output(
        self,
        manifest: OutputStageManifest,
        records: Sequence[StagedOutputRecord],
    ) -> OutputStage:
        if not self.failed_once:
            self.failed_once = True
            self.failed_stage_attempted.set()
            raise OSError("injected output staging outage")
        await self.recovery_release.wait()
        return await super().stage_output(manifest, records)


async def _history(handle: Any) -> list[Any]:
    return [event async for event in handle.fetch_history_events()]


def _output_marker_data(events: Sequence[Any]) -> list[tuple[Any, Any]]:
    from temporalio.bridge.proto.external_data import ExternalStreamMarkerData

    markers: list[tuple[Any, Any]] = []
    for event in events:
        if not event.HasField("marker_recorded_event_attributes"):
            continue
        attributes = event.marker_recorded_event_attributes
        if attributes.marker_name != "core_external_stream":
            continue
        payloads = attributes.details["external_stream"].payloads
        if len(payloads) != 1:
            continue
        marker = ExternalStreamMarkerData()
        marker.ParseFromString(payloads[0].data)
        if marker.HasField("output"):
            markers.append((event, marker))
    return markers


def _output_marker_tokens(events: Sequence[Any]) -> list[str]:
    return [marker.output.stage_token for _, marker in _output_marker_data(events)]


async def _first_failed_task(handle: Any, *, timeout: float = 30) -> Any:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        for event in await _history(handle):
            if event.HasField("workflow_task_failed_event_attributes"):
                return event.workflow_task_failed_event_attributes
        await asyncio.sleep(0.1)
    raise AssertionError("the Workflow produced no failed Workflow Task")


def _failure_types(failure: Any) -> list[str]:
    types: list[str] = []
    while True:
        types.append(failure.application_failure_info.type)
        if not failure.HasField("cause"):
            return types
        failure = failure.cause


async def test_workflow_output_round_trips_through_core_and_client(
    client: Client,
) -> None:
    """Stage, record compact proof, promote after reporting, and decode."""
    backend = _OutputMemoryBackend()
    task_queue = f"tq-{uuid.uuid4()}"
    workflow_id = f"wf-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[PublishExternalOutputWorkflow],
        external_stream_backend=backend,
    ):
        handle = await client.start_workflow(
            PublishExternalOutputWorkflow.run,
            id=workflow_id,
            task_queue=task_queue,
        )
        assert await asyncio.wait_for(handle.result(), 30) == 1

        description = await handle.describe()
        chain = WorkflowChainKey(
            client.namespace,
            workflow_id,
            description.raw_description.workflow_execution_info.first_run_id,
        )
        assert len(backend.stages) == 1
        staged = next(iter(backend.stages.values()))

        # The server result and Worker's post-report History reconciliation
        # race one another. Wait for the healthy-path promotion rather than
        # requiring a reader to repair every successfully committed batch.
        async def wait_until_committed() -> None:
            while next(iter(backend.stages.values())).status is not (
                OutputStageStatus.COMMITTED
            ):
                await asyncio.sleep(0.01)

        await asyncio.wait_for(wait_until_committed(), 30)

        output_client = await ExternalOutputStreamClient.connect(
            backend=backend,
            workflow=chain,
            client=client,
        )

        async def read_all() -> list[str]:
            return [
                item.data
                async for item in output_client.topic("events", type=str).subscribe()
            ]

        assert await asyncio.wait_for(read_all(), 30) == ["workflow-output-secret"]
        assert next(iter(backend.stages.values())).status is OutputStageStatus.COMMITTED

        events = [event async for event in handle.fetch_history_events()]
        markers = [
            event.marker_recorded_event_attributes
            for event in events
            if event.HasField("marker_recorded_event_attributes")
            and event.marker_recorded_event_attributes.marker_name
            == "core_external_stream"
        ]
        assert len(markers) == 1
        envelope = markers[0].details["external_stream"]
        assert len(envelope.payloads) == 1

        from temporalio.bridge.proto.external_data import ExternalStreamMarkerData

        marker = ExternalStreamMarkerData()
        marker.ParseFromString(envelope.payloads[0].data)
        assert marker.HasField("output")
        output = marker.output
        assert output.stage_token == staged.manifest.stage_token
        assert output.run_id == handle.result_run_id
        assert output.provider_id == backend.provider_id
        assert output.provider_format_version == backend.provider_format_version
        assert output.history_floor_event_id == staged.manifest.history_floor_event_id
        assert output.history_floor_event_id > 0
        by_id = {event.event_id: event for event in events}
        assert output.history_floor_event_id in by_id
        assert by_id[output.history_floor_event_id + 1].HasField(
            "workflow_task_scheduled_event_attributes"
        )
        assert [
            (topic.topic, topic.record_count, topic.finished) for topic in output.topics
        ] == [("events", 2, True)]
        assert [
            tuple(segment.record_counts_by_topic) for segment in output.segments
        ] == [(2,)]
        assert len(output.topics[0].logical_fingerprint) == 32

        history_bytes = b"".join(event.SerializeToString() for event in events)
        assert b"workflow-output-secret" not in history_bytes
        assert b"workflow-output-secret" not in envelope.payloads[0].data


async def test_output_latency_flushes_a_retained_workflow_task(
    client: Client,
) -> None:
    """The latency deadline exposes output while the Workflow is still blocked."""
    backend = _OutputMemoryBackend()
    task_queue = f"tq-{uuid.uuid4()}"
    workflow_id = f"wf-{uuid.uuid4()}"
    handle = None
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[PublishOutputWhileInputIsRetainedWorkflow],
        external_stream_backend=backend,
    ):
        started_at = time.monotonic()
        handle = await client.start_workflow(
            PublishOutputWhileInputIsRetainedWorkflow.run,
            id=workflow_id,
            task_queue=task_queue,
        )
        description = await handle.describe()
        chain = WorkflowChainKey(
            client.namespace,
            workflow_id,
            description.raw_description.workflow_execution_info.first_run_id,
        )

        try:
            # The ordinary activation is retainable, so it must not stage on
            # return. Core's one-second output deadline is the first legal
            # boundary that can make this record visible.
            await asyncio.sleep(0.1)
            assert backend.stages == {}

            async def wait_until_committed() -> OutputStage:
                while not backend.stages:
                    await asyncio.sleep(0.01)
                stage = next(iter(backend.stages.values()))
                while stage.status is not OutputStageStatus.COMMITTED:
                    await asyncio.sleep(0.01)
                    stage = next(iter(backend.stages.values()))
                return stage

            staged = await asyncio.wait_for(wait_until_committed(), 30)
            elapsed = time.monotonic() - started_at
            assert elapsed >= 0.75
            assert staged.manifest.record_count == 1
            assert [record.kind for record in staged.records] == [RecordKind.DATA]

            # The output is externally readable even though Workflow code is
            # still waiting for its input record.
            assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
            output_client = await ExternalOutputStreamClient.connect(
                backend=backend,
                workflow=chain,
                client=client,
            )
            subscription = output_client.topic("events", type=str).subscribe()
            item = await asyncio.wait_for(subscription.__anext__(), 30)
            assert item.data == "visible-before-input"

            codec = StreamPayloadCodec(client.data_converter, str)
            await backend.append(
                chain.stream_key("release"),
                StreamRecord(
                    RecordKind.DATA,
                    await codec.encode("release"),
                    "test",
                    0,
                ),
            )
            assert await asyncio.wait_for(handle.result(), 30) == 1
        finally:
            if (await handle.describe()).status == WorkflowExecutionStatus.RUNNING:
                await handle.terminate()


@pytest.mark.timeout(90)
async def test_three_retained_output_windows_write_three_markers_and_wfts(
    client: Client,
) -> None:
    """Two publishes coalesce per window; three crossed windows stay distinct."""
    backend = _OutputMemoryBackend()
    task_queue = f"tq-{uuid.uuid4()}"
    workflow_id = f"wf-{uuid.uuid4()}"
    handle = None
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[ThreeRetainedOutputWindowsWorkflow],
        external_stream_backend=backend,
    ):
        try:
            handle = await client.start_workflow(
                ThreeRetainedOutputWindowsWorkflow.run,
                id=workflow_id,
                task_queue=task_queue,
            )
            description = await handle.describe()
            chain = WorkflowChainKey(
                client.namespace,
                workflow_id,
                description.raw_description.workflow_execution_info.first_run_id,
            )
            codec = StreamPayloadCodec(client.data_converter, str)
            trigger_key = chain.stream_key("triggers")

            async def append_trigger(sequence: int, value: str) -> None:
                await backend.append(
                    trigger_key,
                    StreamRecord(
                        RecordKind.DATA,
                        await codec.encode(value),
                        "latency-window-test",
                        sequence,
                    ),
                )

            async def wait_for_output_markers(count: int) -> list[tuple[Any, Any]]:
                while True:
                    markers = _output_marker_data(await _history(handle))
                    if len(markers) >= count:
                        return markers
                    await asyncio.sleep(0.01)

            for sequence, window in enumerate(("one", "two", "three")):
                await append_trigger(sequence, window)
                markers = await asyncio.wait_for(
                    wait_for_output_markers(sequence + 1), 30
                )
                assert len(markers) == sequence + 1

            assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
            markers = _output_marker_data(await _history(handle))
            assert len(markers) == 3
            completed_event_ids = {
                event.marker_recorded_event_attributes.workflow_task_completed_event_id
                for event, _ in markers
            }
            assert len(completed_event_ids) == 3

            from temporalio.bridge.proto.external_data import ParkReason

            for _, marker in markers:
                assert marker.terminal_boundary == ParkReason.PARK_REASON_OUTPUT_LATENCY
                assert len(marker.output.topics) == 1
                assert marker.output.topics[0].record_count == 2
                assert (
                    sum(
                        segment.record_counts_by_topic[0]
                        for segment in marker.output.segments
                    )
                    == 2
                )

            events = await _history(handle)
            assert completed_event_ids <= {
                event.event_id
                for event in events
                if event.HasField("workflow_task_completed_event_attributes")
            }
            assert len(backend.stages) == 3
            assert all(
                stage.status is OutputStageStatus.COMMITTED
                for stage in backend.stages.values()
            )

            await append_trigger(3, "stop")
            assert await asyncio.wait_for(handle.result(), 30) == 3
            assert len(_output_marker_data(await _history(handle))) == 3
        finally:
            if (
                handle is not None
                and (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
            ):
                await handle.terminate()


async def test_finished_output_survives_continue_as_new_without_backend_reads(
    client: Client,
) -> None:
    backend = _OutputMemoryBackend()
    task_queue = f"tq-{uuid.uuid4()}"
    workflow_id = f"wf-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[FinishOutputAcrossContinueAsNewWorkflow],
        external_stream_backend=backend,
    ):
        handle = await client.start_workflow(
            FinishOutputAcrossContinueAsNewWorkflow.run,
            id=workflow_id,
            task_queue=task_queue,
        )

        assert await asyncio.wait_for(handle.result(), 30) == (
            "successor rejected finished topic"
        )

    # The first Run staged only FINISH. The successor rejected DATA from its
    # reserved continuation header before consulting mutable provider state.
    assert len(backend.stages) == 1
    [stage] = backend.stages.values()
    assert [record.kind.name for record in stage.records] == ["FINISH"]
    assert backend.read_operations == []


async def test_cold_client_repairs_post_report_commit_failure(
    client: Client,
) -> None:
    """A marker stays authoritative when opportunistic promotion fails once."""
    backend = _OutputMemoryBackend()
    backend.commit_failures_remaining = 1
    task_queue = f"tq-{uuid.uuid4()}"
    workflow_id = f"wf-{uuid.uuid4()}"
    chain: WorkflowChainKey
    staged: OutputStage

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[PublishExternalOutputWorkflow],
        external_stream_backend=backend,
    ):
        handle = await client.start_workflow(
            PublishExternalOutputWorkflow.run,
            id=workflow_id,
            task_queue=task_queue,
        )
        assert await asyncio.wait_for(handle.result(), 30) == 1
        await asyncio.wait_for(backend.commit_failed.wait(), 30)

        description = await handle.describe()
        chain = WorkflowChainKey(
            client.namespace,
            workflow_id,
            description.raw_description.workflow_execution_info.first_run_id,
        )
        assert len(backend.stages) == 1
        staged = next(iter(backend.stages.values()))
        assert staged.status is OutputStageStatus.PENDING
        assert backend.commit_attempts == 1

        # The injected commit is reached only after the Worker found the token
        # in exact-run History. Assert that proof at the failure boundary too,
        # before shutting the Worker down and handing recovery to a new client.
        events = [event async for event in handle.fetch_history_events()]
        from temporalio.contrib.external_workflow_streams._output_client import (
            _event_has_output_stage_token,
        )

        assert any(
            _event_has_output_stage_token(event, staged.manifest.stage_token)
            for event in events
        )

    cold_client = await ExternalOutputStreamClient.connect(
        backend=backend,
        workflow=chain,
        client=client,
    )

    async def read_all() -> list[str]:
        return [
            item.data
            async for item in cold_client.topic("events", type=str).subscribe()
        ]

    assert await asyncio.wait_for(read_all(), 30) == ["workflow-output-secret"]
    assert (
        backend.stages[
            (staged.manifest.stage_token, staged.manifest.sub_batch_id)
        ].status
        is OutputStageStatus.COMMITTED
    )
    assert backend.commit_attempts == 2


@pytest.mark.timeout(90)
async def test_reader_waits_at_pending_barrier_until_marker_commit(
    client: Client,
) -> None:
    """A staged record is physically present but unreadable before its proof."""
    backend = _OutputMemoryBackend()
    backend.stage_release = asyncio.Event()
    task_queue = f"tq-{uuid.uuid4()}"
    workflow_id = f"wf-{uuid.uuid4()}"
    handle = None
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[PublishExternalOutputWorkflow],
        external_stream_backend=backend,
    ):
        try:
            handle = await client.start_workflow(
                PublishExternalOutputWorkflow.run,
                id=workflow_id,
                task_queue=task_queue,
            )
            await asyncio.wait_for(backend.stage_placed.wait(), 30)
            description = await handle.describe()
            chain = WorkflowChainKey(
                client.namespace,
                workflow_id,
                description.raw_description.workflow_execution_info.first_run_id,
            )
            output_client = await ExternalOutputStreamClient.connect(
                backend=backend,
                workflow=chain,
                client=client,
            )

            async def read_all() -> list[str]:
                return [
                    item.data
                    async for item in output_client.topic(
                        "events", type=str
                    ).subscribe()
                ]

            read_task = asyncio.create_task(read_all())
            await asyncio.wait_for(backend.pending_read.wait(), 30)
            await asyncio.sleep(0.1)
            assert not read_task.done()
            [stage] = backend.stages.values()
            assert stage.status is OutputStageStatus.PENDING
            assert _output_marker_tokens(await _history(handle)) == []

            backend.stage_release.set()
            assert await asyncio.wait_for(handle.result(), 30) == 1
            assert await asyncio.wait_for(read_task, 30) == ["workflow-output-secret"]
            [stage] = backend.stages.values()
            assert stage.status is OutputStageStatus.COMMITTED
            assert _output_marker_tokens(await _history(handle)) == [
                stage.manifest.stage_token
            ]
        finally:
            backend.stage_release.set()
            if (
                handle is not None
                and (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
            ):
                await handle.terminate()


@pytest.mark.timeout(90)
async def test_rejected_post_stage_completion_never_exposes_phantom_output(
    client: Client,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject the first report after staging; only its fresh retry is visible."""
    backend = _OutputMemoryBackend()
    task_queue = f"tq-{uuid.uuid4()}"
    workflow_id = f"wf-{uuid.uuid4()}"
    original = temporalio.bridge.worker.Worker.complete_workflow_activation
    rejected = asyncio.Event()
    rejected_tokens: list[str] = []

    async def reject_first_output_commit(
        bridge: temporalio.bridge.worker.Worker,
        completion: Any,
    ) -> None:
        if not rejected.is_set() and completion.HasField("successful"):
            commits = [
                command.workflow_output_stream_commit
                for command in completion.successful.commands
                if command.HasField("workflow_output_stream_commit")
            ]
            if commits:
                rejected_token = commits[0].manifest.stage_token
                rejected_tokens.append(rejected_token)
                matching = [
                    stage
                    for stage in backend.stages.values()
                    if stage.manifest.stage_token == rejected_token
                ]
                assert len(matching) == 1
                assert matching[0].status is OutputStageStatus.PENDING
                # Replace the successful report after provider staging. Core
                # reports an ordinary failed WFT, then retries the Workflow.
                completion.failed.failure.message = (
                    "injected rejection after output stage"
                )
                completion.failed.failure.application_failure_info.type = (
                    "InjectedPostStageFailure"
                )
                rejected.set()
        await original(bridge, completion)

    monkeypatch.setattr(
        temporalio.bridge.worker.Worker,
        "complete_workflow_activation",
        reject_first_output_commit,
    )

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[PublishExternalOutputWorkflow],
        external_stream_backend=backend,
    ):
        handle = await client.start_workflow(
            PublishExternalOutputWorkflow.run,
            id=workflow_id,
            task_queue=task_queue,
        )
        await asyncio.wait_for(rejected.wait(), 30)
        assert await asyncio.wait_for(handle.result(), 30) == 1

        [rejected_token] = rejected_tokens
        events = await _history(handle)
        assert any(
            event.HasField("workflow_task_failed_event_attributes") for event in events
        )
        stages = list(backend.stages.values())
        assert len(stages) == 2
        [abandoned] = [
            stage for stage in stages if stage.manifest.stage_token == rejected_token
        ]
        assert abandoned.status is OutputStageStatus.ABORTED

        marker_tokens = _output_marker_tokens(events)
        assert len(marker_tokens) == 1
        assert marker_tokens != [rejected_token]

        description = await handle.describe()
        chain = WorkflowChainKey(
            client.namespace,
            workflow_id,
            description.raw_description.workflow_execution_info.first_run_id,
        )
        output_client = await ExternalOutputStreamClient.connect(
            backend=backend,
            workflow=chain,
            client=client,
        )
        assert [
            item.data
            async for item in output_client.topic("events", type=str).subscribe()
        ] == ["workflow-output-secret"]
        committed = [
            stage
            for stage in backend.stages.values()
            if stage.status is OutputStageStatus.COMMITTED
        ]
        assert len(committed) == 1
        assert marker_tokens == [committed[0].manifest.stage_token]


@pytest.mark.timeout(90)
async def test_concurrent_updates_preserve_their_own_turn_ids(
    client: Client,
) -> None:
    backend = _OutputMemoryBackend()
    task_queue = f"tq-{uuid.uuid4()}"
    workflow_id = f"wf-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[ConcurrentTurnOutputWorkflow],
        external_stream_backend=backend,
    ):
        handle = await client.start_workflow(
            ConcurrentTurnOutputWorkflow.run,
            id=workflow_id,
            task_queue=task_queue,
        )
        description = await handle.describe()
        chain = WorkflowChainKey(
            client.namespace,
            workflow_id,
            description.raw_description.workflow_execution_info.first_run_id,
        )
        output_client = await ExternalOutputStreamClient.connect(
            backend=backend,
            workflow=chain,
            client=client,
        )
        topic = output_client.topic("events", type=str)
        boundary = await topic.tail()
        first_id = f"turn-{uuid.uuid4()}"
        second_id = f"turn-{uuid.uuid4()}"

        returned = await asyncio.gather(
            handle.execute_update(ConcurrentTurnOutputWorkflow.start_turn, first_id),
            handle.execute_update(ConcurrentTurnOutputWorkflow.start_turn, second_id),
        )
        assert returned == [first_id, second_id]
        await handle.execute_update(ConcurrentTurnOutputWorkflow.finish)
        assert await asyncio.wait_for(handle.result(), 30) == "done"

        async def scan_for(own_id: str) -> list[str]:
            seen: list[str] = []
            async for item in topic.subscribe(after=boundary):
                seen.append(item.data)
                if item.data == f"turn_started:{own_id}":
                    return seen
            raise AssertionError(f"no output for {own_id}")

        first_seen, second_seen = await asyncio.gather(
            scan_for(first_id), scan_for(second_id)
        )
        assert first_seen[-1] == f"turn_started:{first_id}"
        assert second_seen[-1] == f"turn_started:{second_id}"
        # Both subscribers use the same boundary; one necessarily sees the
        # other concurrent turn first, but still correlates its own envelope.
        assert sorted((len(first_seen), len(second_seen))) == [1, 2]


@pytest.mark.timeout(90)
async def test_stage_outage_blocks_wft_and_reports_external_storage_cause(
    client: Client,
) -> None:
    backend = _UnavailableOutputMemoryBackend()
    task_queue = f"tq-{uuid.uuid4()}"
    workflow_id = f"wf-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[PublishExternalOutputWorkflow],
        external_stream_backend=backend,
    ):
        handle = await client.start_workflow(
            PublishExternalOutputWorkflow.run,
            id=workflow_id,
            task_queue=task_queue,
        )
        result_task = asyncio.create_task(handle.result())
        await asyncio.wait_for(backend.failed_stage_attempted.wait(), 30)
        await asyncio.sleep(0.05)
        assert not result_task.done()
        assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
        assert backend.stages == {}

        backend.recovery_release.set()
        assert await asyncio.wait_for(result_task, 30) == 1
        failed = await _first_failed_task(handle)
        assert (
            failed.cause
            == temporalio.api.enums.v1.WorkflowTaskFailedCause.WORKFLOW_TASK_FAILED_CAUSE_EXTERNAL_STORAGE_FAILURE
        )
        assert "StreamStorageError" in _failure_types(failed.failure)
        assert len(_output_marker_tokens(await _history(handle))) == 1
        stages = list(backend.stages.values())
        assert len(stages) == 1
        assert stages[0].status is OutputStageStatus.COMMITTED

        description = await handle.describe()
        chain = WorkflowChainKey(
            client.namespace,
            workflow_id,
            description.raw_description.workflow_execution_info.first_run_id,
        )
        output_client = await ExternalOutputStreamClient.connect(
            backend=backend,
            workflow=chain,
            client=client,
        )
        assert [
            item.data
            async for item in output_client.topic("events", type=str).subscribe()
        ] == ["workflow-output-secret"]


@pytest.mark.timeout(90)
async def test_cursor_resume_crosses_rollover_and_continue_as_new(
    client: Client,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _OutputMemoryBackend()
    task_queue = f"tq-{uuid.uuid4()}"
    workflow_id = f"wf-{uuid.uuid4()}"
    original = temporalio.bridge.worker.Worker.complete_workflow_activation
    rollover_requests: list[bool] = []

    async def capture_rollover_request(
        bridge: temporalio.bridge.worker.Worker,
        completion: Any,
    ) -> None:
        if completion.HasField("successful"):
            rollover_requests.extend(
                command.workflow_output_stream_commit.request_rollover
                for command in completion.successful.commands
                if command.HasField("workflow_output_stream_commit")
            )
        await original(bridge, completion)

    monkeypatch.setattr(
        temporalio.bridge.worker.Worker,
        "complete_workflow_activation",
        capture_rollover_request,
    )
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[PublishAcrossRolloverAndContinueAsNewWorkflow],
        external_stream_backend=backend,
    ):
        handle = await client.start_workflow(
            PublishAcrossRolloverAndContinueAsNewWorkflow.run,
            id=workflow_id,
            task_queue=task_queue,
        )
        description = await handle.describe()
        first_run_id = description.raw_description.workflow_execution_info.first_run_id

        async def wait_for_replacement_stage() -> None:
            while len(backend.stages) < 2:
                await asyncio.sleep(0.01)

        try:
            await asyncio.wait_for(wait_for_replacement_stage(), 10)
        except asyncio.TimeoutError:
            events = await _history(handle)
            statuses = [stage.status.name for stage in backend.stages.values()]
            marker_tokens = _output_marker_tokens(events)
            status = (await handle.describe()).status
            await handle.terminate()
            pytest.fail(
                "output capacity did not create its replacement Workflow Task: "
                f"requests={rollover_requests}, stages={statuses}, "
                f"markers={marker_tokens}, "
                f"status={status.name if status is not None else 'NONE'}"
            )
        assert await asyncio.wait_for(handle.result(), 30) == "done"
        assert rollover_requests[:2] == [True, False]

        chain = WorkflowChainKey(client.namespace, workflow_id, first_run_id)
        output_client = await ExternalOutputStreamClient.connect(
            backend=backend,
            workflow=chain,
            client=client,
        )
        topic = output_client.topic("events", type=str)
        initial = topic.subscribe()
        first = await asyncio.wait_for(initial.__anext__(), 30)
        await cast(Any, initial).aclose()
        assert first.data == "before-rollover"

        resumed = topic.subscribe(after=first.offset)
        second = await asyncio.wait_for(resumed.__anext__(), 30)
        third = await asyncio.wait_for(resumed.__anext__(), 30)
        await cast(Any, resumed).aclose()
        assert [second.data, third.data] == [
            "after-rollover",
            "after-continue-as-new",
        ]
        assert len({first.offset, second.offset, third.offset}) == 3

        stages_by_run: dict[str, int] = {}
        for stage in backend.stages.values():
            stages_by_run[stage.manifest.run_id] = (
                stages_by_run.get(stage.manifest.run_id, 0) + 1
            )
            assert stage.status is OutputStageStatus.COMMITTED
        assert stages_by_run[first_run_id] == 2
        successor_runs = {
            run_id: count
            for run_id, count in stages_by_run.items()
            if run_id != first_run_id
        }
        assert list(successor_runs.values()) == [1]


@pytest.mark.timeout(90)
async def test_output_deadline_wins_park_race_and_removes_every_intent(
    client: Client,
) -> None:
    backend = _OutputMemoryBackend()
    backend.expected_park_intents = 2
    backend.recheck_release = asyncio.Event()
    task_queue = f"tq-{uuid.uuid4()}"
    workflow_id = f"wf-{uuid.uuid4()}"
    handle = None
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[PublishWhileTwoInputsParkWorkflow],
        external_stream_backend=backend,
    ):
        try:
            handle = await client.start_workflow(
                PublishWhileTwoInputsParkWorkflow.run,
                id=workflow_id,
                task_queue=task_queue,
            )
            await asyncio.wait_for(backend.all_park_intents_installed.wait(), 30)
            assert len(backend._intents) == 2

            # Keep PrepareExternalStreamPark outstanding until after the
            # already-armed output deadline expires. Core must discard the
            # losing park result and roll back both provider intents.
            await asyncio.sleep(0.75)
            backend.recheck_release.set()

            async def wait_for_flush_and_rollback() -> None:
                while True:
                    committed = any(
                        stage.status is OutputStageStatus.COMMITTED
                        for stage in backend.stages.values()
                    )
                    if committed and not backend._intents:
                        return
                    await asyncio.sleep(0.01)

            await asyncio.wait_for(wait_for_flush_and_rollback(), 30)
            assert set(backend.removed_park_intents) == {1, 2}
            assert len(backend.stages) == 1
            assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
            assert len(_output_marker_tokens(await _history(handle))) == 1
        finally:
            backend.recheck_release.set()
            if (
                handle is not None
                and (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
            ):
                await handle.terminate()
