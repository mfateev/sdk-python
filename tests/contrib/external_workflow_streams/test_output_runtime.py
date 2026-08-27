"""Workflow-thread batching and staging for external output streams."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest

import temporalio.api.common.v1
import temporalio.bridge.proto.external_data
import temporalio.bridge.proto.workflow_activation
import temporalio.bridge.proto.workflow_completion
import temporalio.converter
import temporalio.workflow
from temporalio.contrib.external_workflow_streams._backend import StreamKey
from temporalio.contrib.external_workflow_streams._errors import (
    ExternalStreamCapacityError,
    StreamStorageError,
)
from temporalio.contrib.external_workflow_streams._manager import (
    ReadinessResult,
    StreamSubscriptionManager,
)
from temporalio.contrib.external_workflow_streams._output_api import (
    ExternalOutputStreamOptions,
)
from temporalio.contrib.external_workflow_streams._output_backend import (
    OutputReadResult,
    OutputStage,
    OutputStageManifest,
    OutputStageStatus,
    OutputStreamBackend,
    OutputStreamRecord,
    StagedOutputRecord,
)
from temporalio.contrib.external_workflow_streams._output_codec import (
    fingerprint_logical_frames,
)
from temporalio.contrib.external_workflow_streams._record import (
    BEGINNING,
    Cursor,
    Offset,
    RecordKind,
    StreamRecord,
)
from temporalio.contrib.external_workflow_streams._runtime import (
    WorkflowStreamRuntime,
)
from temporalio.worker._workflow import _WorkflowWorker
from temporalio.worker._workflow_instance import _WorkflowInstanceImpl
from tests.contrib.external_workflow_streams.memory_backend import MemoryStreamBackend


class _OutputMemoryBackend(MemoryStreamBackend, OutputStreamBackend):
    provider_id = "output-memory"
    provider_format_version = 1

    def __init__(self) -> None:
        super().__init__()
        self.stages: dict[tuple[str, int], OutputStage] = {}
        self.stage_calls = 0
        self.stage_failures = 0
        self.stage_payloads: list[tuple[bytes, ...]] = []

    async def stage_output(
        self,
        manifest: OutputStageManifest,
        records: Sequence[StagedOutputRecord],
    ) -> OutputStage:
        self.stage_calls += 1
        self.stage_payloads.append(tuple(record.payload for record in records))
        if self.stage_failures:
            self.stage_failures -= 1
            raise OSError("output backend unavailable")
        identity = (manifest.stage_token, manifest.sub_batch_id)
        existing = self.stages.get(identity)
        if existing is not None:
            return existing
        placed = tuple(
            OutputStreamRecord(
                record.kind,
                record.payload,
                Offset(f"{manifest.sub_batch_id + 1}-{record.publish_index}"),
            )
            for record in records
        )
        stage = OutputStage(manifest, placed, OutputStageStatus.PENDING)
        self.stages[identity] = stage
        return stage

    async def commit_output(self, manifest: OutputStageManifest) -> OutputStage:
        return self._resolve(manifest, OutputStageStatus.COMMITTED)

    async def abort_output(self, manifest: OutputStageManifest) -> OutputStage:
        return self._resolve(manifest, OutputStageStatus.ABORTED)

    def _resolve(
        self, manifest: OutputStageManifest, status: OutputStageStatus
    ) -> OutputStage:
        existing = self.stages[(manifest.stage_token, manifest.sub_batch_id)]
        resolved = OutputStage(existing.manifest, existing.records, status)
        self.stages[(manifest.stage_token, manifest.sub_batch_id)] = resolved
        return resolved

    async def output_stage(self, manifest: OutputStageManifest) -> OutputStage | None:
        return self.stages.get((manifest.stage_token, manifest.sub_batch_id))

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
        del key, after, max_records, block
        return OutputReadResult(())

    async def output_tail(self, key: StreamKey) -> Cursor:
        del key
        return BEGINNING


class _ChangingPayloadCodec(temporalio.converter.PayloadCodec):
    """A valid codec whose wire bytes deliberately change on every call."""

    def __init__(self) -> None:
        self.encode_calls = 0

    async def encode(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> list[temporalio.api.common.v1.Payload]:
        self.encode_calls += 1
        encoded = []
        for payload in payloads:
            copied = temporalio.api.common.v1.Payload()
            copied.CopyFrom(payload)
            copied.data += f":encoding-{self.encode_calls}".encode()
            encoded.append(copied)
        return encoded

    async def decode(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> list[temporalio.api.common.v1.Payload]:
        return list(payloads)


async def _notify(run_id: str, wait_id: int, generation: int) -> str:
    del run_id, wait_id, generation
    return ReadinessResult.ACCEPTED


@pytest.fixture
def backend() -> _OutputMemoryBackend:
    return _OutputMemoryBackend()


@pytest.fixture
def runtime(backend: _OutputMemoryBackend) -> WorkflowStreamRuntime:
    manager = StreamSubscriptionManager(
        backend=backend,
        notify_ready=_notify,
        watch_block=timedelta(milliseconds=1),
    )
    return WorkflowStreamRuntime(
        manager=manager,
        backend=backend,
        run_id="run",
        namespace="namespace",
        workflow_id="workflow",
        first_execution_run_id="first-run",
        data_converter=temporalio.converter.DataConverter.default,
        default_idle_timeout=timedelta(seconds=1),
    )


def _successful_completion(
    *variants: str,
) -> temporalio.bridge.proto.workflow_completion.WorkflowActivationCompletion:
    completion = (
        temporalio.bridge.proto.workflow_completion.WorkflowActivationCompletion()
    )
    completion.successful.SetInParent()
    for variant in variants:
        getattr(completion.successful.commands.add(), variant).SetInParent()
    return completion


def _activation(*, finalize_output_latency: bool = False) -> Any:
    activation = temporalio.bridge.proto.workflow_activation.WorkflowActivation(
        run_id="run",
        history_floor_event_id=7,
    )
    if finalize_output_latency:
        activation.jobs.add().finalize_external_streams.reason = (
            temporalio.bridge.proto.external_data.ParkReason.PARK_REASON_OUTPUT_LATENCY
        )
    return activation


def _park_activation() -> Any:
    activation = _activation()
    activation.jobs.add().prepare_external_stream_park.quiescence_generation = 1
    return activation


class _ParkManager:
    def __init__(self, *, became_ready: bool) -> None:
        self.became_ready = became_ready

    async def prepare_park(self, *args: Any, **kwargs: Any) -> bool:
        del args, kwargs
        return self.became_ready


def _worker() -> _WorkflowWorker:
    worker = object.__new__(_WorkflowWorker)
    worker._pending_external_output_stages = {}
    return worker


async def _publish_one(
    runtime: WorkflowStreamRuntime,
    *,
    max_records: int = 10,
) -> None:
    runtime.begin_activation(7)
    await runtime.publish_output(
        topic="events",
        value="value",
        value_type=str,
        kind=RecordKind.DATA,
        max_publish_latency=timedelta(milliseconds=250),
        max_records=max_records,
        max_logical_bytes=10_000,
    )


async def test_retainable_output_emits_buffered_without_provider_staging(
    runtime: WorkflowStreamRuntime,
    backend: _OutputMemoryBackend,
) -> None:
    await _publish_one(runtime)
    completion = _successful_completion(
        "workflow_stream_progress",
        "workflow_stream_quiescent",
    )

    await _worker()._stage_or_buffer_external_output(_activation(), completion, runtime)

    assert backend.stage_calls == 0
    assert runtime.has_output
    assert [
        command.WhichOneof("variant") for command in completion.successful.commands
    ] == [
        "workflow_stream_progress",
        "workflow_output_stream_buffered",
        "workflow_stream_quiescent",
    ]
    buffered = completion.successful.commands[1].workflow_output_stream_buffered
    assert buffered.max_publish_latency.ToTimedelta() == timedelta(milliseconds=250)


async def test_output_latency_finalization_stages_and_answers_both_commands(
    runtime: WorkflowStreamRuntime,
    backend: _OutputMemoryBackend,
) -> None:
    await _publish_one(runtime)
    worker = _worker()
    worker._external_stream_runtimes = {"run": runtime}
    initial = _successful_completion("workflow_stream_quiescent")
    await worker._stage_or_buffer_external_output(_activation(), initial, runtime)
    assert backend.stage_calls == 0

    activation = _activation(finalize_output_latency=True)
    completion = await worker._handle_external_stream_jobs(
        activation,
        cast(Any, None),
    )
    assert completion is not None
    await worker._stage_or_buffer_external_output(activation, completion, runtime)

    assert backend.stage_calls == 1
    assert not runtime.has_output
    assert [
        command.WhichOneof("variant") for command in completion.successful.commands
    ] == [
        "workflow_output_stream_commit",
        "external_stream_finalized",
    ]
    assert len(worker._pending_external_output_stages["run"]) == 1


async def test_confirmed_park_stages_output_in_the_same_completion(
    runtime: WorkflowStreamRuntime,
    backend: _OutputMemoryBackend,
) -> None:
    await _publish_one(runtime)
    worker = _worker()
    worker._external_stream_runtimes = {"run": runtime}
    worker._external_stream_manager = _ParkManager(became_ready=False)
    activation = _park_activation()

    completion = await worker._handle_external_stream_jobs(
        activation,
        cast(Any, None),
    )
    assert completion is not None
    await worker._stage_or_buffer_external_output(activation, completion, runtime)

    assert backend.stage_calls == 1
    assert not runtime.has_output
    assert [
        command.WhichOneof("variant") for command in completion.successful.commands
    ] == [
        "workflow_output_stream_commit",
        "external_stream_park_result",
    ]
    assert completion.successful.commands[1].external_stream_park_result.HasField(
        "confirmed"
    )


async def test_park_that_became_ready_keeps_output_buffered(
    runtime: WorkflowStreamRuntime,
    backend: _OutputMemoryBackend,
) -> None:
    await _publish_one(runtime)
    worker = _worker()
    worker._external_stream_runtimes = {"run": runtime}
    worker._external_stream_manager = _ParkManager(became_ready=True)
    activation = _park_activation()

    completion = await worker._handle_external_stream_jobs(
        activation,
        cast(Any, None),
    )
    assert completion is not None
    await worker._stage_or_buffer_external_output(activation, completion, runtime)

    assert backend.stage_calls == 0
    assert runtime.has_output
    assert [
        command.WhichOneof("variant") for command in completion.successful.commands
    ] == [
        "workflow_output_stream_buffered",
        "external_stream_park_result",
    ]
    assert completion.successful.commands[1].external_stream_park_result.HasField(
        "became_ready"
    )
    buffered = completion.successful.commands[0].workflow_output_stream_buffered
    assert buffered.max_publish_latency.ToTimedelta() == timedelta(milliseconds=250)


async def test_output_without_quiescent_snapshot_stages_immediately(
    runtime: WorkflowStreamRuntime,
    backend: _OutputMemoryBackend,
) -> None:
    await _publish_one(runtime)
    completion = _successful_completion()

    await _worker()._stage_or_buffer_external_output(_activation(), completion, runtime)

    assert backend.stage_calls == 1
    assert [
        command.WhichOneof("variant") for command in completion.successful.commands
    ] == ["workflow_output_stream_commit"]


async def test_capacity_rollover_bypasses_output_buffering(
    runtime: WorkflowStreamRuntime,
    backend: _OutputMemoryBackend,
) -> None:
    await _publish_one(runtime, max_records=1)
    blocked_publish = asyncio.create_task(
        runtime.publish_output(
            topic="events",
            value="second",
            value_type=str,
            kind=RecordKind.DATA,
            max_publish_latency=timedelta(milliseconds=250),
            max_records=1,
            max_logical_bytes=10_000,
        )
    )
    await asyncio.sleep(0)
    assert runtime.output_rollover_requested
    completion = _successful_completion("workflow_stream_quiescent")

    try:
        await _worker()._stage_or_buffer_external_output(
            _activation(), completion, runtime
        )
        assert backend.stage_calls == 1
        assert [
            command.WhichOneof("variant") for command in completion.successful.commands
        ] == [
            "workflow_output_stream_commit",
            "workflow_stream_quiescent",
        ]
    finally:
        blocked_publish.cancel()
        with pytest.raises(asyncio.CancelledError):
            await blocked_publish


async def test_stage_uses_one_token_and_pre_codec_logical_manifests(
    runtime: WorkflowStreamRuntime, backend: _OutputMemoryBackend
) -> None:
    runtime.begin_activation(7)
    await runtime.publish_output(
        topic="events",
        value={"value": 1},
        value_type=dict,
        kind=RecordKind.DATA,
        max_publish_latency=timedelta(milliseconds=100),
        max_records=10,
        max_logical_bytes=10_000,
    )
    await runtime.publish_output(
        topic="status",
        value=None,
        value_type=str,
        kind=RecordKind.FINISH,
        max_publish_latency=timedelta(milliseconds=50),
        max_records=10,
        max_logical_bytes=10_000,
    )

    staged = await runtime.stage_output(7)

    assert len(staged.topics) == 2
    assert {topic.manifest.stage_token for topic in staged.topics} == {
        staged.stage_token
    }
    assert staged.segment_record_counts == ((1, 1),)
    assert staged.topics[1].finished
    assert all(
        stage.status is OutputStageStatus.PENDING for stage in backend.stages.values()
    )


async def test_changed_codec_bytes_keep_logical_retry_identity_and_first_bytes(
    backend: _OutputMemoryBackend,
) -> None:
    codec = _ChangingPayloadCodec()
    manager = StreamSubscriptionManager(
        backend=backend,
        notify_ready=_notify,
        watch_block=timedelta(milliseconds=1),
    )
    runtime = WorkflowStreamRuntime(
        manager=manager,
        backend=backend,
        run_id="run",
        namespace="namespace",
        workflow_id="workflow",
        first_execution_run_id="first-run",
        data_converter=temporalio.converter.DataConverter(payload_codec=codec),
        default_idle_timeout=timedelta(seconds=1),
    )
    runtime.begin_activation(7)
    await runtime.publish_output(
        topic="events",
        value="same logical value",
        value_type=str,
        kind=RecordKind.DATA,
        max_publish_latency=timedelta(milliseconds=100),
        max_records=10,
        max_logical_bytes=10_000,
    )

    first = await runtime.stage_output(7)
    repeated = await runtime.stage_output(7)

    assert codec.encode_calls == 2
    assert first == repeated
    assert first.topics[0].manifest == repeated.topics[0].manifest
    assert len(first.topics[0].manifest.fingerprint) == 32
    assert backend.stage_payloads[0] != backend.stage_payloads[1]
    stored = backend.stages[(first.stage_token, 0)]
    assert (
        tuple(record.payload for record in stored.records) == backend.stage_payloads[0]
    )
    assert (
        tuple(record.payload for record in stored.records) != backend.stage_payloads[1]
    )


async def test_capacity_publish_waits_for_a_replacement_workflow_task(
    runtime: WorkflowStreamRuntime,
) -> None:
    options: dict[str, Any] = {
        "value_type": str,
        "kind": RecordKind.DATA,
        "max_publish_latency": timedelta(milliseconds=100),
        "max_records": 1,
        "max_logical_bytes": 10_000,
    }
    runtime.begin_activation(7)
    await runtime.publish_output(topic="events", value="one", **options)
    second = asyncio.create_task(
        runtime.publish_output(topic="events", value="two", **options)
    )
    await asyncio.sleep(0)
    assert runtime.output_rollover_requested
    assert not second.done()

    await runtime.stage_output(7)
    runtime.output_stage_recorded()
    runtime.begin_activation(9)
    await second

    staged = await runtime.stage_output(9)
    assert staged.topics[0].manifest.record_count == 1


async def test_stage_outage_is_storage_failure_and_retry_reuses_attempt(
    runtime: WorkflowStreamRuntime, backend: _OutputMemoryBackend
) -> None:
    runtime.begin_activation(7)
    await runtime.publish_output(
        topic="events",
        value="value",
        value_type=str,
        kind=RecordKind.DATA,
        max_publish_latency=timedelta(milliseconds=100),
        max_records=10,
        max_logical_bytes=10_000,
    )
    backend.stage_failures = 1

    with pytest.raises(StreamStorageError, match="events"):
        await runtime.stage_output(7)
    first_token = runtime._output_stage_token

    staged = await runtime.stage_output(7)

    assert staged.stage_token == first_token
    assert backend.stage_calls == 2


async def test_oversized_marker_manifest_is_rejected_before_external_io(
    runtime: WorkflowStreamRuntime, backend: _OutputMemoryBackend
) -> None:
    runtime.begin_activation(7)
    await runtime.publish_output(
        topic="t" * (70 * 1024),
        value="value",
        value_type=str,
        kind=RecordKind.DATA,
        max_publish_latency=timedelta(milliseconds=100),
        max_records=10,
        max_logical_bytes=100 * 1024,
    )

    with pytest.raises(ExternalStreamCapacityError, match="marker budget"):
        await runtime.stage_output(7)

    assert backend.stage_calls == 0
    assert runtime._staged_output is None


async def test_replay_validates_the_recorded_shared_segment_schedule_without_io(
    runtime: WorkflowStreamRuntime, backend: _OutputMemoryBackend
) -> None:
    runtime.begin_output_replay(
        SimpleNamespace(
            schema_version=1,
            fingerprint_version=1,
            topics=[
                SimpleNamespace(
                    topic="events",
                    record_count=1,
                    logical_byte_count=0,
                    logical_fingerprint=b"",
                    finished=False,
                )
            ],
            segments=[SimpleNamespace(record_counts_by_topic=[1])],
        )
    )
    runtime.begin_output_replay_segment()
    await runtime.publish_output(
        topic="events",
        value="value",
        value_type=str,
        kind=RecordKind.DATA,
        max_publish_latency=timedelta(milliseconds=100),
        max_records=1,
        max_logical_bytes=10_000,
    )
    record = runtime._output_records[0]
    fingerprint = fingerprint_logical_frames([record.frame])
    expected = runtime._output_replay_manifest.topics[0]
    expected.logical_byte_count = fingerprint.logical_byte_count
    expected.logical_fingerprint = fingerprint.digest

    runtime.verify_output_replay()

    assert backend.stage_calls == 0


async def test_replay_ignores_live_capacity_and_does_not_leak_live_policy(
    runtime: WorkflowStreamRuntime,
) -> None:
    expected = SimpleNamespace(
        schema_version=1,
        fingerprint_version=1,
        topics=[
            SimpleNamespace(
                topic="events",
                record_count=1,
                logical_byte_count=0,
                logical_fingerprint=b"",
                finished=False,
            )
        ],
        segments=[SimpleNamespace(record_counts_by_topic=[1])],
    )
    runtime.begin_output_replay(expected)
    runtime.begin_output_replay_segment()
    # The recorded value is larger than today's deliberately tiny live limit.
    # Replay must validate it instead of moving or rejecting the old boundary.
    await runtime.publish_output(
        topic="events",
        value="recorded before the limit changed",
        value_type=str,
        kind=RecordKind.DATA,
        max_publish_latency=timedelta(milliseconds=1),
        max_records=1,
        max_logical_bytes=1,
    )
    record = runtime._output_records[0]
    fingerprint = fingerprint_logical_frames([record.frame])
    expected.topics[0].logical_byte_count = fingerprint.logical_byte_count
    expected.topics[0].logical_fingerprint = fingerprint.digest

    assert runtime.output_max_publish_latency is None
    runtime.verify_output_replay()

    runtime.begin_activation(11)
    await runtime.publish_output(
        topic="events",
        value="live",
        value_type=str,
        kind=RecordKind.DATA,
        max_publish_latency=timedelta(milliseconds=250),
        max_records=10,
        max_logical_bytes=10_000,
    )
    assert runtime.output_max_publish_latency == timedelta(milliseconds=250)


async def test_output_replay_performs_no_io_token_mint_or_live_policy_split(
    runtime: WorkflowStreamRuntime,
    backend: _OutputMemoryBackend,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = SimpleNamespace(
        schema_version=1,
        fingerprint_version=1,
        topics=[
            SimpleNamespace(
                topic="events",
                record_count=2,
                logical_byte_count=0,
                logical_fingerprint=b"",
                finished=False,
            )
        ],
        segments=[SimpleNamespace(record_counts_by_topic=[2])],
    )
    runtime.begin_output_replay(expected)
    runtime.begin_output_replay_segment()
    monkeypatch.setattr(
        "temporalio.contrib.external_workflow_streams._runtime.secrets.token_urlsafe",
        lambda _size: pytest.fail("replay minted an output stage token"),
    )

    # Today's live policy would split after the first record and arm a 1 ms
    # flush. Replay must instead reproduce the one recorded two-record segment.
    for value in ("one", "two"):
        await runtime.publish_output(
            topic="events",
            value=value,
            value_type=str,
            kind=RecordKind.DATA,
            max_publish_latency=timedelta(milliseconds=1),
            max_records=1,
            max_logical_bytes=1,
        )
    fingerprint = fingerprint_logical_frames(
        record.frame for record in runtime._output_records
    )
    expected.topics[0].logical_byte_count = fingerprint.logical_byte_count
    expected.topics[0].logical_fingerprint = fingerprint.digest

    assert runtime._output_stage_token is None
    assert not runtime.output_rollover_requested
    assert runtime.output_max_publish_latency is None
    runtime.verify_output_replay()
    assert backend.stage_calls == 0


class _SizedPayloadCodec(temporalio.converter.PayloadCodec):
    def __init__(self, suffix_size: int) -> None:
        self.suffix_size = suffix_size
        self.encode_calls = 0

    async def encode(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> list[temporalio.api.common.v1.Payload]:
        self.encode_calls += 1
        encoded: list[temporalio.api.common.v1.Payload] = []
        for payload in payloads:
            copied = temporalio.api.common.v1.Payload()
            copied.CopyFrom(payload)
            copied.data += b"x" * self.suffix_size
            encoded.append(copied)
        return encoded

    async def decode(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> list[temporalio.api.common.v1.Payload]:
        return list(payloads)


def _runtime_with_codec(
    backend: _OutputMemoryBackend, codec: temporalio.converter.PayloadCodec
) -> WorkflowStreamRuntime:
    return WorkflowStreamRuntime(
        manager=StreamSubscriptionManager(
            backend=backend,
            notify_ready=_notify,
            watch_block=timedelta(milliseconds=1),
        ),
        backend=backend,
        run_id="run",
        namespace="namespace",
        workflow_id="workflow",
        first_execution_run_id="first-run",
        data_converter=temporalio.converter.DataConverter(payload_codec=codec),
        default_idle_timeout=timedelta(seconds=1),
    )


async def test_replay_keeps_recorded_segments_when_codec_wire_size_changes(
    backend: _OutputMemoryBackend,
) -> None:
    live_codec = _SizedPayloadCodec(1)
    live = _runtime_with_codec(backend, live_codec)
    for value in ("one", "two"):
        live.begin_activation(7)
        await live.publish_output(
            topic="events",
            value=value,
            value_type=str,
            kind=RecordKind.DATA,
            max_publish_latency=timedelta(milliseconds=100),
            max_records=10,
            max_logical_bytes=10_000,
        )
    staged = await live.stage_output(7)
    assert staged.segment_record_counts == ((1,), (1,))
    assert live_codec.encode_calls == 2
    live_wire = backend.stage_payloads[-1]

    replay_codec = _SizedPayloadCodec(100_000)
    replay = _runtime_with_codec(backend, replay_codec)
    topic = staged.topics[0]
    expected = SimpleNamespace(
        schema_version=staged.schema_version,
        fingerprint_version=staged.fingerprint_version,
        topics=[
            SimpleNamespace(
                topic=topic.manifest.stream_key.stream_name,
                record_count=topic.manifest.record_count,
                logical_byte_count=topic.manifest.logical_byte_count,
                logical_fingerprint=topic.manifest.fingerprint,
                finished=topic.finished,
            )
        ],
        segments=[
            SimpleNamespace(record_counts_by_topic=list(counts))
            for counts in staged.segment_record_counts
        ],
    )
    replay.begin_output_replay(expected)
    for value in ("one", "two"):
        replay.begin_output_replay_segment()
        await replay.publish_output(
            topic="events",
            value=value,
            value_type=str,
            kind=RecordKind.DATA,
            max_publish_latency=timedelta(milliseconds=1),
            max_records=1,
            max_logical_bytes=1,
        )

    replay.verify_output_replay()
    assert replay_codec.suffix_size > sum(len(payload) for payload in live_wire)
    assert replay_codec.encode_calls == 0
    assert backend.stage_calls == 1


async def test_one_oversized_logical_record_is_refused_without_poisoning_batch(
    runtime: WorkflowStreamRuntime,
    backend: _OutputMemoryBackend,
) -> None:
    runtime.begin_activation(7)
    with pytest.raises(ExternalStreamCapacityError, match="one record"):
        await runtime.publish_output(
            topic="events",
            value="too large",
            value_type=str,
            kind=RecordKind.DATA,
            max_publish_latency=timedelta(milliseconds=100),
            max_records=10,
            max_logical_bytes=1,
        )

    assert not runtime.has_output
    assert not runtime.output_rollover_requested
    assert backend.stage_calls == 0
    await runtime.publish_output(
        topic="events",
        value="fits the next valid policy",
        value_type=str,
        kind=RecordKind.DATA,
        max_publish_latency=timedelta(milliseconds=100),
        max_records=10,
        max_logical_bytes=10_000,
    )
    assert runtime.has_output


async def test_abandon_output_replay_clears_partial_finish_and_expectations(
    runtime: WorkflowStreamRuntime,
) -> None:
    runtime.begin_output_replay(
        SimpleNamespace(
            schema_version=1,
            fingerprint_version=1,
            topics=[],
            segments=[],
        )
    )
    runtime.begin_output_replay_segment()
    await runtime.publish_output(
        topic="events",
        value=None,
        value_type=str,
        kind=RecordKind.FINISH,
        max_publish_latency=timedelta(milliseconds=1),
        max_records=1,
        max_logical_bytes=1,
    )

    runtime.abandon_output_replay()
    runtime.begin_activation(11)
    await runtime.publish_output(
        topic="events",
        value="live after failed replay",
        value_type=str,
        kind=RecordKind.DATA,
        max_publish_latency=timedelta(milliseconds=250),
        max_records=10,
        max_logical_bytes=10_000,
    )

    assert runtime.has_output
    assert runtime.output_max_publish_latency == timedelta(milliseconds=250)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("max_publish_latency", timedelta(0), "max_publish_latency"),
        ("max_records", 0, "max_records"),
        ("max_logical_bytes", 0, "max_logical_bytes"),
    ],
)
def test_output_options_validate_direct_construction(
    field: str, value: Any, message: str
) -> None:
    kwargs = {field: value}
    with pytest.raises(ValueError, match=message):
        ExternalOutputStreamOptions(**kwargs)


async def test_workflow_output_rejects_read_only_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        temporalio.workflow.unsafe,
        "is_read_only",
        lambda: True,
    )
    topic = ExternalOutputStreamOptions().topic("events", type=str)

    with pytest.raises(
        temporalio.workflow.ReadOnlyContextError, match="external output"
    ):
        await topic.publish("not allowed from a query")


class _OutputOnlyReplayRuntime:
    def __init__(self) -> None:
        self.events: list[str] = []

    def begin_output_replay(self, _manifest: Any) -> None:
        self.events.append("begin-output")

    def take_replay_plan(self) -> None:
        return None

    def begin_output_replay_segment(self) -> None:
        self.events.append("segment")

    def resolve_all_pending(self) -> None:
        self.events.append("resolve")


class _OutputOnlyReplayDriver:
    def __init__(self, runtime: _OutputOnlyReplayRuntime) -> None:
        self._external_stream_runtime = runtime
        self._pending_output_replay_finish = False

    def _run_once(self, *, check_conditions: bool) -> None:
        assert check_conditions
        self._external_stream_runtime.events.append("drain")


def test_output_only_replay_drives_all_but_the_trailing_segment() -> None:
    runtime = _OutputOnlyReplayRuntime()
    driver = _OutputOnlyReplayDriver(runtime)
    output = SimpleNamespace(segments=[object(), object(), object()])
    job = SimpleNamespace(
        output=output,
        HasField=lambda field: field == "output",
    )

    apply_replay = cast(Any, _WorkflowInstanceImpl._apply_replay_external_streams)
    apply_replay(driver, job)

    assert runtime.events == [
        "begin-output",
        "segment",
        "resolve",
        "drain",
        "segment",
        "resolve",
        "drain",
        "segment",
        "resolve",
    ]
    assert driver._pending_output_replay_finish


class _CombinedReplayRuntime:
    def __init__(self) -> None:
        self.events: list[str] = []
        self._plan: Any = SimpleNamespace(
            annotation=SimpleNamespace(header=SimpleNamespace(streams={1: object()})),
            segments=tuple(
                SimpleNamespace(deliveries=((1, index),)) for index in range(3)
            ),
            committed_boundaries={1: BEGINNING},
        )

    def begin_output_replay(self, _manifest: Any) -> None:
        self.events.append("begin-output")

    def take_replay_plan(self) -> Any:
        plan, self._plan = self._plan, None
        return plan

    def begin_replay(self, _streams: Any) -> None:
        self.events.append("begin-input")

    def begin_output_replay_segment(self) -> None:
        self.events.append("output-segment")

    def begin_replay_segment(self, _deliveries: Any) -> None:
        self.events.append("input-segment")

    def resolve_all_pending(self) -> None:
        self.events.append("resolve")

    def verify_replay_consumed(self) -> None:
        self.events.append("verify-input")

    def reposition_after_replay(self, _boundaries: Any) -> None:
        self.events.append("reposition-input")

    def verify_output_replay(self) -> None:
        self.events.append("verify-output")

    def abandon_output_replay(self) -> None:
        self.events.append("abandon-output")

    def end_replay(self) -> None:
        self.events.append("end-input")


class _CombinedReplayDriver:
    def __init__(self, runtime: _CombinedReplayRuntime) -> None:
        self._external_stream_runtime = runtime
        self._pending_replay_finish: Any = None
        self._pending_output_replay_finish = False

    def _run_once(self, *, check_conditions: bool) -> None:
        assert check_conditions
        self._external_stream_runtime.events.append("drain")


def test_input_and_output_share_one_replay_segment_drain_schedule() -> None:
    runtime = _CombinedReplayRuntime()
    driver = _CombinedReplayDriver(runtime)
    output = SimpleNamespace(segments=(object(), object(), object()))
    job = SimpleNamespace(
        output=output,
        HasField=lambda field: field == "output",
    )
    apply_replay = cast(Any, _WorkflowInstanceImpl._apply_replay_external_streams)
    finish_replay = cast(Any, _WorkflowInstanceImpl._finish_replay_external_streams)

    apply_replay(driver, job)
    driver._run_once(check_conditions=True)
    finish_replay(driver)

    assert runtime.events == [
        "begin-output",
        "begin-input",
        "output-segment",
        "input-segment",
        "resolve",
        "drain",
        "output-segment",
        "input-segment",
        "resolve",
        "drain",
        "output-segment",
        "input-segment",
        "resolve",
        "drain",
        "verify-input",
        "reposition-input",
        "verify-output",
        "abandon-output",
        "end-input",
    ]
