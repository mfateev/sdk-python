"""Focused client and pending-stage reconciliation tests for output streams."""

from __future__ import annotations

from collections import deque
from collections.abc import AsyncIterator, Sequence
from types import SimpleNamespace
from typing import Any

import pytest

import temporalio.api.history.v1
import temporalio.converter
import temporalio.service
from temporalio.contrib.external_workflow_streams import _output_client
from temporalio.contrib.external_workflow_streams._backend import (
    StreamDirection,
    StreamKey,
)
from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec
from temporalio.contrib.external_workflow_streams._errors import (
    StreamDecodeError,
    StreamIntegrityError,
    StreamStorageError,
)
from temporalio.contrib.external_workflow_streams._output_backend import (
    OutputReadResult,
    OutputStage,
    OutputStageManifest,
    OutputStageStatus,
    OutputStreamBackend,
    OutputStreamRecord,
    PendingOutputBarrier,
    StagedOutputRecord,
)
from temporalio.contrib.external_workflow_streams._output_client import (
    ExternalOutputStreamClient,
    ExternalOutputStreamClientTopic,
)
from temporalio.contrib.external_workflow_streams._producer import WorkflowChainKey
from temporalio.contrib.external_workflow_streams._record import (
    AFTER,
    BEGINNING,
    Cursor,
    Offset,
    RecordKind,
    StreamRecord,
)


class FakeOutputBackend(OutputStreamBackend):
    guarantees_immutability = True
    provider_id = "fake-output"
    provider_format_version = 1

    def __init__(self) -> None:
        self.read_results: deque[OutputReadResult | BaseException] = deque()
        self.read_cursors: list[Cursor] = []
        self.tail = BEGINNING
        self.tail_calls = 0
        self.stages: dict[OutputStageManifest, OutputStage] = {}
        self.committed: list[OutputStageManifest] = []
        self.aborted: list[OutputStageManifest] = []

    async def stage_output(
        self,
        manifest: OutputStageManifest,
        records: Sequence[StagedOutputRecord],
    ) -> OutputStage:
        raise NotImplementedError

    async def commit_output(self, manifest: OutputStageManifest) -> OutputStage:
        self.committed.append(manifest)
        stage = self.stages[manifest]
        resolved = OutputStage(manifest, stage.records, OutputStageStatus.COMMITTED)
        self.stages[manifest] = resolved
        return resolved

    async def abort_output(self, manifest: OutputStageManifest) -> OutputStage:
        self.aborted.append(manifest)
        stage = self.stages[manifest]
        resolved = OutputStage(manifest, stage.records, OutputStageStatus.ABORTED)
        self.stages[manifest] = resolved
        return resolved

    async def output_stage(self, manifest: OutputStageManifest) -> OutputStage | None:
        return self.stages.get(manifest)

    async def append_output(
        self, key: StreamKey, record: StreamRecord
    ) -> OutputStreamRecord:
        raise NotImplementedError

    async def read_output_after(
        self,
        key: StreamKey,
        after: Cursor,
        *,
        max_records: int,
        block: Any = None,
    ) -> OutputReadResult:
        assert key.direction is StreamDirection.OUTPUT
        assert max_records > 0
        self.read_cursors.append(after)
        result = self.read_results.popleft()
        if isinstance(result, BaseException):
            raise result
        return result

    async def output_tail(self, key: StreamKey) -> Cursor:
        assert key.direction is StreamDirection.OUTPUT
        self.tail_calls += 1
        return self.tail

    def compare_offsets(self, left: Offset, right: Offset) -> int:
        return int(left.token) - int(right.token)


class FakeHandle:
    def __init__(
        self,
        *,
        first_run_id: str,
        events: Sequence[temporalio.api.history.v1.HistoryEvent] = (),
        history_error: BaseException | None = None,
    ) -> None:
        self._first_run_id = first_run_id
        self._events = events
        self._history_error = history_error

    async def describe(self) -> Any:
        return SimpleNamespace(
            raw_description=SimpleNamespace(
                workflow_execution_info=SimpleNamespace(first_run_id=self._first_run_id)
            )
        )

    def fetch_history_events(self) -> AsyncIterator[Any]:
        async def iterate() -> AsyncIterator[Any]:
            if self._history_error is not None:
                raise self._history_error
            for event in self._events:
                yield event

        return iterate()


class FakeClient:
    namespace = "ns"
    data_converter = temporalio.converter.DataConverter.default

    def __init__(self) -> None:
        self.events: Sequence[temporalio.api.history.v1.HistoryEvent] = ()
        self.history_error: BaseException | None = None
        self.history_run_ids: list[str] = []

    def get_workflow_handle(
        self, workflow_id: str, *, run_id: str | None = None
    ) -> FakeHandle:
        assert workflow_id == "wf"
        if run_id is not None:
            self.history_run_ids.append(run_id)
        return FakeHandle(
            first_run_id="first-run",
            events=self.events if run_id is not None else (),
            history_error=self.history_error if run_id is not None else None,
        )


CHAIN = WorkflowChainKey("ns", "wf", "first-run")
OUTPUT_KEY = CHAIN.stream_key("events", direction=StreamDirection.OUTPUT)


def history_event(event_id: int, attributes: str) -> Any:
    event = temporalio.api.history.v1.HistoryEvent(event_id=event_id)
    getattr(event, attributes).SetInParent()
    return event


def manifest(*, floor: int = 1, token: str = "stage-1") -> OutputStageManifest:
    return OutputStageManifest(
        stream_key=OUTPUT_KEY,
        provider_id="fake-output",
        provider_format_version=1,
        stage_token=token,
        run_id="current-run",
        history_floor_event_id=floor,
        sub_batch_id=0,
        fingerprint_version=1,
        fingerprint=b"f" * 32,
        record_count=1,
        logical_byte_count=10,
    )


def output_record(
    offset: int, payload: bytes = b"", *, kind: RecordKind = RecordKind.DATA
) -> OutputStreamRecord:
    return OutputStreamRecord(kind=kind, payload=payload, offset=Offset(str(offset)))


async def connected(
    backend: FakeOutputBackend, client: FakeClient
) -> ExternalOutputStreamClient:
    return await ExternalOutputStreamClient.connect(
        backend=backend,
        workflow=CHAIN,
        client=client,  # type: ignore[arg-type]
    )


async def test_committed_read_is_typed_resumable_and_backend_only() -> None:
    backend = FakeOutputBackend()
    client = FakeClient()
    codec = StreamPayloadCodec(temporalio.converter.DataConverter.default, str)
    first = output_record(4, await codec.encode("first"))
    second = output_record(5, await codec.encode("second"))
    finish = output_record(6, kind=RecordKind.FINISH)
    backend.read_results.extend(
        [
            OutputReadResult((first,)),
            OutputReadResult((second, finish)),
        ]
    )

    topic = (await connected(backend, client)).topic("events", type=str)
    items = [item async for item in topic.subscribe(after=AFTER(Offset("3")))]

    assert [(item.data, item.offset) for item in items] == [
        ("first", Offset("4")),
        ("second", Offset("5")),
    ]
    assert backend.read_cursors == [AFTER(Offset("3")), AFTER(Offset("4"))]
    assert client.history_run_ids == []


async def test_an_item_offset_can_be_passed_directly_as_the_resume_cursor() -> None:
    backend = FakeOutputBackend()
    client = FakeClient()
    backend.read_results.append(
        OutputReadResult((output_record(4, kind=RecordKind.FINISH),))
    )

    topic: ExternalOutputStreamClientTopic[Any] = (
        await connected(backend, client)
    ).topic("events")
    assert [item async for item in topic.subscribe(after=Offset("3"))] == []

    assert backend.read_cursors == [AFTER(Offset("3"))]


async def test_tail_is_the_backend_committed_boundary_without_history() -> None:
    backend = FakeOutputBackend()
    backend.tail = AFTER(Offset("12"))
    client = FakeClient()

    topic: ExternalOutputStreamClientTopic[Any] = (
        await connected(backend, client)
    ).topic("events")

    assert await topic.tail() == AFTER(Offset("12"))
    assert backend.tail_calls == 1
    assert client.history_run_ids == []


async def test_pending_token_in_exact_run_history_commits_before_read_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = FakeOutputBackend()
    client = FakeClient()
    staged = manifest()
    placed = output_record(10, b"payload")
    backend.stages[staged] = OutputStage(staged, (placed,), OutputStageStatus.PENDING)
    barrier = PendingOutputBarrier(staged, Offset("10"))
    client.events = (
        history_event(1, "workflow_execution_started_event_attributes"),
        history_event(2, "workflow_task_completed_event_attributes"),
        history_event(3, "marker_recorded_event_attributes"),
    )
    monkeypatch.setattr(
        _output_client,
        "_event_has_output_stage_token",
        lambda event, token: event.event_id == 3 and token == "stage-1",
    )

    topic: ExternalOutputStreamClientTopic[Any] = (
        await connected(backend, client)
    ).topic("events")

    assert await topic._resolve_pending(barrier)
    assert backend.committed == [staged]
    assert backend.aborted == []
    assert client.history_run_ids == ["current-run"]


async def test_repeating_the_same_history_reconciliation_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = FakeOutputBackend()
    client = FakeClient()
    staged = manifest()
    placed = output_record(10, b"payload")
    backend.stages[staged] = OutputStage(staged, (placed,), OutputStageStatus.PENDING)
    barrier = PendingOutputBarrier(staged, placed.offset)
    client.events = (
        history_event(1, "workflow_execution_started_event_attributes"),
        history_event(2, "workflow_task_completed_event_attributes"),
        history_event(3, "marker_recorded_event_attributes"),
    )
    monkeypatch.setattr(
        _output_client,
        "_event_has_output_stage_token",
        lambda event, token: event.event_id == 3 and token == "stage-1",
    )
    topic: ExternalOutputStreamClientTopic[Any] = (
        await connected(backend, client)
    ).topic("events")

    assert await topic._resolve_pending(barrier)
    assert await topic._resolve_pending(barrier)
    assert backend.committed == [staged, staged]
    assert backend.stages[staged].status is OutputStageStatus.COMMITTED


def test_real_shared_marker_envelope_exposes_the_output_stage_token() -> None:
    from temporalio.bridge.proto.external_data import ExternalStreamMarkerData

    marker = ExternalStreamMarkerData()
    marker.output.stage_token = "stage-1"
    event = history_event(3, "marker_recorded_event_attributes")
    attributes = event.marker_recorded_event_attributes
    attributes.marker_name = "core_external_stream"
    attributes.details[
        "external_stream"
    ].payloads.add().data = marker.SerializeToString()

    assert _output_client._event_has_output_stage_token(event, "stage-1")
    assert not _output_client._event_has_output_stage_token(event, "another-stage")


async def test_subscribe_stops_at_barrier_and_resumes_from_committed_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = FakeOutputBackend()
    client = FakeClient()
    codec = StreamPayloadCodec(temporalio.converter.DataConverter.default, str)
    prefix = output_record(5, await codec.encode("before"))
    staged_record = output_record(10, await codec.encode("staged"))
    finish = output_record(11, kind=RecordKind.FINISH)
    staged = manifest()
    backend.stages[staged] = OutputStage(
        staged, (staged_record,), OutputStageStatus.PENDING
    )
    backend.read_results.extend(
        [
            OutputReadResult(
                (prefix,), PendingOutputBarrier(staged, staged_record.offset)
            ),
            OutputReadResult((staged_record, finish)),
        ]
    )
    client.events = (
        history_event(1, "workflow_execution_started_event_attributes"),
        history_event(2, "workflow_task_completed_event_attributes"),
        history_event(3, "marker_recorded_event_attributes"),
    )
    monkeypatch.setattr(
        _output_client,
        "_event_has_output_stage_token",
        lambda event, token: event.event_id == 3 and token == "stage-1",
    )
    topic = (await connected(backend, client)).topic("events", type=str)

    items = [item async for item in topic.subscribe()]

    assert [item.data for item in items] == ["before", "staged"]
    assert backend.read_cursors == [BEGINNING, AFTER(prefix.offset)]
    assert backend.committed == [staged]


async def test_first_task_boundary_without_token_aborts_pending_stage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = FakeOutputBackend()
    client = FakeClient()
    staged = manifest()
    backend.stages[staged] = OutputStage(
        staged, (output_record(10),), OutputStageStatus.PENDING
    )
    client.events = (
        history_event(1, "workflow_execution_started_event_attributes"),
        history_event(2, "workflow_task_failed_event_attributes"),
    )
    monkeypatch.setattr(
        _output_client,
        "_event_has_output_stage_token",
        lambda event, token: False,
    )

    topic: ExternalOutputStreamClientTopic[Any] = (
        await connected(backend, client)
    ).topic("events")

    assert await topic._resolve_pending(PendingOutputBarrier(staged, Offset("10")))
    assert backend.aborted == [staged]
    assert backend.committed == []


async def test_no_durable_boundary_leaves_the_stage_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = FakeOutputBackend()
    client = FakeClient()
    staged = manifest()
    backend.stages[staged] = OutputStage(
        staged, (output_record(10),), OutputStageStatus.PENDING
    )
    client.events = (
        history_event(1, "workflow_execution_started_event_attributes"),
        history_event(2, "workflow_task_scheduled_event_attributes"),
        history_event(3, "workflow_task_started_event_attributes"),
    )
    monkeypatch.setattr(
        _output_client,
        "_event_has_output_stage_token",
        lambda event, token: False,
    )

    topic: ExternalOutputStreamClientTopic[Any] = (
        await connected(backend, client)
    ).topic("events")

    assert not await topic._resolve_pending(PendingOutputBarrier(staged, Offset("10")))
    assert backend.committed == [] and backend.aborted == []


async def test_disconnected_speculative_update_stays_pending_until_history_loss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Elapsed retries cannot guess the outcome of an unreported Update task."""
    backend = FakeOutputBackend()
    client = FakeClient()
    staged = manifest()
    placed = output_record(10, b"speculative-update-output")
    backend.stages[staged] = OutputStage(staged, (placed,), OutputStageStatus.PENDING)
    barrier = PendingOutputBarrier(staged, placed.offset)

    # The admitted Update exists durably, but there is no deciding Workflow
    # Task terminal event above the exact floor. This is the shape left by a
    # client disconnect while the Update's speculative task is still in flight.
    client.events = (
        history_event(1, "workflow_execution_started_event_attributes"),
        history_event(2, "workflow_execution_update_admitted_event_attributes"),
        history_event(3, "workflow_task_scheduled_event_attributes"),
        history_event(4, "workflow_task_started_event_attributes"),
    )
    monkeypatch.setattr(
        _output_client,
        "_event_has_output_stage_token",
        lambda event, token: False,
    )
    topic: ExternalOutputStreamClientTopic[Any] = (
        await connected(backend, client)
    ).topic("events")

    assert not await topic._resolve_pending(barrier)
    assert not await topic._resolve_pending(barrier)
    assert backend.stages[staged].status is OutputStageStatus.PENDING
    assert backend.committed == [] and backend.aborted == []
    assert client.history_run_ids == ["current-run", "current-run"]

    # Once retention removes that exact Run, lack of evidence becomes an
    # integrity failure. It still must not be guessed into an abort.
    client.history_error = temporalio.service.RPCError(
        "history expired",
        temporalio.service.RPCStatusCode.NOT_FOUND,
        b"",
    )
    with pytest.raises(StreamIntegrityError, match="no longer available"):
        await topic._resolve_pending(barrier)
    assert backend.stages[staged].status is OutputStageStatus.PENDING
    assert backend.committed == [] and backend.aborted == []


async def test_history_loss_is_integrity_but_an_outage_is_storage() -> None:
    backend = FakeOutputBackend()
    client = FakeClient()
    topic: ExternalOutputStreamClientTopic[Any] = (
        await connected(backend, client)
    ).topic("events")

    client.events = (history_event(2, "workflow_task_completed_event_attributes"),)
    with pytest.raises(StreamIntegrityError, match="exact floor event"):
        await topic._history_decision(manifest())

    client.history_error = temporalio.service.RPCError(
        "server unavailable",
        temporalio.service.RPCStatusCode.UNAVAILABLE,
        b"",
    )
    with pytest.raises(StreamStorageError, match="Temporal History"):
        await topic._history_decision(manifest())

    client.history_error = temporalio.service.RPCError(
        "history expired",
        temporalio.service.RPCStatusCode.NOT_FOUND,
        b"",
    )
    with pytest.raises(StreamIntegrityError, match="no longer available"):
        await topic._history_decision(manifest())


async def test_provider_cannot_return_a_record_beyond_pending_barrier() -> None:
    backend = FakeOutputBackend()
    client = FakeClient()
    staged = manifest()
    backend.read_results.append(
        OutputReadResult(
            (output_record(11),),
            PendingOutputBarrier(staged, Offset("10")),
        )
    )
    topic: ExternalOutputStreamClientTopic[Any] = (
        await connected(backend, client)
    ).topic("events")

    with pytest.raises(StreamIntegrityError, match="pending barrier"):
        await anext(topic.subscribe())
    assert client.history_run_ids == []


async def test_backend_and_decode_failures_keep_the_existing_taxonomy() -> None:
    backend = FakeOutputBackend()
    client = FakeClient()
    topic = (await connected(backend, client)).topic("events", type=str)
    backend.read_results.append(ConnectionError("backend down"))

    with pytest.raises(StreamStorageError, match="backend down"):
        await anext(topic.subscribe())

    backend.read_results.append(OutputReadResult((output_record(1, b"not-proto"),)))
    with pytest.raises(StreamDecodeError):
        await anext(topic.subscribe())
