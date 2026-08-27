"""Direct Activity/external producer tests for output streams."""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Sequence
from typing import Any

import pytest

import temporalio.api.common.v1
import temporalio.converter
from temporalio.contrib.external_workflow_streams._backend import (
    AppendConflictError,
    StreamDirection,
    StreamKey,
)
from temporalio.contrib.external_workflow_streams._output_backend import (
    OutputReadResult,
    OutputStage,
    OutputStageManifest,
    OutputStreamBackend,
    OutputStreamRecord,
    StagedOutputRecord,
)
from temporalio.contrib.external_workflow_streams._output_producer import (
    ExternalOutputStreamProducer,
    OutputAppendNotAcknowledgedError,
)
from temporalio.contrib.external_workflow_streams._producer import WorkflowChainKey
from temporalio.contrib.external_workflow_streams._record import (
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
        self.append_calls: list[tuple[StreamKey, StreamRecord]] = []
        self.stored: dict[
            tuple[StreamKey, str, int], tuple[StreamRecord, OutputStreamRecord]
        ] = {}
        self.fail_before_store_once = False
        self.fail_after_store_once = False
        self.block_data = False
        self.data_append_started = asyncio.Event()
        self.release_data_append = asyncio.Event()
        self._next_offset = 1

    async def stage_output(
        self,
        manifest: OutputStageManifest,
        records: Sequence[StagedOutputRecord],
    ) -> OutputStage:
        raise NotImplementedError

    async def commit_output(self, manifest: OutputStageManifest) -> OutputStage:
        raise NotImplementedError

    async def abort_output(self, manifest: OutputStageManifest) -> OutputStage:
        raise NotImplementedError

    async def output_stage(self, manifest: OutputStageManifest) -> OutputStage | None:
        raise NotImplementedError

    async def append_output(
        self, key: StreamKey, record: StreamRecord
    ) -> OutputStreamRecord:
        assert key.direction is StreamDirection.OUTPUT
        self.append_calls.append((key, record))
        if self.fail_before_store_once:
            self.fail_before_store_once = False
            raise ConnectionError("append outcome unknown before reply")

        identity = (key, record.producer_session_id, record.sequence)
        existing = self.stored.get(identity)
        if existing is not None:
            original, placed = existing
            if original != record:
                raise AppendConflictError(record.idempotency_key)
            return placed

        if self.block_data and record.kind is RecordKind.DATA:
            self.data_append_started.set()
            await self.release_data_append.wait()
        placed = OutputStreamRecord(
            kind=record.kind,
            payload=record.payload,
            offset=Offset(str(self._next_offset)),
        )
        self._next_offset += 1
        self.stored[identity] = (record, placed)
        if self.fail_after_store_once:
            self.fail_after_store_once = False
            raise ConnectionError("append committed but acknowledgement was lost")
        return placed

    async def read_output_after(
        self,
        key: StreamKey,
        after: Cursor,
        *,
        max_records: int,
        block: Any = None,
    ) -> OutputReadResult:
        raise NotImplementedError

    async def output_tail(self, key: StreamKey) -> Cursor:
        return BEGINNING

    def compare_offsets(self, left: Offset, right: Offset) -> int:
        return int(left.token) - int(right.token)


CHAIN = WorkflowChainKey("ns", "wf", "first-run")


def producer(
    backend: FakeOutputBackend,
    *,
    data_converter: temporalio.converter.DataConverter = temporalio.converter.DataConverter.default,
) -> ExternalOutputStreamProducer:
    return ExternalOutputStreamProducer(
        backend=backend,
        workflow=CHAIN,
        data_converter=data_converter,
        session_id="activity-session",
    )


async def test_publish_uses_output_identity_and_workflow_serialization_context() -> (
    None
):
    observed: list[temporalio.converter.SerializationContext | None] = []

    class RecordingCodec(
        temporalio.converter.PayloadCodec,
        temporalio.converter.WithSerializationContext,
    ):
        def __init__(
            self,
            context: temporalio.converter.SerializationContext | None = None,
        ) -> None:
            self.context = context

        def with_context(
            self, context: temporalio.converter.SerializationContext
        ) -> RecordingCodec:
            return RecordingCodec(context)

        async def encode(
            self, payloads: Sequence[temporalio.api.common.v1.Payload]
        ) -> list[temporalio.api.common.v1.Payload]:
            observed.append(self.context)
            return list(payloads)

        async def decode(
            self, payloads: Sequence[temporalio.api.common.v1.Payload]
        ) -> list[temporalio.api.common.v1.Payload]:
            return list(payloads)

    backend = FakeOutputBackend()
    output = producer(
        backend,
        data_converter=temporalio.converter.DataConverter(
            payload_codec=RecordingCodec()
        ),
    )

    placed = await output.topic("events", type=str).publish("hello")

    key, record = backend.append_calls[0]
    assert key.direction is StreamDirection.OUTPUT
    assert key.stream_name == "events"
    assert record.kind is RecordKind.DATA
    assert record.producer_session_id == "activity-session"
    assert record.sequence == 0
    assert placed.offset == Offset("1")
    assert observed == [
        temporalio.converter.WorkflowSerializationContext(
            namespace="ns", workflow_id="wf"
        )
    ]


async def test_finish_waits_for_preceding_publish_and_closes_all_topic_handles() -> (
    None
):
    backend = FakeOutputBackend()
    backend.block_data = True
    output = producer(backend)
    topic = output.topic("events", type=str)
    other_handle = output.topic("events", type=str)

    publish_task = asyncio.create_task(topic.publish("first"))
    await backend.data_append_started.wait()
    finish_task = asyncio.create_task(other_handle.finish_writing())
    backend.release_data_append.set()
    await asyncio.gather(publish_task, finish_task)

    records = [record for _, record in backend.append_calls]
    assert [record.kind for record in records] == [
        RecordKind.DATA,
        RecordKind.FINISH,
    ]
    assert [record.sequence for record in records] == [0, 1]
    assert records[1].payload == b""
    with pytest.raises(RuntimeError, match="already finished"):
        await topic.publish("too late")
    with pytest.raises(RuntimeError, match="already finished"):
        await other_handle.finish_writing()


async def test_ambiguous_append_retries_exact_record_without_duplicate() -> None:
    backend = FakeOutputBackend()
    backend.fail_after_store_once = True
    topic = producer(backend).topic("events", type=str)

    with pytest.raises(OutputAppendNotAcknowledgedError) as unknown:
        await topic.publish("once")
    with pytest.raises(OutputAppendNotAcknowledgedError) as blocked:
        await topic.publish("must not draw another sequence")
    assert blocked.value.record == unknown.value.record

    recovered = await topic.resolve_append(unknown.value.record)
    following = await topic.publish("after recovery")

    assert recovered.offset == Offset("1")
    assert following.offset == Offset("2")
    assert len(backend.stored) == 2
    assert [record.sequence for _, record in backend.append_calls] == [0, 0, 1]


async def test_conflict_while_resolving_is_definitive_not_another_unknown() -> None:
    backend = FakeOutputBackend()
    backend.fail_before_store_once = True
    topic = producer(backend).topic("events", type=str)

    with pytest.raises(OutputAppendNotAcknowledgedError) as unknown:
        await topic.publish("original")
    conflicting = dataclasses.replace(unknown.value.record, payload=b"different")
    await backend.append_output(unknown.value.stream_key, conflicting)

    with pytest.raises(AppendConflictError):
        await topic.resolve_append(unknown.value.record)

    # The provider answered definitively, so the failed recovery is no longer
    # reported as an unknown operation on every later call.
    placed = await topic.publish("next")
    assert placed.offset == Offset("2")
