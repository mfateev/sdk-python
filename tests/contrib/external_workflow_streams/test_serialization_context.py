"""A stream record is converted with the same context as every other payload.

A ``DataConverter`` may carry a codec or a payload converter that derives what
it does from *which Workflow* the payload belongs to -- an encryption key per
Workflow ID is the reason ``WithSerializationContext`` exists at all. The Worker
already binds a :py:class:`~temporalio.converter.WorkflowSerializationContext`
around every payload an activation carries, on both halves: ``decode_activation``
on the Worker's loop and the Workflow instance's own converters on the Workflow
thread.

A stream record is a payload like any other and has to arrive the same way. If
it does not, a per-Workflow key decrypts nothing: the record fails to decode,
the error travels with it, and the Workflow Task is retried forever against a
record that will never decode -- reported as a converter mismatch when the
configuration is in fact correct.

The producer is half of the same statement, and the more dangerous half. Both
sides being context-*free* is self-consistent and works; fixing only the
consumer would break every deployment that works today, because the producer
would encrypt with no context while the consumer decrypts with a
Workflow-derived key. These tests therefore pin both sides to the same context.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Sequence
from datetime import timedelta

import pytest

import temporalio.api.common.v1
import temporalio.converter
from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.external_workflow_streams._backend import StreamKey
from temporalio.contrib.external_workflow_streams._producer import (
    ExternalStreamProducer,
    WorkflowChainKey,
)
from temporalio.contrib.external_workflow_streams._record import (
    BEGINNING,
    RecordKind,
    StreamRecord,
)
from temporalio.converter import (
    DataConverter,
    DefaultPayloadConverter,
    PayloadCodec,
    SerializationContext,
    WithSerializationContext,
    WorkflowSerializationContext,
)
from temporalio.worker import Worker
from tests.contrib.external_workflow_streams.memory_backend import MemoryStreamBackend

with workflow.unsafe.imports_passed_through():
    from temporalio.contrib.external_workflow_streams._api import external_stream


#: Marks the one payload these tests care about. Every other payload an
#: activation carries -- the Workflow's own argument, its result -- travels
#: through the same converter, and the Worker's handling of those is not what is
#: under test here.
_SENTINEL = b"context-probe"

#: Contexts the codec saw, in the order it saw them. Module-level because the
#: converter is constructed by the Worker, not by the test.
_codec_contexts: list[SerializationContext | None] = []

#: Contexts the payload converter saw while converting a stream record. Written
#: from the Workflow thread, which is the half a codec-only probe cannot reach.
_converter_contexts: list[SerializationContext | None] = []


class ContextRequiredCodec(PayloadCodec, WithSerializationContext):
    """A codec that cannot decode a stream record without a context.

    Deliberately *fails* rather than merely recording: a codec keyed on the
    Workflow ID has nothing to decrypt with when it is handed no context, and a
    probe that succeeded either way would pass against a converter that was
    never bound at all.

    Only the sentinel payload is treated specially, so the Worker's own
    ``decode_activation`` work -- which is separately context-bound already --
    contributes no observations of its own.
    """

    def __init__(self, context: SerializationContext | None = None) -> None:
        self.context = context

    def with_context(self, context: SerializationContext) -> ContextRequiredCodec:
        return ContextRequiredCodec(context)

    async def encode(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> list[temporalio.api.common.v1.Payload]:
        return list(payloads)

    async def decode(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> list[temporalio.api.common.v1.Payload]:
        for payload in payloads:
            if _SENTINEL not in payload.data:
                continue
            # Recorded before the refusal, so the assertion below can name the
            # context that was actually supplied instead of only reporting that
            # nothing worked.
            _codec_contexts.append(self.context)
            if self.context is None:
                raise RuntimeError(
                    "a stream record was decoded with no serialization context"
                )
        return list(payloads)


class ContextRequiredPayloadConverter(
    DefaultPayloadConverter, WithSerializationContext
):
    """The same refusal, on the synchronous half that runs on the Workflow thread.

    The codec probe cannot reach here: ``prepare()`` runs the codec on the
    Worker's loop and ``convert()`` runs the payload converter inside
    ``activate()``, and the two halves are handed converters from two different
    places. A fix applied to only one of them leaves the other context-free.
    """

    def __init__(self, context: SerializationContext | None = None) -> None:
        super().__init__()
        self.context = context

    def with_context(
        self, context: SerializationContext
    ) -> ContextRequiredPayloadConverter:
        return ContextRequiredPayloadConverter(context)

    def from_payloads(
        self,
        payloads: Sequence[temporalio.api.common.v1.Payload],
        type_hints: list[type] | None = None,
    ) -> list[object]:
        for payload in payloads:
            if _SENTINEL not in payload.data:
                continue
            _converter_contexts.append(self.context)
            if self.context is None:
                raise RuntimeError(
                    "a stream record was converted with no serialization context"
                )
        return super().from_payloads(payloads, type_hints)


@workflow.defn
class CountProbeTokensWorkflow:
    """Consumes records and returns only how many, never the values."""

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


@pytest.fixture
def backend() -> MemoryStreamBackend:
    return MemoryStreamBackend()


async def _await_observation(seen: list[SerializationContext | None]) -> None:
    """Waits for the probe to fire, so a missing context is not read as a hang.

    A context-free decode raises, the error travels with the record, and the
    Workflow Task is retried forever -- so waiting on the Workflow's *result*
    would turn a wrong context into a timeout minutes later. The context is
    observable before any of that, which is what makes the assertion sharp.
    """
    for _ in range(200):
        if seen:
            return
        await asyncio.sleep(0.1)


async def _publish_probe_record(
    backend: MemoryStreamBackend, key: StreamKey, converter: DataConverter
) -> None:
    from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec

    codec = StreamPayloadCodec(converter, str)
    await backend.append(
        key,
        StreamRecord(RecordKind.DATA, await codec.encode(_SENTINEL.decode()), "p", 0),
    )


async def _stream_key(client: Client, handle) -> StreamKey:  # type: ignore[no-untyped-def]
    description = await handle.describe()
    return StreamKey(
        client.namespace,
        handle.id,
        description.raw_description.workflow_execution_info.first_run_id,
        "tokens",
    )


# --- the consumer -----------------------------------------------------------


async def test_a_stream_records_codec_runs_with_the_consuming_workflows_context(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """The asynchronous half, on the Worker's loop.

    The manager prepares records for every Run on the Worker, so it cannot hold
    a bound converter of its own; the context has to come from the record's own
    subscription. Getting it from the manager's construction instead would
    decode one Workflow's records under another Workflow's key.
    """
    _codec_contexts.clear()
    config = client.config()
    config["data_converter"] = DataConverter(payload_codec=ContextRequiredCodec())
    probe_client = Client(**config)

    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        probe_client,
        task_queue=task_queue,
        workflows=[CountProbeTokensWorkflow],
        external_stream_backends={"tokens-memory": backend},
    ):
        handle = await probe_client.start_workflow(
            CountProbeTokensWorkflow.run,
            1,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        key = await _stream_key(client, handle)
        await _publish_probe_record(backend, key, DataConverter.default)
        await _await_observation(_codec_contexts)

        assert _codec_contexts == [
            WorkflowSerializationContext(
                namespace=client.namespace, workflow_id=handle.id
            )
        ], (
            "the stream record's codec ran with the wrong serialization "
            "context; a codec keyed on the Workflow decrypts nothing here, "
            "while every other payload in the same activation is bound"
        )
        assert await asyncio.wait_for(handle.result(), 30) == 1


async def test_a_stream_records_conversion_runs_with_the_consuming_workflows_context(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """The synchronous half, inside ``activate()``.

    ``convert()`` runs on the Workflow thread through the runtime's converter,
    which is a different object from the manager's. The Workflow's own argument
    is converted with a bound converter on this very thread, so a record that is
    not is inconsistent with the activation it arrived in.
    """
    _converter_contexts.clear()
    config = client.config()
    config["data_converter"] = DataConverter(
        payload_converter_class=ContextRequiredPayloadConverter
    )
    probe_client = Client(**config)

    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        probe_client,
        task_queue=task_queue,
        workflows=[CountProbeTokensWorkflow],
        external_stream_backends={"tokens-memory": backend},
    ):
        handle = await probe_client.start_workflow(
            CountProbeTokensWorkflow.run,
            1,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        key = await _stream_key(client, handle)
        await _publish_probe_record(backend, key, DataConverter.default)
        await _await_observation(_converter_contexts)

        assert _converter_contexts[:1] == [
            WorkflowSerializationContext(
                namespace=client.namespace, workflow_id=handle.id
            )
        ], (
            "the stream record was converted on the Workflow thread with the "
            "wrong serialization context, although the Workflow's own argument "
            "was converted with the right one on the same thread"
        )
        assert await asyncio.wait_for(handle.result(), 30) == 1


async def test_a_replayed_record_is_prepared_with_the_recorded_streams_context(
    backend: MemoryStreamBackend,
) -> None:
    """Replay prepares from the annotation, and must reach the same context.

    A replayed record never passes a subscription: the Workflow has not run far
    enough to have called ``subscribe()`` again, so the live path's source for
    the context does not exist yet. The annotation's own header carries the
    stream key, which is why it records it -- and a replay prepared without it
    would decode nothing on exactly the Runs that a Worker restart produces,
    while the live path worked.
    """
    from temporalio.contrib.external_workflow_streams._annotation import (
        Annotation,
        AnnotationHeader,
        Run,
        Segment,
        SegmentEndReason,
        StreamBinding,
        encode_annotation,
    )
    from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec
    from temporalio.contrib.external_workflow_streams._manager import (
        ReadinessResult,
        StreamSubscriptionManager,
    )
    from temporalio.contrib.external_workflow_streams._record import AFTER

    _codec_contexts.clear()
    key = StreamKey("ns", "wf-replayed", uuid.uuid4().hex, "tokens")
    placed = await backend.append(
        key,
        StreamRecord(
            RecordKind.DATA,
            await StreamPayloadCodec(DataConverter.default, str).encode(
                _SENTINEL.decode()
            ),
            "p",
            0,
        ),
    )
    assert placed.offset is not None

    async def notify(run_id: str, wait_id: int, generation: int) -> str:
        return ReadinessResult.ACCEPTED

    manager = StreamSubscriptionManager(
        backends={"tokens": backend},
        notify_ready=notify,
        data_converter=DataConverter(payload_codec=ContextRequiredCodec()),
        watch_block=timedelta(milliseconds=10),
    )
    try:
        await manager.prepare_replay(
            "run-1",
            encode_annotation(
                Annotation(
                    header=AnnotationHeader(
                        streams={
                            1: StreamBinding(
                                stream_key=key,
                                start_cursor=BEGINNING,
                                backend_name="tokens",
                                provider_id=MemoryStreamBackend.provider_id,
                                provider_format_version=(
                                    MemoryStreamBackend.provider_format_version
                                ),
                            )
                        }
                    ),
                    segments=(
                        Segment(
                            runs=(
                                Run(
                                    wait_id=1,
                                    first_offset=placed.offset,
                                    last_offset=placed.offset,
                                    count=1,
                                ),
                            ),
                            end_reason=SegmentEndReason.NO_DATA_AVAILABLE,
                        ),
                    ),
                    terminal={1: AFTER(placed.offset)},
                )
            ),
        )
    finally:
        await manager.shutdown()

    assert _codec_contexts == [
        WorkflowSerializationContext(namespace="ns", workflow_id="wf-replayed")
    ], (
        "the replayed record was prepared with the wrong serialization "
        "context, so a Worker restart would fail to decode records the live "
        "path decodes without complaint"
    )


# --- the producer -----------------------------------------------------------


async def test_the_producer_encodes_with_the_consuming_workflows_context(
    backend: MemoryStreamBackend,
) -> None:
    """The half that must not be forgotten.

    Producer and consumer share one ``DataConverter``, so they must also share
    one context. Two context-*free* sides are self-consistent and work; one side
    bound and the other not is a mismatch that appears only on the far side, as
    a decode failure long after the append reported success.

    The chain key is the only Workflow identity a producer has, and it is the
    right one: it names the whole Continue-As-New chain, which is what the
    stream spans and what ``workflow_id`` means on the consumer too.
    """
    recorded: list[SerializationContext | None] = []

    class RecordingCodec(PayloadCodec, WithSerializationContext):
        def __init__(self, context: SerializationContext | None = None) -> None:
            self.context = context

        def with_context(self, context: SerializationContext) -> RecordingCodec:
            return RecordingCodec(context)

        async def encode(
            self, payloads: Sequence[temporalio.api.common.v1.Payload]
        ) -> list[temporalio.api.common.v1.Payload]:
            recorded.append(self.context)
            return list(payloads)

        async def decode(
            self, payloads: Sequence[temporalio.api.common.v1.Payload]
        ) -> list[temporalio.api.common.v1.Payload]:
            return list(payloads)

    producer = ExternalStreamProducer(
        backend=backend,
        workflow=WorkflowChainKey("ns", "wf", "run-1"),
        data_converter=DataConverter(payload_codec=RecordingCodec()),
        session_id="s",
    )
    await producer.topic("tokens", type=str).publish("a", wake=False)

    assert recorded == [
        WorkflowSerializationContext(namespace="ns", workflow_id="wf")
    ], (
        "the producer encoded the record with a different serialization "
        "context than the consuming Worker decodes it with; the two sides "
        "share one converter and must share one context"
    )


async def test_a_producers_record_decodes_on_a_context_bound_consumer(
    backend: MemoryStreamBackend,
) -> None:
    """The round trip, with a context that actually changes the bytes.

    A codec that transforms differently per Workflow is the only probe that can
    tell "both sides bound to the same context" from "both sides bound to
    nothing": the first survives this, and so does today's context-free pair,
    but a fix applied to one side alone does not.
    """

    class PerWorkflowCodec(PayloadCodec, WithSerializationContext):
        """XORs with a key derived from the Workflow ID, as a real one would."""

        def __init__(self, context: SerializationContext | None = None) -> None:
            self.context = context

        def with_context(self, context: SerializationContext) -> PerWorkflowCodec:
            return PerWorkflowCodec(context)

        def _mask(self) -> int:
            assert isinstance(self.context, WorkflowSerializationContext), (
                "this codec has no key without a Workflow context"
            )
            return sum(self.context.workflow_id.encode()) % 251 or 7

        def _apply(
            self, payloads: Sequence[temporalio.api.common.v1.Payload]
        ) -> list[temporalio.api.common.v1.Payload]:
            mask = self._mask()
            out: list[temporalio.api.common.v1.Payload] = []
            for payload in payloads:
                copied = temporalio.api.common.v1.Payload()
                copied.CopyFrom(payload)
                copied.data = bytes(b ^ mask for b in payload.data)
                out.append(copied)
            return out

        async def encode(
            self, payloads: Sequence[temporalio.api.common.v1.Payload]
        ) -> list[temporalio.api.common.v1.Payload]:
            return self._apply(payloads)

        async def decode(
            self, payloads: Sequence[temporalio.api.common.v1.Payload]
        ) -> list[temporalio.api.common.v1.Payload]:
            return self._apply(payloads)

    from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec

    converter = DataConverter(payload_codec=PerWorkflowCodec())
    producer = ExternalStreamProducer(
        backend=backend,
        workflow=WorkflowChainKey("ns", "wf", "run-1"),
        data_converter=converter,
        session_id="s",
    )
    await producer.topic("tokens", type=str).publish("a", wake=False)

    key = StreamKey("ns", "wf", "run-1", "tokens")
    records = await backend.read_after(key, BEGINNING, max_records=10, block=None)
    data = [r for r in records if not r.is_control]
    assert len(data) == 1

    consumer = StreamPayloadCodec(
        converter.with_context(
            WorkflowSerializationContext(namespace="ns", workflow_id="wf")
        ),
        str,
    )
    assert await consumer.decode(data[0].payload) == "a"
