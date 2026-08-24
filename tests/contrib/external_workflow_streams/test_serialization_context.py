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

PYTEST_DONT_REWRITE: sandboxed fixture Workflows re-import this module, so pytest's
injected imports would make sandbox validation depend on pytest's import locks.
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
from temporalio.worker import Replayer, Worker
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

#: The same two observations, from the chain-keyed probes an offline replay is
#: driven through. Kept apart from the pair above so the replay assertions can
#: be exact rather than a suffix of everything the process has ever converted.
_chain_codec_contexts: list[SerializationContext | None] = []
_chain_converter_contexts: list[SerializationContext | None] = []


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


def _chain_tag(context: SerializationContext | None) -> bytes:
    """The Workflow identity a chain-keyed probe writes into the bytes.

    Both the namespace and the Workflow id, because an offline ``Replayer``
    keeps the Workflow id and substitutes only the namespace: a probe keyed on
    the id alone reports success against a converter that never saw the
    recorded namespace at all.
    """
    assert isinstance(context, WorkflowSerializationContext), (
        f"a stream record's payload was handled with no Workflow context: {context}"
    )
    return f"|{context.namespace}|{context.workflow_id}".encode()


def _chain_apply(
    payloads: Sequence[temporalio.api.common.v1.Payload],
    context: SerializationContext | None,
    seen: list[SerializationContext | None],
    encoding: bool,
) -> list[temporalio.api.common.v1.Payload]:
    """Appends or strips the tag, refusing bytes written under another context.

    A probe that only *recorded* the context would pass against a converter
    bound to the wrong Workflow, because a wrong context still decodes -- to a
    different value, silently. Tagging makes the mismatch the failure it would
    be for a real per-Workflow key.

    Only the sentinel payload is touched, so the Workflow's own argument and
    result -- which an offline replay legitimately handles under the replay
    harness's own namespace -- travel through untouched.
    """
    out: list[temporalio.api.common.v1.Payload] = []
    for payload in payloads:
        if _SENTINEL not in payload.data:
            out.append(payload)
            continue
        if not encoding:
            seen.append(context)
        tag = _chain_tag(context)
        copied = temporalio.api.common.v1.Payload()
        copied.CopyFrom(payload)
        if encoding:
            copied.data = payload.data + tag
        else:
            if not payload.data.endswith(tag):
                raise RuntimeError(
                    "a stream record written under one Workflow context was "
                    f"read back under {context}"
                )
            copied.data = payload.data[: -len(tag)]
        out.append(copied)
    return out


class ChainKeyedCodec(PayloadCodec, WithSerializationContext):
    """The asynchronous half, keyed on the Workflow the record belongs to."""

    def __init__(self, context: SerializationContext | None = None) -> None:
        self.context = context

    def with_context(self, context: SerializationContext) -> ChainKeyedCodec:
        return ChainKeyedCodec(context)

    async def encode(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> list[temporalio.api.common.v1.Payload]:
        return _chain_apply(payloads, self.context, _chain_codec_contexts, True)

    async def decode(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> list[temporalio.api.common.v1.Payload]:
        return _chain_apply(payloads, self.context, _chain_codec_contexts, False)


class ChainKeyedPayloadConverter(DefaultPayloadConverter, WithSerializationContext):
    """The synchronous half, keyed the same way and run on the Workflow thread.

    The half an offline ``Replayer`` gets wrong on its own: the manager binds
    the codec to the stream key the marker recorded, while the converter that
    runs inside ``activate()`` belongs to a runtime the harness built under its
    own namespace.
    """

    def __init__(self, context: SerializationContext | None = None) -> None:
        super().__init__()
        self.context = context

    def with_context(self, context: SerializationContext) -> ChainKeyedPayloadConverter:
        return ChainKeyedPayloadConverter(context)

    def to_payloads(
        self, values: Sequence[object]
    ) -> list[temporalio.api.common.v1.Payload]:
        return _chain_apply(
            super().to_payloads(values), self.context, _chain_converter_contexts, True
        )

    def from_payloads(
        self,
        payloads: Sequence[temporalio.api.common.v1.Payload],
        type_hints: list[type] | None = None,
    ) -> list[object]:
        return super().from_payloads(
            _chain_apply(payloads, self.context, _chain_converter_contexts, False),
            type_hints,
        )


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


async def test_an_offline_replay_converts_a_record_under_the_recorded_context(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """Both halves of one record, through the real ``Replayer``.

    A ``Replayer`` runs under its own ``ReplayNamespace`` -- deliberately, and
    :meth:`_verify_binding` accommodates it by comparing only the stream name
    from a recorded binding. The record's *conversion* has to make the same
    accommodation from the other side: the manager prepares a replayed record
    under the stream key the marker recorded, so a converter left on the
    harness's namespace would see two different Workflow identities while
    decoding one payload -- and a namespace-keyed converter then refuses a
    history that is entirely valid.

    Live execution cannot show this. There the recorded key and the runtime's
    own identity are the same Workflow, so both halves agree whether or not
    anything binds them separately. Only a harness that supplies its own
    namespace pulls them apart, which is why this runs the history back through
    the tool a user would.
    """
    assert client.namespace != "ReplayNamespace", (
        "the Replayer's default namespace matches the live one, so this test "
        "cannot tell a bound converter from an unbound one"
    )
    _chain_codec_contexts.clear()
    _chain_converter_contexts.clear()
    converter = DataConverter(
        payload_converter_class=ChainKeyedPayloadConverter,
        payload_codec=ChainKeyedCodec(),
    )
    config = client.config()
    config["data_converter"] = converter
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
        # Written under the *producing* Workflow's context, which is what the
        # marker goes on to record and therefore what replay has to reproduce.
        await _publish_probe_record(
            backend,
            key,
            converter.with_context(
                WorkflowSerializationContext(
                    namespace=key.namespace, workflow_id=key.workflow_id
                )
            ),
        )
        assert await asyncio.wait_for(handle.result(), 30) == 1
        history = await handle.fetch_history()

    assert _chain_codec_contexts and _chain_converter_contexts, (
        "the live run decoded no stream record, so there is nothing recorded "
        "for the replay to disagree with"
    )
    # Only the replay's observations are asserted on; the live run's are the
    # subject of the two tests above.
    _chain_codec_contexts.clear()
    _chain_converter_contexts.clear()

    result = await Replayer(
        workflows=[CountProbeTokensWorkflow],
        data_converter=converter,
        external_stream_backends={"tokens-memory": backend},
    ).replay_workflow(history, raise_on_replay_failure=False)

    assert result.replay_failure is None, (
        "replaying a valid history failed because the record was converted "
        "under the replay harness's namespace rather than the one the marker "
        f"recorded: {result.replay_failure}"
    )
    recorded = WorkflowSerializationContext(
        namespace=client.namespace, workflow_id=handle.id
    )
    assert _chain_codec_contexts == [recorded], (
        "the asynchronous half of the replayed record's decoding did not run "
        f"with the recorded context: {_chain_codec_contexts}"
    )
    assert _chain_converter_contexts == [recorded], (
        "the synchronous half ran with a different context than the "
        "asynchronous half of the same record; one payload was decoded under "
        f"two Workflow identities: {_chain_converter_contexts}"
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
