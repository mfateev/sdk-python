"""Activity and external-process producers for output streams."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic

import temporalio.converter
from temporalio.contrib.external_workflow_streams._backend import (
    AppendConflictError,
    StreamDirection,
    StreamKey,
)
from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec
from temporalio.contrib.external_workflow_streams._output_backend import (
    OutputStreamBackend,
    OutputStreamRecord,
)
from temporalio.contrib.external_workflow_streams._producer import (
    WorkflowChainKey,
    _default_session_id,
    _verify_chain_key,
)
from temporalio.contrib.external_workflow_streams._record import (
    RecordKind,
    StreamRecord,
)
from temporalio.types import AnyType

if TYPE_CHECKING:
    import temporalio.client

__all__ = [
    "ExternalOutputStreamProducer",
    "ExternalOutputStreamProducerTopic",
    "OutputAppendNotAcknowledgedError",
]


class OutputAppendNotAcknowledgedError(Exception):
    """A direct output append may or may not have reached the provider.

    Re-run :meth:`ExternalOutputStreamProducerTopic.resolve_append` with the
    exact record carried here.  The provider's idempotency rule makes that safe;
    calling ``publish`` again is not safe because it draws another sequence.
    """

    def __init__(self, *, stream_key: StreamKey, record: StreamRecord) -> None:
        """Capture the exact record required for safe ambiguity recovery."""
        super().__init__(
            "the output append did not report an outcome; resolve it with the "
            "same record before publishing another value"
        )
        self.stream_key = stream_key
        self.record = record


@dataclass(eq=False)
class _AppendOperation:
    sequence: int
    settled: asyncio.Event = field(default_factory=asyncio.Event)
    failure: BaseException | None = None


class ExternalOutputStreamProducer:
    """A chain-bound producer whose records are committed immediately."""

    def __init__(
        self,
        *,
        backend: Any,
        workflow: WorkflowChainKey,
        data_converter: temporalio.converter.DataConverter,
        session_id: str,
    ) -> None:
        """Create a producer after validating its provider and session identity."""
        if not isinstance(backend, OutputStreamBackend):
            raise TypeError("backend does not implement OutputStreamBackend")
        if type(backend).guarantees_immutability is not True:
            raise ValueError("an output backend must guarantee immutable record bytes")
        if not type(backend).provider_id:
            raise ValueError("an output backend must declare a provider_id")
        if not session_id:
            raise ValueError("an output producer session ID may not be empty")
        self._backend = backend
        self._workflow = workflow
        self._data_converter = data_converter.with_context(
            temporalio.converter.WorkflowSerializationContext(
                namespace=workflow.namespace,
                workflow_id=workflow.workflow_id,
            )
        )
        self._session_id = session_id
        self._sequence = 0
        self._operations: dict[StreamKey, list[_AppendOperation]] = {}
        self._unresolved: dict[StreamKey, list[StreamRecord]] = {}
        self._closing: set[StreamKey] = set()
        self._finished: set[StreamKey] = set()

    @classmethod
    async def connect(
        cls,
        *,
        backend: OutputStreamBackend,
        workflow: WorkflowChainKey,
        client: temporalio.client.Client,
        data_converter: temporalio.converter.DataConverter | None = None,
        session_id: str | None = None,
    ) -> ExternalOutputStreamProducer:
        """Verify the chain binding before allowing the first append."""
        await _verify_chain_key(client, workflow)
        return cls(
            backend=backend,
            workflow=workflow,
            data_converter=data_converter or client.data_converter,
            session_id=session_id or _default_session_id(),
        )

    @property
    def session_id(self) -> str:
        """The retry-stable idempotency session."""
        return self._session_id

    @property
    def workflow(self) -> WorkflowChainKey:
        """The Workflow chain receiving these records."""
        return self._workflow

    def topic(
        self, name: str, *, type: type[AnyType] | None = None
    ) -> ExternalOutputStreamProducerTopic[AnyType]:
        """Create a typed direct publisher for one output topic."""
        if not name:
            raise ValueError("an output topic needs a non-empty name")
        key = self._workflow.stream_key(
            name,
            direction=StreamDirection.OUTPUT,
        )
        return ExternalOutputStreamProducerTopic(
            producer=self,
            stream_key=key,
            codec=StreamPayloadCodec(self._data_converter, type),
        )

    def _next_sequence(self) -> int:
        sequence = self._sequence
        self._sequence += 1
        return sequence


class ExternalOutputStreamProducerTopic(Generic[AnyType]):
    """One immediately committed output topic."""

    def __init__(
        self,
        *,
        producer: ExternalOutputStreamProducer,
        stream_key: StreamKey,
        codec: StreamPayloadCodec[AnyType],
    ) -> None:
        """Create a typed publisher bound to one output topic."""
        self._producer = producer
        self._stream_key = stream_key
        self._codec = codec

    @property
    def stream_key(self) -> StreamKey:
        """The direction-isolated physical stream identity."""
        return self._stream_key

    def _refuse_unresolved(self) -> None:
        records = self._producer._unresolved.get(self._stream_key)
        if records:
            raise OutputAppendNotAcknowledgedError(
                stream_key=self._stream_key,
                record=records[0],
            )

    def _refuse_closed(self) -> None:
        if self._stream_key in self._producer._finished:
            raise RuntimeError("this output topic is already finished")
        if self._stream_key in self._producer._closing:
            raise RuntimeError("this output topic is already being finished")

    def _remember_unresolved(self, record: StreamRecord) -> None:
        records = self._producer._unresolved.setdefault(self._stream_key, [])
        if record not in records:
            records.append(record)

    def _forget_unresolved(self, record: StreamRecord) -> None:
        records = self._producer._unresolved[self._stream_key]
        records.remove(record)
        if not records:
            del self._producer._unresolved[self._stream_key]

    async def _append(self, record: StreamRecord) -> OutputStreamRecord:
        try:
            return await self._producer._backend.append_output(self._stream_key, record)
        except AppendConflictError:
            raise
        except asyncio.CancelledError:
            self._remember_unresolved(record)
            raise OutputAppendNotAcknowledgedError(
                stream_key=self._stream_key,
                record=record,
            ) from None
        except Exception as err:
            # Provider-specific transport exceptions cannot reliably distinguish
            # a refusal from a lost acknowledgement. Repeating the exact append
            # is the only recovery safe in both cases.
            self._remember_unresolved(record)
            raise OutputAppendNotAcknowledgedError(
                stream_key=self._stream_key,
                record=record,
            ) from err

    async def resolve_append(self, record: StreamRecord) -> OutputStreamRecord:
        """Resolve an unknown append by repeating its exact identity and bytes."""
        outstanding = self._producer._unresolved.get(self._stream_key, ())
        if record not in outstanding:
            raise ValueError("record is not this topic's unresolved output append")
        try:
            placed = await self._producer._backend.append_output(
                self._stream_key, record
            )
        except AppendConflictError:
            self._forget_unresolved(record)
            if record.kind is RecordKind.FINISH:
                self._producer._closing.discard(self._stream_key)
            raise
        except (Exception, asyncio.CancelledError) as err:
            raise OutputAppendNotAcknowledgedError(
                stream_key=self._stream_key,
                record=record,
            ) from err
        self._forget_unresolved(record)
        if record.kind is RecordKind.FINISH:
            self._producer._closing.discard(self._stream_key)
            self._producer._finished.add(self._stream_key)
        return placed

    async def publish(self, value: AnyType) -> OutputStreamRecord:
        """Append one immediately committed data record."""
        self._refuse_unresolved()
        self._refuse_closed()
        sequence = self._producer._next_sequence()
        operation = _AppendOperation(sequence)
        operations = self._producer._operations.setdefault(self._stream_key, [])
        operations.append(operation)
        try:
            record = StreamRecord(
                kind=RecordKind.DATA,
                payload=await self._codec.encode(value),
                producer_session_id=self._producer.session_id,
                sequence=sequence,
            )
            placed = await self._append(record)
        except BaseException as err:
            operation.failure = err
            operation.settled.set()
            raise
        else:
            operation.settled.set()
            return placed
        finally:
            operations.remove(operation)

    async def finish_writing(self) -> OutputStreamRecord:
        """Append the ordered terminal after every earlier publish settles."""
        self._refuse_unresolved()
        self._refuse_closed()
        self._producer._closing.add(self._stream_key)
        sequence = self._producer._next_sequence()
        try:
            preceding = tuple(self._producer._operations.get(self._stream_key, ()))
            for operation in preceding:
                await operation.settled.wait()
                if operation.failure is not None:
                    raise RuntimeError(
                        f"output publish {operation.sequence} did not append; "
                        "refusing to place a terminal in front of it"
                    ) from operation.failure
            record = StreamRecord(
                kind=RecordKind.FINISH,
                payload=b"",
                producer_session_id=self._producer.session_id,
                sequence=sequence,
            )
            placed = await self._append(record)
        except OutputAppendNotAcknowledgedError:
            # The terminal may already be durable, so the topic stays closed to
            # new publishes until resolve_append settles that exact record.
            raise
        except BaseException:
            self._producer._closing.discard(self._stream_key)
            raise
        else:
            self._producer._closing.discard(self._stream_key)
            self._producer._finished.add(self._stream_key)
            return placed
