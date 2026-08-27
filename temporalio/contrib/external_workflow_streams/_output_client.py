"""External clients for Workflow-originated output streams.

Committed output is read directly from the configured provider. Temporal is
consulted only when the provider reports a pending stage at the head of the
topic: History is then the authority that proves whether that exact stage
committed or was discarded with its Workflow Task attempt.
"""

from __future__ import annotations

import asyncio
import hmac
from collections.abc import AsyncIterator
from dataclasses import dataclass
from enum import Enum
from typing import Any, Final, Generic, cast

import temporalio.client
import temporalio.converter
import temporalio.service
from temporalio.contrib.external_workflow_streams._backend import (
    StreamDirection,
    StreamKey,
)
from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec
from temporalio.contrib.external_workflow_streams._errors import (
    StreamError,
    StreamIntegrityError,
    StreamStorageError,
    classify_read_failure,
)
from temporalio.contrib.external_workflow_streams._output_backend import (
    OutputReadResult,
    OutputStage,
    OutputStageConflictError,
    OutputStageManifest,
    OutputStageNotFoundError,
    OutputStageResolutionError,
    OutputStageStatus,
    OutputStreamBackend,
    PendingOutputBarrier,
)
from temporalio.contrib.external_workflow_streams._producer import (
    ChainKeyMismatchError,
    WorkflowChainKey,
    _verify_chain_key,
)
from temporalio.contrib.external_workflow_streams._record import (
    AFTER,
    BEGINNING,
    Cursor,
    Offset,
    RecordKind,
)
from temporalio.types import AnyType

__all__ = [
    "ExternalOutputStreamClient",
    "ExternalOutputStreamClientTopic",
    "ExternalOutputStreamItem",
]

_EXTERNAL_STREAM_MARKER_NAME: Final = "core_external_stream"
_EXTERNAL_STREAM_MARKER_DETAILS_KEY: Final = "external_stream"
_READ_BATCH_SIZE: Final = 100
_PENDING_RECONCILIATION_BACKOFF_SECONDS: Final = 1.0


@dataclass(frozen=True)
class ExternalOutputStreamItem(Generic[AnyType]):
    """One decoded output value and its opaque provider resume offset."""

    data: AnyType
    offset: Offset


class ExternalOutputStreamClient:
    """A verified client connection to one Workflow execution chain."""

    def __init__(
        self,
        *,
        backend: Any,
        workflow: WorkflowChainKey,
        client: temporalio.client.Client,
        data_converter: temporalio.converter.DataConverter,
    ) -> None:
        """Create a reader after validating its output provider contract."""
        if not isinstance(backend, OutputStreamBackend):
            raise TypeError("backend does not implement OutputStreamBackend")
        if type(backend).guarantees_immutability is not True:
            raise ValueError("an output backend must guarantee immutable record bytes")
        if not type(backend).provider_id:
            raise ValueError("an output backend must declare a provider_id")
        self._backend = backend
        self._workflow = workflow
        self._client = client
        self._data_converter = data_converter.with_context(
            temporalio.converter.WorkflowSerializationContext(
                namespace=workflow.namespace,
                workflow_id=workflow.workflow_id,
            )
        )

    @classmethod
    async def connect(
        cls,
        *,
        backend: OutputStreamBackend,
        workflow: WorkflowChainKey,
        client: temporalio.client.Client,
        data_converter: temporalio.converter.DataConverter | None = None,
    ) -> ExternalOutputStreamClient:
        """Verify the chain binding and create an external output reader.

        The Temporal client remains attached only for resolving a pending
        staging barrier. Ordinary reads and :meth:`tail` are backend-only.
        """
        try:
            await _verify_chain_key(client, workflow)
        except ChainKeyMismatchError:
            raise
        except StreamError:
            raise
        except Exception as err:
            raise StreamStorageError(
                f"could not verify external output Workflow chain: {err}"
            ) from err
        return cls(
            backend=backend,
            workflow=workflow,
            client=client,
            data_converter=data_converter or client.data_converter,
        )

    @property
    def workflow(self) -> WorkflowChainKey:
        """The Workflow chain whose output this client reads."""
        return self._workflow

    def topic(
        self, name: str, *, type: type[AnyType] | None = None
    ) -> ExternalOutputStreamClientTopic[AnyType]:
        """Create a typed reader for one output topic."""
        if not name:
            raise ValueError("an output topic needs a non-empty name")
        return ExternalOutputStreamClientTopic(
            backend=self._backend,
            stream_key=self._workflow.stream_key(
                name, direction=StreamDirection.OUTPUT
            ),
            codec=StreamPayloadCodec(self._data_converter, type),
            client=self._client,
            workflow=self._workflow,
        )


class ExternalOutputStreamClientTopic(Generic[AnyType]):
    """A typed, resumable external reader for one output topic."""

    def __init__(
        self,
        *,
        backend: OutputStreamBackend,
        stream_key: StreamKey,
        codec: StreamPayloadCodec[AnyType],
        client: temporalio.client.Client,
        workflow: WorkflowChainKey,
    ) -> None:
        """Create a typed reader bound to one direction-isolated topic."""
        self._backend = backend
        self._stream_key = stream_key
        self._codec = codec
        self._client = client
        self._workflow = workflow

    async def tail(self) -> Cursor:
        """Return the boundary after the readable committed prefix."""
        try:
            return await self._backend.output_tail(self._stream_key)
        except StreamError:
            raise
        except Exception as err:
            raise StreamStorageError(
                f"could not read external output tail for {self._stream_key}: {err}"
            ) from err

    async def subscribe(
        self, *, after: Cursor | Offset = BEGINNING
    ) -> AsyncIterator[ExternalOutputStreamItem[AnyType]]:
        """Yield committed values strictly after ``after``.

        A pending stage is a hard ordering barrier. The committed prefix before
        it may be yielded, but the client never reads or yields a later offset
        until History has positively committed or aborted the pending stage.
        """
        candidate = cast(object, after)
        if isinstance(candidate, Offset):
            cursor = AFTER(candidate)
        elif isinstance(candidate, Cursor):
            cursor = candidate
        else:
            raise TypeError(
                f"after must be a Cursor or Offset, got {type(candidate).__name__}"
            )
        while True:
            result = await self._read_after(cursor)
            self._validate_read_result(cursor, result)
            for record in result.records:
                cursor = AFTER(record.offset)
                if record.kind is RecordKind.FINISH:
                    return
                if record.kind.is_control:
                    continue
                try:
                    data = await self._codec.decode(record.payload)
                except asyncio.CancelledError:
                    raise
                except Exception as err:
                    raise classify_read_failure(
                        range_validated=True, cause=err
                    ) from err
                yield ExternalOutputStreamItem(data=data, offset=record.offset)

            if result.pending is not None:
                resolved = await self._resolve_pending(result.pending)
                if not resolved:
                    await asyncio.sleep(_PENDING_RECONCILIATION_BACKOFF_SECONDS)

    async def _read_after(self, after: Cursor) -> OutputReadResult:
        try:
            return await self._backend.read_output_after(
                self._stream_key,
                after,
                max_records=_READ_BATCH_SIZE,
            )
        except StreamError:
            raise
        except Exception as err:
            raise StreamStorageError(
                f"could not read external output for {self._stream_key}: {err}"
            ) from err

    def _validate_read_result(self, after: Cursor, result: OutputReadResult) -> None:
        """Refuse provider output that crosses or misorders a pending barrier."""
        previous = after.offset
        for record in result.records:
            if (
                previous is not None
                and self._backend.compare_offsets(previous, record.offset) >= 0
            ):
                raise StreamIntegrityError(
                    "external output provider returned records outside the requested "
                    f"strictly-after order for {self._stream_key}"
                )
            previous = record.offset

        pending = result.pending
        if pending is None:
            return
        if pending.manifest.stream_key != self._stream_key:
            raise StreamIntegrityError(
                "external output provider returned a pending barrier for another "
                f"topic while reading {self._stream_key}"
            )
        if (
            pending.manifest.provider_id != type(self._backend).provider_id
            or pending.manifest.provider_format_version
            != type(self._backend).provider_format_version
        ):
            raise StreamIntegrityError(
                "external output provider returned a pending barrier carrying a "
                "different provider binding"
            )
        if (
            previous is not None
            and self._backend.compare_offsets(previous, pending.offset) >= 0
        ):
            raise StreamIntegrityError(
                "external output provider returned data at or beyond an unresolved "
                f"pending barrier for {self._stream_key}"
            )

    async def _resolve_pending(self, pending: PendingOutputBarrier) -> bool:
        """Resolve one pending head from the exact producing Run's History.

        Returns ``False`` only when History contains neither this token nor a
        later durable task/Workflow boundary, so the stage must remain pending.
        """
        decision = await self._history_decision(pending.manifest)
        if decision is None:
            return False
        await _apply_output_stage_decision(
            backend=self._backend,
            manifest=pending.manifest,
            decision=decision,
        )
        return True

    async def _history_decision(
        self, manifest: OutputStageManifest
    ) -> _HistoryDecision | None:
        return await _history_decision(
            client=self._client,
            workflow_id=self._workflow.workflow_id,
            manifest=manifest,
        )

    @staticmethod
    def _validate_resolved_stage(
        manifest: OutputStageManifest,
        stage: OutputStage,
        *,
        expected_status: OutputStageStatus,
    ) -> None:
        _validate_resolved_stage(
            manifest,
            stage,
            expected_status=expected_status,
        )


class _HistoryDecision(Enum):
    COMMIT = "commit"
    ABORT = "abort"


async def _history_decision(
    *,
    client: temporalio.client.Client,
    workflow_id: str,
    manifest: OutputStageManifest,
) -> _HistoryDecision | None:
    """Prove one staged token's outcome from its exact producing History."""
    try:
        events = [
            event
            async for event in client.get_workflow_handle(
                workflow_id,
                run_id=manifest.run_id,
            ).fetch_history_events()
        ]
    except temporalio.service.RPCError as err:
        if err.status in (
            temporalio.service.RPCStatusCode.NOT_FOUND,
            temporalio.service.RPCStatusCode.DATA_LOSS,
        ):
            raise StreamIntegrityError(
                "History required to resolve external output stage "
                f"{manifest.stage_token!r} is no longer available: {err}"
            ) from err
        raise StreamStorageError(
            "could not read Temporal History while resolving external output "
            f"stage {manifest.stage_token!r}: {err}"
        ) from err
    except StreamError:
        raise
    except Exception as err:
        raise StreamStorageError(
            "could not read Temporal History while resolving external output "
            f"stage {manifest.stage_token!r}: {err}"
        ) from err

    floor = manifest.history_floor_event_id
    if floor and not any(event.event_id == floor for event in events):
        raise StreamIntegrityError(
            "History no longer contains the exact floor event needed to resolve "
            f"external output stage {manifest.stage_token!r}: {floor}"
        )
    relevant = [event for event in events if event.event_id > floor]

    # A task's marker and closing event commit in one server transaction, but
    # the closing event precedes command events in History. Search the complete
    # response for the positive token proof before considering any boundary
    # proof of absence.
    for event in relevant:
        if _event_has_output_stage_token(event, manifest.stage_token):
            return _HistoryDecision.COMMIT
    if any(_is_deciding_boundary(event) for event in relevant):
        return _HistoryDecision.ABORT
    return None


async def _apply_output_stage_decision(
    *,
    backend: OutputStreamBackend,
    manifest: OutputStageManifest,
    decision: _HistoryDecision,
) -> None:
    """Apply a positively proven History decision idempotently."""
    try:
        if decision is _HistoryDecision.COMMIT:
            stage = await backend.commit_output(manifest)
            expected_status = OutputStageStatus.COMMITTED
        else:
            stage = await backend.abort_output(manifest)
            expected_status = OutputStageStatus.ABORTED
    except (
        OutputStageConflictError,
        OutputStageNotFoundError,
        OutputStageResolutionError,
    ) as err:
        raise StreamIntegrityError(
            "external output pending stage could not be reconciled with its "
            f"History decision: {err}"
        ) from err
    except StreamError:
        raise
    except Exception as err:
        raise StreamStorageError(
            f"could not {decision.value} external output stage "
            f"{manifest.stage_token}: {err}"
        ) from err
    _validate_resolved_stage(
        manifest,
        stage,
        expected_status=expected_status,
    )


async def _reconcile_output_stage(  # pyright: ignore[reportUnusedFunction]
    *,
    backend: OutputStreamBackend,
    client: temporalio.client.Client,
    workflow_id: str,
    manifest: OutputStageManifest,
) -> bool:
    """Resolve after reporting, leaving ambiguous outcomes safely pending."""
    decision = await _history_decision(
        client=client,
        workflow_id=workflow_id,
        manifest=manifest,
    )
    if decision is None:
        return False
    await _apply_output_stage_decision(
        backend=backend,
        manifest=manifest,
        decision=decision,
    )
    return True


def _validate_resolved_stage(
    manifest: OutputStageManifest,
    stage: OutputStage,
    *,
    expected_status: OutputStageStatus,
) -> None:
    """Refuse a provider which resolved a different immutable stage."""
    if stage.manifest != manifest or stage.status is not expected_status:
        raise StreamIntegrityError(
            "external output provider returned a different stage or status "
            "after applying a History reconciliation decision"
        )


def _is_deciding_boundary(event: Any) -> bool:
    """Whether this event proves that the staged token can no longer appear."""
    return any(
        event.HasField(field)
        for field in (
            "workflow_task_completed_event_attributes",
            "workflow_task_failed_event_attributes",
            "workflow_task_timed_out_event_attributes",
            "workflow_execution_completed_event_attributes",
            "workflow_execution_failed_event_attributes",
            "workflow_execution_timed_out_event_attributes",
            "workflow_execution_canceled_event_attributes",
            "workflow_execution_terminated_event_attributes",
            "workflow_execution_continued_as_new_event_attributes",
        )
    )


def _event_has_output_stage_token(event: Any, stage_token: str) -> bool:
    """Read output commit proofs from the shared external-stream marker."""
    if not event.HasField("marker_recorded_event_attributes"):
        return False
    attributes = event.marker_recorded_event_attributes
    if attributes.marker_name != _EXTERNAL_STREAM_MARKER_NAME:
        return False

    payloads = attributes.details.get(_EXTERNAL_STREAM_MARKER_DETAILS_KEY)
    if payloads is None or len(payloads.payloads) != 1:
        raise StreamIntegrityError(
            "an external stream marker has no singular marker envelope"
        )

    from temporalio.bridge.proto.external_data import ExternalStreamMarkerData

    marker = ExternalStreamMarkerData()
    try:
        marker.ParseFromString(payloads.payloads[0].data)
    except Exception as err:
        raise StreamIntegrityError(
            f"an external stream marker envelope could not be decoded: {err}"
        ) from err

    output = _marker_output_commit(marker)
    return output is not None and _stage_tokens_equal(output.stage_token, stage_token)


def _marker_output_commit(marker: Any) -> Any | None:
    """Return the shared marker's output proof, when this task had output."""
    if "output" not in marker.DESCRIPTOR.fields_by_name:
        raise StreamIntegrityError(
            "this SDK's external stream marker schema cannot inspect output "
            "commit proofs, so a pending stage cannot be resolved safely"
        )
    return marker.output if marker.HasField("output") else None


def _stage_tokens_equal(recorded: str | bytes, expected: str) -> bool:
    if isinstance(recorded, bytes):
        return hmac.compare_digest(recorded, expected.encode())
    return hmac.compare_digest(recorded, expected)
