"""External Workflow Streams for Temporal Workflows.

.. warning::
    This package is experimental and may change in future versions.

High-volume stream payloads live in a pluggable external backend such as Redis
Streams and never enter Temporal History. Deterministic replay is preserved
with compact markers recording consumed offset ranges and the
availability/blocking boundaries the original execution observed.

Workflow code creates topics with :data:`external_stream` and subscribes to
them. A Worker receives a :class:`StreamBackend` through
``Worker(external_stream_backend=...)``. External processes publish with an
:class:`ExternalStreamProducer` bound to the Workflow's
:class:`WorkflowChainKey`.

The complementary output direction uses :data:`external_output_stream` inside
Workflow code, :class:`ExternalOutputStreamProducer` in Activities and external
processes, and :class:`ExternalOutputStreamClient` for resumable external
consumers. Output payloads are staged behind a pending provider barrier until a
compact marker in Temporal History proves their producing Workflow Task was
accepted.

This is a **mirror image** of the shipped
:py:mod:`temporalio.contrib.workflow_streams`, not a replacement for it, and
the two coexist (ADR-001). No name here may begin with
``__temporal_workflow_stream``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from temporalio.contrib.external_workflow_streams._api import (
    ExternalStreamOptions,
    ExternalStreamSubscription,
    ExternalStreamTopic,
    external_stream,
    merge,
)
from temporalio.contrib.external_workflow_streams._backend import (
    AppendConflictError,
    ParkIntent,
    ParkIntentRemoval,
    StreamBackend,
    StreamDirection,
    StreamKey,
)
from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec
from temporalio.contrib.external_workflow_streams._errors import (
    ConcurrentStreamConsumerError,
    ExternalStreamCapacityError,
    StreamDecodeError,
    StreamError,
    StreamIntegrityError,
    StreamStorageError,
)
from temporalio.contrib.external_workflow_streams._output_api import (
    ExternalOutputStreamOptions,
    ExternalOutputStreamTopic,
    external_output_stream,
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
    OutputStreamRecord,
    PendingOutputBarrier,
    StagedOutputRecord,
)
from temporalio.contrib.external_workflow_streams._output_client import (
    ExternalOutputStreamClient,
    ExternalOutputStreamClientTopic,
    ExternalOutputStreamItem,
)
from temporalio.contrib.external_workflow_streams._output_producer import (
    ExternalOutputStreamProducer,
    ExternalOutputStreamProducerTopic,
    OutputAppendNotAcknowledgedError,
)
from temporalio.contrib.external_workflow_streams._producer import (
    AppendNotAcknowledgedError,
    ChainKeyMismatchError,
    ExternalStreamProducer,
    ExternalStreamProducerTopic,
    PrecedingWriteFailedError,
    WakeNotAcknowledgedError,
    WorkflowChainKey,
)
from temporalio.contrib.external_workflow_streams._record import (
    AFTER,
    BEGINNING,
    Cursor,
    IdempotencyKey,
    Offset,
    OffsetComparator,
    RecordKind,
    StreamRecord,
)
from temporalio.contrib.external_workflow_streams._wake import WakeRequest

if TYPE_CHECKING:
    from temporalio.contrib.external_workflow_streams._redis import RedisStreamBackend

__all__ = [
    "AFTER",
    "BEGINNING",
    "AppendConflictError",
    "AppendNotAcknowledgedError",
    "ChainKeyMismatchError",
    "ConcurrentStreamConsumerError",
    "Cursor",
    "ExternalStreamCapacityError",
    "ExternalOutputStreamClient",
    "ExternalOutputStreamClientTopic",
    "ExternalOutputStreamItem",
    "ExternalOutputStreamOptions",
    "ExternalOutputStreamProducer",
    "ExternalOutputStreamProducerTopic",
    "ExternalOutputStreamTopic",
    "ExternalStreamOptions",
    "ExternalStreamProducer",
    "ExternalStreamProducerTopic",
    "ExternalStreamSubscription",
    "ExternalStreamTopic",
    "IdempotencyKey",
    "Offset",
    "OffsetComparator",
    "OutputAppendNotAcknowledgedError",
    "OutputReadResult",
    "OutputStage",
    "OutputStageConflictError",
    "OutputStageManifest",
    "OutputStageNotFoundError",
    "OutputStageResolutionError",
    "OutputStageStatus",
    "OutputStreamBackend",
    "OutputStreamRecord",
    "ParkIntent",
    "ParkIntentRemoval",
    "PrecedingWriteFailedError",
    "RecordKind",
    "RedisStreamBackend",
    "StreamBackend",
    "StreamDecodeError",
    "StreamError",
    "StreamIntegrityError",
    "StreamDirection",
    "StreamKey",
    "StreamPayloadCodec",
    "StreamRecord",
    "StreamStorageError",
    "StagedOutputRecord",
    "PendingOutputBarrier",
    "WakeNotAcknowledgedError",
    "WakeRequest",
    "WorkflowChainKey",
    "external_stream",
    "external_output_stream",
    "merge",
]


def __getattr__(name: str) -> Any:
    """Lazily expose the Redis provider without importing it in Workflows."""
    if name == "RedisStreamBackend":
        from temporalio.contrib.external_workflow_streams._redis import (
            RedisStreamBackend,
        )

        return RedisStreamBackend
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
