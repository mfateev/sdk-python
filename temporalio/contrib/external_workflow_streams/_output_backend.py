"""Provider contract for Workflow-originated external output streams.

This contract is separate from :class:`StreamBackend`. Input-only providers
remain valid: output staging adds a pending/committed state machine and an
ordering barrier that an ordinary append-only input stream does not promise.

The unit of staging is one topic sub-batch from one Workflow Task attempt. A
Worker-minted ``stage_token`` identifies the attempt, while ``sub_batch_id``
distinguishes topic batches covered by that token. Records are invisible while
pending. A reader stops at the first unresolved batch even when later committed
Activity records already have provider offsets.
"""

from __future__ import annotations

import abc
import enum
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import ClassVar

from temporalio.contrib.external_workflow_streams._backend import (
    DEFAULT_WATCH_BLOCK,
    StreamDirection,
    StreamKey,
)
from temporalio.contrib.external_workflow_streams._record import (
    Cursor,
    Offset,
    RecordKind,
    StreamRecord,
)

__all__ = [
    "OutputReadResult",
    "OutputStage",
    "OutputStageConflictError",
    "OutputStageManifest",
    "OutputStageNotFoundError",
    "OutputStageResolutionError",
    "OutputStageStatus",
    "OutputStreamBackend",
    "OutputStreamRecord",
    "PendingOutputBarrier",
    "StagedOutputRecord",
]


@enum.unique
class OutputStageStatus(enum.Enum):
    """The durable visibility state of a staged output sub-batch."""

    PENDING = "pending"
    COMMITTED = "committed"
    ABORTED = "aborted"


@dataclass(frozen=True)
class OutputStageManifest:
    """Immutable logical identity and reconciliation proof for one sub-batch.

    Encoded record bytes are deliberately absent. A payload codec may use
    randomness, so an idempotent retry compares this pre-codec manifest and
    preserves the first successfully staged encoded bytes.
    """

    stream_key: StreamKey
    provider_id: str
    provider_format_version: int
    stage_token: str
    run_id: str
    history_floor_event_id: int
    sub_batch_id: int
    fingerprint_version: int
    fingerprint: bytes
    record_count: int
    logical_byte_count: int

    def __post_init__(self) -> None:
        """Reject identities that could not safely be reconciled."""
        if self.stream_key.direction is not StreamDirection.OUTPUT:
            raise ValueError("an output stage manifest requires an OUTPUT stream key")
        for field_name in ("provider_id", "stage_token", "run_id"):
            if not getattr(self, field_name):
                raise ValueError(
                    f"an output stage manifest needs a non-empty {field_name}"
                )
        for field_name in (
            "history_floor_event_id",
            "provider_format_version",
            "fingerprint_version",
            "record_count",
        ):
            if getattr(self, field_name) < 1:
                raise ValueError(
                    f"an output stage manifest needs a positive {field_name}"
                )
        for field_name in (
            "sub_batch_id",
            "logical_byte_count",
        ):
            if getattr(self, field_name) < 0:
                raise ValueError(
                    f"an output stage manifest needs a non-negative {field_name}"
                )
        if len(self.fingerprint) != 32:
            raise ValueError("an output stage SHA-256 fingerprint must be 32 bytes")


@dataclass(frozen=True)
class StagedOutputRecord:
    """One encoded record offered as part of a pending Workflow sub-batch.

    ``publish_index`` is local to the topic sub-batch. The backend assigns the
    provider offset, and a retry of the same manifest must return the originally
    stored bytes rather than replacing them with this attempt's encoding.
    """

    publish_index: int
    kind: RecordKind
    payload: bytes

    def __post_init__(self) -> None:
        """Validate the deterministic identity and control-record shape."""
        if self.publish_index < 0:
            raise ValueError("an output publish index must be non-negative")
        if self.kind.is_control and self.payload:
            raise ValueError(f"a {self.kind.name} output record carries no payload")


@dataclass(frozen=True)
class OutputStreamRecord:
    """One provider-placed output record returned to a client."""

    kind: RecordKind
    payload: bytes
    offset: Offset

    def __post_init__(self) -> None:
        """Keep terminal and fence records payload-free after storage."""
        if self.kind.is_control and self.payload:
            raise ValueError(f"a {self.kind.name} output record carries no payload")


@dataclass(frozen=True)
class OutputStage:
    """The backend's durable view of one Workflow output sub-batch."""

    manifest: OutputStageManifest
    records: tuple[OutputStreamRecord, ...]
    status: OutputStageStatus

    def __post_init__(self) -> None:
        """Require the placed records to agree with the sealed manifest."""
        if len(self.records) != self.manifest.record_count:
            raise ValueError(
                "an output stage record count does not match its manifest: "
                f"{len(self.records)} != {self.manifest.record_count}"
            )


@dataclass(frozen=True)
class PendingOutputBarrier:
    """The unresolved stage preventing a client from reading later offsets."""

    manifest: OutputStageManifest
    offset: Offset
    """The first physical position occupied by this pending sub-batch."""


@dataclass(frozen=True)
class OutputReadResult:
    """A committed prefix and the unresolved barrier immediately after it."""

    records: tuple[OutputStreamRecord, ...]
    pending: PendingOutputBarrier | None = None


class OutputStageConflictError(Exception):
    """A stage token/sub-batch identity was reused with another manifest."""

    def __init__(self, manifest: OutputStageManifest) -> None:
        """Capture the immutable stage identity that conflicted."""
        super().__init__(
            f"output stage {manifest.stage_token}/{manifest.sub_batch_id} was "
            "already sealed with a different logical manifest"
        )
        self.manifest = manifest


class OutputStageNotFoundError(Exception):
    """A commit or abort named a stage the provider does not hold."""

    def __init__(self, manifest: OutputStageManifest) -> None:
        """Capture the immutable stage identity that was absent."""
        super().__init__(
            f"output stage {manifest.stage_token}/{manifest.sub_batch_id} was not found"
        )
        self.manifest = manifest


class OutputStageResolutionError(Exception):
    """A terminal stage transition tried to reverse an existing decision."""

    def __init__(
        self,
        manifest: OutputStageManifest,
        *,
        current: OutputStageStatus,
        requested: OutputStageStatus,
    ) -> None:
        """Capture the attempted reversal of a terminal stage decision."""
        super().__init__(
            f"output stage {manifest.stage_token}/{manifest.sub_batch_id} is "
            f"already {current.value} and cannot become {requested.value}"
        )
        self.manifest = manifest
        self.current = current
        self.requested = requested


class OutputStreamBackend(abc.ABC):
    """A provider supporting externally visible Workflow output.

    Providers may implement this contract, :class:`StreamBackend`, or both.
    Implementing the input contract alone does not imply transactional output
    staging support.
    """

    guarantees_immutability: ClassVar[bool | None] = None
    provider_id: ClassVar[str] = ""
    provider_format_version: ClassVar[int] = 1

    @abc.abstractmethod
    async def stage_output(
        self,
        manifest: OutputStageManifest,
        records: Sequence[StagedOutputRecord],
    ) -> OutputStage:
        """Durably append and seal an immutable pending sub-batch.

        The records become a barrier at their assigned positions but are not
        readable. Repeating the call with the same complete manifest is an
        idempotent success returning the original records and offsets, even if
        a randomized codec supplied different encoded bytes on the retry.
        Reusing ``(stage_token, sub_batch_id)`` with any different manifest
        raises :class:`OutputStageConflictError`. The provider validates that
        record count and publish indexes match the manifest before writing any
        part of the batch.
        """

    @abc.abstractmethod
    async def commit_output(self, manifest: OutputStageManifest) -> OutputStage:
        """Make this exact pending stage visible, idempotently.

        Repeating a committed transition returns the committed stage. An
        unknown stage raises :class:`OutputStageNotFoundError`; an aborted stage
        raises :class:`OutputStageResolutionError` and is never resurrected.
        """

    @abc.abstractmethod
    async def abort_output(self, manifest: OutputStageManifest) -> OutputStage:
        """Resolve this exact pending stage as skipped, idempotently.

        Aborted positions remain coordination boundaries but yield no records.
        An unknown stage raises :class:`OutputStageNotFoundError`; a committed
        stage raises :class:`OutputStageResolutionError` and is never hidden.
        """

    @abc.abstractmethod
    async def output_stage(self, manifest: OutputStageManifest) -> OutputStage | None:
        """Inspect the exact stage, including its reconciliation metadata.

        Returns ``None`` only when the identity has never been staged. A stage's
        manifest, encoded records, offsets, and terminal status are immutable.
        """

    @abc.abstractmethod
    async def append_output(
        self, key: StreamKey, record: StreamRecord
    ) -> OutputStreamRecord:
        """Append an immediately committed Activity/external producer record.

        ``key`` must have direction ``OUTPUT``. The append is idempotent on the
        input record's ``(session_id, sequence)`` and byte identity exactly as
        :meth:`StreamBackend.append`: an identical retry returns the original
        output record and offset; different bytes under the same key raise
        :class:`~temporalio.contrib.external_workflow_streams._backend.AppendConflictError`.
        It may not pass an already-positioned pending stage in client reads.
        """

    @abc.abstractmethod
    async def read_output_after(
        self,
        key: StreamKey,
        after: Cursor,
        *,
        max_records: int,
        block: timedelta | None = DEFAULT_WATCH_BLOCK,
    ) -> OutputReadResult:
        """Read the committed prefix strictly after ``after``.

        Records are returned in provider order and limited to ``max_records``.
        Reading stops before the first pending stage. If that barrier is the
        next unresolved position, ``pending`` names it; no record after it may
        be returned even when already committed. Aborted stages yield nothing
        and do not remain barriers. An empty result with no pending barrier is
        ordinary watch idleness.
        """

    @abc.abstractmethod
    async def output_tail(self, key: StreamKey) -> Cursor:
        """Return the end boundary of the currently readable committed prefix.

        The boundary never advances past a pending stage, because a caller that
        resumes after the returned cursor must still see that stage if it later
        commits. It may advance across aborted coordination positions. This is
        a position boundary only and does not correlate output to an Update.
        """

    @abc.abstractmethod
    def compare_offsets(self, left: Offset, right: Offset) -> int:
        """Three-way comparison under this provider's output offset order."""
