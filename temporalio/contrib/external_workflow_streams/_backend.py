"""The stream backend contract (P2).

What a provider must implement to be registrable. The contract is small on
purpose -- every operation here exists because replay or parking cannot be made
correct without it.

The obligations that are easy to get subtly wrong, and that the conformance
suite exists to catch:

- **The range read is inclusive of both endpoints; the watch is exclusive.**
  These are two different operations, not one with a flag. Replay issues an
  inclusive read for a range the marker already names, and never asks the
  backend what comes next. Implementing the inclusive read with exclusive
  semantics is invisible until the first replay drops a record.
- **Offsets are compared by the provider's rule, not lexically.** Redis stream
  ids compare as numeric ``(ms, seq)`` tuples, and string comparison is wrong
  the moment the millisecond component changes width.
- **A cursor is a boundary, never the identity of a future record.** No
  operation may require naming a record that does not exist yet, or a consumer
  parked at the tail could not resume.
- **Append is idempotent on identity, not on key alone.** Reusing a
  ``(session_id, sequence)`` with byte-identical content is a no-op returning
  the original offset; reusing it with *different* bytes is an error, because
  silently accepting it would let a retried producer overwrite history.
- **Records are immutable.** Mandatory, and checked at registration rather than
  compensated for at runtime.
"""

from __future__ import annotations

import abc
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta
from typing import ClassVar, Final

from temporalio.contrib.external_workflow_streams._record import (
    AFTER,
    Cursor,
    IdempotencyKey,
    Offset,
    StreamRecord,
)

__all__ = [
    "AppendConflictError",
    "StreamBackend",
    "StreamKey",
]


@dataclass(frozen=True)
class StreamKey:
    """What identifies a stream, for the whole life of a Continue-As-New chain.

    ``first_execution_run_id`` rather than the current Run ID: it prevents
    collisions after Workflow ID reuse while staying stable across the chain,
    so a new Run continues the same stream rather than starting a fresh one.
    """

    namespace: str
    workflow_id: str
    first_execution_run_id: str
    stream_name: str

    def __post_init__(self) -> None:
        for field_name in (
            "namespace",
            "workflow_id",
            "first_execution_run_id",
            "stream_name",
        ):
            if not getattr(self, field_name):
                raise ValueError(f"a stream key needs a non-empty {field_name}")

    def __str__(self) -> str:
        return (
            f"{self.namespace}/{self.workflow_id}/"
            f"{self.first_execution_run_id}/{self.stream_name}"
        )


class AppendConflictError(Exception):
    """An idempotency key was reused with different bytes.

    Distinct from a successful idempotent retry, which returns the original
    offset and appends nothing. Accepting this quietly would let a retried
    producer attempt with changed input rewrite what a consumer may already
    have delivered.
    """

    def __init__(self, key: IdempotencyKey) -> None:
        super().__init__(
            f"idempotency key {key} was already used with different content; "
            "an append is idempotent on identity, not on the key alone"
        )
        self.key = key


#: How long a watch blocks before returning empty, when no timeout is given.
DEFAULT_WATCH_BLOCK: Final = timedelta(seconds=5)


class StreamBackend(abc.ABC):
    """A stream provider.

    Instances live **outside** the Workflow sandbox, on the Worker, and are
    named from Workflow code rather than imported into it.
    """

    guarantees_immutability: ClassVar[bool | None] = None
    """Whether this provider guarantees a record's bytes cannot change.

    ``None`` means *undeclared*, which is what an implementer who never
    considered the question leaves it as. Registration requires ``True``, so
    both "forgot" and "cannot" are rejected at Worker construction -- loudly,
    before any Workflow can name the backend -- rather than at replay, quietly,
    after data has already been consumed.

    The guarantee is what makes the four cheap range checks sufficient: given
    it, the only damage replay has to detect is a record that is no longer
    there.
    """

    provider_id: ClassVar[str] = ""
    """Stable identifier recorded in every annotation header."""

    provider_format_version: ClassVar[int] = 1
    """Bumped when this provider changes how it stores a record."""

    # --- required operations ------------------------------------------------

    @abc.abstractmethod
    async def append(self, key: StreamKey, record: StreamRecord) -> StreamRecord:
        """Appends one record, idempotently on ``(session_id, sequence)``.

        Returns:
            The record as stored, with its assigned offset. For a repeat append
            of byte-identical content this is the **original** offset and
            nothing new is written.

        Raises:
            AppendConflictError: The key was used before with different bytes.
        """

    @abc.abstractmethod
    async def read_range(
        self, key: StreamKey, first: Offset, last: Offset
    ) -> list[StreamRecord]:
        """Reads ``[first, last]`` -- **inclusive of both endpoints**.

        This is the replay read. The marker already names the range, so this
        must never consult "what comes next"; it returns exactly what is
        present in the closed interval, in increasing offset order, and says
        nothing about what is missing. Detecting a missing record is the
        caller's job, and it can only do it if this read is honest about what
        it found.
        """

    @abc.abstractmethod
    async def read_after(
        self,
        key: StreamKey,
        after: Cursor,
        *,
        max_records: int,
        block: timedelta | None = DEFAULT_WATCH_BLOCK,
    ) -> list[StreamRecord]:
        """Reads records **strictly after** ``after``, blocking up to ``block``.

        This is the live read. ``after`` is a boundary, so ``BEGINNING`` means
        the start of the stream and ``AFTER(x)`` means "whatever follows the
        record at ``x``" -- which must work whether or not such a record exists
        yet, because that is exactly the state a consumer parked at the tail is
        in.

        Returns an empty list when the block elapses with nothing new. Blocking
        is a provider concern and never reaches the Workflow thread.
        """

    @abc.abstractmethod
    def compare_offsets(self, left: Offset, right: Offset) -> int:
        """Three-way comparison under **this provider's** ordering rule.

        Synchronous and pure: replay validation calls it once per adjacent pair
        of a range, and an ordering that needed I/O would put backend latency
        inside a validation loop.
        """

    # --- provided on top of the required operations -------------------------

    async def watch(
        self,
        key: StreamKey,
        after: Cursor,
        *,
        max_records: int = 100,
        block: timedelta | None = DEFAULT_WATCH_BLOCK,
    ) -> AsyncIterator[StreamRecord]:
        """Yields records forever, starting strictly after ``after``.

        A convenience over :meth:`read_after` for prefetch loops. The cursor
        advances to ``AFTER(last delivered)``, so a resumption after any number
        of empty blocks continues from where delivery stopped rather than from
        where the stream currently is.
        """
        cursor = after
        while True:
            batch = await self.read_after(
                key, cursor, max_records=max_records, block=block
            )
            for record in batch:
                yield record
            if batch:
                last = batch[-1].offset
                assert last is not None, "a stored record always has an offset"
                cursor = AFTER(last)

    def is_before(self, left: Offset, right: Offset) -> bool:
        return self.compare_offsets(left, right) < 0

    def strictly_increasing(self, offsets: list[Offset]) -> bool:
        """Whether ``offsets`` ascend under this provider's ordering rule."""
        return all(self.compare_offsets(a, b) < 0 for a, b in zip(offsets, offsets[1:]))
