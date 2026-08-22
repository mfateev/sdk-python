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
import enum
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
    "ParkIntent",
    "StreamBackend",
    "StreamKey",
]


@dataclass(frozen=True)
class ParkIntent:
    """One subscription's declaration that it is about to park.

    The key is ``(stream key, wait_id)``; everything here is the value.
    """

    wait_id: int
    cursor: Cursor
    """Where this subscription stopped consuming. What ``recheck`` reads past."""
    park_generation: int
    """The quiescence generation being parked. A wake Signal names this."""
    run_id: str
    """The Run that installed it -- part of the value so a new Run's intent
    replaces its predecessor's for the same key rather than accumulating."""

    def __post_init__(self) -> None:
        if self.park_generation < 1:
            # 0 is the reserved unparked-wake sentinel, so a real park
            # generation can never take it.
            raise ValueError(
                f"a park generation is a quiescence generation and starts at 1, "
                f"got {self.park_generation}"
            )


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


class ParkIntentRemoval(enum.Enum):
    """What a conditional park-intent removal found at the key.

    Three outcomes rather than a Boolean, because two of them mean the intent
    the consumer named is gone and one means it never got out of the way, and
    the consumer acts differently on each: a removal that happened ends a wake
    suppression that may owe a record an announcement, while a live intent that
    replaced it does not.
    """

    REMOVED = 1
    """The intent matched the named Run and park generation, and is gone."""

    ABSENT = 2
    """Nothing was installed at the key.

    Reported for a key that is already clear -- including one this provider
    itself cleared on a call whose reply never arrived. It is *not* the same as
    a key some other intent holds.
    """

    MISMATCH = 3
    """A different Run or park generation is installed, and was left alone."""


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

    # --- parking (P2b) ------------------------------------------------------

    supports_leased_claims: ClassVar[bool] = False
    """Whether :meth:`claim_park_generation` expires claims and permits takeover.

    An unleased claim introduces a failure mode with no recovery: a producer
    that crashes between claiming a generation and sending its wake Signal
    strands that generation, and every other producer concludes the wake is
    already handled -- so the parked Workflow waits forever with data sitting
    in the stream.

    A provider that cannot lease must declare ``False`` and expose
    **observe-only** semantics instead: :meth:`claim_park_generation` always
    grants, and every producer signals idempotently. That costs duplicate
    Signals, which are harmless, rather than lost ones, which are not.
    """

    @abc.abstractmethod
    async def install_park_intent(self, key: StreamKey, intent: ParkIntent) -> None:
        """Records that one **subscription** intends to park.

        Keyed ``(stream key, wait_id)``, never by stream key alone: two
        subscriptions in one Workflow to the same stream are two independent
        waits, and a stream-keyed intent would have one overwrite the other --
        after which only one of them could ever be woken.

        The current Run ID is part of the intent's *value*, not its key, so a
        new Run's intent deterministically replaces its predecessor's rather
        than accumulating beside it.
        """

    @abc.abstractmethod
    async def remove_park_intent(self, key: StreamKey, wait_id: int) -> None:
        """Removes one subscription's intent. Idempotent."""

    @abc.abstractmethod
    async def remove_park_intent_if_matches(
        self,
        key: StreamKey,
        wait_id: int,
        *,
        run_id: str,
        park_generation: int,
    ) -> ParkIntentRemoval:
        """Atomically removes an intent only when its identity still matches.

        The comparison and removal must be one backend operation. A delayed
        cleanup can overlap a Continue-As-New successor installing a new intent
        at the same ``(stream key, wait_id)``; implementing this as
        :meth:`park_intent` followed by :meth:`remove_park_intent` can delete
        that successor's live park.

        All three outcomes must be reported distinctly. Collapsing
        :attr:`ParkIntentRemoval.ABSENT` into
        :attr:`ParkIntentRemoval.MISMATCH` tells the consumer that someone
        else's park is in the way when in fact the key is clear -- which is what
        a retry after a lost reply sees, the reply to a delete this provider
        already performed.
        """

    @abc.abstractmethod
    async def park_intent(self, key: StreamKey, wait_id: int) -> ParkIntent | None:
        """The installed intent, if any."""

    @abc.abstractmethod
    async def recheck(self, key: StreamKey, wait_id: int) -> bool:
        """Whether any record now sits past the installed intent's cursor.

        Called after every intent is installed, and it is this recheck -- not
        the intent -- that closes the append/park race: a producer appends
        before it observes the park generation, so an append is either seen
        here or paired with a wake Signal.
        """

    @abc.abstractmethod
    async def claim_park_generation(
        self,
        key: StreamKey,
        wait_id: int,
        park_generation: int,
        *,
        claimant: str,
        lease: timedelta,
    ) -> bool:
        """Tries to take responsibility for waking ``park_generation``.

        Returns ``True`` when this caller now holds the claim -- including when
        it is renewing a claim it already held, so a producer can extend rather
        than losing it mid-flight.

        A provider that declares ``supports_leased_claims = False`` must always
        return ``True``: observe-only semantics, where every producer signals.
        """

    @abc.abstractmethod
    async def parked_wait_ids(self, key: StreamKey) -> list[int]:
        """Every subscription with an installed intent on this stream.

        A producer knows the stream it published to and nothing about the
        Workflow's subscriptions -- ``wait_id`` is allocated by a per-Run counter
        inside ``subscribe()``, which no producer can see. The other five
        parking operations all take a ``wait_id``, so without this one a producer
        that has just appended has no way to address a wake at all.

        Enumeration rather than a new concept: intents are already keyed
        ``(stream key, wait_id)``, and this is the ``wait_id`` half of that key
        for one stream.
        """

    @abc.abstractmethod
    async def current_park_generation(self, key: StreamKey, wait_id: int) -> int | None:
        """The generation a producer should name in its wake Signal.

        ``None`` means no confirmed park is installed for this subscription, in
        which case a producer sends an *unparked* wake -- generation 0 -- rather
        than staying silent.
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
