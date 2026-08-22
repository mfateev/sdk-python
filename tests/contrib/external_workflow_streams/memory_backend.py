"""A conforming in-memory backend, used to validate the conformance suite.

Its offsets are shaped like Redis stream ids -- ``<ms>-<seq>`` compared as a
numeric pair -- so the checks that catch lexical comparison and width-crossing
boundaries are meaningful here rather than only against a real Redis. The
millisecond component comes from :attr:`now_ms`, which tests set directly, so
those boundaries are reachable without sleeping.
"""

from __future__ import annotations

import asyncio
import time
from datetime import timedelta

from temporalio.contrib.external_workflow_streams._backend import (
    DEFAULT_WATCH_BLOCK,
    AppendConflictError,
    ParkIntent,
    StreamBackend,
    StreamKey,
)
from temporalio.contrib.external_workflow_streams._record import (
    Cursor,
    IdempotencyKey,
    Offset,
    StreamRecord,
)


def parse(offset: Offset) -> tuple[int, int]:
    """``"12-3"`` as ``(12, 3)``. The provider's ordering rule, in one place."""
    ms, _, seq = offset.token.partition("-")
    return int(ms), int(seq or 0)


class MemoryStreamBackend(StreamBackend):
    """A correct reference implementation."""

    guarantees_immutability = True
    provider_id = "memory"
    provider_format_version = 1

    def __init__(self) -> None:
        self.now_ms = 1
        self._records: dict[StreamKey, list[StreamRecord]] = {}
        self._by_key: dict[tuple[StreamKey, IdempotencyKey], StreamRecord] = {}
        self._appended = asyncio.Event()
        #: Every read_range call, for tests that assert replay's call count.
        self.range_reads: list[tuple[Offset, Offset]] = []
        #: Park intents, keyed `(stream key, wait_id)` -- never by stream alone.
        self._intents: dict[tuple[StreamKey, int], ParkIntent] = {}
        #: `(stream key, wait_id) -> (claimant, generation, expires_at)`.
        self._claims: dict[tuple[StreamKey, int], tuple[str, int, float]] = {}

    # --- required operations ------------------------------------------------

    async def append(self, key: StreamKey, record: StreamRecord) -> StreamRecord:
        existing = self._by_key.get((key, record.idempotency_key))
        if existing is not None:
            # Idempotent on *identity*: same bytes is a no-op returning the
            # original offset, different bytes is an error.
            if existing.to_fields() == record.to_fields():
                return existing
            raise AppendConflictError(record.idempotency_key)

        stream = self._records.setdefault(key, [])
        seq = sum(1 for r in stream if parse(r.offset).__getitem__(0) == self.now_ms)  # type: ignore[arg-type]
        placed = record.placed_at(Offset(f"{self.now_ms}-{seq}"))
        stream.append(placed)
        self._by_key[(key, record.idempotency_key)] = placed

        self._appended.set()
        self._appended.clear()
        return placed

    async def read_range(
        self, key: StreamKey, first: Offset, last: Offset
    ) -> list[StreamRecord]:
        self.range_reads.append((first, last))
        lo, hi = parse(first), parse(last)
        return [
            r
            for r in self._records.get(key, [])
            if lo <= parse(r.offset) <= hi  # type: ignore[arg-type]
        ]

    async def read_after(
        self,
        key: StreamKey,
        after: Cursor,
        *,
        max_records: int,
        block: timedelta | None = DEFAULT_WATCH_BLOCK,
    ) -> list[StreamRecord]:
        deadline = None if block is None else block.total_seconds()
        while True:
            found = self._after(key, after)[:max_records]
            if found or deadline is None or deadline <= 0:
                return found
            waiter = asyncio.ensure_future(self._appended.wait())
            try:
                await asyncio.wait_for(waiter, deadline)
            except asyncio.TimeoutError:
                return []
            finally:
                # `wait_for` cancels the outer future but the inner `Event.wait`
                # task survives it, and a leaked one keeps the event's waiter
                # list growing for the life of the process.
                if not waiter.done():
                    waiter.cancel()

    def _after(self, key: StreamKey, after: Cursor) -> list[StreamRecord]:
        stream = self._records.get(key, [])
        if after.is_beginning:
            return list(stream)
        bound = parse(after.offset)  # type: ignore[arg-type]
        return [r for r in stream if parse(r.offset) > bound]  # type: ignore[arg-type]

    def compare_offsets(self, left: Offset, right: Offset) -> int:
        a, b = parse(left), parse(right)
        return (a > b) - (a < b)

    # --- parking ------------------------------------------------------------

    supports_leased_claims = True

    async def install_park_intent(self, key: StreamKey, intent: ParkIntent) -> None:
        self._intents[(key, intent.wait_id)] = intent

    async def remove_park_intent(self, key: StreamKey, wait_id: int) -> None:
        self._intents.pop((key, wait_id), None)
        self._claims.pop((key, wait_id), None)

    async def remove_park_intent_if_matches(
        self,
        key: StreamKey,
        wait_id: int,
        *,
        run_id: str,
        park_generation: int,
    ) -> bool:
        intent = self._intents.get((key, wait_id))
        if intent is None or (
            intent.run_id != run_id or intent.park_generation != park_generation
        ):
            return False
        self._intents.pop((key, wait_id), None)
        self._claims.pop((key, wait_id), None)
        return True

    async def parked_wait_ids(self, key: StreamKey) -> list[int]:
        return sorted(wait_id for (stored, wait_id) in self._intents if stored == key)

    async def park_intent(self, key: StreamKey, wait_id: int) -> ParkIntent | None:
        return self._intents.get((key, wait_id))

    async def recheck(self, key: StreamKey, wait_id: int) -> bool:
        intent = self._intents.get((key, wait_id))
        if intent is None:
            return False
        return bool(self._after(key, intent.cursor))

    async def claim_park_generation(
        self,
        key: StreamKey,
        wait_id: int,
        park_generation: int,
        *,
        claimant: str,
        lease: timedelta,
    ) -> bool:
        now = time.monotonic()
        held = self._claims.get((key, wait_id))
        if held is not None:
            holder, generation, expires_at = held
            # A live claim held by someone else, for this generation, blocks.
            # An expired one is taken over -- that is the whole point of a
            # lease, and without it a producer that crashed here would strand
            # the generation forever.
            if (
                generation == park_generation
                and holder != claimant
                and expires_at > now
            ):
                return False
        self._claims[(key, wait_id)] = (
            claimant,
            park_generation,
            now + lease.total_seconds(),
        )
        return True

    async def current_park_generation(self, key: StreamKey, wait_id: int) -> int | None:
        intent = self._intents.get((key, wait_id))
        return None if intent is None else intent.park_generation

    # --- test affordances ---------------------------------------------------

    async def expire_claims_for_test(self) -> None:
        """Ages every claim past its lease.

        Stands in for a producer that crashed between claiming and signaling --
        the failure the lease exists for -- without making the test wait out a
        real lease.
        """
        self._claims = {
            key: (holder, generation, 0.0)
            for key, (holder, generation, _) in self._claims.items()
        }

    async def delete_for_test(self, key: StreamKey, offset: Offset) -> None:
        """Removes a record, standing in for XDEL, trimming, or retention loss.

        Deletion is permitted -- what immutability forbids is *rewriting* a
        record in place, which is why replay only has to detect absence.
        """
        stream = self._records.get(key, [])
        self._records[key] = [r for r in stream if r.offset != offset]

    def all_records(self, key: StreamKey) -> list[StreamRecord]:
        return list(self._records.get(key, []))
