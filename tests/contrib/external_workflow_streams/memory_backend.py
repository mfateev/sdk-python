"""A conforming in-memory backend, used to validate the conformance suite.

Its offsets are shaped like Redis stream ids -- ``<ms>-<seq>`` compared as a
numeric pair -- so the checks that catch lexical comparison and width-crossing
boundaries are meaningful here rather than only against a real Redis. The
millisecond component comes from :attr:`now_ms`, which tests set directly, so
those boundaries are reachable without sleeping.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio.contrib.external_workflow_streams._backend import (
    DEFAULT_WATCH_BLOCK,
    AppendConflictError,
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

    def _after(self, key: StreamKey, after: Cursor) -> list[StreamRecord]:
        stream = self._records.get(key, [])
        if after.is_beginning:
            return list(stream)
        bound = parse(after.offset)  # type: ignore[arg-type]
        return [r for r in stream if parse(r.offset) > bound]  # type: ignore[arg-type]

    def compare_offsets(self, left: Offset, right: Offset) -> int:
        a, b = parse(left), parse(right)
        return (a > b) - (a < b)

    # --- test affordances ---------------------------------------------------

    def delete(self, key: StreamKey, offset: Offset) -> None:
        """Removes a record, standing in for XDEL, trimming, or retention loss.

        Deletion is permitted -- what immutability forbids is *rewriting* a
        record in place, which is why replay only has to detect absence.
        """
        stream = self._records.get(key, [])
        self._records[key] = [r for r in stream if r.offset != offset]

    def all_records(self, key: StreamKey) -> list[StreamRecord]:
        return list(self._records.get(key, []))
