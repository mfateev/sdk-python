"""P2 — the conformance suite, and proof that it fails a broken backend.

The first half runs every check against the conforming reference backend. The
second half runs a *single* check against a stub broken in exactly one way and
asserts it fails with the message naming that obligation. A suite that only
ever passes is not evidence of anything.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import cast

import pytest

from temporalio.contrib.external_workflow_streams._backend import (
    StreamBackend,
    StreamKey,
)
from temporalio.contrib.external_workflow_streams._record import (
    AFTER,
    Cursor,
    Offset,
    RecordKind,
    StreamRecord,
)
from tests.contrib.external_workflow_streams import conformance
from tests.contrib.external_workflow_streams.conformance import (
    CONFORMANCE_CHECKS,
    Check,
)
from tests.contrib.external_workflow_streams.memory_backend import (
    MemoryStreamBackend,
    parse,
)


@pytest.fixture
def stream_key() -> StreamKey:
    return StreamKey("ns", "wf", uuid.uuid4().hex, "tokens")


@pytest.fixture
def backend() -> MemoryStreamBackend:
    return MemoryStreamBackend()


# --- the reference backend conforms -----------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("check", CONFORMANCE_CHECKS, ids=lambda c: c.__name__)
async def test_reference_backend_conforms(
    check: Check, backend: MemoryStreamBackend, stream_key: StreamKey
) -> None:
    await check(backend, stream_key)


def test_the_suite_is_not_silently_empty() -> None:
    assert len(CONFORMANCE_CHECKS) >= 16
    assert len({c.__name__ for c in CONFORMANCE_CHECKS}) == len(CONFORMANCE_CHECKS)


# --- deliberately broken backends -------------------------------------------


class ExclusiveRangeBackend(MemoryStreamBackend):
    """Implements the inclusive range read with exclusive-of-first semantics.

    The realistic mistake: reaching for the same primitive the watch uses.
    """

    async def read_range(
        self, key: StreamKey, first: Offset, last: Offset
    ) -> list[StreamRecord]:
        got = await super().read_range(key, first, last)
        return [r for r in got if r.offset != first]


class LexicalOffsetBackend(MemoryStreamBackend):
    """Compares offsets as strings."""

    def compare_offsets(self, left: Offset, right: Offset) -> int:
        return (left.token > right.token) - (left.token < right.token)


class NameableCursorBackend(MemoryStreamBackend):
    """Validates the resume token against a record that must follow it.

    Stands for a provider whose cursor is a record identity rather than a
    boundary: a position with nothing after it is not a position it can
    express, so a consumer that has drained to the tail has nothing legal to
    hold. This is the state parking puts every subscription in, which is why
    the contract forbids it.
    """

    async def read_after(
        self,
        key: StreamKey,
        after: Cursor,
        *,
        max_records: int,
        block: timedelta | None = None,
    ) -> list[StreamRecord]:
        if not after.is_beginning:
            bound = parse(after.offset)  # type: ignore[arg-type]
            if not any(parse(r.offset) > bound for r in self.all_records(key)):  # type: ignore[arg-type]
                raise ValueError(
                    f"cursor {after.offset} names no position: nothing follows it"
                )
        return await super().read_after(
            key, after, max_records=max_records, block=block
        )


class KeyOnlyIdempotencyBackend(MemoryStreamBackend):
    """Idempotent on the key alone, ignoring whether the bytes match."""

    async def append(self, key: StreamKey, record: StreamRecord) -> StreamRecord:
        existing = self._by_key.get((key, record.idempotency_key))
        if existing is not None:
            return existing
        return await super().append(key, record)


class InclusiveWatchBackend(MemoryStreamBackend):
    """Re-delivers the record its boundary names."""

    async def read_after(
        self,
        key: StreamKey,
        after: Cursor,
        *,
        max_records: int,
        block: timedelta | None = None,
    ) -> list[StreamRecord]:
        if after.is_beginning:
            return await super().read_after(
                key, after, max_records=max_records, block=block
            )
        bound = parse(after.offset)  # type: ignore[arg-type]
        return [r for r in self.all_records(key) if parse(r.offset) >= bound][  # type: ignore[arg-type]
            :max_records
        ]


class CountingRangeBackend(MemoryStreamBackend):
    """Reads "however many records fit between the endpoints, starting at first".

    The realistic mistake: treating ``last`` as a length hint rather than as a
    closing endpoint, so a gap is silently filled from beyond the range and a
    trimmed stream reads back with the right count.
    """

    async def read_range(
        self, key: StreamKey, first: Offset, last: Offset
    ) -> list[StreamRecord]:
        want = len(
            [
                r
                for r in self.all_records(key)
                if parse(first) <= parse(r.offset) <= parse(last)  # type: ignore[arg-type]
            ]
        )
        # Whatever was asked for, deliver `want` records starting at `first` --
        # reaching past `last` when the interval has holes in it.
        from_first = [
            r
            for r in self.all_records(key)
            if parse(r.offset) >= parse(first)  # type: ignore[arg-type]
        ]
        return from_first[: max(want, 3)]


class UndeclaredImmutabilityBackend(MemoryStreamBackend):
    """Never considered the question."""

    guarantees_immutability = None


class DeniedImmutabilityBackend(MemoryStreamBackend):
    """Considered it and cannot make the guarantee."""

    guarantees_immutability = False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("broken", "check", "reason"),
    [
        pytest.param(
            ExclusiveRangeBackend,
            conformance.check_range_read_includes_both_endpoints,
            "inclusive of both endpoints",
            id="exclusive-range-read",
        ),
        pytest.param(
            LexicalOffsetBackend,
            conformance.check_offsets_are_not_compared_lexically,
            "lexically",
            id="lexical-offset-comparison",
        ),
        pytest.param(
            InclusiveWatchBackend,
            conformance.check_watch_is_exclusive_of_its_boundary,
            "strictly after",
            id="inclusive-watch",
        ),
        pytest.param(
            KeyOnlyIdempotencyBackend,
            conformance.check_reappend_with_different_bytes_is_rejected,
            "must raise AppendConflictError",
            id="idempotent-on-key-alone",
        ),
        pytest.param(
            CountingRangeBackend,
            conformance.check_range_read_reports_a_missing_record_as_missing,
            "rather than substituting another",
            id="substitutes-for-a-deleted-record",
        ),
        pytest.param(
            UndeclaredImmutabilityBackend,
            conformance.check_immutability_is_declared,
            "must declare guarantees_immutability",
            id="immutability-undeclared",
        ),
        pytest.param(
            DeniedImmutabilityBackend,
            conformance.check_immutability_is_declared,
            "must declare guarantees_immutability",
            id="immutability-denied",
        ),
    ],
)
async def test_a_broken_backend_fails_for_the_right_reason(
    broken: type[StreamBackend], check: Check, reason: str, stream_key: StreamKey
) -> None:
    with pytest.raises(AssertionError, match=reason):
        await check(broken(), stream_key)


@pytest.mark.asyncio
async def test_a_backend_needing_a_nameable_cursor_fails_the_tail_check(
    stream_key: StreamKey,
) -> None:
    """Fails, though not with an assertion -- it cannot express the state at all.

    Worth its own case: the failure mode is "the provider refuses the cursor",
    not "the provider returned the wrong records", and a suite that only caught
    AssertionError would let it through.
    """
    with pytest.raises((AssertionError, ValueError)):
        await conformance.check_a_consumer_parked_at_the_tail_resumes(
            NameableCursorBackend(), stream_key
        )


# --- the derived helpers on the ABC -----------------------------------------


@pytest.mark.asyncio
async def test_watch_iterates_across_batches(
    backend: MemoryStreamBackend, stream_key: StreamKey
) -> None:
    """The cursor advances past what was delivered, not to the current tail."""
    for i, payload in enumerate([b"a", b"b", b"c"]):
        await backend.append(stream_key, StreamRecord(RecordKind.DATA, payload, "s", i))

    seen = []
    watcher = cast(
        AsyncGenerator[StreamRecord, None],
        backend.watch(
            stream_key, Cursor(), max_records=1, block=timedelta(milliseconds=10)
        ),
    )
    async for record in watcher:
        seen.append(record.payload)
        if len(seen) == 3:
            break
    await watcher.aclose()

    assert seen == [b"a", b"b", b"c"]


@pytest.mark.asyncio
async def test_strictly_increasing_uses_the_provider_comparator(
    backend: MemoryStreamBackend,
) -> None:
    assert backend.strictly_increasing([Offset("9-0"), Offset("10-0")])
    assert not backend.strictly_increasing([Offset("10-0"), Offset("9-0")])
    assert not backend.strictly_increasing([Offset("9-0"), Offset("9-0")])


@pytest.mark.asyncio
async def test_read_after_blocks_until_a_record_arrives(
    backend: MemoryStreamBackend, stream_key: StreamKey
) -> None:
    """Blocking is the provider's, so latency never reaches the Workflow thread."""
    import asyncio

    placed = await backend.append(
        stream_key, StreamRecord(RecordKind.DATA, b"a", "s", 0)
    )
    assert placed.offset is not None

    async def append_soon() -> None:
        await asyncio.sleep(0.05)
        await backend.append(stream_key, StreamRecord(RecordKind.DATA, b"b", "s", 1))

    task = asyncio.ensure_future(append_soon())
    got = await backend.read_after(
        stream_key, AFTER(placed.offset), max_records=10, block=timedelta(seconds=2)
    )
    await task

    assert [r.payload for r in got] == [b"b"]
