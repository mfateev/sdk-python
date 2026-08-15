"""The backend conformance suite (P2).

**The suite is the deliverable, not the interface.** Each check below encodes
one obligation from the backend contract and fails with a message naming the
obligation, so a provider author reads what they broke rather than a diff of
two lists.

Checks are plain async functions taking ``(backend, key)`` and raising
``AssertionError``. That shape is what lets two different callers use them: a
provider's own test parametrizes over :data:`CONFORMANCE_CHECKS` and expects
every one to pass, while the suite's own tests call a single check against a
deliberately-broken stub and assert it fails *for the right reason*. A suite
that cannot be shown to fail is not evidence of anything.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import timedelta

from temporalio.contrib.external_workflow_streams._backend import (
    AppendConflictError,
    StreamBackend,
    StreamKey,
)
from temporalio.contrib.external_workflow_streams._record import (
    AFTER,
    BEGINNING,
    Offset,
    RecordKind,
    StreamRecord,
)

Check = Callable[[StreamBackend, StreamKey], Awaitable[None]]

NO_BLOCK = timedelta(0)


def _data(session: str, seq: int, payload: bytes) -> StreamRecord:
    return StreamRecord(RecordKind.DATA, payload, session, seq)


def _fence(session: str, seq: int) -> StreamRecord:
    return StreamRecord(RecordKind.WRITE_FENCE, b"", session, seq)


async def _append_all(
    backend: StreamBackend, key: StreamKey, payloads: list[bytes], session: str = "s"
) -> list[StreamRecord]:
    return [
        await backend.append(key, _data(session, i, p)) for i, p in enumerate(payloads)
    ]


# --- ordering ---------------------------------------------------------------


async def check_offsets_are_totally_ordered(
    backend: StreamBackend, key: StreamKey
) -> None:
    """Appended records must ascend under the provider's own comparator."""
    placed = await _append_all(backend, key, [b"a", b"b", b"c"])
    offsets = [r.offset for r in placed]

    assert all(o is not None for o in offsets), (
        "append must return the record with its assigned offset"
    )
    assert backend.strictly_increasing(offsets), (  # type: ignore[arg-type]
        f"appended offsets must strictly increase under compare_offsets, got {offsets}"
    )


async def check_offsets_are_not_compared_lexically(
    backend: StreamBackend, key: StreamKey
) -> None:
    """Ordering must survive a width change in the offset's leading component.

    This is the check that catches string comparison, and it is invisible
    without it: ``"9-0" < "10-0"`` is true numerically and false lexically, so
    a lexical comparator is correct for nine appends out of ten and then
    silently reorders a range.
    """
    early, late = await _append_at_widening_boundary(backend, key)

    assert backend.compare_offsets(early, late) < 0, (
        f"offset {early} was appended before {late}, so compare_offsets must "
        "order it first; comparing offsets lexically fails here"
    )
    assert backend.compare_offsets(late, early) > 0, (
        "compare_offsets must be antisymmetric"
    )
    assert backend.compare_offsets(early, early) == 0, (
        "compare_offsets must report an offset as equal to itself"
    )


async def _append_at_widening_boundary(
    backend: StreamBackend, key: StreamKey
) -> tuple[Offset, Offset]:
    """Two offsets whose leading components differ in digit width.

    A provider that assigns offsets from a clock it exposes (the in-memory
    reference) is driven across the boundary directly. Anything else -- a real
    Redis -- gets whatever it assigns, and the check still holds because the
    comparator must order *its own* offsets correctly either way.
    """
    now = getattr(backend, "now_ms", None)
    if now is not None:
        backend.now_ms = 9  # type: ignore[attr-defined]
        early = await backend.append(key, _data("widen", 0, b"early"))
        backend.now_ms = 10  # type: ignore[attr-defined]
        late = await backend.append(key, _data("widen", 1, b"late"))
    else:
        early = await backend.append(key, _data("widen", 0, b"early"))
        late = await backend.append(key, _data("widen", 1, b"late"))
    assert early.offset is not None and late.offset is not None
    return early.offset, late.offset


# --- the inclusive range read -----------------------------------------------


async def check_range_read_includes_both_endpoints(
    backend: StreamBackend, key: StreamKey
) -> None:
    """``read_range(first, last)`` is closed on both ends.

    A provider implementing it with exclusive semantics passes every live test
    and drops the boundary records on the first replay, which is why this is
    checked directly rather than inferred from a round trip.
    """
    placed = await _append_all(backend, key, [b"a", b"b", b"c"])
    first, last = placed[0].offset, placed[-1].offset
    assert first is not None and last is not None

    got = await backend.read_range(key, first, last)

    assert [r.payload for r in got] == [b"a", b"b", b"c"], (
        "read_range must be inclusive of both endpoints; it returned "
        f"{[r.payload for r in got]} for the full range"
    )


async def check_range_read_of_one_record_returns_it(
    backend: StreamBackend, key: StreamKey
) -> None:
    """The degenerate range ``[x, x]`` contains exactly the record at ``x``."""
    placed = await _append_all(backend, key, [b"a", b"b"])
    only = placed[0].offset
    assert only is not None

    got = await backend.read_range(key, only, only)

    assert [r.payload for r in got] == [b"a"], (
        f"read_range({only}, {only}) must return exactly that record, got "
        f"{[r.payload for r in got]}"
    )


async def check_range_read_reports_a_missing_record_as_missing(
    backend: StreamBackend, key: StreamKey
) -> None:
    """A deleted record must be absent, not substituted for.

    Integrity loss must never resolve to an alternate stream result. The bug
    this catches is a range read implemented as "N records starting at
    ``first``" rather than as the closed interval ``[first, last]``: it returns
    the right *count* by reaching past ``last``, so a trimmed stream reads back
    as intact and replay delivers a record the original run never saw.

    A fourth record is appended beyond the range precisely so there is
    something for such a provider to reach for.
    """
    placed = await _append_all(backend, key, [b"a", b"b", b"c", b"d"])
    first, middle, last = placed[0].offset, placed[1].offset, placed[2].offset
    assert first is not None and middle is not None and last is not None

    delete = getattr(backend, "delete", None)
    if delete is None:
        return  # This provider cannot be made to lose a record on demand.
    delete(key, middle)

    got = await backend.read_range(key, first, last)

    assert [r.payload for r in got] == [b"a", b"c"], (
        "read_range must report a deleted record as absent rather than "
        f"substituting another from beyond the range, got {[r.payload for r in got]}"
    )


# --- the exclusive watch ----------------------------------------------------


async def check_watch_is_exclusive_of_its_boundary(
    backend: StreamBackend, key: StreamKey
) -> None:
    """``read_after(AFTER(x))`` must not re-deliver the record at ``x``.

    An inclusive watch re-delivers the last consumed record after every
    resumption, which a Workflow sees as a duplicate.
    """
    placed = await _append_all(backend, key, [b"a", b"b"])
    first = placed[0].offset
    assert first is not None

    got = await backend.read_after(key, AFTER(first), max_records=10, block=NO_BLOCK)

    assert [r.payload for r in got] == [b"b"], (
        "read_after must return records strictly after its boundary; it "
        f"returned {[r.payload for r in got]} after the first record"
    )


async def check_beginning_reads_from_the_start(
    backend: StreamBackend, key: StreamKey
) -> None:
    """``BEGINNING`` is a boundary before every record, not an offset."""
    await _append_all(backend, key, [b"a", b"b"])

    got = await backend.read_after(key, BEGINNING, max_records=10, block=NO_BLOCK)

    assert [r.payload for r in got] == [b"a", b"b"], (
        "read_after(BEGINNING) must return every record from the start, got "
        f"{[r.payload for r in got]}"
    )


async def check_a_consumer_parked_at_the_tail_resumes(
    backend: StreamBackend, key: StreamKey
) -> None:
    """The core cursor-semantics check.

    A consumer that has drained the stream holds ``AFTER(last)`` -- a boundary
    past the tail. It must be able to resume from there once a record whose id
    it could not have predicted arrives. A provider that requires the cursor to
    name an existing record cannot express this state at all.
    """
    placed = await _append_all(backend, key, [b"a"])
    tail = placed[0].offset
    assert tail is not None

    # Parked: nothing beyond the boundary yet, and the boundary names a record
    # that is the last one in existence.
    empty = await backend.read_after(key, AFTER(tail), max_records=10, block=NO_BLOCK)
    assert empty == [], (
        f"read_after(AFTER({tail})) must be empty while {tail} is the tail, got {empty}"
    )

    unpredictable = await backend.append(key, _data("later", 0, b"surprise"))

    resumed = await backend.read_after(key, AFTER(tail), max_records=10, block=NO_BLOCK)

    assert [r.payload for r in resumed] == [b"surprise"], (
        "a consumer parked at the tail must resume once a record arrives, "
        f"without having to name its id in advance; got {[r.payload for r in resumed]}"
    )
    assert resumed[0].offset == unpredictable.offset


async def check_watch_respects_max_records(
    backend: StreamBackend, key: StreamKey
) -> None:
    """Backpressure is the buffer bound, so the read must honour it."""
    await _append_all(backend, key, [b"a", b"b", b"c", b"d"])

    got = await backend.read_after(key, BEGINNING, max_records=2, block=NO_BLOCK)

    assert len(got) == 2, f"read_after must return at most max_records, got {len(got)}"


async def check_watch_returns_empty_rather_than_hanging(
    backend: StreamBackend, key: StreamKey
) -> None:
    """An elapsed block returns empty; it does not raise and does not hang."""
    got = await backend.read_after(
        key, BEGINNING, max_records=10, block=timedelta(milliseconds=50)
    )

    assert got == [], f"an elapsed block must return empty, got {got}"


# --- idempotent append ------------------------------------------------------


async def check_identical_reappend_is_a_no_op(
    backend: StreamBackend, key: StreamKey
) -> None:
    """A retried Activity attempt must not duplicate its record."""
    first = await backend.append(key, _data("retry", 0, b"payload"))
    again = await backend.append(key, _data("retry", 0, b"payload"))

    assert again.offset == first.offset, (
        "re-appending byte-identical content under the same idempotency key "
        f"must return the original offset {first.offset}, got {again.offset}"
    )

    assert first.offset is not None
    in_range = await backend.read_range(key, first.offset, first.offset)
    assert len(in_range) == 1, (
        f"an idempotent re-append must write nothing, but {len(in_range)} "
        "records occupy the original offset"
    )


async def check_reappend_with_different_bytes_is_rejected(
    backend: StreamBackend, key: StreamKey
) -> None:
    """Idempotency is on identity, not on the key alone.

    A provider that accepts this lets a retried producer attempt with changed
    input overwrite what a consumer may already have delivered -- and the
    divergence surfaces much later, as an unrelated nondeterminism error.
    """
    await backend.append(key, _data("conflict", 0, b"original"))

    try:
        await backend.append(key, _data("conflict", 0, b"different"))
    except AppendConflictError:
        return
    raise AssertionError(
        "re-appending different bytes under an existing idempotency key must "
        "raise AppendConflictError; the append was accepted instead"
    )


async def check_different_sessions_do_not_collide(
    backend: StreamBackend, key: StreamKey
) -> None:
    """The key is ``(session_id, sequence)``, so sequence alone is not enough."""
    one = await backend.append(key, _data("session-a", 0, b"a"))
    two = await backend.append(key, _data("session-b", 0, b"b"))

    assert one.offset != two.offset, (
        "two producers each writing their own sequence 0 must get distinct "
        "offsets; the idempotency key includes the session id"
    )


# --- control records --------------------------------------------------------


async def check_control_records_share_the_data_offset_sequence(
    backend: StreamBackend, key: StreamKey
) -> None:
    """A fence occupies an offset and advances the cursor like any record."""
    before = await backend.append(key, _data("s", 0, b"a"))
    fence = await backend.append(key, _fence("s", 1))
    after = await backend.append(key, _data("s", 2, b"b"))

    offsets = [before.offset, fence.offset, after.offset]
    assert backend.strictly_increasing(offsets), (  # type: ignore[arg-type]
        f"a write fence must take its place in the offset sequence, got {offsets}"
    )

    assert before.offset is not None and after.offset is not None
    got = await backend.read_range(key, before.offset, after.offset)
    kinds = [r.kind for r in got]
    assert kinds == [RecordKind.DATA, RecordKind.WRITE_FENCE, RecordKind.DATA], (
        f"a range read must return control records in place, got {kinds}"
    )


async def check_records_round_trip_unchanged(
    backend: StreamBackend, key: StreamKey
) -> None:
    """Every field a producer wrote must read back exactly."""
    written = StreamRecord(RecordKind.DATA, b"\x00\xff bytes", "session-x", 42)
    placed = await backend.append(key, written)

    assert placed.offset is not None
    (got,) = await backend.read_range(key, placed.offset, placed.offset)

    assert got.payload == written.payload, "payload bytes must round-trip exactly"
    assert got.producer_session_id == written.producer_session_id
    assert got.sequence == written.sequence
    assert got.kind == written.kind


# --- registration precondition ----------------------------------------------


async def check_immutability_is_declared(
    backend: StreamBackend, key: StreamKey
) -> None:
    """Declared, not merely true. Undeclared is a registration failure.

    The registry is what enforces this at Worker construction; the check is
    here so a provider author finds out while writing the provider rather than
    when a Worker refuses to start.
    """
    assert type(backend).guarantees_immutability is True, (
        f"{type(backend).__name__} must declare guarantees_immutability = True; "
        f"it is {type(backend).guarantees_immutability!r}. The guarantee is a "
        "precondition for registration, not a runtime mode."
    )
    assert type(backend).provider_id, (
        f"{type(backend).__name__} must declare a provider_id; every annotation "
        "header records it"
    )


#: Every check, in the order a provider author most usefully reads them.
CONFORMANCE_CHECKS: list[Check] = [
    check_immutability_is_declared,
    check_offsets_are_totally_ordered,
    check_offsets_are_not_compared_lexically,
    check_range_read_includes_both_endpoints,
    check_range_read_of_one_record_returns_it,
    check_range_read_reports_a_missing_record_as_missing,
    check_watch_is_exclusive_of_its_boundary,
    check_beginning_reads_from_the_start,
    check_a_consumer_parked_at_the_tail_resumes,
    check_watch_respects_max_records,
    check_watch_returns_empty_rather_than_hanging,
    check_identical_reappend_is_a_no_op,
    check_reappend_with_different_bytes_is_rejected,
    check_different_sessions_do_not_collide,
    check_control_records_share_the_data_offset_sequence,
    check_records_round_trip_unchanged,
]
