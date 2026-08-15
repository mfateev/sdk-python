"""P1 — record model, offsets, cursor boundaries, control records."""

from __future__ import annotations

import pytest

from temporalio.contrib.external_workflow_streams._record import (
    AFTER,
    BEGINNING,
    Cursor,
    IdempotencyKey,
    Offset,
    RecordKind,
    StreamRecord,
)


def redis_like_compare(left: Offset, right: Offset) -> int:
    """The ``(ms, seq)`` numeric ordering, used here to stand in for a provider."""

    def parts(offset: Offset) -> tuple[int, int]:
        ms, _, seq = offset.token.partition("-")
        return int(ms), int(seq or 0)

    return (parts(left) > parts(right)) - (parts(left) < parts(right))


# --- offsets ----------------------------------------------------------------


def test_offset_round_trips() -> None:
    offset = Offset("1700000000000-3")
    assert Offset.deserialize(offset.serialize()) == offset


def test_offset_is_hashable_and_equatable() -> None:
    assert Offset("1-0") == Offset("1-0")
    assert len({Offset("1-0"), Offset("1-0"), Offset("1-1")}) == 2


def test_offset_rejects_an_empty_token() -> None:
    with pytest.raises(ValueError, match="may not be empty"):
        Offset("")


def test_offset_does_not_expose_lexical_ordering() -> None:
    """The failure this prevents is invisible until a millisecond width change.

    ``"9-0" < "10-0"`` is false lexically and true numerically, so an offset
    that supported ``<`` would silently order wrongly for one provider in ten.
    """
    with pytest.raises(TypeError):
        _ = Offset("9-0") < Offset("10-0")  # type: ignore[operator]

    assert redis_like_compare(Offset("9-0"), Offset("10-0")) < 0


# --- cursors ----------------------------------------------------------------


def test_cursor_forms_round_trip() -> None:
    for cursor in (BEGINNING, AFTER(Offset("1700000000000-0"))):
        assert Cursor.deserialize(cursor.serialize()) == cursor


def test_beginning_is_distinguishable_from_a_provider_beginning_sentinel() -> None:
    """Redis spells its beginning ``0-0``; that is an offset, not ``BEGINNING``."""
    assert BEGINNING != AFTER(Offset("0-0"))
    assert BEGINNING.serialize() != AFTER(Offset("0-0")).serialize()
    assert BEGINNING.is_beginning
    assert not AFTER(Offset("0-0")).is_beginning


def test_cursor_deserialize_rejects_junk() -> None:
    with pytest.raises(ValueError, match="not a serialized cursor"):
        Cursor.deserialize("1700000000000-0")


def test_after_excludes_up_to_and_including_its_offset() -> None:
    cursor = AFTER(Offset("100-0"))

    assert cursor.excludes(Offset("99-9"), redis_like_compare)
    assert cursor.excludes(Offset("100-0"), redis_like_compare)
    assert not cursor.excludes(Offset("100-1"), redis_like_compare)


def test_beginning_excludes_nothing() -> None:
    assert not BEGINNING.excludes(Offset("0-1"), redis_like_compare)


def test_after_names_a_boundary_past_the_tail() -> None:
    """A consumer parked at the tail names the last record, never the next one.

    Nothing here requires the record after ``100-0`` to exist -- which is the
    whole point of a boundary cursor.
    """
    assert str(AFTER(Offset("100-0"))) == "AFTER(100-0)"


# --- records ----------------------------------------------------------------


def test_record_round_trips_through_fields() -> None:
    record = StreamRecord(
        kind=RecordKind.DATA,
        payload=b"\x00\xffhello",
        producer_session_id="session-a",
        sequence=7,
    ).placed_at(Offset("100-0"))

    assert StreamRecord.from_fields(Offset("100-0"), record.to_fields()) == record


def test_record_round_trips_from_bytes_keyed_fields() -> None:
    """A provider that did not decode its responses must still round-trip."""
    record = StreamRecord(RecordKind.DATA, b"x", "session-a", 0).placed_at(
        Offset("100-0")
    )
    raw = {key.encode(): value for key, value in record.to_fields().items()}

    assert StreamRecord.from_fields(Offset("100-0"), raw) == record


def test_fence_round_trips_and_carries_no_payload() -> None:
    fence = StreamRecord(RecordKind.WRITE_FENCE, b"", "session-a", 8).placed_at(
        Offset("101-0")
    )

    assert fence.is_control
    assert StreamRecord.from_fields(Offset("101-0"), fence.to_fields()) == fence


def test_a_fence_with_a_payload_is_rejected() -> None:
    with pytest.raises(ValueError, match="carries no payload"):
        StreamRecord(RecordKind.WRITE_FENCE, b"nope", "session-a", 0)


def test_from_fields_names_what_is_missing() -> None:
    with pytest.raises(ValueError, match="missing field"):
        StreamRecord.from_fields(Offset("1-0"), {"__tes_kind": b"1"})


def test_data_and_control_share_one_offset_sequence() -> None:
    """The fence sits between data records and advances the cursor like them."""
    placed = [
        StreamRecord(RecordKind.DATA, b"a", "s", 0).placed_at(Offset("100-0")),
        StreamRecord(RecordKind.DATA, b"b", "s", 1).placed_at(Offset("100-1")),
        StreamRecord(RecordKind.WRITE_FENCE, b"", "s", 2).placed_at(Offset("100-2")),
        StreamRecord(RecordKind.DATA, b"c", "other", 0).placed_at(Offset("100-3")),
    ]

    offsets = [r.offset for r in placed]
    assert all(
        redis_like_compare(a, b) < 0  # type: ignore[arg-type]
        for a, b in zip(offsets, offsets[1:])
    )
    assert [r.is_control for r in placed] == [False, False, True, False]


def test_record_offset_is_absent_until_the_provider_places_it() -> None:
    unplaced = StreamRecord(RecordKind.DATA, b"a", "s", 0)

    assert unplaced.offset is None
    assert unplaced.placed_at(Offset("1-0")).offset == Offset("1-0")


def test_to_fields_excludes_the_offset() -> None:
    """Two appends under one idempotency key must be byte-comparable.

    Including the offset would make every re-append look like different bytes,
    turning the idempotent retry path into a spurious conflict.
    """
    record = StreamRecord(RecordKind.DATA, b"a", "s", 0)

    assert record.to_fields() == record.placed_at(Offset("1-0")).to_fields()


# --- idempotency keys -------------------------------------------------------


def test_idempotency_key_comes_from_session_and_sequence() -> None:
    record = StreamRecord(RecordKind.DATA, b"a", "session-a", 4)

    assert record.idempotency_key == IdempotencyKey("session-a", 4)


def test_idempotency_key_rejects_a_missing_session() -> None:
    with pytest.raises(ValueError, match="session id may not be empty"):
        IdempotencyKey("", 0)


@pytest.mark.parametrize("bad", [-1, -100])
def test_negative_sequences_are_rejected(bad: int) -> None:
    with pytest.raises(ValueError, match="non-negative"):
        IdempotencyKey("s", bad)
    with pytest.raises(ValueError, match="non-negative"):
        StreamRecord(RecordKind.DATA, b"a", "s", bad)
