"""The record model: offsets, cursor boundaries, and stream records (P1).

Nothing here touches Temporal or a provider. These are the value types every
other piece of the feature is expressed in.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Final, Mapping, Protocol

__all__ = [
    "AFTER",
    "BEGINNING",
    "Cursor",
    "IdempotencyKey",
    "Offset",
    "OffsetComparator",
    "RecordKind",
    "StreamRecord",
]


@enum.unique
class RecordKind(enum.IntEnum):
    """What a record is, from the runtime's point of view.

    Data and control records share one offset sequence -- a fence occupies an
    offset exactly as a data record does, and advances the cursor the same way.
    They differ only in whether the runtime yields them to Workflow code.
    """

    DATA = 1
    """Carries a payload for Workflow code."""

    WRITE_FENCE = 2
    """A producer session declaring its preceding writes are all appended.

    Consumed by the runtime, never yielded to Workflow code. It does not close
    the stream and asserts nothing about other producers.
    """

    @property
    def is_control(self) -> bool:
        return self is not RecordKind.DATA


@dataclass(frozen=True, order=False)
class Offset:
    """A provider's position token: opaque, stable, and serializable.

    Deliberately **not** comparable with ``<``. Offsets are totally ordered,
    but by their provider's rule rather than lexically -- Redis stream IDs are
    ``(milliseconds, sequence)`` tuples, and string comparison is wrong as soon
    as the millisecond component changes width. Ordering goes through the
    provider's ``compare_offsets``; see :class:`OffsetComparator`.
    """

    token: str

    def __post_init__(self) -> None:
        if not self.token:
            raise ValueError("an offset token may not be empty")

    def __str__(self) -> str:
        return self.token

    def serialize(self) -> str:
        return self.token

    @classmethod
    def deserialize(cls, token: str) -> Offset:
        return cls(token)


class OffsetComparator(Protocol):
    """A provider's total order over its own offsets.

    Returns a negative number, zero, or a positive number, in the manner of a
    three-way comparison.
    """

    def __call__(self, left: Offset, right: Offset, /) -> int: ...


@dataclass(frozen=True, order=False)
class Cursor:
    """A position **boundary**, never the identity of a record (ADR-002).

    Two forms only::

        BEGINNING            # the provider's beginning-of-stream boundary
        AFTER(offset)        # immediately following the record at ``offset``

    ``AFTER(x)`` names a boundary whether or not a record after ``x`` exists
    yet, which is what lets a consumer park at the tail without naming the id
    of a record nobody has written.
    """

    offset: Offset | None = None
    """``None`` is the beginning-of-stream boundary."""

    @property
    def is_beginning(self) -> bool:
        return self.offset is None

    def __str__(self) -> str:
        return "BEGINNING" if self.is_beginning else f"AFTER({self.offset})"

    def serialize(self) -> str:
        """Round-trips through :meth:`deserialize`.

        The two forms are distinguished by a prefix rather than by emptiness so
        a provider whose beginning sentinel is a real token (Redis uses
        ``0-0``) cannot be confused for ``BEGINNING``.
        """
        if self.is_beginning:
            return "B"
        assert self.offset is not None
        return f"A{self.offset.serialize()}"

    @classmethod
    def deserialize(cls, encoded: str) -> Cursor:
        if encoded == "B":
            return BEGINNING
        if encoded.startswith("A"):
            return AFTER(Offset.deserialize(encoded[1:]))
        raise ValueError(f"not a serialized cursor: {encoded!r}")

    def follows(self, other: Offset, compare: OffsetComparator) -> bool:
        """Whether this boundary lies at or after the record at ``other``.

        ``BEGINNING`` follows nothing; ``AFTER(x)`` follows every offset up to
        and including ``x``.
        """
        if self.is_beginning:
            return False
        assert self.offset is not None
        return compare(other, self.offset) <= 0

    def excludes(self, candidate: Offset, compare: OffsetComparator) -> bool:
        """Whether a record at ``candidate`` sits at or before this boundary.

        This is the predicate a watch applies: it reads records strictly after
        the boundary, so anything this excludes has already been consumed.
        """
        return self.follows(candidate, compare)


BEGINNING: Final[Cursor] = Cursor()
"""The beginning-of-stream boundary."""


def AFTER(offset: Offset) -> Cursor:  # noqa: N802 -- reads as the grammar's form
    """The boundary immediately following the record at ``offset``."""
    return Cursor(offset)


@dataclass(frozen=True)
class IdempotencyKey:
    """What makes an append idempotent under Activity retry.

    Idempotency is on ``(session_id, sequence)`` **and identity**: reusing a
    key with byte-identical content is a no-op returning the original offset,
    and reusing it with different bytes is an error (ADR-020).
    """

    session_id: str
    sequence: int

    def __post_init__(self) -> None:
        if not self.session_id:
            raise ValueError("a producer session id may not be empty")
        if self.sequence < 0:
            raise ValueError(f"sequence must be non-negative, got {self.sequence}")

    def __str__(self) -> str:
        return f"{self.session_id}/{self.sequence}"


#: Provider-neutral field names. A provider stores records as fields keyed by
#: these; the prefix keeps them from colliding with anything a provider or an
#: operator adds alongside.
_FIELD_KIND: Final = "__tes_kind"
_FIELD_PAYLOAD: Final = "__tes_payload"
_FIELD_SESSION: Final = "__tes_session"
_FIELD_SEQUENCE: Final = "__tes_seq"

RESERVED_FIELDS: Final[frozenset[str]] = frozenset(
    {_FIELD_KIND, _FIELD_PAYLOAD, _FIELD_SESSION, _FIELD_SEQUENCE}
)


@dataclass(frozen=True)
class StreamRecord:
    """One appended record, data or control.

    ``offset`` is assigned by the provider at append time and is therefore
    absent until then; the producer side builds a record without one and the
    provider returns the placed record.
    """

    kind: RecordKind
    payload: bytes
    producer_session_id: str
    sequence: int
    offset: Offset | None = field(default=None)

    def __post_init__(self) -> None:
        if self.kind.is_control and self.payload:
            raise ValueError(f"a {self.kind.name} record carries no payload")
        if self.sequence < 0:
            raise ValueError(f"sequence must be non-negative, got {self.sequence}")

    @property
    def is_control(self) -> bool:
        """Control records advance the cursor but are never yielded."""
        return self.kind.is_control

    @property
    def idempotency_key(self) -> IdempotencyKey:
        return IdempotencyKey(self.producer_session_id, self.sequence)

    def placed_at(self, offset: Offset) -> StreamRecord:
        """This record as the provider stored it."""
        return StreamRecord(
            kind=self.kind,
            payload=self.payload,
            producer_session_id=self.producer_session_id,
            sequence=self.sequence,
            offset=offset,
        )

    def to_fields(self) -> dict[str, bytes]:
        """The provider-neutral field encoding, excluding the offset.

        The offset is the provider's to assign, so it is not part of what gets
        written -- which is also what makes two appends under one idempotency
        key byte-comparable.
        """
        return {
            _FIELD_KIND: str(int(self.kind)).encode(),
            _FIELD_PAYLOAD: self.payload,
            _FIELD_SESSION: self.producer_session_id.encode(),
            _FIELD_SEQUENCE: str(self.sequence).encode(),
        }

    @classmethod
    def from_fields(
        cls, offset: Offset, fields: Mapping[str, bytes] | Mapping[bytes, bytes]
    ) -> StreamRecord:
        """Rebuild a record a provider stored, tolerating bytes-keyed maps.

        Redis hands back ``bytes`` keys unless the client decodes responses,
        and a provider should not have to normalize before calling this.
        """
        decoded: dict[str, bytes] = {}
        for raw_key, raw_value in fields.items():
            key = raw_key.decode() if isinstance(raw_key, bytes) else raw_key
            value = (
                raw_value if isinstance(raw_value, bytes) else str(raw_value).encode()
            )
            decoded[key] = value

        missing = RESERVED_FIELDS - decoded.keys()
        if missing:
            raise ValueError(
                f"record at {offset} is missing field(s): {sorted(missing)}"
            )

        return cls(
            kind=RecordKind(int(decoded[_FIELD_KIND])),
            payload=decoded[_FIELD_PAYLOAD],
            producer_session_id=decoded[_FIELD_SESSION].decode(),
            sequence=int(decoded[_FIELD_SEQUENCE]),
            offset=offset,
        )
