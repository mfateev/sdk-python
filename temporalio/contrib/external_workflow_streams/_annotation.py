"""The replay annotation codec (P5).

The opaque bytes Python encodes, Core stores in a marker, and Python decodes on
replay. **Core never parses any of it** -- it appends observation deltas to a
byte buffer and hands the result back.

That last fact drives the encoding's shape. Core accumulates by byte
concatenation, so an annotation is a sequence of self-delimiting **frames**:

.. code-block:: text

    annotation := schema_version, header_frame, segment_frame*, terminal_frame

    header  := provider_id, provider_format_version
             , streams[]                    // wait_id -> (stream_key, start_cursor)
    segment := run*, segment_end_reason
    run     := (wait_id, first_offset, last_offset, count, control_positions)
    terminal := blocked_snapshot[]           // wait_id -> BEGINNING | AFTER(offset)

Concatenating the deltas of one Workflow Task therefore *is* the annotation,
with no reassembly step that could disagree with Core's.

Two properties the encoding is built around:

- **No field is per-record.** A run costs two offsets, a count, and a sparse
  control list whether it covers ten records or a hundred thousand. This is
  what makes marker size scale with cross-stream schedule transitions rather
  than with item count.
- **Both run endpoints are recorded, not a start plus a count.** Backend
  offsets are ordered but not dense, so ``(first, count)`` does not determine
  where a run ends -- and a deletion inside the range would be undetectable.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Final

from temporalio.contrib.external_workflow_streams._backend import StreamKey
from temporalio.contrib.external_workflow_streams._record import (
    AFTER,
    BEGINNING,
    Cursor,
    Offset,
)

__all__ = [
    "MAX_ANNOTATION_BYTES",
    "Annotation",
    "AnnotationAccumulator",
    "AnnotationDecodeError",
    "AnnotationHeader",
    "Run",
    "Segment",
    "SegmentEndReason",
    "StreamBinding",
    "decode_annotation",
    "encode_annotation",
]

SCHEMA_VERSION: Final = 1
"""Leads the encoding, so a marker written by an older SDK stays readable.

It is also the extension point that keeps ADR-003's accepted risk bounded: a
per-record content-hash mode can be added later without a format break.
"""

MAX_ANNOTATION_BYTES: Final = 64 * 1024
"""Hard cap on one marker's annotation, well below the server's event limit.

A constant rather than a guideline: it is enforced while encoding, so an
annotation can never exceed it. The runtime rolls the Workflow Task over
instead of growing the marker.
"""

ROLLOVER_HIGH_WATER: Final = 0.75
"""Fraction of the budget at which the encoder asks for a rollover.

The margin exists because the request has to travel to Core and come back:
a rollover asked for at 100% would arrive too late to prevent the overflow it
was meant to avoid.
"""

_FRAME_HEADER: Final = 0x01
_FRAME_SEGMENT: Final = 0x02
_FRAME_TERMINAL: Final = 0x03

_CURSOR_BEGINNING: Final = 0x00
_CURSOR_AFTER: Final = 0x01


@enum.unique
class SegmentEndReason(enum.IntEnum):
    """Why one activation's segment ended.

    Recorded because replay must reproduce not only *what* was delivered but
    *where the runtime returned control with nothing further available* --
    those boundaries are what make ``wait_condition`` predicates fire the same
    number of times.
    """

    NO_DATA_AVAILABLE = 1
    BATCH_LIMIT = 2
    FENCE_REACHED = 3
    BUDGET_ROLLOVER = 4
    """This batch continues in the following marker."""


class AnnotationDecodeError(Exception):
    """The annotation bytes are not a well-formed annotation."""


class AnnotationBudgetExceeded(Exception):
    """Encoding would push the annotation past :data:`MAX_ANNOTATION_BYTES`.

    Should be unreachable in practice: the encoder asks for a rollover at the
    high-water mark, long before this. It is raised rather than assumed away so
    a future encoding change that grows a frame is caught here rather than by
    the server rejecting an oversized event.
    """


# --- the decoded shape ------------------------------------------------------


@dataclass(frozen=True)
class StreamBinding:
    """What a ``wait_id`` was subscribed to, and where it started.

    ``start_cursor`` is explicit rather than re-derived: it is what makes an
    annotation with no segments at all -- a subscription to an empty stream --
    a complete replay instruction.
    """

    stream_key: StreamKey
    start_cursor: Cursor


@dataclass(frozen=True)
class Run:
    """A maximal set of consecutive deliveries from **one** stream.

    Alternating streams produce one run per delivery; a single-stream batch of
    100,000 records produces one run. That difference is the whole reason the
    schedule is encoded as runs.
    """

    wait_id: int
    first_offset: Offset
    last_offset: Offset
    count: int
    control_positions: tuple[int, ...] = ()
    """Sparse relative indices within the run at which a control record sat.

    Sparse, not one kind tag per record: fences are rare by construction, so
    this keeps a run's control encoding proportional to the number of fences
    rather than to the number of records.
    """

    def __post_init__(self) -> None:
        if self.count < 1:
            raise ValueError(f"a run covers at least one record, got {self.count}")
        if any(not 0 <= p < self.count for p in self.control_positions):
            raise ValueError(
                f"control positions {self.control_positions} fall outside a run "
                f"of {self.count} record(s)"
            )
        if list(self.control_positions) != sorted(set(self.control_positions)):
            raise ValueError(
                f"control positions must be strictly increasing, got "
                f"{self.control_positions}"
            )


@dataclass(frozen=True)
class Segment:
    """One original activation's worth of deliveries.

    ``runs`` may be empty and that is meaningful: an activation that drained
    and found nothing still ran one event-loop drain, and replay must reproduce
    it or conditions fire a different number of times.
    """

    runs: tuple[Run, ...]
    end_reason: SegmentEndReason


@dataclass(frozen=True)
class AnnotationHeader:
    provider_id: str
    provider_format_version: int
    streams: dict[int, StreamBinding] = field(default_factory=dict)
    schema_version: int = SCHEMA_VERSION


@dataclass(frozen=True)
class Annotation:
    """A complete marker annotation.

    ``terminal`` is ``None`` only for an annotation still being accumulated.
    Core refuses to write a marker for one without a terminal, so a decoded
    annotation always has one.
    """

    header: AnnotationHeader
    segments: tuple[Segment, ...] = ()
    terminal: dict[int, Cursor] | None = None


# --- primitives -------------------------------------------------------------


def _put_uvarint(out: bytearray, value: int) -> None:
    if value < 0:
        raise ValueError(f"cannot encode a negative value: {value}")
    while True:
        byte = value & 0x7F
        value >>= 7
        out.append(byte | (0x80 if value else 0))
        if not value:
            return


def _put_str(out: bytearray, value: str) -> None:
    encoded = value.encode()
    _put_uvarint(out, len(encoded))
    out += encoded


def _put_cursor(out: bytearray, cursor: Cursor) -> None:
    """Cursors and offsets are distinct types in the encoding, on purpose.

    A boundary and a record identity are not interchangeable, and an encoding
    that spelled them the same way would let one be read as the other.
    """
    if cursor.is_beginning:
        out.append(_CURSOR_BEGINNING)
        return
    assert cursor.offset is not None
    out.append(_CURSOR_AFTER)
    _put_str(out, cursor.offset.serialize())


def _put_stream_key(out: bytearray, key: StreamKey) -> None:
    _put_str(out, key.namespace)
    _put_str(out, key.workflow_id)
    _put_str(out, key.first_execution_run_id)
    _put_str(out, key.stream_name)


class _Reader:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self._at = 0

    @property
    def exhausted(self) -> bool:
        return self._at >= len(self._data)

    def _take(self, n: int) -> bytes:
        if self._at + n > len(self._data):
            raise AnnotationDecodeError(
                f"annotation truncated: wanted {n} byte(s) at offset {self._at}, "
                f"only {len(self._data) - self._at} remain"
            )
        chunk = self._data[self._at : self._at + n]
        self._at += n
        return chunk

    def byte(self) -> int:
        return self._take(1)[0]

    def uvarint(self) -> int:
        value = 0
        shift = 0
        while True:
            byte = self.byte()
            value |= (byte & 0x7F) << shift
            if not byte & 0x80:
                return value
            shift += 7
            if shift > 63:
                raise AnnotationDecodeError("varint is too long to be valid")

    def string(self) -> str:
        return self._take(self.uvarint()).decode()

    def cursor(self) -> Cursor:
        kind = self.byte()
        if kind == _CURSOR_BEGINNING:
            return BEGINNING
        if kind == _CURSOR_AFTER:
            return AFTER(Offset.deserialize(self.string()))
        raise AnnotationDecodeError(f"unknown cursor kind {kind:#x}")

    def stream_key(self) -> StreamKey:
        return StreamKey(self.string(), self.string(), self.string(), self.string())


# --- frame encoding ---------------------------------------------------------


def encode_header(header: AnnotationHeader) -> bytes:
    """The schema version and header frame, which lead every annotation."""
    out = bytearray()
    _put_uvarint(out, header.schema_version)
    out.append(_FRAME_HEADER)
    _put_str(out, header.provider_id)
    _put_uvarint(out, header.provider_format_version)
    _put_uvarint(out, len(header.streams))
    for wait_id in sorted(header.streams):
        binding = header.streams[wait_id]
        _put_uvarint(out, wait_id)
        _put_stream_key(out, binding.stream_key)
        _put_cursor(out, binding.start_cursor)
    return bytes(out)


def encode_segment(segment: Segment) -> bytes:
    out = bytearray()
    out.append(_FRAME_SEGMENT)
    _put_uvarint(out, len(segment.runs))
    for run in segment.runs:
        _put_uvarint(out, run.wait_id)
        _put_str(out, run.first_offset.serialize())
        _put_str(out, run.last_offset.serialize())
        _put_uvarint(out, run.count)
        _put_uvarint(out, len(run.control_positions))
        previous = 0
        for position in run.control_positions:
            # Delta-encoded: control positions ascend, so the gaps are smaller
            # numbers than the absolute indices and cost fewer varint bytes.
            _put_uvarint(out, position - previous)
            previous = position
    out.append(int(segment.end_reason))
    return bytes(out)


def encode_terminal(blocked: dict[int, Cursor]) -> bytes:
    out = bytearray()
    out.append(_FRAME_TERMINAL)
    _put_uvarint(out, len(blocked))
    for wait_id in sorted(blocked):
        _put_uvarint(out, wait_id)
        _put_cursor(out, blocked[wait_id])
    return bytes(out)


def encode_annotation(annotation: Annotation) -> bytes:
    """A whole annotation, as Core would have accumulated it."""
    parts = [encode_header(annotation.header)]
    parts.extend(encode_segment(s) for s in annotation.segments)
    if annotation.terminal is not None:
        parts.append(encode_terminal(annotation.terminal))
    return b"".join(parts)


def decode_annotation(data: bytes) -> Annotation:
    """Parses a complete annotation.

    Accepts an annotation with no terminal so a partially accumulated one can
    be inspected; Core is what refuses to *write* a marker for one.
    """
    reader = _Reader(data)
    schema_version = reader.uvarint()
    if schema_version != SCHEMA_VERSION:
        raise AnnotationDecodeError(
            f"annotation schema version {schema_version} is not supported by this "
            f"SDK, which writes and reads version {SCHEMA_VERSION}"
        )

    if reader.byte() != _FRAME_HEADER:
        raise AnnotationDecodeError("an annotation must begin with its header frame")
    provider_id = reader.string()
    provider_format_version = reader.uvarint()
    streams = {}
    for _ in range(reader.uvarint()):
        wait_id = reader.uvarint()
        streams[wait_id] = StreamBinding(reader.stream_key(), reader.cursor())
    header = AnnotationHeader(
        provider_id, provider_format_version, streams, schema_version
    )

    segments: list[Segment] = []
    terminal: dict[int, Cursor] | None = None
    while not reader.exhausted:
        frame = reader.byte()
        if frame == _FRAME_SEGMENT:
            if terminal is not None:
                raise AnnotationDecodeError("a segment frame follows the terminal")
            runs = []
            for _ in range(reader.uvarint()):
                wait_id = reader.uvarint()
                first = Offset.deserialize(reader.string())
                last = Offset.deserialize(reader.string())
                count = reader.uvarint()
                positions: list[int] = []
                previous = 0
                for _ in range(reader.uvarint()):
                    previous += reader.uvarint()
                    positions.append(previous)
                runs.append(Run(wait_id, first, last, count, tuple(positions)))
            segments.append(Segment(tuple(runs), SegmentEndReason(reader.byte())))
        elif frame == _FRAME_TERMINAL:
            if terminal is not None:
                raise AnnotationDecodeError("an annotation has one terminal, not two")
            terminal = {}
            for _ in range(reader.uvarint()):
                # Read into locals rather than `terminal[reader.uvarint()] =
                # reader.cursor()`: Python evaluates the assigned value before
                # the subscript, which would read the two fields backwards.
                wait_id = reader.uvarint()
                terminal[wait_id] = reader.cursor()
        else:
            raise AnnotationDecodeError(f"unknown frame tag {frame:#x}")

    return Annotation(header, tuple(segments), terminal)


# --- accumulation and the byte budget ---------------------------------------


class AnnotationAccumulator:
    """Builds one Workflow Task's annotation, one observation delta at a time.

    Mirrors what Core holds: every delta this emits is appended verbatim to
    ``ExternalWaitSet.replay_annotation``, so the concatenation of everything
    emitted here *is* the annotation the marker carries. There is no separate
    assembly step that could disagree.

    The budget is enforced here, while encoding, rather than checked afterwards
    -- by the time an oversized annotation exists it is too late to do anything
    but discard work.
    """

    def __init__(
        self,
        header: AnnotationHeader,
        *,
        max_bytes: int = MAX_ANNOTATION_BYTES,
        high_water: float = ROLLOVER_HIGH_WATER,
    ) -> None:
        self._max_bytes = max_bytes
        self._high_water_bytes = int(max_bytes * high_water)
        self._emitted: list[bytes] = []
        self._size = 0
        self._terminated = False
        self._emit(encode_header(header))

    @property
    def size(self) -> int:
        """Bytes accumulated so far, which is what the marker will carry."""
        return self._size

    @property
    def request_rollover(self) -> bool:
        """Whether the next progress report should ask Core to roll over.

        Set once the high-water mark is passed. Core then rolls the task over
        *without* a finalization round trip, because the progress report
        carrying this flag already carried the terminal.
        """
        return self._size >= self._high_water_bytes

    @property
    def terminated(self) -> bool:
        return self._terminated

    def accumulated(self) -> bytes:
        """Everything emitted so far, concatenated -- what Core now holds."""
        return b"".join(self._emitted)

    def add_segment(self, segment: Segment) -> bytes:
        """Encodes one activation's segment and returns it as a delta."""
        if self._terminated:
            raise ValueError("cannot add a segment after the terminal")
        return self._emit(encode_segment(segment))

    def add_terminal(self, blocked: dict[int, Cursor]) -> bytes:
        """Encodes the blocked snapshot that closes this annotation."""
        if self._terminated:
            raise ValueError("an annotation has one terminal, not two")
        delta = self._emit(encode_terminal(blocked))
        self._terminated = True
        return delta

    def _emit(self, frame: bytes) -> bytes:
        if self._size + len(frame) > self._max_bytes:
            raise AnnotationBudgetExceeded(
                f"encoding {len(frame)} more byte(s) would take the annotation to "
                f"{self._size + len(frame)}, past the {self._max_bytes}-byte budget; "
                "the runtime should have rolled the Workflow Task over at the "
                f"{self._high_water_bytes}-byte high-water mark"
            )
        self._emitted.append(frame)
        self._size += len(frame)
        return frame
