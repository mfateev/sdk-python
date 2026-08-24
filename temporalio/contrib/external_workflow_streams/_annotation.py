"""The replay annotation codec (P5).

The opaque bytes Python encodes, Core stores in a marker, and Python decodes on
replay. **Core never parses any of it** -- it appends observation deltas to a
byte buffer and hands the result back.

That last fact drives the encoding's shape. Core accumulates by byte
concatenation, so an annotation is a sequence of self-delimiting **frames**:

.. code-block:: text

    annotation := schema_version, header_frame
                , (bindings_frame | segment_frame)*, terminal_frame

    header   := streams[]                    // wait_id -> binding
    bindings := streams[]                    // the same, for waits added later
    binding  := (stream_key, start_cursor, backend_name
               , provider_id, provider_format_version)
    segment  := run*, segment_end_reason
    run      := (wait_id, first_offset, last_offset, count, control_positions)
    terminal := blocked_snapshot[]           // wait_id -> BEGINNING | AFTER(offset)

The provider identity is **per wait**, not per annotation. One topic per
backend is what the API allows, so an annotation-wide provider label cannot say
which of two registered backends owns a given wait -- and two instances of the
same provider (two Redis clusters, two key prefixes) share a provider id, so the
label does not even distinguish them. The binding therefore names the
Worker-registered ``backend_name`` the Workflow itself chose, and carries the
provider identity of that backend so replay can refuse to read through an
implementation that is not the one that wrote the bytes.

A subscription may be created at **any** activation of a retained Workflow
Task, which is later than the header frame that already went to Core. Core
appends bytes and never rewrites them, so a header cannot be extended in place;
the binding rides its own frame instead, emitted with the delta of the
activation that registered the wait and before the segment that first records a
run for it. Decoding merges every bindings frame into ``header.streams``, so
what replay reads is one complete ``wait_id -> binding`` table however late a
wait joined. Without this a wait registered after the first delta reaches the
marker as runs and a terminal entry with no stream key, no backend, and no start
cursor -- and replay of *unchanged* code fails as "the Workflow did not create"
that wait.

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
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Final

import temporalio.exceptions
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
    "encode_bindings",
    "encoded_run_size",
    "encoded_segment_size",
]

SCHEMA_VERSION: Final = 2
"""Leads the encoding, so a marker written by an older SDK stays readable.

It is also the extension point that keeps ADR-003's accepted risk bounded: a
per-record content-hash mode can be added later without a format break.

Version 2 moved the provider identity from the header into each
:class:`StreamBinding` and added the ``backend_name`` that selects the backend
instance.

The bindings frame added later is **not** a version bump, deliberately. It is
purely additive: every annotation written before it decodes byte-identically,
and the grammar is self-describing through its frame tags, so a decoder that
does not know the tag fails loudly on it rather than misreading the bytes. A
version exists to tell a reader what it is looking at, and here the tags already
do -- nothing needs to assume that version 2 implies no bindings frame. No version-1 decoder is kept: the feature is private and unreleased,
so no marker written by version 1 exists anywhere but in a test fixture. A
version-1 annotation is rejected by :func:`decode_annotation` rather than
silently read as though its single provider label applied to every wait.
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
_FRAME_BINDINGS: Final = 0x04

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


class AnnotationBudgetExceeded(temporalio.exceptions.ApplicationError):
    """Encoding would push the annotation past :data:`MAX_ANNOTATION_BYTES`.

    Unreachable if the three preventions hold, and each closes a way it used to
    be reachable:

    - the closing frames are **reserved** rather than checked, so the terminal
      and any late bindings frame always fit (see
      :attr:`AnnotationAccumulator.reserved`);
    - the runtime **stops delivering** rather than growing a segment it could not
      then record, and asks Core to roll the Workflow Task over;
    - ``subscribe()`` **refuses** a subscription set whose own header and
      terminal could not fit an empty annotation, at the point the Workflow makes
      it.

    An :py:class:`~temporalio.exceptions.ApplicationError` marked
    non-retryable, and that is the substance of this class rather than a detail.
    A plain exception here fails the *Workflow Task*, and the server retries
    Workflow Task failures forever: the encoding that overflowed overflows again
    on every retry, so the Workflow is stuck permanently with no marker, no
    terminal, and no rollover ever requested. ADR-007 rejects exactly that
    check-and-fail behaviour. Failing the Workflow instead is still bad news, but
    it is bounded, visible, and reported once.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message, type="AnnotationBudgetExceeded", non_retryable=True)


# --- the decoded shape ------------------------------------------------------


@dataclass(frozen=True)
class StreamBinding:
    """What a ``wait_id`` was subscribed to, where it started, and through what.

    ``start_cursor`` is explicit rather than re-derived: it is what makes an
    annotation with no segments at all -- a subscription to an empty stream --
    a complete replay instruction.

    The last three fields are what let replay bind a wait to **one** backend.
    They divide along who chose them, and that division is what decides how a
    mismatch is reported:

    - ``stream_key`` and ``backend_name`` are chosen by Workflow code, so a
      mismatch is row four of the failure taxonomy -- nondeterminism, fixed by
      versioning the Workflow.
    - ``provider_id`` and ``provider_format_version`` are properties of whatever
      the Worker registered under that name, so a mismatch is a deployment
      problem: the Workflow is unchanged and the backend is undamaged.
    """

    stream_key: StreamKey
    start_cursor: Cursor
    backend_name: str
    """The Worker-registered name the Workflow's ``topic(backend=...)`` named.

    Recorded because a provider id cannot select an instance: two Redis
    clusters, or two key prefixes on one cluster, are different stores that
    declare the same provider. The name is part of the Workflow's own
    definition, so it is stable across Workers of a deployment in the way a
    Worker-local object identity never could be.
    """
    provider_id: str
    """The provider the named backend declared when this wait was recorded."""
    provider_format_version: int
    """That provider's on-the-wire format version when this wait was recorded.

    Checked on replay rather than merely stored: a backend implementation that
    keeps its provider id while changing how it lays records out would otherwise
    be read as though nothing had happened.
    """


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
        """Validate the run length and sparse control positions."""
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
    """The bindings, and nothing else.

    Holds **every** wait's binding once decoded, including the ones that arrived
    in a later bindings frame because their ``subscribe()`` call ran after the
    header had already gone to Core. Where a binding was carried is an encoding
    detail; the decoded table is flat, so replay never has to ask when a wait
    joined.

    There is deliberately no annotation-wide provider here. One existed through
    schema version 1 and was taken from whichever subscription happened to be
    registered first, which made it wrong for every other wait in a
    multi-backend annotation.
    """

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

    def bindings(self) -> dict[int, StreamBinding]:
        streams: dict[int, StreamBinding] = {}
        for _ in range(self.uvarint()):
            # Read into locals rather than nesting the calls in the constructor:
            # argument evaluation order is not the thing that should decide which
            # field a byte lands in.
            wait_id = self.uvarint()
            stream_key = self.stream_key()
            start_cursor = self.cursor()
            backend_name = self.string()
            provider_id = self.string()
            provider_format_version = self.uvarint()
            streams[wait_id] = StreamBinding(
                stream_key,
                start_cursor,
                backend_name,
                provider_id,
                provider_format_version,
            )
        return streams


# --- frame encoding ---------------------------------------------------------


def _put_bindings(out: bytearray, streams: Mapping[int, StreamBinding]) -> None:
    _put_uvarint(out, len(streams))
    for wait_id in sorted(streams):
        binding = streams[wait_id]
        _put_uvarint(out, wait_id)
        _put_stream_key(out, binding.stream_key)
        _put_cursor(out, binding.start_cursor)
        _put_str(out, binding.backend_name)
        _put_str(out, binding.provider_id)
        _put_uvarint(out, binding.provider_format_version)


def encode_header(header: AnnotationHeader) -> bytes:
    """The schema version and header frame, which lead every annotation."""
    out = bytearray()
    _put_uvarint(out, header.schema_version)
    out.append(_FRAME_HEADER)
    _put_bindings(out, header.streams)
    return bytes(out)


def encode_bindings(streams: Mapping[int, StreamBinding]) -> bytes:
    """Bindings for waits that were registered after the header was emitted.

    The same body as the header frame under a different tag. A second *header*
    frame would have been ambiguous -- it also carries the schema version, and
    a decoder would have to decide whether the later one replaced the earlier --
    whereas a bindings frame says exactly one thing: these waits exist too.
    """
    out = bytearray()
    out.append(_FRAME_BINDINGS)
    _put_bindings(out, streams)
    return bytes(out)


def _put_run(out: bytearray, run: Run) -> None:
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


def encode_segment(segment: Segment) -> bytes:
    out = bytearray()
    out.append(_FRAME_SEGMENT)
    _put_uvarint(out, len(segment.runs))
    for run in segment.runs:
        _put_run(out, run)
    out.append(int(segment.end_reason))
    return bytes(out)


def encoded_run_size(run: Run) -> int:
    """What one run costs inside a segment frame, in bytes.

    Exposed because the runtime has to know what an *open* segment costs before
    it is closed: a segment frame that no longer fits the annotation's remaining
    budget cannot be deferred to the next annotation -- its deliveries happened
    in this Workflow Task and the marker for that task is where replay must find
    them -- so the only place to act is before the records that would grow it are
    handed over. Measured rather than estimated per record, because a run's cost
    is dominated by two provider-supplied offset strings whose length this side
    does not choose.
    """
    out = bytearray()
    _put_run(out, run)
    return len(out)


def encoded_segment_size(run_sizes: Sequence[int]) -> int:
    """What a segment frame costs, given what each of its runs costs.

    Takes the per-run sizes rather than the runs so that a caller extending one
    run at a time re-measures only the run it changed.
    """
    out = bytearray()
    _put_uvarint(out, len(run_sizes))
    # Frame tag, the run count, the runs themselves, and the end-reason byte.
    return 1 + len(out) + sum(run_sizes) + 1


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
    streams = reader.bindings()

    segments: list[Segment] = []
    terminal: dict[int, Cursor] | None = None
    while not reader.exhausted:
        frame = reader.byte()
        if frame == _FRAME_BINDINGS:
            if terminal is not None:
                raise AnnotationDecodeError("a bindings frame follows the terminal")
            for wait_id, binding in reader.bindings().items():
                if wait_id in streams:
                    # A wait is bound once. A second binding for the same id
                    # would leave replay choosing between two stream keys, and
                    # whichever it chose could be the one the records were not
                    # written to.
                    raise AnnotationDecodeError(
                        f"external stream wait {wait_id} is bound twice in one "
                        "annotation"
                    )
                streams[wait_id] = binding
        elif frame == _FRAME_SEGMENT:
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

    return Annotation(
        AnnotationHeader(streams, schema_version), tuple(segments), terminal
    )


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
        """Start an accumulator with its immutable header and byte limits."""
        self._max_bytes = max_bytes
        self._high_water_bytes = int(max_bytes * high_water)
        self._emitted: list[bytes] = []
        self._size = 0
        self._closing = 0
        self._spill = 0
        self._terminated = False
        self._emit(encode_header(header), closing=True)

    @property
    def size(self) -> int:
        """Bytes accumulated so far, which is what the marker will carry."""
        return self._size

    @property
    def reserved(self) -> int:
        """Bytes held back from the ordinary run of segments.

        Two parts, and they are held back against different things:

        - **closing** -- the terminal, plus a bindings frame for any wait
          registered since the header went out. Neither may ever be refused: both
          record something that has already happened, and an annotation whose
          terminal does not fit is one Core writes *without* a terminal --
          durable, and undecodable past the frame after it. Only they may spend
          this.
        - **spill** -- a margin a segment frame may overrun into. The runtime
          stops delivering before a segment it could not record, but it prices a
          record it has not seen yet, and a provider chooses how long its offsets
          are. The margin is what turns a misprice into a rollover instead of a
          refusal; an annotation that dips into it has already asked Core to end
          the Workflow Task.
        """
        return self._closing + self._spill

    def reserve(self, closing: int, *, spill: int = 0) -> None:
        """Sets what closing costs, and how much a segment may overrun."""
        self._closing = closing
        self._spill = spill

    @property
    def headroom(self) -> int:
        """Bytes available to a segment before it starts spending the margin.

        What the runtime measures affordability against. Zero does not mean the
        annotation is full; it means the next segment overruns into the spill and
        the Workflow Task has to end.
        """
        return max(0, self._max_bytes - self._closing - self._spill - self._size)

    def fits(self, frame_bytes: int) -> bool:
        """Whether a segment of this size fits without spending the margin."""
        return frame_bytes <= self.headroom

    @property
    def request_rollover(self) -> bool:
        """Whether the next progress report should ask Core to roll over.

        Set once the high-water mark is passed. Core then rolls the task over
        *without* a finalization round trip, because the progress report
        carrying this flag already carried the terminal.

        The high-water mark alone is not the whole condition -- it is a fraction
        of the budget, and a frame can be larger than the fraction that is left.
        The runtime adds the other half by refusing to grow a segment it could not
        then record; see ``WorkflowStreamRuntime.request_rollover``.
        """
        return self._size >= self._high_water_bytes

    @property
    def terminated(self) -> bool:
        """Whether a terminal frame has already been emitted."""
        return self._terminated

    def accumulated(self) -> bytes:
        """Everything emitted so far, concatenated -- what Core now holds."""
        return b"".join(self._emitted)

    def add_bindings(self, streams: Mapping[int, StreamBinding]) -> bytes:
        """Encodes bindings for waits registered after the header went out.

        Its own frame rather than an amended header: Core appends the deltas it
        is given and never rewrites what it already holds, so the only way a
        binding decided later can reach the marker is to be appended after the
        bytes that preceded it.
        """
        if self._terminated:
            raise ValueError("cannot bind a wait after the terminal")
        if not streams:
            raise ValueError("a bindings frame binds at least one wait")
        # A closing frame for budget purposes: the wait it binds was admitted by
        # `register`, which charged this frame into the reserve at the time, and
        # refusing it now would leave the wait in the terminal with no stream key,
        # no backend, and no start cursor -- which replay reports as a wait the
        # Workflow never created.
        return self._emit(encode_bindings(streams), closing=True)

    def add_segment(self, segment: Segment) -> bytes:
        """Encodes one activation's segment and returns it as a delta."""
        if self._terminated:
            raise ValueError("cannot add a segment after the terminal")
        return self._emit(encode_segment(segment))

    def add_terminal(self, blocked: dict[int, Cursor]) -> bytes:
        """Encodes the blocked snapshot that closes this annotation."""
        if self._terminated:
            raise ValueError("an annotation has one terminal, not two")
        delta = self._emit(encode_terminal(blocked), closing=True)
        self._terminated = True
        return delta

    def _emit(self, frame: bytes, *, closing: bool = False) -> bytes:
        # A closing frame -- the header, a bindings frame, the terminal -- may
        # spend everything, including the margin, because it is what makes the
        # annotation readable at all. A segment may spend the margin but not the
        # closing reserve: overrunning into the margin is a rollover, overrunning
        # past it is the failure this is the last line against.
        cap = self._max_bytes if closing else self._max_bytes - self._closing
        if self._size + len(frame) > cap:
            raise AnnotationBudgetExceeded(
                f"encoding {len(frame)} more byte(s) would take the annotation to "
                f"{self._size + len(frame)}, past the {cap}-byte limit "
                f"({self._max_bytes}-byte budget less {self._closing} reserved for "
                "the frames that close it); the runtime should have stopped "
                "delivering and asked Core to roll the Workflow Task over before "
                "this"
            )
        self._emitted.append(frame)
        self._size += len(frame)
        return frame
