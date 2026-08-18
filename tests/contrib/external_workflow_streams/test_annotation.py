"""P5 — the replay annotation codec."""

from __future__ import annotations

import pytest

from temporalio.contrib.external_workflow_streams._annotation import (
    MAX_ANNOTATION_BYTES,
    ROLLOVER_HIGH_WATER,
    SCHEMA_VERSION,
    Annotation,
    AnnotationAccumulator,
    AnnotationBudgetExceeded,
    AnnotationDecodeError,
    AnnotationHeader,
    Run,
    Segment,
    SegmentEndReason,
    StreamBinding,
    decode_annotation,
    encode_annotation,
    encode_bindings,
)
from temporalio.contrib.external_workflow_streams._backend import StreamKey
from temporalio.contrib.external_workflow_streams._record import (
    AFTER,
    BEGINNING,
    Cursor,
    Offset,
)

KEY = StreamKey("ns", "wf", "run-1", "tokens")
OTHER_KEY = StreamKey("ns", "wf", "run-1", "tool-events")


def binding(key: StreamKey, start_cursor: Cursor = BEGINNING) -> StreamBinding:
    """A binding naming its own backend, as every real one does.

    The provider identity lives on the binding rather than on the header: one
    annotation can carry waits on several backends, so a single label could be
    right for at most one of them.
    """
    return StreamBinding(
        stream_key=key,
        start_cursor=start_cursor,
        backend_name="tokens",
        provider_id="redis-streams",
        provider_format_version=1,
    )


def header(streams: dict[int, StreamBinding] | None = None) -> AnnotationHeader:
    return AnnotationHeader(
        streams=streams if streams is not None else {1: binding(KEY)},
    )


def offset(ms: int, seq: int = 0) -> Offset:
    return Offset(f"{ms}-{seq}")


# --- round trips ------------------------------------------------------------


def test_a_full_annotation_round_trips() -> None:
    annotation = Annotation(
        header=header({1: binding(KEY, AFTER(offset(50)))}),
        segments=(
            Segment(
                runs=(Run(1, offset(100), offset(199), 100, (7, 42)),),
                end_reason=SegmentEndReason.NO_DATA_AVAILABLE,
            ),
            Segment(
                runs=(Run(1, offset(200), offset(205), 6),),
                end_reason=SegmentEndReason.FENCE_REACHED,
            ),
        ),
        terminal={1: AFTER(offset(205))},
    )

    assert decode_annotation(encode_annotation(annotation)) == annotation


def test_schema_version_leads_the_encoding() -> None:
    """Readable before anything version-dependent is interpreted."""
    encoded = encode_annotation(Annotation(header(), terminal={1: BEGINNING}))

    assert encoded[0] == SCHEMA_VERSION
    assert decode_annotation(encoded).header.schema_version == SCHEMA_VERSION


def test_an_unknown_schema_version_is_refused_by_name() -> None:
    encoded = bytearray(encode_annotation(Annotation(header(), terminal={})))
    encoded[0] = 99

    with pytest.raises(AnnotationDecodeError, match="schema version 99"):
        decode_annotation(bytes(encoded))


def test_an_empty_segment_round_trips() -> None:
    """An activation that drained and found nothing still ran one drain.

    Dropping it would make ``wait_condition`` predicates fire a different
    number of times on replay than they did live.
    """
    annotation = Annotation(
        header(),
        segments=(Segment(runs=(), end_reason=SegmentEndReason.NO_DATA_AVAILABLE),),
        terminal={1: BEGINNING},
    )

    decoded = decode_annotation(encode_annotation(annotation))

    assert decoded == annotation
    assert decoded.segments[0].runs == ()


def test_an_annotation_with_no_segments_round_trips() -> None:
    """A subscription to an empty stream: header and terminal carry everything."""
    annotation = Annotation(header(), segments=(), terminal={1: BEGINNING})

    decoded = decode_annotation(encode_annotation(annotation))

    assert decoded == annotation
    assert decoded.segments == ()
    assert decoded.header.streams[1].start_cursor == BEGINNING


def test_cursors_and_offsets_stay_distinct_types() -> None:
    """A boundary and a record identity are not interchangeable.

    ``AFTER(100-0)`` in the terminal and the record offset ``100-0`` in a run
    must decode as a Cursor and an Offset respectively, not as each other.
    """
    annotation = Annotation(
        header(),
        segments=(
            Segment(
                (Run(1, offset(100), offset(100), 1),), SegmentEndReason.BATCH_LIMIT
            ),
        ),
        terminal={1: AFTER(offset(100))},
    )

    decoded = decode_annotation(encode_annotation(annotation))

    assert decoded.segments[0].runs[0].first_offset == offset(100)
    assert decoded.terminal is not None
    assert decoded.terminal[1] == AFTER(offset(100))
    assert decoded.terminal[1] != offset(100)


def test_beginning_is_not_the_same_as_after_a_zero_offset() -> None:
    annotation = Annotation(header(), terminal={1: BEGINNING, 2: AFTER(Offset("0-0"))})

    decoded = decode_annotation(encode_annotation(annotation))

    assert decoded.terminal is not None
    assert decoded.terminal[1].is_beginning
    assert not decoded.terminal[2].is_beginning


def test_sparse_control_positions_round_trip() -> None:
    run = Run(1, offset(100), offset(999), 500, (0, 250, 499))

    decoded = decode_annotation(
        encode_annotation(
            Annotation(
                header(),
                segments=(Segment((run,), SegmentEndReason.FENCE_REACHED),),
                terminal={1: AFTER(offset(999))},
            )
        )
    )

    assert decoded.segments[0].runs[0].control_positions == (0, 250, 499)


@pytest.mark.parametrize(
    ("positions", "count", "reason"),
    [
        ((5,), 3, "outside a run"),
        ((2, 1), 5, "strictly increasing"),
        ((1, 1), 5, "strictly increasing"),
    ],
)
def test_malformed_control_positions_are_rejected_at_construction(
    positions: tuple[int, ...], count: int, reason: str
) -> None:
    with pytest.raises(ValueError, match=reason):
        Run(1, offset(1), offset(9), count, positions)


def test_a_run_covers_at_least_one_record() -> None:
    with pytest.raises(ValueError, match="at least one record"):
        Run(1, offset(1), offset(1), 0)


@pytest.mark.parametrize(
    "corrupt",
    [b"", b"\x01", b"\x01\x02", b"\x01\x01\x05abc"],
    ids=["empty", "version-only", "wrong-first-frame", "truncated-header"],
)
def test_corrupt_bytes_raise_a_decode_error(corrupt: bytes) -> None:
    with pytest.raises(AnnotationDecodeError):
        decode_annotation(corrupt)


def test_two_terminals_are_refused() -> None:
    doubled = encode_annotation(Annotation(header(), terminal={1: BEGINNING}))
    doubled += doubled[doubled.index(b"\x03") :]

    with pytest.raises(AnnotationDecodeError, match="one terminal, not two"):
        decode_annotation(doubled)


# --- runs are what keep markers small ---------------------------------------


def test_a_large_single_stream_batch_encodes_as_one_run() -> None:
    """100,000 records, one run. This is the claim the whole design rests on."""
    annotation = Annotation(
        header(),
        segments=(
            Segment(
                (Run(1, offset(1), offset(100_000), 100_000),),
                SegmentEndReason.NO_DATA_AVAILABLE,
            ),
        ),
        terminal={1: AFTER(offset(100_000))},
    )

    decoded = decode_annotation(encode_annotation(annotation))

    assert len(decoded.segments[0].runs) == 1
    assert decoded.segments[0].runs[0].count == 100_000


def test_encoded_size_stays_flat_as_a_single_stream_batch_grows() -> None:
    """Byte size, not run count.

    A run-count assertion says nothing about what a future per-record field
    would cost; measuring bytes is what actually catches one being added.
    """

    def encoded_size(count: int) -> int:
        return len(
            encode_annotation(
                Annotation(
                    header(),
                    segments=(
                        Segment(
                            (Run(1, offset(1), offset(count), count),),
                            SegmentEndReason.NO_DATA_AVAILABLE,
                        ),
                    ),
                    terminal={1: AFTER(offset(count))},
                )
            )
        )

    small, large = encoded_size(100), encoded_size(100_000)

    # Three orders of magnitude more records. The only growth permitted is in
    # the varints holding the count and the offset tokens themselves.
    assert large - small <= 16, (
        f"{small} bytes for 100 records but {large} for 100,000 -- something in "
        "the encoding is per-record"
    )


def test_alternating_streams_cost_one_run_per_delivery() -> None:
    """The honest worst case, stated as a test rather than assumed away."""
    runs = tuple(Run(1 + (i % 2), offset(i), offset(i), 1) for i in range(1, 21))
    annotation = Annotation(
        header({1: binding(KEY), 2: binding(OTHER_KEY)}),
        segments=(Segment(runs, SegmentEndReason.NO_DATA_AVAILABLE),),
        terminal={1: AFTER(offset(19)), 2: AFTER(offset(20))},
    )

    decoded = decode_annotation(encode_annotation(annotation))

    assert len(decoded.segments[0].runs) == 20
    assert [r.wait_id for r in decoded.segments[0].runs[:4]] == [2, 1, 2, 1]


# --- accumulation: deltas concatenate into the annotation -------------------


def test_a_delta_sequence_concatenates_into_the_annotation() -> None:
    """What Core accumulates by byte-append must equal what encoding produces.

    Core never parses a delta, so if these two ever diverged nothing would
    notice until replay read a marker that could not be decoded.
    """
    head = header()
    segments = (
        Segment((Run(1, offset(100), offset(110), 11),), SegmentEndReason.BATCH_LIMIT),
        Segment((), SegmentEndReason.NO_DATA_AVAILABLE),
        Segment(
            (Run(1, offset(111), offset(120), 10),), SegmentEndReason.FENCE_REACHED
        ),
    )
    terminal = {1: AFTER(offset(120))}

    accumulator = AnnotationAccumulator(head)
    deltas = [accumulator.add_segment(s) for s in segments]
    deltas.append(accumulator.add_terminal(terminal))

    core_would_hold = accumulator.accumulated()

    assert core_would_hold == encode_annotation(Annotation(head, segments, terminal))
    assert decode_annotation(core_would_hold) == Annotation(head, segments, terminal)
    # And the header rode the accumulator's first emission, not a delta.
    assert core_would_hold.endswith(b"".join(deltas))


def test_the_accumulator_reports_the_size_the_marker_will_carry() -> None:
    accumulator = AnnotationAccumulator(header())
    accumulator.add_segment(
        Segment((Run(1, offset(1), offset(9), 9),), SegmentEndReason.BATCH_LIMIT)
    )
    accumulator.add_terminal({1: AFTER(offset(9))})

    assert accumulator.size == len(accumulator.accumulated())


# --- a wait bound after the header ------------------------------------------


def test_a_wait_bound_after_the_header_decodes_into_it() -> None:
    """A subscription can be created at any activation of a retained task.

    The header frame for that task has usually already gone to Core by then,
    and Core appends deltas rather than rewriting them, so the binding rides a
    frame of its own. What replay reads is still one flat table: a wait that
    joined late is indistinguishable, once decoded, from one that was there
    from the start -- which is what lets `prepare_replay` resolve its backend
    and stream key at all.
    """
    accumulator = AnnotationAccumulator(header())
    accumulator.add_segment(
        Segment((Run(1, offset(100), offset(100), 1),), SegmentEndReason.BATCH_LIMIT)
    )
    accumulator.add_bindings({2: binding(OTHER_KEY)})
    accumulator.add_segment(
        Segment(
            (Run(2, offset(200), offset(200), 1),), SegmentEndReason.NO_DATA_AVAILABLE
        )
    )
    accumulator.add_terminal({1: AFTER(offset(100)), 2: AFTER(offset(200))})

    decoded = decode_annotation(accumulator.accumulated())

    assert decoded.header.streams == {1: binding(KEY), 2: binding(OTHER_KEY)}
    assert decoded.terminal == {1: AFTER(offset(100)), 2: AFTER(offset(200))}
    assert [run.wait_id for segment in decoded.segments for run in segment.runs] == [
        1,
        2,
    ]


def test_a_late_binding_keeps_its_own_start_cursor() -> None:
    """Not wherever the waits already in the header have reached.

    A wait registered part-way through a Workflow Task begins at the boundary
    its own `subscribe()` was given. Recording the annotation-wide position
    instead would start replay of that wait past records it in fact received.
    """
    accumulator = AnnotationAccumulator(header({1: binding(KEY, AFTER(offset(100)))}))
    accumulator.add_bindings({2: binding(OTHER_KEY)})
    accumulator.add_terminal({1: AFTER(offset(100)), 2: BEGINNING})

    decoded = decode_annotation(accumulator.accumulated())

    assert decoded.header.streams[1].start_cursor == AFTER(offset(100))
    assert decoded.header.streams[2].start_cursor == BEGINNING


def test_a_bindings_frame_is_the_header_frame_body_under_its_own_tag() -> None:
    """Locked to exact bytes, like the golden annotation and for the reason.

    A second *header* frame would have been the smaller change and is what the
    encoding deliberately does not do: a header frame also carries the schema
    version, so a decoder meeting two of them has to decide whether the later
    replaces the earlier. The distinct tag says one thing only.
    """
    assert encode_bindings({2: binding(OTHER_KEY)}).hex() == "".join(
        [
            "04",  # bindings frame
            "01",  # one stream
            "02",  # wait id 2
            "02" + "ns".encode().hex(),
            "02" + "wf".encode().hex(),
            "05" + "run-1".encode().hex(),
            "0b" + "tool-events".encode().hex(),
            "00",  # start cursor BEGINNING
            "06" + "tokens".encode().hex(),  # backend name
            "0d" + "redis-streams".encode().hex(),  # provider id
            "01",  # provider format version 1
        ]
    )


def test_a_wait_bound_twice_is_refused() -> None:
    """Two bindings for one wait leave replay choosing between stream keys.

    Whichever it chose could be the one the recorded records were not written
    to, and the range read would then fail as integrity loss against a backend
    nothing is wrong with.
    """
    accumulator = AnnotationAccumulator(header())
    accumulator.add_bindings({1: binding(OTHER_KEY)})
    accumulator.add_terminal({1: BEGINNING})

    with pytest.raises(AnnotationDecodeError, match="bound twice"):
        decode_annotation(accumulator.accumulated())


def test_bindings_after_the_terminal_are_refused() -> None:
    """Both halves: the encoder cannot emit one and the decoder rejects one.

    The annotation ends at its terminal. A binding appended past it belongs to
    a Workflow Task whose marker has already been decided, and Core would write
    it into that marker rather than the next one.
    """
    accumulator = AnnotationAccumulator(header())
    accumulator.add_terminal({1: BEGINNING})
    with pytest.raises(ValueError, match="after the terminal"):
        accumulator.add_bindings({2: binding(OTHER_KEY)})

    with pytest.raises(AnnotationDecodeError, match="follows the terminal"):
        decode_annotation(
            encode_annotation(Annotation(header(), terminal={1: BEGINNING}))
            + encode_bindings({2: binding(OTHER_KEY)})
        )


def test_a_bindings_frame_binds_at_least_one_wait() -> None:
    """An empty frame costs bytes and says nothing, so it is a caller error."""
    accumulator = AnnotationAccumulator(header())
    with pytest.raises(ValueError, match="at least one wait"):
        accumulator.add_bindings({})


def test_a_segment_after_the_terminal_is_refused() -> None:
    accumulator = AnnotationAccumulator(header())
    accumulator.add_terminal({1: BEGINNING})

    with pytest.raises(ValueError, match="after the terminal"):
        accumulator.add_segment(Segment((), SegmentEndReason.NO_DATA_AVAILABLE))


def test_a_second_terminal_is_refused() -> None:
    accumulator = AnnotationAccumulator(header())
    accumulator.add_terminal({1: BEGINNING})

    with pytest.raises(ValueError, match="one terminal, not two"):
        accumulator.add_terminal({1: BEGINNING})


# --- the byte budget --------------------------------------------------------


def test_rollover_is_requested_before_the_budget_is_reached() -> None:
    accumulator = AnnotationAccumulator(header())
    assert not accumulator.request_rollover

    run = Run(1, offset(1), offset(2), 2)
    while not accumulator.request_rollover:
        accumulator.add_segment(Segment((run,), SegmentEndReason.BATCH_LIMIT))

    assert accumulator.size >= MAX_ANNOTATION_BYTES * ROLLOVER_HIGH_WATER
    # The margin is the point: the request has to reach Core and come back, so
    # asking at 100% would arrive too late to prevent the overflow.
    assert accumulator.size < MAX_ANNOTATION_BYTES


def test_an_alternating_two_stream_batch_asks_for_rollover_rather_than_overflowing() -> (
    None
):
    """Bounded marker size is bought with additional Workflow Tasks.

    An alternating workload has a schedule transition per record, so it cannot
    be range-compressed. The encoder must hit the high-water mark and ask for a
    rollover, not grow past the budget.
    """
    accumulator = AnnotationAccumulator(
        header({1: binding(KEY), 2: binding(OTHER_KEY)})
    )

    delivered = 0
    while not accumulator.request_rollover:
        runs = tuple(
            Run(1 + (i % 2), offset(delivered + i), offset(delivered + i), 1)
            for i in range(50)
        )
        delivered += 50
        accumulator.add_segment(Segment(runs, SegmentEndReason.BATCH_LIMIT))

    assert accumulator.size <= MAX_ANNOTATION_BYTES
    assert delivered > 0


def test_exceeding_the_hard_budget_raises_rather_than_growing() -> None:
    """Unreachable in practice; raised so a future encoding change is caught here.

    The alternative is the server rejecting an oversized event, which surfaces
    as an unexplained Workflow Task failure a long way from the cause.
    """
    tiny = AnnotationAccumulator(header(), max_bytes=120, high_water=0.9)

    with pytest.raises(AnnotationBudgetExceeded, match="past the 120-byte budget"):
        for i in range(100):
            tiny.add_segment(
                Segment(
                    (Run(1, offset(i), offset(i + 1), 2),), SegmentEndReason.BATCH_LIMIT
                )
            )


# --- golden file ------------------------------------------------------------

#: A fixed annotation and its exact bytes. Regenerating this to make a test
#: pass is the mistake it exists to catch: any change here is a wire-format
#: change, and markers already in History were written by the old encoder.
#:
#: This constant was regenerated once, for schema version 2, which moved the
#: provider identity out of the header and onto each binding. That is allowed
#: exactly because the feature is private and unreleased -- there is no History
#: anywhere holding a version-1 marker. `decode_annotation` refuses version 1
#: rather than reading it, so the regeneration cannot silently reinterpret an
#: older marker.
GOLDEN_ANNOTATION = Annotation(
    header=AnnotationHeader(
        streams={
            1: StreamBinding(
                stream_key=StreamKey("ns", "wf", "run-1", "tokens"),
                start_cursor=BEGINNING,
                backend_name="tokens",
                provider_id="redis-streams",
                provider_format_version=1,
            )
        },
    ),
    segments=(
        Segment(
            runs=(Run(1, Offset("100-0"), Offset("104-0"), 5, (2,)),),
            end_reason=SegmentEndReason.FENCE_REACHED,
        ),
        Segment(runs=(), end_reason=SegmentEndReason.NO_DATA_AVAILABLE),
    ),
    terminal={1: AFTER(Offset("104-0"))},
)

GOLDEN_BYTES = bytes.fromhex(
    "".join(
        [
            "02",  # schema version 2
            "01",  # header frame
            "01",  # one stream
            "01",  # wait id 1
            "02" + "ns".encode().hex(),
            "02" + "wf".encode().hex(),
            "05" + "run-1".encode().hex(),
            "06" + "tokens".encode().hex(),
            "00",  # start cursor BEGINNING
            "06" + "tokens".encode().hex(),  # backend name
            "0d" + "redis-streams".encode().hex(),  # provider id
            "01",  # provider format version 1
            "02",  # segment frame
            "01",  # one run
            "01",  # wait id 1
            "05" + "100-0".encode().hex(),
            "05" + "104-0".encode().hex(),
            "05",  # count 5
            "01",  # one control position
            "02",  # at relative index 2
            "03",  # FENCE_REACHED
            "02",  # segment frame
            "00",  # no runs
            "01",  # NO_DATA_AVAILABLE
            "03",  # terminal frame
            "01",  # one entry
            "01",  # wait id 1
            "01",  # AFTER
            "05" + "104-0".encode().hex(),
        ]
    )
)


def test_golden_bytes_are_unchanged() -> None:
    """Silent format drift is what this catches.

    If this fails, the encoding changed. Bump `SCHEMA_VERSION` and keep the old
    decoder working -- do not regenerate the constant.
    """
    assert encode_annotation(GOLDEN_ANNOTATION).hex() == GOLDEN_BYTES.hex()


def test_golden_bytes_decode_to_the_golden_annotation() -> None:
    assert decode_annotation(GOLDEN_BYTES) == GOLDEN_ANNOTATION


def test_encoded_size_stays_flat_with_sparse_control_records() -> None:
    """The realistic shape, and the one where a per-record cost could hide.

    ``control_positions`` is the only field that carries per-record information
    at all, which makes it the natural place for the encoding to start scaling
    with record count. It stays sparse -- a handful of fence positions in a batch
    of any size -- so the marker's cost must track the number of *control*
    records, not the number of records.

    Measured in bytes rather than by inspecting the field, because what matters
    is what reaches History.
    """

    def encoded_size(count: int) -> int:
        # A fence every ~10,000 records: what a few long-running producers
        # finishing at intervals actually looks like.
        controls = tuple(range(0, count, max(1, count // 10)))
        return len(
            encode_annotation(
                Annotation(
                    header(),
                    segments=(
                        Segment(
                            (
                                Run(
                                    1,
                                    offset(1),
                                    offset(count),
                                    count,
                                    control_positions=controls,
                                ),
                            ),
                            SegmentEndReason.NO_DATA_AVAILABLE,
                        ),
                    ),
                    terminal={1: AFTER(offset(count))},
                )
            )
        )

    small, large = encoded_size(100), encoded_size(100_000)

    # Both carry ten control positions; only the positions themselves grow, and
    # only as varints.
    assert large - small <= 32, (
        f"{small} bytes for 100 records but {large} for 100,000, both with ten "
        "control records -- the control encoding is scaling with record count "
        "rather than with control count"
    )


def test_encoded_size_grows_only_with_the_number_of_control_records() -> None:
    """The other half: a marker *may* grow with fences, and only with them.

    Stated as a bound rather than left implicit, because "sparse" is a claim
    about the producer's behaviour and this is what makes the encoding's side of
    it checkable.
    """

    def encoded_size(controls: int) -> int:
        return len(
            encode_annotation(
                Annotation(
                    header(),
                    segments=(
                        Segment(
                            (
                                Run(
                                    1,
                                    offset(1),
                                    offset(1000),
                                    1000,
                                    control_positions=tuple(range(controls)),
                                ),
                            ),
                            SegmentEndReason.NO_DATA_AVAILABLE,
                        ),
                    ),
                    terminal={1: AFTER(offset(1000))},
                )
            )
        )

    per_control = encoded_size(101) - encoded_size(1)

    assert 100 <= per_control <= 300, (
        f"100 extra control records cost {per_control} bytes; a sparse position "
        "list should cost a small constant each"
    )
