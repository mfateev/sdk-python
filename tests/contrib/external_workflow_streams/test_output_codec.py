"""Canonical pre-codec output frames and fingerprints."""

from __future__ import annotations

import hashlib
import struct

import pytest

import temporalio.api.common.v1
from temporalio.contrib.external_workflow_streams._output_codec import (
    LOGICAL_FINGERPRINT_VERSION,
    canonical_logical_record_frame,
    fingerprint_logical_frames,
    fingerprint_logical_records,
)
from temporalio.contrib.external_workflow_streams._record import RecordKind


def lp(value: bytes) -> bytes:
    return struct.pack(">Q", len(value)) + value


def test_canonical_frame_has_an_exact_version_one_encoding() -> None:
    payload = temporalio.api.common.v1.Payload(
        metadata={"z": b"last", "a": b"first"},
        data=b"payload",
    )

    frame = canonical_logical_record_frame("events", RecordKind.DATA, payload)

    assert frame == b"".join(
        (
            lp(b"events"),
            lp(b"\x01"),
            struct.pack(">Q", 2),
            lp(b"a"),
            lp(b"first"),
            lp(b"z"),
            lp(b"last"),
            lp(b"payload"),
        )
    )


def test_metadata_insertion_order_does_not_change_frame_or_fingerprint() -> None:
    first = temporalio.api.common.v1.Payload(data=b"data")
    first.metadata["z"] = b"last"
    first.metadata["a"] = b"first"
    second = temporalio.api.common.v1.Payload(data=b"data")
    second.metadata["a"] = b"first"
    second.metadata["z"] = b"last"

    first_frame = canonical_logical_record_frame("events", RecordKind.DATA, first)
    second_frame = canonical_logical_record_frame("events", RecordKind.DATA, second)

    assert first_frame == second_frame
    assert fingerprint_logical_frames([first_frame]) == fingerprint_logical_frames(
        [second_frame]
    )


def test_batch_hashes_ordered_length_prefixed_frames() -> None:
    frames = [b"a", b"bc"]

    fingerprint = fingerprint_logical_frames(frames)

    assert fingerprint.version == LOGICAL_FINGERPRINT_VERSION == 1
    assert fingerprint.digest == hashlib.sha256(lp(b"a") + lp(b"bc")).digest()
    assert fingerprint.logical_byte_count == 3
    assert fingerprint.record_count == 2


def test_fingerprint_changes_with_order_topic_kind_metadata_or_data() -> None:
    base = temporalio.api.common.v1.Payload(
        metadata={"encoding": b"json/plain"}, data=b"one"
    )
    changed_metadata = temporalio.api.common.v1.Payload(
        metadata={"encoding": b"binary/plain"}, data=b"one"
    )
    changed_data = temporalio.api.common.v1.Payload(
        metadata={"encoding": b"json/plain"}, data=b"two"
    )
    variants: list[
        list[
            tuple[
                str,
                RecordKind,
                temporalio.api.common.v1.Payload | None,
            ]
        ]
    ] = [
        [("other", RecordKind.DATA, base)],
        [("events", RecordKind.DATA, changed_metadata)],
        [("events", RecordKind.DATA, changed_data)],
        [
            ("events", RecordKind.DATA, base),
            ("events", RecordKind.FINISH, None),
        ],
        [
            ("events", RecordKind.FINISH, None),
            ("events", RecordKind.DATA, base),
        ],
    ]
    baseline = fingerprint_logical_records([("events", RecordKind.DATA, base)])

    assert all(fingerprint_logical_records(variant) != baseline for variant in variants)
    assert fingerprint_logical_records(variants[-1]) != fingerprint_logical_records(
        variants[-2]
    )


def test_length_prefixes_prevent_field_and_record_repartitioning() -> None:
    topic_a = canonical_logical_record_frame(
        "a", RecordKind.DATA, temporalio.api.common.v1.Payload(data=b"bc")
    )
    topic_ab = canonical_logical_record_frame(
        "ab", RecordKind.DATA, temporalio.api.common.v1.Payload(data=b"c")
    )

    assert topic_a != topic_ab
    assert fingerprint_logical_frames([b"a", b"bc"]) != fingerprint_logical_frames(
        [b"ab", b"c"]
    )


def test_finish_has_a_canonical_payload_free_frame() -> None:
    frame = canonical_logical_record_frame("events", RecordKind.FINISH, None)

    assert frame == b"".join(
        (
            lp(b"events"),
            lp(b"\x03"),
            struct.pack(">Q", 0),
            lp(b""),
        )
    )


def test_logical_frame_rejects_invalid_payload_shapes() -> None:
    with pytest.raises(ValueError, match="DATA.*needs a Payload"):
        canonical_logical_record_frame("events", RecordKind.DATA, None)
    with pytest.raises(ValueError, match="FINISH.*carries no Payload"):
        canonical_logical_record_frame(
            "events", RecordKind.FINISH, temporalio.api.common.v1.Payload()
        )
    with pytest.raises(ValueError, match="non-empty topic"):
        canonical_logical_record_frame(
            "", RecordKind.DATA, temporalio.api.common.v1.Payload()
        )
