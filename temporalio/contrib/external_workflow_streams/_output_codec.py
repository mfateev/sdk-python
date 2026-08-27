"""Canonical logical framing for Workflow-originated output.

Workflow code runs only the deterministic payload converter. The resulting
``Payload`` is framed and fingerprinted before any asynchronous payload codec
or external-storage transform runs, so codec randomness and compression cannot
change replay identity or capacity boundaries.
"""

from __future__ import annotations

import hashlib
import struct
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Final

import temporalio.api.common.v1
from temporalio.contrib.external_workflow_streams._record import RecordKind

__all__ = [
    "LOGICAL_FINGERPRINT_VERSION",
    "LogicalOutputFingerprint",
    "canonical_logical_record_frame",
    "fingerprint_logical_frames",
    "fingerprint_logical_records",
]

LOGICAL_FINGERPRINT_VERSION: Final = 1
"""The canonical frame and batch hash format implemented here."""

_LENGTH = struct.Struct(">Q")
"""Unsigned 64-bit network-order lengths used by fingerprint version 1."""


@dataclass(frozen=True)
class LogicalOutputFingerprint:
    """The replay identity and deterministic capacity cost of one batch."""

    version: int
    digest: bytes
    logical_byte_count: int
    record_count: int


def _length_prefixed(value: bytes) -> bytes:
    if len(value) >= 1 << 64:
        raise ValueError("a logical output frame field is too large to encode")
    return _LENGTH.pack(len(value)) + value


def canonical_logical_record_frame(
    topic: str,
    kind: object,
    payload: temporalio.api.common.v1.Payload | None,
) -> bytes:
    """Build one fingerprint-version-1 logical record frame.

    The format is, in order: length-prefixed UTF-8 topic; length-prefixed
    unsigned record kind; unsigned metadata-entry count; each metadata key and
    value length-prefixed with keys sorted by their UTF-8 bytes; and
    length-prefixed raw payload data. Control records use an empty metadata set
    and empty data. The ``Payload`` protobuf itself is never serialized.
    """
    if not topic:
        raise ValueError("a logical output record needs a non-empty topic")
    if not isinstance(kind, RecordKind):
        raise TypeError(
            f"a logical output record kind must be RecordKind, got {type(kind).__name__}"
        )
    if kind is RecordKind.DATA and payload is None:
        raise ValueError("a DATA logical output record needs a Payload")
    if kind.is_control and payload is not None:
        raise ValueError(f"a {kind.name} logical output record carries no Payload")

    topic_bytes = topic.encode("utf-8")
    # RecordKind is an unsigned protocol number. Minimal big-endian encoding is
    # unambiguous because the byte string itself is length-prefixed.
    kind_value = int(kind)
    kind_bytes = kind_value.to_bytes(max(1, (kind_value.bit_length() + 7) // 8), "big")
    metadata = () if payload is None else payload.metadata.items()
    sorted_metadata = sorted(metadata, key=lambda item: item[0].encode("utf-8"))
    if len(sorted_metadata) >= 1 << 64:
        raise ValueError("a logical output Payload has too many metadata entries")

    frame = bytearray()
    frame.extend(_length_prefixed(topic_bytes))
    frame.extend(_length_prefixed(kind_bytes))
    frame.extend(_LENGTH.pack(len(sorted_metadata)))
    for key, value in sorted_metadata:
        frame.extend(_length_prefixed(key.encode("utf-8")))
        frame.extend(_length_prefixed(value))
    frame.extend(_length_prefixed(b"" if payload is None else payload.data))
    return bytes(frame)


def fingerprint_logical_frames(
    frames: Iterable[bytes],
) -> LogicalOutputFingerprint:
    """Hash ordered, length-prefixed canonical frames with SHA-256.

    ``logical_byte_count`` is the sum of the canonical frame lengths, excluding
    the batch-level prefixes used only to prevent adjacent frames from being
    repartitioned into the same hash input.
    """
    digest = hashlib.sha256()
    logical_byte_count = 0
    record_count = 0
    for frame in frames:
        digest.update(_length_prefixed(frame))
        logical_byte_count += len(frame)
        record_count += 1
    return LogicalOutputFingerprint(
        version=LOGICAL_FINGERPRINT_VERSION,
        digest=digest.digest(),
        logical_byte_count=logical_byte_count,
        record_count=record_count,
    )


def fingerprint_logical_records(
    records: Iterable[tuple[str, RecordKind, temporalio.api.common.v1.Payload | None]],
) -> LogicalOutputFingerprint:
    """Frame and fingerprint an ordered sequence of logical output records."""
    return fingerprint_logical_frames(
        canonical_logical_record_frame(topic, kind, payload)
        for topic, kind, payload in records
    )
