"""Must-understand Continue-As-New state for finished output topics."""

from __future__ import annotations

from dataclasses import dataclass

import temporalio.api.common.v1
from temporalio.contrib.external_workflow_streams._annotation import (
    AnnotationDecodeError,
    _put_str,
    _put_uvarint,
    _Reader,
)

__all__ = [
    "OUTPUT_CONTINUATION_HEADER",
    "OutputContinuation",
    "decode_output_continuation",
    "encode_output_continuation",
    "read_output_continuation_header",
    "write_output_continuation_header",
]

OUTPUT_CONTINUATION_HEADER = "__temporal_external_output_stream_continuation"
_OUTPUT_CONTINUATION_ENCODING = b"binary/plain"
_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class OutputContinuation:
    """The output topics whose explicit terminal committed in this chain."""

    finished_topics: frozenset[str]
    provider_id: str
    provider_format_version: int

    def __post_init__(self) -> None:
        """Reject continuation state that cannot identify a durable provider."""
        if not self.finished_topics:
            raise ValueError("output continuation state needs a finished topic")
        if any(not topic for topic in self.finished_topics):
            raise ValueError("an output continuation topic may not be empty")
        if not self.provider_id:
            raise ValueError("output continuation state needs a provider ID")
        if self.provider_format_version < 1:
            raise ValueError(
                "output continuation state needs a positive format version"
            )


def encode_output_continuation(continuation: OutputContinuation) -> bytes:
    """Encode finished topics and their provider binding deterministically."""
    out = bytearray()
    _put_uvarint(out, _SCHEMA_VERSION)
    _put_str(out, continuation.provider_id)
    _put_uvarint(out, continuation.provider_format_version)
    _put_uvarint(out, len(continuation.finished_topics))
    for topic in sorted(continuation.finished_topics):
        _put_str(out, topic)
    return bytes(out)


def decode_output_continuation(raw: bytes) -> OutputContinuation:
    """Decode a supported output continuation or fail closed."""
    reader = _Reader(raw)
    version = reader.uvarint()
    if version != _SCHEMA_VERSION:
        raise AnnotationDecodeError(
            "the external output Continue-As-New header is schema version "
            f"{version}, but this Worker understands {_SCHEMA_VERSION}"
        )
    provider_id = reader.string()
    provider_format_version = reader.uvarint()
    topics = frozenset(reader.string() for _ in range(reader.uvarint()))
    return OutputContinuation(topics, provider_id, provider_format_version)


def write_output_continuation_header(
    continuation: OutputContinuation,
) -> temporalio.api.common.v1.Payload:
    """Wrap output continuation bytes without using the user converter."""
    return temporalio.api.common.v1.Payload(
        metadata={"encoding": _OUTPUT_CONTINUATION_ENCODING},
        data=encode_output_continuation(continuation),
    )


def read_output_continuation_header(
    headers: dict[str, temporalio.api.common.v1.Payload] | None,
) -> OutputContinuation | None:
    """Read predecessor output state, with absence meaning a first Run."""
    if not headers:
        return None
    payload = headers.get(OUTPUT_CONTINUATION_HEADER)
    if payload is None:
        return None
    if payload.metadata.get("encoding") != _OUTPUT_CONTINUATION_ENCODING:
        raise AnnotationDecodeError(
            "the external output Continue-As-New header has an unsupported encoding"
        )
    return decode_output_continuation(payload.data)
