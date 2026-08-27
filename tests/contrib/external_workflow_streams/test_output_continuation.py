"""Continue-As-New state for explicitly finished output topics."""

from __future__ import annotations

import pytest

from temporalio.contrib.external_workflow_streams._annotation import (
    AnnotationDecodeError,
)
from temporalio.contrib.external_workflow_streams._output_continuation import (
    OUTPUT_CONTINUATION_HEADER,
    OutputContinuation,
    decode_output_continuation,
    encode_output_continuation,
    read_output_continuation_header,
    write_output_continuation_header,
)


def state(*topics: str) -> OutputContinuation:
    return OutputContinuation(frozenset(topics), "redis-streams", 1)


def test_output_continuation_round_trips_stably() -> None:
    first = state("status", "events")
    second = state("events", "status")

    assert encode_output_continuation(first) == encode_output_continuation(second)
    assert decode_output_continuation(encode_output_continuation(first)) == first


def test_output_continuation_uses_a_separate_must_understand_header() -> None:
    payload = write_output_continuation_header(state("events"))

    assert read_output_continuation_header(
        {OUTPUT_CONTINUATION_HEADER: payload}
    ) == state("events")
    assert read_output_continuation_header({}) is None


def test_output_continuation_rejects_unknown_schema_or_encoding() -> None:
    payload = write_output_continuation_header(state("events"))
    payload.data = b"\x02" + payload.data[1:]
    with pytest.raises(AnnotationDecodeError, match="schema version 2"):
        read_output_continuation_header({OUTPUT_CONTINUATION_HEADER: payload})

    payload = write_output_continuation_header(state("events"))
    payload.metadata["encoding"] = b"json/plain"
    with pytest.raises(AnnotationDecodeError, match="unsupported encoding"):
        read_output_continuation_header({OUTPUT_CONTINUATION_HEADER: payload})
