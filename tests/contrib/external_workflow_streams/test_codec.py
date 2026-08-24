"""P4 — payload encoding through the Workflow's DataConverter."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

import temporalio.api.common.v1
import temporalio.converter
from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec
from temporalio.contrib.external_workflow_streams._record import (
    Offset,
    RecordKind,
    StreamRecord,
)


@dataclass
class Token:
    """A custom type, to prove the topic's declared type reaches the converter."""

    text: str
    index: int


DEFAULT = temporalio.converter.DataConverter.default


@pytest.mark.asyncio
async def test_str_round_trips() -> None:
    codec = StreamPayloadCodec(DEFAULT, str)

    assert await codec.decode(await codec.encode("hello")) == "hello"


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["", "unicode: é中", "a" * 10_000])
async def test_awkward_strings_round_trip(value: str) -> None:
    codec = StreamPayloadCodec(DEFAULT, str)

    assert await codec.decode(await codec.encode(value)) == value


@pytest.mark.asyncio
async def test_a_custom_type_round_trips_as_that_type() -> None:
    codec = StreamPayloadCodec(DEFAULT, Token)

    decoded = await codec.decode(await codec.encode(Token("hi", 3)))

    # Not merely equal-looking: the declared type is what makes this a `Token`
    # rather than the `dict` the default converter would otherwise infer.
    assert isinstance(decoded, Token)
    assert decoded == Token("hi", 3)


@pytest.mark.asyncio
async def test_without_a_declared_type_the_converter_infers() -> None:
    untyped: StreamPayloadCodec[Any] = StreamPayloadCodec(DEFAULT, None)

    decoded = await untyped.decode(await untyped.encode(Token("hi", 3)))

    assert decoded == {"text": "hi", "index": 3}


@pytest.mark.asyncio
async def test_encoded_bytes_are_a_payload_envelope() -> None:
    """The envelope is what lets a consumer decode without out-of-band metadata."""
    codec = StreamPayloadCodec(DEFAULT, str)

    payload = temporalio.api.common.v1.Payload()
    payload.ParseFromString(await codec.encode("hello"))

    assert payload.metadata["encoding"] == b"json/plain"


@pytest.mark.asyncio
async def test_with_type_rebinds_without_touching_the_converter() -> None:
    codec = StreamPayloadCodec(DEFAULT, str)
    rebound = codec.with_type(Token)

    assert rebound.data_converter is codec.data_converter
    assert rebound.value_type is Token
    assert codec.value_type is str


@pytest.mark.asyncio
async def test_a_payload_encoded_for_one_type_decodes_under_another() -> None:
    """A converter mismatch surfaces here, on the consumer, not as damage.

    The bytes are intact; only the reader's expectation is wrong. Classifying
    that as a decode failure rather than integrity loss is what keeps an
    operator from being sent to repair an undamaged backend.
    """
    produced = await StreamPayloadCodec(DEFAULT, str).encode("not a token")

    with pytest.raises(Exception):
        await StreamPayloadCodec(DEFAULT, Token).decode(produced)


@pytest.mark.asyncio
async def test_undecodable_bytes_raise_rather_than_returning_a_value() -> None:
    with pytest.raises(Exception):
        await StreamPayloadCodec(DEFAULT, str).decode(b"\xff\xfe not a payload")


@pytest.mark.asyncio
async def test_a_record_carries_the_encoded_payload_unchanged() -> None:
    """The codec's output survives the record's own field round trip."""
    codec = StreamPayloadCodec(DEFAULT, Token)
    encoded = await codec.encode(Token("round", 1))

    record = StreamRecord(RecordKind.DATA, encoded, "session-a", 0).placed_at(
        Offset("1-0")
    )
    rebuilt = StreamRecord.from_fields(Offset("1-0"), record.to_fields())

    assert await codec.decode(rebuilt.payload) == Token("round", 1)
