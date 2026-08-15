"""Payload encoding for stream records (P4).

A record's ``payload`` bytes are a serialized
:py:class:`temporalio.api.common.v1.Payload`, not a bare value encoding. Keeping
the envelope means the payload's own ``encoding`` and ``messageType`` metadata
travel with it, so a consumer decodes exactly what the producer wrote rather
than having to be told out of band.

Producer and consumer must use the **same** ``DataConverter``, including any
codec. A mismatch is detected here, at decode time on the consumer, and the
replay read path classifies it as a decode failure rather than as stream
integrity loss -- the stream is fine; the configuration is not.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Generic

import temporalio.api.common.v1
import temporalio.converter
from temporalio.types import AnyType

__all__ = ["StreamPayloadCodec"]


@dataclass(frozen=True)
class StreamPayloadCodec(Generic[AnyType]):
    """Encodes and decodes one topic's values.

    The type is carried by the topic -- ``topic("tokens", type=str)`` -- and is
    passed to the converter as a decode hint, so a topic declared as ``str``
    yields ``str`` rather than whatever the default converter happens to infer.
    """

    data_converter: temporalio.converter.DataConverter
    value_type: type[AnyType] | None = None

    async def encode(self, value: AnyType) -> bytes:
        """Serializes one value to a record's payload bytes."""
        payloads = await self.data_converter.encode([value])
        if len(payloads) != 1:
            # `DataConverter.encode` is documented as free to return fewer
            # payloads than values. One value must still produce one payload,
            # and a converter that collapses it would silently drop a record.
            raise ValueError(
                f"encoding one stream value produced {len(payloads)} payloads; "
                "exactly one is required"
            )
        return payloads[0].SerializeToString()

    async def decode(self, payload: bytes) -> AnyType:
        """Parses a record's payload bytes back into a value.

        Raises whatever the converter or codec raises. Classification into the
        failure taxonomy belongs to the replay read path, which is the only
        caller that knows whether the record's range validated first.
        """
        parsed = temporalio.api.common.v1.Payload()
        parsed.ParseFromString(payload)
        type_hints = None if self.value_type is None else [self.value_type]
        values = await self.data_converter.decode([parsed], type_hints)
        if len(values) != 1:
            raise ValueError(
                f"decoding one stream record produced {len(values)} values; "
                "exactly one is required"
            )
        return values[0]

    def with_type(self, value_type: type[Any] | None) -> StreamPayloadCodec[Any]:
        """This codec bound to a different topic's declared type."""
        return StreamPayloadCodec(self.data_converter, value_type)
