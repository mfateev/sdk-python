"""Payload encoding for stream records (P4).

A record's ``payload`` bytes are a serialized
:py:class:`temporalio.api.common.v1.Payload`, not a bare value encoding. Keeping
the envelope means the payload's own ``encoding`` and ``messageType`` metadata
travel with it, so a consumer decodes exactly what the producer wrote rather
than having to be told out of band.

Producer and consumer must use the **same** ``DataConverter``, including any
codec. A mismatch is detected on the consumer, and is classified as a decode
failure rather than as stream integrity loss -- the stream is fine; the
configuration is not.

Decoding on the consumer is **two halves, run in two different places**:

- :py:meth:`StreamPayloadCodec.prepare`, on the **Worker's loop**:
  external-payload retrieval and the user's ``PayloadCodec``. Arbitrary
  asynchronous work, and none of it depends on the value's type.
- :py:meth:`StreamPayloadCodec.convert`, on the **Workflow thread**:
  ``from_payloads`` with the topic's declared type. Synchronous, performs no
  I/O, and is the only half that needs to know the type at all.

That is the same split the Worker already applies to every other payload an
activation carries: ``decode_activation`` is awaited before ``activate()`` is
handed to the executor, and the payload converter runs inside it. A record whose
codec ran on the Workflow thread would perform real I/O inside a deterministic
event loop, turn the codec's awaits into Workflow commands, and put a
multi-second KMS round trip under a 2-second deadlock timeout.
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

        Both halves at once. Correct anywhere a coroutine may await arbitrary
        work -- the producer side, and tests -- and **wrong on the Workflow
        thread**, which is why the consumer side calls :meth:`prepare` and
        :meth:`convert` separately instead.

        Raises whatever the converter or codec raises. Classification into the
        failure taxonomy belongs to the caller, which is the only side that
        knows whether the record's range validated first.
        """
        return self.convert(await self.prepare(payload))

    async def prepare(self, payload: bytes) -> temporalio.api.common.v1.Payload:
        """The asynchronous half of decoding, and the half that has no type.

        ``DataConverter.decode`` is three steps: external-payload retrieval, the
        user's ``PayloadCodec``, and the payload *converter*. The first two are
        arbitrary asynchronous work -- a network fetch, a KMS round trip -- and
        neither of them depends on the topic's declared type. They belong on the
        Worker's loop, which is exactly where the Worker already awaits them for
        every other payload an activation carries.

        Returns the payload as the payload converter will see it. What is left
        is :meth:`convert`, which is synchronous and needs no I/O.
        """
        parsed = temporalio.api.common.v1.Payload()
        parsed.ParseFromString(payload)
        # A deliberate reach into two private helpers rather than an oversight:
        # this split is the whole point, and `DataConverter` exposes no public
        # "everything except the payload converter" entry point. `decode()`
        # itself is these two calls followed by `from_payloads`, so preparing
        # here and converting in `convert` is byte-for-byte the same work in the
        # same order -- only on two different threads.
        retrieved = await self.data_converter._external_retrieve_payload_sequence(
            [parsed]
        )
        decoded = await self.data_converter._decode_payload_sequence(retrieved)
        if len(decoded) != 1:
            raise ValueError(
                f"preparing one stream record produced {len(decoded)} payloads; "
                "exactly one is required"
            )
        return decoded[0]

    def parse_unprepared(self, payload: bytes) -> temporalio.api.common.v1.Payload:
        """The envelope parse alone, for a converter with nothing to prepare.

        A ``DataConverter`` with no codec and no external storage has an empty
        asynchronous half, so a record that never passed through
        :meth:`prepare` still converts correctly. One with either of them does
        not, and the difference is not detectable from the bytes: a codec's
        output is just another payload. Rather than return a plausible wrong
        value, this refuses -- an unprepared record on the Workflow thread means
        the record reached it by a path that does not prepare, which is a
        routing defect and not a user's converter mismatch.
        """
        if self.data_converter._decode_payload_has_effect:
            raise RuntimeError(
                "an external stream record reached the Workflow thread without "
                "its DataConverter's asynchronous half having been applied. "
                "Preparing it here would run a payload codec or an external "
                "payload fetch inside activate(); every delivery path must "
                "prepare records on the Worker's loop instead."
            )
        parsed = temporalio.api.common.v1.Payload()
        parsed.ParseFromString(payload)
        return parsed

    def convert(self, prepared: temporalio.api.common.v1.Payload) -> AnyType:
        """The synchronous half: the topic's declared type, applied.

        The only step that needs the type, and the only one a Workflow thread
        may run: ``from_payloads`` performs no I/O and awaits nothing, which is
        what every ordinary activation payload's conversion already does on this
        same thread.
        """
        type_hints = None if self.value_type is None else [self.value_type]
        values = self.data_converter.payload_converter.from_payloads(
            [prepared], type_hints
        )
        if len(values) != 1:
            raise ValueError(
                f"decoding one stream record produced {len(values)} values; "
                "exactly one is required"
            )
        return values[0]

    def with_type(self, value_type: type[Any] | None) -> StreamPayloadCodec[Any]:
        """This codec bound to a different topic's declared type."""
        return StreamPayloadCodec(self.data_converter, value_type)
