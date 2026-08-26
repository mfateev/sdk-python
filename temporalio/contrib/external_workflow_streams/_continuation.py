"""The Continue-As-New cursor (P15).

A stream spans a whole Continue-As-New chain, so a new Run must resume from the
position its predecessor committed -- on replay as well as live.

**A cursor is never derived from mutable backend state, on any Run.** Reading the
current position from the backend, or from coordination state keyed by chain,
would give replay whatever the backend holds *now* rather than what the Run
originally started from, and two replays of one history could then diverge. The
position therefore travels in a reserved internal header on the Continue-As-New
command, persisted in the new Run's ``WorkflowExecutionStarted`` and restored
before any subscription is established (ADR-022).

It populates the same annotation-header ``start_cursor`` field that a first
execution fills with ``BEGINNING``, so replay reads an explicit starting boundary
in **every** case -- including the case where the stream was empty for the
subscription's entire life.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import temporalio.api.common.v1
from temporalio.contrib.external_workflow_streams._annotation import (
    AnnotationDecodeError,
    _put_cursor,
    _put_str,
    _put_uvarint,
    _Reader,
)
from temporalio.contrib.external_workflow_streams._record import Cursor

__all__ = [
    "CONTINUATION_HEADER",
    "Continuation",
    "decode_continuation",
    "encode_continuation",
    "read_continuation_header",
    "write_continuation_header",
]

CONTINUATION_HEADER = "__temporal_external_stream_continuation"
"""Reserved, and in this feature's namespace rather than ``workflow_stream``'s.

``temporalio.contrib.workflow_streams`` reserves ``__temporal_workflow_stream_*``
for a different feature that coexists with this one (ADR-001).
"""

CONTINUATION_ENCODING = b"binary/plain"
"""Serialized by this module, not by the user's ``DataConverter``.

The header is written by one Run and read by the next, and a chain whose Runs
were deployed with different converter configuration would otherwise restart at
an unreadable cursor -- silently, since an unreadable cursor looks exactly like
no cursor at all.
"""

_SCHEMA_VERSION = 1
"""The only continuation schema this unreleased feature understands."""


@dataclass(frozen=True)
class Continuation:
    """Where each subscription had got to when the Run continued as new.

    Keyed by ``wait_id`` so two subscriptions to one stream restore
    independently: they are separate waits with their own cursors, and a
    stream-keyed continuation would restart one of them at the other's position.

    The three maps beside ``cursors`` are the **binding the cursor was produced
    under**, and each is there because without it a cursor can be restored into
    something it does not describe:

    - ``stream_names`` so a renumbered subscription is detectable. Restoring a
      cursor onto a differently-numbered wait would resume one stream at
      another's offset, which the backend would accept and no later check would
      catch.
    - ``provider_ids`` and ``provider_format_versions`` because a Worker can
      be reconfigured to a different implementation. Marker replay checks both
      before interpreting recorded offsets; the first live read in a successor
      Run happens before that Run has written a marker that could perform the
      check, so the continuation has to carry them itself.
    """

    cursors: dict[int, Cursor]
    stream_names: dict[int, str]
    provider_ids: dict[int, str] = field(default_factory=dict)
    provider_format_versions: dict[int, int] = field(default_factory=dict)


def encode_continuation(continuation: Continuation) -> bytes:
    """Encode current continuation state deterministically."""
    out = bytearray()
    _put_uvarint(out, _SCHEMA_VERSION)
    _put_uvarint(out, len(continuation.cursors))
    # Sorted, so the same state always encodes to the same bytes. The header is
    # re-derived from live state every time the terminal command is built, and
    # bytes that depended on dict insertion order would put a different cursor
    # in front of the successor on a Workflow Task the server retried.
    for wait_id in sorted(continuation.cursors):
        _put_uvarint(out, wait_id)
        _put_cursor(out, continuation.cursors[wait_id])
        _put_str(out, continuation.stream_names.get(wait_id, ""))
        _put_str(out, continuation.provider_ids[wait_id])
        _put_uvarint(out, continuation.provider_format_versions[wait_id])
    return bytes(out)


def decode_continuation(raw: bytes) -> Continuation:
    """Decode and validate a supported continuation schema."""
    reader = _Reader(raw)
    version = reader.uvarint()
    if version != _SCHEMA_VERSION:
        raise AnnotationDecodeError(
            f"the Continue-As-New cursor header is schema version {version}, but "
            f"this Worker understands {_SCHEMA_VERSION}. The previous Run of this "
            "chain was executed by a newer SDK."
        )
    cursors: dict[int, Cursor] = {}
    stream_names: dict[int, str] = {}
    provider_ids: dict[int, str] = {}
    provider_format_versions: dict[int, int] = {}
    for _ in range(reader.uvarint()):
        wait_id = reader.uvarint()
        cursors[wait_id] = reader.cursor()
        stream_names[wait_id] = reader.string()
        provider_ids[wait_id] = reader.string()
        provider_format_versions[wait_id] = reader.uvarint()
    return Continuation(
        cursors=cursors,
        stream_names=stream_names,
        provider_ids=provider_ids,
        provider_format_versions=provider_format_versions,
    )


def write_continuation_header(
    continuation: Continuation,
) -> temporalio.api.common.v1.Payload:
    """Wrap encoded continuation state in its reserved Payload envelope."""
    return temporalio.api.common.v1.Payload(
        metadata={"encoding": CONTINUATION_ENCODING},
        data=encode_continuation(continuation),
    )


def read_continuation_header(
    headers: dict[str, temporalio.api.common.v1.Payload] | None,
) -> Continuation | None:
    """The continuation a predecessor Run left, or ``None`` on a first execution.

    ``None`` is not a failure: it is what every chain's first Run sees, and the
    subscription then starts at ``BEGINNING``.
    """
    if not headers:
        return None
    payload = headers.get(CONTINUATION_HEADER)
    if payload is None:
        return None
    return decode_continuation(payload.data)
