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

_SCHEMA_VERSION = 2
"""What a live Continue-As-New writes.

Version 1 carried only a cursor and a stream name per wait, which is not enough
to say *what the cursor is a position in*: an offset means nothing outside the
store that produced it. Version 2 carries the whole binding, so restoration can
refuse a cursor whose backend moved instead of handing it to a store that will
accept it and skip records.

The bump is **not** gated behind an SDK internal flag. Core matches a
Continue-As-New command to its ``WorkflowExecutionContinuedAsNew`` event by
command type alone -- it never compares the command's headers with the recorded
ones -- so a replay that regenerates the command at this version cannot
disagree with a History written at version 1. The only bytes that reach History
are a live completion's, and a live completion is free to write the current
version. :attr:`Continuation.schema_version` is where such a gate would attach
if that ever stopped being true.
"""

_DECODABLE_SCHEMA_VERSIONS = (1, 2)
"""Every version this Worker can read.

A chain in flight when the Worker fleet was upgraded has a version 1 header
sitting in its successor's ``WorkflowExecutionStarted`` already, and that Run has
to start somewhere other than ``BEGINNING``.
"""


@dataclass(frozen=True)
class Continuation:
    """Where each subscription had got to when the Run continued as new.

    Keyed by ``wait_id`` so two subscriptions to one stream restore
    independently: they are separate waits with their own cursors, and a
    stream-keyed continuation would restart one of them at the other's position.

    The four maps beside ``cursors`` are the **binding the cursor was produced
    under**, and each is there because without it a cursor can be restored into
    something it does not describe:

    - ``stream_names`` so a renumbered subscription is detectable. Restoring a
      cursor onto a differently-numbered wait would resume one stream at
      another's offset, which the backend would accept and no later check would
      catch.
    - ``backend_names`` because two backends can hold entirely different
      records under the same offset syntax. A successor that kept the wait and
      the stream but named another backend would resume in that backend at an
      unrelated boundary and silently skip everything before it.
    - ``provider_ids`` and ``provider_format_versions`` because a Worker can
      keep the backend *name* and map it to a different implementation. Marker
      replay checks both before interpreting recorded offsets; the first live
      read in a successor Run happens before that Run has written a marker that
      could perform the check, so the continuation has to carry them itself.

    Absent or empty entries mean *not recorded* rather than *empty*: that is what
    a version 1 header decodes to, and restoration skips the checks it cannot
    make rather than reporting a mismatch against nothing.
    """

    cursors: dict[int, Cursor]
    stream_names: dict[int, str]
    backend_names: dict[int, str] = field(default_factory=dict)
    provider_ids: dict[int, str] = field(default_factory=dict)
    provider_format_versions: dict[int, int] = field(default_factory=dict)
    schema_version: int = _SCHEMA_VERSION
    """The version :func:`encode_continuation` will write this state at.

    Carried on the value rather than fixed by the encoder so a decoded header
    re-encodes to the bytes it came from, and so a version can be pinned from
    outside if a Run ever has to reproduce what an older Worker wrote.
    """


def encode_continuation(continuation: Continuation) -> bytes:
    version = continuation.schema_version
    if version not in _DECODABLE_SCHEMA_VERSIONS:
        raise ValueError(
            f"cannot encode a Continue-As-New cursor header at schema version {version}"
        )
    out = bytearray()
    _put_uvarint(out, version)
    _put_uvarint(out, len(continuation.cursors))
    # Sorted, so the same state always encodes to the same bytes. The header is
    # re-derived from live state every time the terminal command is built, and
    # bytes that depended on dict insertion order would put a different cursor
    # in front of the successor on a Workflow Task the server retried.
    for wait_id in sorted(continuation.cursors):
        _put_uvarint(out, wait_id)
        _put_cursor(out, continuation.cursors[wait_id])
        _put_str(out, continuation.stream_names.get(wait_id, ""))
        if version >= 2:
            _put_str(out, continuation.backend_names.get(wait_id, ""))
            _put_str(out, continuation.provider_ids.get(wait_id, ""))
            _put_uvarint(out, continuation.provider_format_versions.get(wait_id, 0))
    return bytes(out)


def decode_continuation(raw: bytes) -> Continuation:
    reader = _Reader(raw)
    version = reader.uvarint()
    if version not in _DECODABLE_SCHEMA_VERSIONS:
        understood = ", ".join(str(v) for v in _DECODABLE_SCHEMA_VERSIONS)
        raise AnnotationDecodeError(
            f"the Continue-As-New cursor header is schema version {version}, but "
            f"this Worker understands {understood}. The previous Run of this "
            "chain was executed by a newer SDK."
        )
    cursors: dict[int, Cursor] = {}
    stream_names: dict[int, str] = {}
    backend_names: dict[int, str] = {}
    provider_ids: dict[int, str] = {}
    provider_format_versions: dict[int, int] = {}
    for _ in range(reader.uvarint()):
        wait_id = reader.uvarint()
        cursors[wait_id] = reader.cursor()
        stream_names[wait_id] = reader.string()
        if version >= 2:
            backend_names[wait_id] = reader.string()
            provider_ids[wait_id] = reader.string()
            provider_format_versions[wait_id] = reader.uvarint()
    return Continuation(
        cursors=cursors,
        stream_names=stream_names,
        backend_names=backend_names,
        provider_ids=provider_ids,
        provider_format_versions=provider_format_versions,
        # The version it arrived at, so re-encoding it reproduces its bytes
        # rather than silently upgrading a header this Worker only read.
        schema_version=version,
    )


def write_continuation_header(
    continuation: Continuation,
) -> temporalio.api.common.v1.Payload:
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
