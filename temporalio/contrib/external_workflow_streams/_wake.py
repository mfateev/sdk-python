"""The producer wake-signal path (P14).

The server-visible wakeup, used whenever no open Workflow Task can accept local
readiness. It carries no stream payload -- only enough identity for Core to
decide whether it is a wake, a stale claim, or someone else's chain.

Two properties are load-bearing and neither is available from the public Signal
API, which is why this path does not reuse it:

**Codec bypass.** The envelope goes out through a raw ``SignalWorkflowExecution``
built with the protocol's own serialization rather than the user's
``DataConverter`` (ADR-025). Core is the component that must *read* this Signal,
and Core has no access to a user codec -- a codec that encrypts payloads would
make the envelope unreadable to the only reader that matters. Nothing user-owned
is in it, so bypassing the codec leaks nothing.

**Stable request ID.** The Temporal ``request_id`` is derived deterministically
from the wake's identity, so a producer retrying after an ambiguous failure sends
the identical request and the server deduplicates it. The public Signal path
generates a fresh UUID per attempt, which would turn every retry into a second
wake.
"""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

import temporalio.api.common.v1
import temporalio.api.workflowservice.v1
from temporalio.bridge.proto.external_stream.external_stream_pb2 import WakeSignal

if TYPE_CHECKING:
    import temporalio.client
    from temporalio.contrib.external_workflow_streams._producer import WorkflowChainKey

__all__ = [
    "WakeRequest",
    "new_sender_identity",
    "send_wake_signal",
    "wake_request_id",
]

WAKE_SIGNAL_NAME = "__temporal_external_stream_wake"
"""Fixed, and versioned by the envelope rather than by the name.

Distinct from every ``__temporal_workflow_stream_*`` name already reserved by
``temporalio.contrib.workflow_streams`` (ADR-001), which is a different feature
that coexists with this one.
"""

WAKE_SIGNAL_MESSAGE_TYPE = "coresdk.external_stream.WakeSignal"
WAKE_SIGNAL_ENCODING = b"binary/protobuf"
WAKE_SIGNAL_ENVELOPE_VERSION = 1

UNPARKED_WAKE_GENERATION = 0
"""No confirmed park is known; ask for a Workflow Task anyway (ADR-023).

Not silence. The runtime rechecks every active subscription on wakeup regardless
of which stream the Signal named, so an unnecessary unparked wake costs at most
one empty Workflow Task -- while staying silent because no park was observed
would lose the record until something else happened to wake the Workflow.
"""

_REQUEST_ID_NAMESPACE = uuid.UUID("6f1f8a1e-6f26-5f3f-9f2b-4b0a2c7d8e10")
"""A fixed namespace, so the derivation is stable across processes and versions.

Deriving a UUID rather than passing the tuple through keeps the request ID
inside the length the server accepts while staying a pure function of its
inputs.
"""


def new_sender_identity(client_identity: str = "") -> str:
    """One sender instance's identity for unparked wakes.

    Unique per *sender*, not per client identity. Two Workers in one process
    share a ``Client`` and therefore one client identity, so deriving from that
    alone gives their first unparked wakes byte-identical request IDs -- the
    server deduplicates the second, no Workflow Task is created, and the Run
    stalls. That is precisely the loss the sender identity exists to prevent.

    Drawn once and held for the sender's lifetime, which is what "fixed across
    retries of that one attempt" requires: a value redrawn per attempt would
    turn the shutdown sweep's in-grace retry into a second wake.

    Random, which is safe here: the wake Signal is sent from the Worker's own
    event loop and never from Workflow code, so it is not replay-visible and
    cannot affect determinism. The client identity is kept as a prefix so a
    request ID stays traceable to a Worker in server-side logs.
    """
    return f"{client_identity}#{uuid.uuid4()}"


@dataclass(frozen=True)
class WakeRequest:
    """One wake attempt's complete identity.

    Everything the request ID is derived from lives here, so "the same attempt"
    is a property of a value rather than of how carefully a caller re-passed six
    arguments.
    """

    namespace: str
    workflow_id: str
    first_execution_run_id: str
    stream_name: str
    wait_id: int
    park_generation: int
    sender_identity: str = ""
    wake_counter: int = 0

    @property
    def is_unparked(self) -> bool:
        """Whether this wake requests a task outside a confirmed park."""
        return self.park_generation == UNPARKED_WAKE_GENERATION

    def __post_init__(self) -> None:
        """Validate the identity required for server-side deduplication."""
        if self.is_unparked and not self.sender_identity:
            raise ValueError(
                "an unparked wake needs a sender identity. Generation 0 carries "
                "no attempt identity of its own, so without it two Workers "
                "shutting down at different times would derive the same request "
                "ID and the server would deduplicate the second wake away -- "
                "turning a correct retry mechanism into silent loss."
            )


def wake_request_id(request: WakeRequest) -> str:
    """The deterministic ``request_id`` for one wake attempt.

    A **parked** wake is identified by its generation: a generation is woken
    once, so every producer that races to wake it derives the same ID and the
    server keeps one.

    An **unparked** wake has no generation to identify it, so the derivation
    additionally includes the sender's identity -- unique per sender *instance*,
    see :py:func:`new_sender_identity` -- and a per-sender monotonic counter,
    both held fixed across retries of that one attempt. Two senders' unparked
    wakes must stay distinct even though neither knows about the other.
    """
    parts = [
        request.namespace,
        request.workflow_id,
        request.first_execution_run_id,
        request.stream_name,
        str(request.wait_id),
        str(request.park_generation),
    ]
    if request.is_unparked:
        parts += [request.sender_identity, str(request.wake_counter)]
    # Length-prefixed rather than delimiter-joined: a stream literally named
    # "a\x00b" must not collide with the pair ("a", "b").
    material = b"".join(
        len(encoded).to_bytes(4, "big") + encoded
        for encoded in (part.encode("utf-8") for part in parts)
    )
    return str(uuid.uuid5(_REQUEST_ID_NAMESPACE, hashlib.sha256(material).hexdigest()))


def wake_envelope(request: WakeRequest, *, producer_session_id: str = "") -> bytes:
    """The Signal's single argument, serialized at the protocol level."""
    return WakeSignal(
        envelope_version=WAKE_SIGNAL_ENVELOPE_VERSION,
        stream_name=request.stream_name,
        wait_id=request.wait_id,
        park_generation=request.park_generation,
        first_execution_run_id=request.first_execution_run_id,
        producer_session_id=producer_session_id,
    ).SerializeToString()


def build_signal_request(
    request: WakeRequest,
    *,
    identity: str,
    producer_session_id: str = "",
) -> temporalio.api.workflowservice.v1.SignalWorkflowExecutionRequest:
    """The raw request, built without touching the user's ``DataConverter``.

    Addressed to the Workflow ID with **no Run ID**, so it always lands on the
    current Run of the chain -- a wake sent while a Continue-As-New is in flight
    must reach the successor, not fail against a Run that has already closed.
    ``first_execution_run_id`` inside the envelope is what lets Core tell the
    chain apart from a reused Workflow ID.
    """
    return temporalio.api.workflowservice.v1.SignalWorkflowExecutionRequest(
        namespace=request.namespace,
        workflow_execution=temporalio.api.common.v1.WorkflowExecution(
            workflow_id=request.workflow_id
        ),
        signal_name=WAKE_SIGNAL_NAME,
        input=temporalio.api.common.v1.Payloads(
            payloads=[
                temporalio.api.common.v1.Payload(
                    metadata={
                        "encoding": WAKE_SIGNAL_ENCODING,
                        "messageType": WAKE_SIGNAL_MESSAGE_TYPE.encode(),
                    },
                    data=wake_envelope(
                        request, producer_session_id=producer_session_id
                    ),
                )
            ]
        ),
        identity=identity,
        request_id=wake_request_id(request),
    )


async def send_wake_signal(
    client: temporalio.client.Client,
    request: WakeRequest,
    *,
    producer_session_id: str = "",
) -> str:
    """Sends one wake and returns the request ID it was sent under.

    Retrying with the same :class:`WakeRequest` is safe and is the intended
    recovery from an ambiguous failure: the server deduplicates on the request
    ID, so the second attempt resolves the first rather than waking twice.
    """
    signal_request = build_signal_request(
        request,
        identity=client.service_client.config.identity,
        producer_session_id=producer_session_id,
    )
    await client.workflow_service.signal_workflow_execution(signal_request)
    return signal_request.request_id


def wake_request_for(
    workflow: WorkflowChainKey,
    *,
    stream_name: str,
    wait_id: int,
    park_generation: int | None,
    sender_identity: str,
    wake_counter: int = 0,
) -> WakeRequest:
    """Builds the request from a chain key and an observed generation.

    ``park_generation=None`` -- no confirmed park for this subscription --
    becomes an unparked wake rather than no wake at all.
    """
    return WakeRequest(
        namespace=workflow.namespace,
        workflow_id=workflow.workflow_id,
        first_execution_run_id=workflow.first_execution_run_id,
        stream_name=stream_name,
        wait_id=wait_id,
        park_generation=(
            UNPARKED_WAKE_GENERATION if park_generation is None else park_generation
        ),
        sender_identity=sender_identity,
        wake_counter=wake_counter,
    )
