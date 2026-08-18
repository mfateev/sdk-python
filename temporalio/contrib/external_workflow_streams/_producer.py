"""The producer side (P6a binding, P6 append).

The producer handle is a **different type** from the Workflow-side one, and not
the same object passed across a process boundary. A Workflow handle is bound to
the running Workflow's identity and the Worker's backend registry; a producer
handle is constructed explicitly from credentials the producer holds.

Every binding input is explicit, because none of them can be inferred:

- **The Workflow chain key**, including the first execution Run ID.
  ``temporalio.activity.Info`` exposes ``workflow_run_id`` but *not* the first
  execution Run ID, so an Activity cannot derive the key -- the Workflow passes
  it in and the producer verifies it by describing the Workflow before its
  first append. Publishing under an unverified key is a configuration error,
  not a silent no-op.
- **A backend connection**, **a Temporal client** (for the wake Signal), and
  **the same DataConverter** the consuming Workflow uses.
- **A stable producer session ID**, which is what makes append idempotent under
  Activity retry. Activities default it to something derived from the
  Activity's own identity so a retried attempt reuses it. Plain processes must
  supply one: a random default would look like it worked and duplicate every
  record on the first retry.

The **stream name appears exactly once**, in :meth:`ExternalStreamProducer.topic`.
``connect()`` takes the chain key and ``topic(name)`` completes it into the full
stream identity, so one connection serves several topics and no two arguments
can disagree about the name.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Generic

import temporalio.activity
import temporalio.converter
from temporalio.contrib.external_workflow_streams._backend import (
    StreamBackend,
    StreamKey,
)
from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec
from temporalio.contrib.external_workflow_streams._record import (
    Offset,
    RecordKind,
    StreamRecord,
)
from temporalio.contrib.external_workflow_streams._wake import (
    WakeRequest,
    send_wake_signal,
    wake_request_for,
)
from temporalio.types import AnyType

DEFAULT_WAKE_CLAIM_LEASE = timedelta(seconds=30)
"""Long enough to cover a Signal round trip, short enough to recover from a crash.

The lease bounds how long a claim taken by a producer that then died keeps
saying a wake is in flight. It is not what makes the wake safe -- expiry alone
schedules nobody to take over, which is why a producer that loses the claim
signals anyway rather than treating the claim as an acknowledgement.
"""

if TYPE_CHECKING:
    import temporalio.client

__all__ = [
    "ExternalStreamProducer",
    "ExternalStreamProducerTopic",
    "WakeNotAcknowledgedError",
    "WorkflowChainKey",
]


class WakeNotAcknowledgedError(Exception):
    """The record is durable but the wake step did not complete.

    Raised rather than swallowed, because the two halves fail differently and the
    caller can only act on one of them: the append has already succeeded and must
    not be retried, while the wake must be. Retrying the wake with the same
    inputs is safe -- it derives the same request ID and the server deduplicates
    it -- so this is a resumable state, not a lost record.
    """

    def __init__(self, message: str, *, pending: list[WakeRequest]) -> None:
        super().__init__(message)
        self.pending = pending
        """The wakes still owed, ready to be retried verbatim."""
        self.offset: Offset | None = None
        """Where the record landed, when raised from :meth:`publish`.

        Set because the two halves fail differently and the caller can only act
        on one of them: the append succeeded and must not be retried, the wake
        did not and must be.
        """


@dataclass(frozen=True)
class WorkflowChainKey:
    """The Workflow chain a producer publishes to.

    A *chain* key, not a Run key: the stream spans the whole Continue-As-New
    chain, and ``first_execution_run_id`` is what stays stable across it while
    still preventing collisions after Workflow ID reuse.
    """

    namespace: str
    workflow_id: str
    first_execution_run_id: str

    def __post_init__(self) -> None:
        for field_name in ("namespace", "workflow_id", "first_execution_run_id"):
            if not getattr(self, field_name):
                raise ValueError(
                    f"a Workflow chain key needs a non-empty {field_name}; it is "
                    "passed in by the Workflow rather than inferred, because an "
                    "Activity cannot derive the first execution Run ID"
                )

    def stream_key(self, stream_name: str) -> StreamKey:
        return StreamKey(
            namespace=self.namespace,
            workflow_id=self.workflow_id,
            first_execution_run_id=self.first_execution_run_id,
            stream_name=stream_name,
        )


class ChainKeyMismatchError(Exception):
    """The chain key does not describe the Workflow the server knows about.

    Loud on purpose. Publishing under a wrong key writes records into a stream
    no consumer is watching, and the Workflow simply waits out its idle timeout
    with nothing to show for it.
    """


def _default_session_id() -> str:
    """A session ID stable across an Activity's retries, or a clear error.

    Derived from the Activity's identity and deliberately **not** its attempt
    number: the whole point is that attempt 2 reuses attempt 1's key so its
    re-appends are recognised as the same records.
    """
    try:
        info = temporalio.activity.info()
    except RuntimeError:
        raise ValueError(
            "a producer session ID is required and has no default outside an "
            "Activity. It is what makes append idempotent under retry, so a "
            "random default would look correct and duplicate every record the "
            "first time the producer was retried."
        ) from None
    return f"activity:{info.workflow_run_id}:{info.activity_id}"


class ExternalStreamProducer:
    """A bound producer connection. Serves any number of topics."""

    def __init__(
        self,
        *,
        backend: StreamBackend,
        workflow: WorkflowChainKey,
        data_converter: temporalio.converter.DataConverter,
        session_id: str,
        client: temporalio.client.Client | None = None,
    ) -> None:
        self._backend = backend
        self._workflow = workflow
        self._data_converter = data_converter
        self._session_id = session_id
        self._client = client
        self._sequence = 0
        self._wake_counter = 0

    @staticmethod
    async def connect(
        *,
        backend: StreamBackend,
        workflow: WorkflowChainKey,
        client: temporalio.client.Client,
        data_converter: temporalio.converter.DataConverter | None = None,
        session_id: str | None = None,
    ) -> ExternalStreamProducer:
        """Binds a producer, verifying the chain key before it can append.

        Args:
            backend: A provider instance. A plain process constructs one
                directly; there is no Worker registry out here to name.
            workflow: The chain key, passed in by the Workflow.
            client: Used to verify the chain key, and later to send the wake
                Signal. Required -- an unverified binding is a configuration
                error, not a degraded mode.
            data_converter: Must match the consuming Workflow's, including any
                codec. Defaults to the client's.
            session_id: Stable across retries. Defaults inside an Activity to a
                value derived from the Activity's identity; required outside
                one.

        Raises:
            ChainKeyMismatchError: The server's first execution Run ID for this
                Workflow ID is not the one given.
        """
        await _verify_chain_key(client, workflow)
        return ExternalStreamProducer(
            backend=backend,
            workflow=workflow,
            data_converter=data_converter or client.data_converter,
            session_id=session_id or _default_session_id(),
            client=client,
        )

    @property
    def session_id(self) -> str:
        return self._session_id

    @property
    def workflow(self) -> WorkflowChainKey:
        return self._workflow

    def topic(
        self, name: str, *, type: type[AnyType] | None = None
    ) -> ExternalStreamProducerTopic[Any]:
        """A handle for one stream.

        The only place the stream name appears on the producer side.
        """
        if not name:
            raise ValueError("a topic needs a non-empty name")
        return ExternalStreamProducerTopic(
            producer=self,
            stream_key=self._workflow.stream_key(name),
            codec=StreamPayloadCodec(self._data_converter, type),
        )

    def _next_wake_counter(self) -> int:
        """Distinguishes this sender's unparked wakes from each other.

        An unparked wake names generation 0, which carries no attempt identity,
        so without a per-sender counter every unparked wake from one sender would
        derive the same request ID and the server would deduplicate all but the
        first away. Held fixed across *retries* of one attempt -- it advances per
        attempt, not per send.
        """
        self._wake_counter += 1
        return self._wake_counter

    def _next_sequence(self) -> int:
        """The next sequence number in this producer session.

        Per *connection*, not per topic: the pair ``(session_id, sequence)`` is
        the idempotency key, and restarting the count for each topic would make
        two topics' first records collide.
        """
        sequence = self._sequence
        self._sequence += 1
        return sequence


class ExternalStreamProducerTopic(Generic[AnyType]):
    """One stream, from the producer's side.

    Has ``publish`` and ``finish_writing`` and no ``subscribe`` -- the two sides
    are mirror images, not one type used twice.
    """

    def __init__(
        self,
        *,
        producer: ExternalStreamProducer,
        stream_key: StreamKey,
        codec: StreamPayloadCodec[AnyType],
    ) -> None:
        self._producer = producer
        self._stream_key = stream_key
        self._codec = codec

    @property
    def stream_key(self) -> StreamKey:
        return self._stream_key

    async def publish(
        self,
        value: AnyType,
        *,
        wake: bool = True,
        lease: timedelta = DEFAULT_WAKE_CLAIM_LEASE,
    ) -> Offset:
        """Appends one record and completes only once its wake is acknowledged.

        Idempotent under Activity retry through ``(session_id, sequence)``:
        re-appending byte-identical content is a no-op returning the original
        offset, and the same key with *different* bytes is an error rather than
        an overwrite.

        **Returning means the record is durable and a parked Workflow has been
        told about it** -- told by *this* call, which sent the wake itself and
        had it accepted. It never means that some other producer claimed
        responsibility for sending one. That combination is what makes the
        "durable producer" row of the wakeup-durability boundary true. An append
        that lands but is never signalled leaves the Workflow parked on data
        already sitting in the stream, and a ``publish()`` that returned success
        there would report a delivery that never happened.

        The wake happens after the append and never instead of it: only
        successfully appended records may trigger wakeup, since a wake for a
        record that did not land produces a Workflow Task that finds nothing.

        Args:
            wake: Set ``False`` to append without waking, then call :meth:`wake`
                once for the batch. The wake is idempotent either way --
                every producer waking one generation derives the same request ID
                and the server deduplicates -- so this saves round trips rather
                than changing the outcome. The record is durable but **un-signalled** until
                that call completes, which is the un-acknowledged state made
                explicit rather than hidden.

        Raises:
            WakeNotAcknowledgedError: The record is durable; the wake is not.
                ``.offset`` says where the record landed and ``.pending`` carries
                the wakes still owed, so the caller retries the half that failed
                rather than re-appending the half that did not.
        """
        record = StreamRecord(
            kind=RecordKind.DATA,
            payload=await self._codec.encode(value),
            producer_session_id=self._producer.session_id,
            sequence=self._producer._next_sequence(),
        )
        placed = await self._producer._backend.append(self._stream_key, record)
        assert placed.offset is not None
        if wake:
            try:
                await self.wake(lease=lease)
            except WakeNotAcknowledgedError as err:
                # Re-raised carrying the offset: the caller has no other way to
                # learn that the append it must *not* retry did succeed.
                err.offset = placed.offset
                raise
        return placed.offset

    async def wake(
        self,
        *,
        lease: timedelta = DEFAULT_WAKE_CLAIM_LEASE,
    ) -> list[str]:
        """Step 2 and 3 of the send sequence: observe or claim, then Signal.

        Only ever called **after** a successful append -- only successfully
        appended records may trigger wakeup, since a wake for a record that did
        not land produces a Workflow Task that finds nothing.

        For each subscription parked on this stream, the current generation is
        claimed under a renewable lease -- and the Signal is then sent **whether
        or not the claim was granted**.

        Losing the claim means another producer *intends* to send. It is not
        evidence that one did: a lease permits takeover after it expires, but it
        schedules nobody to take over, so a producer that crashed between
        claiming and signalling strands the generation until some later producer
        happens to append again. Staying silent there would leave a parked
        Workflow on a record already sitting in the stream while this call
        reported an acknowledged wake. The only recovery open to the caller was
        to send the wake itself -- exactly what :meth:`retry_wake` does with a
        pending request -- so this sends it now instead of reporting a failure
        whose only fix is the same send.

        The duplicate that costs is the one that creates a second Workflow Task,
        and this is not one: a **parked** wake's request ID is derived from the
        generation and ignores sender identity, so racing producers issue
        byte-identical requests and the server deduplicates them into a single
        wake. A granted claim therefore saves a round trip, not a wakeup. A
        caller appending a batch saves many more of them with ``wake=False``
        followed by one :meth:`wake`.

        A provider that cannot lease declares ``supports_leased_claims = False``
        and always grants, which is now the same behaviour every provider gets.

        Subscriptions with no installed intent get an *unparked* wake rather than
        nothing (ADR-023). The Workflow may be cached with no open Workflow Task,
        or evicted; neither is visible from out here, and staying silent in
        either case loses the record until something else happens to wake the
        Workflow.

        Returns:
            The request ID of each Signal sent -- one per parked subscription,
            or a single unparked wake when nothing on this stream is parked.

        Raises:
            WakeNotAcknowledgedError: A Signal failed. The record is durable;
                the wake is not. ``.pending`` carries the wakes still owed.
        """
        producer = self._producer
        if producer._client is None:
            raise RuntimeError(
                "waking requires a Temporal client, and this producer was built "
                "without one. Use ExternalStreamProducer.connect(), which "
                "requires it."
            )

        backend = producer._backend
        parked = await backend.parked_wait_ids(self._stream_key)
        # An unparked wake still needs a wait id for the envelope; 0 is the
        # "no particular subscription" value, and Python rechecks every active
        # subscription on wakeup regardless of which one the Signal named.
        targets: list[tuple[int, int | None]] = [
            (wait_id, await backend.current_park_generation(self._stream_key, wait_id))
            for wait_id in parked
        ]
        if not targets:
            targets = [(0, None)]

        requests: list[WakeRequest] = []
        for wait_id, generation in targets:
            if generation is not None:
                # Claimed, and then signalled whichever way the claim went. The
                # claim is how a provider learns a wake is in flight and how an
                # abandoned one becomes takeable after its lease, so it is still
                # taken -- but its answer is not an acknowledgement, and the
                # result is deliberately unused. See this method's docstring.
                await backend.claim_park_generation(
                    self._stream_key,
                    wait_id,
                    generation,
                    claimant=producer.session_id,
                    lease=lease,
                )
            requests.append(
                wake_request_for(
                    producer.workflow,
                    stream_name=self._stream_key.stream_name,
                    wait_id=wait_id,
                    park_generation=generation,
                    sender_identity=producer.session_id,
                    wake_counter=(
                        producer._next_wake_counter() if generation is None else 0
                    ),
                )
            )

        sent: list[str] = []
        for index, request in enumerate(requests):
            try:
                sent.append(
                    await send_wake_signal(
                        producer._client,
                        request,
                        producer_session_id=producer.session_id,
                    )
                )
            except Exception as err:
                raise WakeNotAcknowledgedError(
                    f"the record was appended but its wake was not acknowledged: "
                    f"{err}. Retrying with the same request is safe -- it derives "
                    "the same request ID and the server deduplicates it.",
                    pending=requests[index:],
                ) from err
        return sent

    async def retry_wake(self, pending: list[WakeRequest]) -> list[str]:
        """Re-sends the wakes a failed attempt still owed.

        Takes the requests verbatim rather than recomputing them: recomputing
        would draw a fresh wake counter for an unparked wake, derive a different
        request ID, and defeat the deduplication that makes the retry safe.
        """
        producer = self._producer
        assert producer._client is not None
        sent: list[str] = []
        for index, request in enumerate(pending):
            try:
                sent.append(
                    await send_wake_signal(
                        producer._client,
                        request,
                        producer_session_id=producer.session_id,
                    )
                )
            except Exception as err:
                raise WakeNotAcknowledgedError(
                    f"the wake retry did not complete: {err}",
                    pending=pending[index:],
                ) from err
        return sent

    async def finish_writing(
        self,
        *,
        wake: bool = True,
        lease: timedelta = DEFAULT_WAKE_CLAIM_LEASE,
    ) -> Offset:
        """Appends an ordered write fence. **Does not close the stream.**

        The fence means only: every write in *this* producer session preceding
        it has been appended. It asserts nothing about other producers, and a
        later record from one does not violate it -- it simply wakes the
        Workflow and consumption resumes.

        A fence on one stream alone does not park the Workflow Task either; the
        task parks early only when every active subscription is immediately
        parkable.
        """
        fence = StreamRecord(
            kind=RecordKind.WRITE_FENCE,
            payload=b"",
            producer_session_id=self._producer.session_id,
            sequence=self._producer._next_sequence(),
        )
        placed = await self._producer._backend.append(self._stream_key, fence)
        assert placed.offset is not None
        if wake:
            # A fence is the record most likely to find the Workflow parked --
            # it is what a producer appends when it has nothing more to say --
            # so an unsignalled one strands the Workflow for its whole idle
            # timeout at exactly the moment it was waiting to be told.
            try:
                await self.wake(lease=lease)
            except WakeNotAcknowledgedError as err:
                err.offset = placed.offset
                raise
        return placed.offset


async def _verify_chain_key(
    client: temporalio.client.Client, workflow: WorkflowChainKey
) -> None:
    """Describes the Workflow and checks the first execution Run ID matches."""
    if client.namespace != workflow.namespace:
        raise ChainKeyMismatchError(
            f"the client is connected to namespace {client.namespace!r} but the "
            f"chain key names {workflow.namespace!r}"
        )

    description = await client.get_workflow_handle(workflow.workflow_id).describe()
    actual = description.raw_description.workflow_execution_info.first_run_id
    if actual != workflow.first_execution_run_id:
        raise ChainKeyMismatchError(
            f"Workflow {workflow.workflow_id!r} in namespace "
            f"{workflow.namespace!r} has first execution Run ID {actual!r}, not "
            f"{workflow.first_execution_run_id!r}. Publishing under the wrong key "
            "writes records into a stream no consumer is watching, so this is "
            "refused rather than accepted silently."
        )
