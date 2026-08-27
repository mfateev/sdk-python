"""The producer side (P6a binding, P6 append).

The producer handle is a **different type** from the Workflow-side one, and not
the same object passed across a process boundary. A Workflow handle is bound to
the running Workflow's identity and backend configuration; a producer
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

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Generic

import temporalio.activity
import temporalio.converter
from temporalio.contrib.external_workflow_streams._backend import (
    AppendConflictError,
    StreamBackend,
    StreamDirection,
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
    "AppendNotAcknowledgedError",
    "ChainKeyMismatchError",
    "ExternalStreamProducer",
    "ExternalStreamProducerTopic",
    "PrecedingWriteFailedError",
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

    **Cancellation after the append arrives this way too**, with
    :attr:`cancelled` set. It leaves the identical state -- a durable record and
    an unsent wake -- and a bare ``CancelledError`` there would be
    indistinguishable from cancellation *before* the append: the caller would not
    know whether the value still had to be published, and its obvious move,
    calling ``publish()`` again, draws a fresh sequence number and appends a
    second record (ADR-036).
    """

    def __init__(
        self,
        message: str,
        *,
        pending: list[WakeRequest],
        restart: bool = False,
        cancelled: bool = False,
    ) -> None:
        """Capture the wakes still owed after a durable append."""
        super().__init__(message)
        self.pending = pending
        """The wakes still owed, ready to be retried verbatim."""
        self.cancelled = cancelled
        """Whether what ended the wake was cancellation rather than a failure.

        Set because the two are the same *state* -- durable record, unsent wake,
        one recovery -- and a caller that wants to honour the cancellation after
        recovering the wake still has to know it was asked to stop. The recovery
        itself does not depend on this: ``pending`` and ``restart`` say what to do
        whatever ended the attempt (ADR-036).
        """
        self.restart = restart
        """Whether the caller must call :meth:`.wake` again instead of retrying.

        The wake is three steps -- observe the parked set, claim the generation,
        Signal -- and only the third produces the requests
        :meth:`ProducerTopicHandle.retry_wake` re-sends. A failure in the first
        two leaves nothing to re-send, so ``pending`` is empty and retrying it
        would silently do nothing at all: the record would stay durable and
        unannounced while the caller believed it had recovered.

        ``True`` therefore says "no wake was composed; compose one". Calling
        :meth:`ProducerTopicHandle.wake` again is safe and is the whole recovery
        -- it re-observes the parked set, and a parked wake's request ID is
        derived from the generation rather than from the sender, so a wake some
        other producer already sent deduplicates against it.
        """
        self.offset: Offset | None = None
        """Where the record landed, when raised from :meth:`publish`.

        Set because the two halves fail differently and the caller can only act
        on one of them: the append succeeded and must not be retried, the wake
        did not and must be.
        """


class AppendNotAcknowledgedError(Exception):
    """The append's outcome is unknown: the record may or may not be durable.

    Raised when :meth:`StreamBackend.append` neither returns nor *refuses* -- a
    cancellation delivered while it was in flight, a dropped connection, a
    provider exception that says nothing about what the server did with the
    write. A remote backend commits on its own side and only then answers, so an
    append that did not answer is not an append that did not happen: the Redis
    provider runs an atomic script server-side and receives its result in a
    separate client-side step, and losing the client between the two leaves a
    durable record whose offset nobody holds.

    Reading that window as failure breaks the producer's contract in both
    directions at once, which is why it gets a type of its own. Retrying
    ``publish()`` draws a **new** sequence number, and therefore a new
    idempotency key, so a record that did land is appended a second time. Not
    retrying can leave a durable record that no wake was ever sent for, which
    strands a parked Workflow for its whole idle timeout. A bare
    ``CancelledError`` or a bare ``ConnectionError`` supports neither choice,
    because neither carries the identity of the record whose fate is in
    question.

    So the record is carried out instead -- byte-identical, still holding its
    ``(session_id, sequence)`` -- and
    :meth:`ExternalStreamProducerTopic.resolve_append` re-appends *that* record.
    Under the backend contract that one call is correct whichever way the first
    attempt went: a repeat append of byte-identical content under a used key
    writes nothing and returns the original offset, and a key the backend never
    saw is appended now (ADR-038).

    Until it is resolved, the stream refuses further appends from this producer
    with this same error, since that is the caller move that duplicates the
    record.

    Distinct from :class:`WakeNotAcknowledgedError`, which is the *next* window
    along: there the offset is known and only the wake is owed. Resolving this
    one produces that offset, after which a failing wake raises the wake error in
    the ordinary way.
    """

    def __init__(
        self,
        message: str,
        *,
        stream_key: StreamKey,
        record: StreamRecord,
        wake: bool,
        lease: timedelta,
        cancelled: bool = False,
    ) -> None:
        """Capture an append whose durable outcome must be resolved."""
        super().__init__(message)
        self.stream_key = stream_key
        """The stream the append was for. Where it must be settled.

        Carried because a record does not name its own stream and the backend's
        idempotency scope does: `(session_id, sequence)` is unused on every
        *other* stream, so the same record handed to another topic's
        ``resolve_append`` would append a second copy there rather than
        deduplicate. The recovery refuses that, and this is what a caller
        holding several topics matches on.
        """
        self.record = record
        """The exact record whose fate is unknown. What ``resolve_append`` takes.

        Carried rather than described, because idempotency is on identity: a
        payload the caller re-encodes into different bytes under the same key is
        an ``AppendConflictError`` rather than the no-op the recovery depends on.
        """
        self.wake = wake
        """Whether the interrupted call was going to wake, so recovery can too."""
        self.lease = lease
        """The claim lease the interrupted call was going to use."""
        self.cancelled = cancelled
        """Whether cancellation ended any attempt to settle this operation.

        Reported rather than propagated, for the reason ADR-036 already gives
        about the wake: a caller that wants to honour the cancellation re-raises
        *after* resolving the append, and that is the only order that leaves
        nothing owed. Sticky across recovery attempts: a later transport failure
        must not erase a cancellation the caller still has to honour.
        """


class PrecedingWriteFailedError(Exception):
    """A write fence was not appended, because an earlier write was not either.

    Raised from :meth:`ExternalStreamProducerTopic.finish_writing` when a
    ``publish()`` invoked earlier on the same stream ended without a durable
    record. The fence means every write in this producer session preceding it
    has been appended, and a consumer that drains through one may park on that
    assertion, so a fence written over a hole would tell it the batch is
    complete when it is short a record.

    Reachable only from *concurrent* calls: a publish that failed before the
    fence was invoked is already finished and the fence covers what it says it
    covers. Nothing was appended for this fence, so the recovery is the caller's
    choice between the two halves -- publish the failed value again, which draws
    a new sequence number and lands ahead of a later fence, or accept the batch
    without it. Either way ``finish_writing()`` again appends the fence: the
    failed write is no longer outstanding.

    Distinct from :class:`AppendNotAcknowledgedError`, which the fence raises
    instead when the earlier append's outcome is *unknown* rather than failed.
    There the record may well be durable, the stream refuses appends until it is
    settled, and the recovery is that operation's rather than this call's.
    """

    def __init__(self, message: str, *, stream_key: StreamKey, sequence: int) -> None:
        """Identify the earlier failed write that makes a fence unsafe."""
        super().__init__(message)
        self.stream_key = stream_key
        """The stream whose fence was refused."""
        self.sequence = sequence
        """The sequence number of the earlier write that did not land.

        Names the operation rather than describing it: the failure itself is the
        ``__cause__``, and the caller already saw it raised from its own call.
        """


@dataclass(frozen=True)
class _UnresolvedAppend:
    """One append whose outcome the producer never learned.

    The *operation*, not just its record. What the interrupted call owed is more
    than the bytes: whether a wake was due, under which lease, and whether
    cancellation is still to be honoured once the state is settled. Holding only
    the record meant a later refusal had to invent those three from whatever call
    happened to be refused, so a caller following the refusal's own instructions
    could drop a wake the unresolved record required.

    Keyed by stream because idempotency is scoped per stream: the same
    ``(session_id, sequence)`` is unused on every other one, so a record settled
    against the wrong topic appends a second copy rather than deduplicating.
    """

    stream_key: StreamKey
    record: StreamRecord
    wake: bool
    lease: timedelta
    cancelled: bool
    operation: _StreamOperation | None = None
    """The append order entry whose outcome this recovery decides, if any.

    A fence has none: nothing waits behind one. A publish has exactly one, and
    it is the *only* place a resolution can be reported back to a fence that
    already captured that operation while its outcome was unknown -- see
    :meth:`ExternalStreamProducerTopic.resolve_append`. Without it, "the record
    is no longer unresolved" is all a waiting fence can see, and both a durable
    resolution and an ``AppendConflictError`` produce that.
    """

    def error(self, message: str) -> AppendNotAcknowledgedError:
        """Reports this operation's current canonical recovery."""
        return AppendNotAcknowledgedError(
            message,
            stream_key=self.stream_key,
            record=self.record,
            wake=self.wake,
            lease=self.lease,
            cancelled=self.cancelled,
        )


@dataclass(eq=False)
class _StreamOperation:
    """One **publish** on a stream, from the moment it draws its sequence.

    The entry in the producer's per-stream append order. It exists so that
    :meth:`ExternalStreamProducerTopic.finish_writing` can tell which earlier
    calls on that stream have not reached the backend yet -- which nothing else
    records. ``publish()`` draws its sequence *before* awaiting the codec, so a
    publish that is still encoding is invisible to :meth:`_refuse_while_unresolved`
    (it has no unresolved append) and to the backend (it has appended nothing).

    **A fence is not one of these.** The order exists to hold a fence behind the
    *data writes* that precede it, which is the whole of what a fence asserts;
    two fences make independent assertions about the publishes each of them came
    after, so neither has to wait for the other. Putting them in the same ledger
    made a fence that never reached the backend -- cancelled while waiting, or
    refused -- look to a later fence like a data write that went missing, and a
    valid fence was then refused with ``PrecedingWriteFailedError``.

    Compared by identity rather than by field, because two operations may hold
    the same values and the order removes exactly the one that settled.
    """

    sequence: int

    settled: asyncio.Event = field(default_factory=asyncio.Event)
    """Set once the append has an outcome -- durable, refused, or unknown."""

    failure: BaseException | None = None
    """Why no durable record came of it, or ``None`` if one did.

    Read only after :attr:`settled`, and only by a fence waiting behind this
    operation. The caller of the failed call has already been raised at.

    Not final while it holds an :class:`AppendNotAcknowledgedError`: that is the
    one outcome the producer does not yet know, and
    :meth:`ExternalStreamProducerTopic.resolve_append` replaces it with the
    durable or refused answer recovery learned.
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
        """Require every component of the Workflow chain identity."""
        for field_name in ("namespace", "workflow_id", "first_execution_run_id"):
            if not getattr(self, field_name):
                raise ValueError(
                    f"a Workflow chain key needs a non-empty {field_name}; it is "
                    "passed in by the Workflow rather than inferred, because an "
                    "Activity cannot derive the first execution Run ID"
                )

    def stream_key(
        self,
        stream_name: str,
        *,
        direction: StreamDirection = StreamDirection.INPUT,
    ) -> StreamKey:
        """Return the durable key for one stream in this Workflow chain."""
        return StreamKey(
            namespace=self.namespace,
            workflow_id=self.workflow_id,
            first_execution_run_id=self.first_execution_run_id,
            stream_name=stream_name,
            direction=direction,
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
        """Bind a backend, converter, and producer session to one chain."""
        self._backend = backend
        self._workflow = workflow
        #: Bound to the Workflow these records are for, which is the same
        #: context the consuming Worker decodes them under. The two sides share
        #: one converter and must therefore share one context: a producer that
        #: encrypts under no context while the consumer decrypts under a
        #: Workflow-derived key is a mismatch that the append reports as
        #: success and that only surfaces on the far side, as a decode failure
        #: against a configuration that is in fact correct.
        #:
        #: A producer is bound to exactly one chain, so this is settled once
        #: here rather than per topic. `workflow_id` and not a Run: the chain
        #: key spans Continue-As-New, and a record written for one Run is read
        #: by its successors.
        self._data_converter = data_converter.with_context(
            temporalio.converter.WorkflowSerializationContext(
                namespace=workflow.namespace,
                workflow_id=workflow.workflow_id,
            )
        )
        self._session_id = session_id
        self._client = client
        self._sequence = 0
        self._wake_counter = 0
        #: Per stream, the appends that never reported an outcome.
        #:
        #: Kept because the operation *is* the recovery: only these exact bytes
        #: under these exact `(session_id, sequence)` pairs re-append as a no-op
        #: if the first attempt landed, and only what the interrupted call owed
        #: says whether settling it still has a wake to send. A list rather than
        #: a single slot because concurrent publishes to one stream are supported
        #: and more than one of them can be interrupted; entries are added only
        #: when an append fails to answer, so a publish that is merely still in
        #: flight registers nothing and cannot block its own sibling.
        #:
        #: **Per producer instance, deliberately.** This is what binds recovery
        #: to the object that still holds the session's sequence and wake
        #: counters; a replacement producer built with the same session id has
        #: both back at zero and recovers by re-running the same calls instead
        #: (ADR-038).
        self._unresolved: dict[StreamKey, list[_UnresolvedAppend]] = {}
        #: Per stream, the operations that have drawn a sequence number and not
        #: yet reached an append outcome. The ordered append coordinator a write
        #: fence needs.
        #:
        #: A fence asserts that every preceding write in this session has been
        #: appended, and nothing made that true: an earlier `publish()` could
        #: still be inside its codec -- holding a lower sequence number, having
        #: appended nothing, and unresolved nowhere -- while `finish_writing()`
        #: appended the fence and sent the batch's only wake. A consumer then
        #: parks on a fence whose data lands behind it, with the one wake that
        #: would have moved it already spent.
        #:
        #: Keyed by stream because that is the scope of the fence's claim, and
        #: held on the producer rather than the handle because `topic()` returns
        #: a fresh handle per call: two handles for one name are one stream and
        #: must join one order. Only the fence waits -- concurrent publishes have
        #: no defined order between them to preserve, so ordering their appends
        #: would serialize a batch for nothing.
        self._appends: dict[StreamKey, list[_StreamOperation]] = {}

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
        """The idempotency session shared by this producer's appends."""
        return self._session_id

    @property
    def workflow(self) -> WorkflowChainKey:
        """The Workflow chain receiving this producer's records."""
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
        """Bind a typed codec and stream key to a producer."""
        self._producer = producer
        self._stream_key = stream_key
        self._codec = codec

    @property
    def stream_key(self) -> StreamKey:
        """The durable identity of this producer topic."""
        return self._stream_key

    def _refuse_while_unresolved(self) -> None:
        """Refuses a new append while an earlier one's outcome is unknown.

        This is the caller move that duplicates a record, so it is the one the
        producer stops. A new ``publish()`` draws a fresh sequence number and
        therefore a fresh idempotency key, so if the unresolved append did land,
        the value is now in the stream twice under two keys the backend has no
        way to relate. Refusing also keeps the recovery in order: the unresolved
        record is re-appended before anything is appended behind it.

        The error it raises is **the outstanding operation's**, not this call's.
        Reporting the refused call's ``wake`` and ``lease`` beside the older
        call's record would make the error's own instructions wrong: a caller
        following them settles the record with no wake, and a parked Workflow
        sits on a durable record nobody announced.

        Checked at entry, so a publish already past this point when a sibling
        becomes unresolved still completes -- concurrent publishes have no
        defined order between them to preserve.
        """
        outstanding = self._producer._unresolved.get(self._stream_key)
        if not outstanding:
            return
        pending = outstanding[0]
        more = (
            ""
            if len(outstanding) == 1
            else f" ({len(outstanding)} appends on this stream are unsettled)"
        )
        raise pending.error(
            f"the append of {pending.record.idempotency_key} on stream "
            f"{self._stream_key} has never been acknowledged, so this stream "
            "will not take another append: a new one draws a fresh sequence "
            "number, and if that record did land the value would be in the "
            f"stream twice{more}. Settle it with resolve_append(`.record`), "
            "which owes the wake this error reports rather than the one the "
            "refused call asked for."
        )

    def _begin(self, sequence: int) -> _StreamOperation:
        """Joins this publish to its stream's append order.

        Synchronous, and called in the same uninterrupted step as
        :meth:`_refuse_while_unresolved` and the sequence draw. An operation
        registered after its first await would be invisible to a fence that
        began in between, which is the entire race this closes.
        """
        outstanding = self._producer._appends.setdefault(self._stream_key, [])
        operation = _StreamOperation(sequence=sequence)
        outstanding.append(operation)
        return operation

    def _preceding_publishes(self) -> tuple[_StreamOperation, ...]:
        """The publishes a fence invoked now has to wait behind.

        Snapshotted at invocation rather than re-read at wait time, because a
        fence's claim is about the calls that came *before* it. A publish invoked
        behind the fence is outside that claim -- it takes its place in the
        stream after the fence, exactly as a later publish does -- and a fence
        that waited for one would be held by work the caller started after
        asking for the fence.

        Synchronous, and taken in the same uninterrupted step as the sequence
        draw, for the reason :meth:`_begin` gives. A fence does not join the
        order it reads: see :class:`_StreamOperation`.
        """
        return tuple(self._producer._appends.get(self._stream_key, ()))

    def _settle(
        self, operation: _StreamOperation, failure: BaseException | None = None
    ) -> None:
        """Reports this call's append outcome and releases anything behind it.

        Called once the append has an outcome and **not** after the wake. The
        fence asserts that preceding writes are *appended*, which a durable
        record with a still-owed Signal already satisfies; holding the fence for
        the wake as well would stall it on a state its own claim says nothing
        about, and on the failure of one leave a caller unable to fence at all.
        """
        operation.failure = failure
        operation.settled.set()
        outstanding = self._producer._appends.get(self._stream_key)
        if outstanding is None:
            return
        outstanding[:] = [held for held in outstanding if held is not operation]
        if not outstanding:
            del self._producer._appends[self._stream_key]

    async def _await_preceding_appends(
        self, preceding: tuple[_StreamOperation, ...]
    ) -> None:
        """Holds a fence until every earlier publish on this stream has landed.

        What makes :meth:`finish_writing`'s claim true rather than merely
        documented. Each earlier operation is waited on rather than polled, and
        it settles as soon as its append returns, so a fence with nothing
        outstanding ahead of it does not yield at all.

        Every outcome is read *after* the last wait rather than as each one
        settles, because an outcome read earlier can still change: an append
        whose answer was lost settles as unknown, and the
        :meth:`resolve_append` that turns that into a durable record or an
        ``AppendConflictError`` can happen while this fence is still waiting on a
        later publish. Reading in the loop took the stale unknown and passed over
        a write that recovery had by then proved absent.

        Then, in order:

        - An append still *unresolved* refuses the fence with that operation's
          canonical error -- the same refusal :meth:`_refuse_while_unresolved`
          gives at entry, because the stream takes no further append until it is
          settled (ADR-038). Reported ahead of any outright failure, since it is
          the one that blocks the recovery for the other: republishing a failed
          value is itself refused while an append is unsettled.
        - An earlier *failure* is propagated as
          :class:`PrecedingWriteFailedError` instead of being passed over: the
          caller asked for that write before it asked for the fence, and a fence
          appended over the hole tells a consumer the batch is complete when it
          is short a record.
        """
        for earlier in preceding:
            await earlier.settled.wait()
        # Re-checked here and not only at entry: an append can lose its answer at
        # any point while this fence is held, including one from a publish that
        # began behind it, and appending the fence in front of a record that may
        # yet be settled into the stream is the ordering this method exists to
        # refuse. No await separates it from the scan below, so neither reads a
        # state the other has already moved past.
        self._refuse_while_unresolved()
        for earlier in preceding:
            failure = earlier.failure
            if failure is None:
                continue
            raise PrecedingWriteFailedError(
                f"the write at sequence {earlier.sequence} on stream "
                f"{self._stream_key} did not produce a durable record, so this "
                "fence was not appended: a fence means every preceding write in "
                "this producer session has been appended, and a consumer that "
                "drains through one may park on that. Publish the value again "
                "-- it draws a new sequence number and lands ahead of a later "
                "fence -- or accept the batch without it; either way "
                "finish_writing() again appends the fence, since the failed "
                "write is no longer outstanding.",
                stream_key=self._stream_key,
                sequence=earlier.sequence,
            ) from failure

    def _remember(self, pending: _UnresolvedAppend) -> _UnresolvedAppend:
        """Makes ``pending`` the canonical state for this unsettled operation.

        A recovery may deliberately change the wake or lease. If its own append
        then loses the response, the *recovery* is now the interrupted operation,
        so retaining the older policy makes the next defaulted attempt contradict
        the error it was handed. Cancellation is different: once delivered it is
        still owed after every later failure, so it accumulates rather than being
        replaced. The append order entry is the *first* attempt's, for the same
        reason: it is the publish a fence captured, and no recovery of it creates
        another.
        """
        outstanding = self._producer._unresolved.setdefault(self._stream_key, [])
        for index, held in enumerate(outstanding):
            if held.record != pending.record:
                continue
            pending = _UnresolvedAppend(
                stream_key=pending.stream_key,
                record=pending.record,
                wake=pending.wake,
                lease=pending.lease,
                cancelled=held.cancelled or pending.cancelled,
                operation=held.operation,
            )
            outstanding[index] = pending
            return pending
        outstanding.append(pending)
        return pending

    def _forget(self, record: StreamRecord) -> None:
        """Drops a settled append from the unresolved set.

        Matched on the idempotency key, where :meth:`_remember` and
        :meth:`_outstanding` match on the whole record. Not a discrepancy that
        is reachable: a record with a stored key but different bytes cannot be
        in the set, because `_outstanding` refuses it with a ``ValueError``
        before the backend is touched and `publish` always draws an unused key.
        If either of those ever stops holding, this drops both entries for the
        key while the other two treat them as distinct, and one unsettled append
        disappears without being settled.
        """
        outstanding = self._producer._unresolved.get(self._stream_key)
        if not outstanding:
            return
        remaining = [
            held
            for held in outstanding
            if held.record.idempotency_key != record.idempotency_key
        ]
        if remaining:
            self._producer._unresolved[self._stream_key] = remaining
        else:
            del self._producer._unresolved[self._stream_key]

    async def _append(
        self,
        record: StreamRecord,
        *,
        wake: bool,
        lease: timedelta,
        operation: _StreamOperation | None,
    ) -> StreamRecord:
        """The append, with its acknowledgement window made explicit.

        Everything that is neither a return nor a contractual refusal leaves as
        :class:`AppendNotAcknowledgedError`, because from here the two are
        genuinely indistinguishable: the backend may have committed and lost the
        answer. ``AppendConflictError`` is the exception, and the only one the
        contract defines -- the key was used with *different* bytes, so this
        record did not land and re-appending it would raise the identical error.

        ``KeyboardInterrupt`` and ``SystemExit`` are not converted either, for
        the reason ADR-036 gives: the interpreter is going away and there is no
        caller left to recover.

        ``operation`` is the append order entry this record belongs to, carried
        onto the unresolved state so a recovery of it can report the outcome it
        learns back to a fence holding that entry. ``None`` for a fence's own
        append, which nothing waits behind.
        """
        producer = self._producer
        try:
            placed = await producer._backend.append(self._stream_key, record)
        except AppendConflictError:
            self._forget(record)
            raise
        except (Exception, asyncio.CancelledError) as err:
            pending = _UnresolvedAppend(
                stream_key=self._stream_key,
                record=record,
                wake=wake,
                lease=lease,
                cancelled=isinstance(err, asyncio.CancelledError),
                operation=operation,
            )
            pending = self._remember(pending)
            raise pending.error(
                f"the append of {record.idempotency_key} did not report an "
                f"outcome: {err!r}. Whether it landed is unknown -- a backend "
                "commits before it answers -- so settle it with "
                "resolve_append(`.record`) rather than by publishing again."
            ) from err
        assert placed.offset is not None
        self._forget(record)
        return placed

    async def _wake_for(
        self, placed: StreamRecord, *, wake: bool, lease: timedelta
    ) -> Offset:
        """Steps 2 and 3 for a record now known to be durable."""
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

    def _outstanding(self, record: StreamRecord) -> _UnresolvedAppend:
        """This topic's unsettled append for ``record``, or why there is none.

        The lookup **is** the safety check, and it is three checks at once. It
        binds the recovery to the stream, because a record does not name its own
        stream and `(session_id, sequence)` is unused on every other one -- so a
        record settled against the wrong topic appends a second copy of the value
        rather than deduplicating, and leaves the real stream still blocked. It
        binds the recovery to the exact bytes, because idempotency is on identity
        and a re-encoded payload under the same key is an ``AppendConflictError``
        rather than the no-op this depends on. And it binds the recovery to the
        producer *instance*, which is the one that still holds the session's
        sequence and wake counters.

        Every failure is a ``ValueError`` naming which of the three it was, and
        every one of them is raised before the backend is touched.
        """
        if record.offset is not None:
            raise ValueError(
                f"record {record.idempotency_key} already carries offset "
                f"{record.offset}, which means the backend acknowledged it. "
                "There is nothing unresolved about it; if a wake is still owed, "
                "that is what wake() and retry_wake() are for."
            )
        producer = self._producer
        for held in producer._unresolved.get(self._stream_key, ()):
            if held.record == record:
                return held

        if record.producer_session_id != producer.session_id:
            raise ValueError(
                f"record {record.idempotency_key} was written by producer "
                f"session {record.producer_session_id!r}, not by this one "
                f"({producer.session_id!r}). Only the session that drew the "
                "sequence number can re-append under that key; from any other "
                "session the same bytes are a different record."
            )
        elsewhere = [
            key
            for key, outstanding in producer._unresolved.items()
            if any(held.record == record for held in outstanding)
        ]
        if elsewhere:
            raise ValueError(
                f"record {record.idempotency_key} has an unsettled append on "
                f"stream {elsewhere[0]}, not on {self._stream_key}. Append "
                "idempotency is scoped to the stream, so settling it here would "
                "not deduplicate against the copy that may already be on the "
                "other stream -- it would append the value a second time, on a "
                "topic no consumer of it is watching, and leave the first "
                "stream blocked. Settle it on the topic the error names."
            )
        same_key = [
            held
            for held in producer._unresolved.get(self._stream_key, ())
            if held.record.idempotency_key == record.idempotency_key
        ]
        if same_key:
            raise ValueError(
                f"the unsettled append under {record.idempotency_key} does not "
                "hold these bytes. Idempotency is on identity, so re-appending "
                "different content under that key is an AppendConflictError "
                "rather than the no-op the recovery depends on. Pass the "
                "`.record` the error carried, unmodified."
            )
        raise ValueError(
            f"this producer has no unsettled append under "
            f"{record.idempotency_key} on stream {self._stream_key}, so there "
            "is nothing here to settle. If the producer that made it is gone, "
            "the recovery is not this call: rebuild the producer with the same "
            "session id and re-run the same calls in the same order. That "
            "re-derives the same sequence numbers and re-appends the same "
            "bytes, which the backend deduplicates -- and unlike this call it "
            "leaves the new session's own counters correct, where settling here "
            "would let its next publish reuse a sequence number and its next "
            "unparked wake reuse a request ID (ADR-038)."
        )

    async def resolve_append(
        self,
        record: StreamRecord,
        *,
        wake: bool | None = None,
        lease: timedelta | None = None,
    ) -> Offset:
        """Settles an append :class:`AppendNotAcknowledgedError` left unknown.

        Takes the ``.record`` that error carried and re-appends **it**, which is
        the whole of why this is not ``publish()`` again. The backend contract
        makes the one call right for both possible histories: byte-identical
        content under a used ``(session_id, sequence)`` writes nothing and
        returns the original offset, and a key the backend never saw is appended
        now. Either way the stream ends with exactly one copy of the record and
        the caller ends with its offset.

        Must be called on the topic the append was for, from the producer that
        made it. Both are checked before the backend is touched; see
        :meth:`_outstanding` for what each one prevents.

        **This is where an unknown outcome becomes a known one**, so it is also
        where a fence waiting behind that operation is told which one it became.
        A durable record clears the operation's failure; an
        ``AppendConflictError`` replaces it, because that is the contract's one
        definite refusal and it says this record did not land. Leaving the
        operation holding its original "unknown" and letting a fence infer
        durability from the record no longer being unresolved read both the same
        way, and a conflict then released a fence claiming a write that was
        never in the stream.

        The wake runs afterwards on exactly the terms :meth:`publish` describes,
        so a coordination or Signal failure here raises
        :class:`WakeNotAcknowledgedError` carrying that offset.

        Safe to call repeatedly: if this attempt is itself interrupted, it raises
        :class:`AppendNotAcknowledgedError` again with the same record, and the
        next attempt is the same call.

        Args:
            record: The ``.record`` the error carried, unmodified.
            wake: Defaults to **what the interrupted call was going to do**,
                rather than to ``True``. The recovery finishes that operation, so
                inventing a wake policy for it is how a fence appended with
                ``wake=False`` acquires a Signal, and how a record published with
                ``wake=True`` loses one. Pass a value only to override
                deliberately.
            lease: Likewise defaults to the interrupted call's.

        Raises:
            ValueError: The record is not this topic's outstanding append --
                wrong topic, wrong bytes, wrong session, or nothing outstanding
                at all. Raised before any backend call.

            AppendConflictError: The key was used with different bytes, so this
                record did not land and re-appending it cannot change that. The
                operation is now definitively failed rather than unknown, and a
                fence behind it raises :class:`PrecedingWriteFailedError` from
                this.
        """
        pending = self._outstanding(record)
        wake = pending.wake if wake is None else wake
        lease = pending.lease if lease is None else lease
        try:
            placed = await self._append(
                record, wake=wake, lease=lease, operation=pending.operation
            )
        except AppendConflictError as err:
            self._resolve(pending.operation, err)
            raise
        # Only the append is reported: the fence's claim is about records being
        # appended, which this one now is whatever the wake below does.
        self._resolve(pending.operation, None)
        return await self._wake_for(placed, wake=wake, lease=lease)

    def _resolve(
        self, operation: _StreamOperation | None, failure: BaseException | None
    ) -> None:
        """Replaces an operation's unknown outcome with the recovered one.

        Already ``settled`` -- the call that lost the answer set that before it
        raised -- so this only rewrites what a fence reads there. That is why
        :meth:`_await_preceding_appends` reads every outcome after its last wait
        rather than as each one settles.
        """
        if operation is None:
            return
        operation.failure = failure
        operation.settled.set()

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
                rather than re-appending the half that did not. **Every** failure
                after the append arrives this way, including one from the
                coordination steps that precede the Signal -- those set
                ``.restart`` and leave ``.pending`` empty, meaning the recovery is
                another :meth:`wake` rather than a :meth:`retry_wake`. This
                matters because it is the only thing that tells the caller not to
                retry ``publish()``: a second call draws a new sequence number and
                therefore a new idempotency key, so it appends the record twice.

                **Cancellation delivered after the append also arrives this way**,
                with ``.cancelled`` set, because it leaves the same state and has
                the same recovery. A bare ``CancelledError`` there would be
                indistinguishable from cancellation before the append -- it carries
                no offset, no ``pending`` and no ``restart`` -- so the caller could
                neither wake the record nor safely re-publish it (ADR-036).
                Cancellation delivered *before* the append is reached -- while
                the payload is still encoding -- still raises ``CancelledError``:
                nothing was sent to the backend, so there is nothing to say about
                a record.

            AppendNotAcknowledgedError: The append itself neither returned nor
                refused, so whether the record is durable is **unknown**. This is
                its own outcome and not a failure: a backend commits on its own
                side before it answers, so a cancellation or a lost connection in
                that window can leave a durable record with no offset in hand.
                ``.record`` carries the exact bytes and identity to re-append,
                and :meth:`resolve_append` is the only safe way forward --
                calling ``publish()`` again draws a new sequence number and
                appends the value twice if the first one landed. Until it is
                resolved, this stream refuses further appends with the same error
                (ADR-038).

            AppendConflictError: The idempotency key was used before with
                different bytes. Unlike the above this *is* a refusal: nothing
                landed for this record and re-appending it cannot change that.
        """
        # The sequence is drawn **before** the encode is awaited, and that
        # ordering is the whole of the idempotency key's stability. The key is
        # `(session_id, sequence)`, a retried Activity reuses the session id
        # deliberately, and the payload codec is allowed to do real I/O -- an
        # external payload store, a KMS round trip. Drawing the number after that
        # await hands identities out in *encode-completion* order, so two
        # concurrent publishes exchange sequence numbers whenever the store
        # answers in the other order. On the same stream the backend then sees
        # each stable key reused with different bytes and raises
        # `AppendConflictError`; across topics, where deduplication is per stream
        # key, the swap appends duplicates instead. Either way a valid concurrent
        # Activity becomes permanently non-retryable on timing alone.
        #
        # Drawn here, identities follow *invocation* order, which is the order the
        # retry re-runs the same calls in. An encode that raises leaves its number
        # unused, which costs nothing: a gap in the sequence is not observable --
        # offsets come from the provider -- and the retry reuses the same number
        # for the same call.
        self._refuse_while_unresolved()
        sequence = self._producer._next_sequence()
        # Registered in the same step, and for the same reason the sequence is
        # drawn in it: a concurrent `finish_writing()` has to be able to see this
        # call before the encode below hands the event loop away, or its fence
        # asserts durability for a record that has not been appended yet.
        operation = self._begin(sequence)
        try:
            record = StreamRecord(
                kind=RecordKind.DATA,
                payload=await self._codec.encode(value),
                producer_session_id=self._producer.session_id,
                sequence=sequence,
            )
            placed = await self._append(
                record, wake=wake, lease=lease, operation=operation
            )
        except (Exception, asyncio.CancelledError) as err:
            # Settled with the failure rather than merely dropped: a fence behind
            # this call is waiting on it, and what it does next depends on
            # whether a record landed. Settled *before* the raise, so the two are
            # one uninterrupted step.
            self._settle(operation, err)
            raise
        self._settle(operation)
        return await self._wake_for(placed, wake=wake, lease=lease)

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
            WakeNotAcknowledgedError: The wake did not complete. The record is
                durable; the wake is not. A failed Signal fills ``.pending`` with
                the wakes still owed, for :meth:`retry_wake`. A failure in the
                observe or claim steps that precede it leaves ``.pending`` empty
                and sets ``.restart``, because nothing was composed and calling
                this method again is the recovery. **Cancellation is one of the
                ways an attempt fails here**, not an exception to it: this method
                is only ever reached after a durable append, so cancellation leaves
                the same durable-but-unannounced record and gets the same
                ``pending``/``restart`` recovery, with ``.cancelled`` set
                (ADR-036).
        """
        producer = self._producer
        if producer._client is None:
            raise RuntimeError(
                "waking requires a Temporal client, and this producer was built "
                "without one. Use ExternalStreamProducer.connect(), which "
                "requires it."
            )

        backend = producer._backend
        # The coordination steps are inside the same guarantee as the Signal, and
        # every caller reaches here *after* a durable append. A provider outage in
        # any of them used to escape as whatever the provider raised -- a bare
        # `ConnectionError` -- which told the caller nothing about the record that
        # had already landed. `publish()` catches only the durable-but-
        # unacknowledged error, so the raw exception passed straight through it
        # and lost the offset with it; retrying `publish()` then appended a
        # *second* record, because the sequence had already advanced and the
        # idempotency key with it.
        try:
            parked = await backend.parked_wait_ids(self._stream_key)
            # An unparked wake still needs a wait id for the envelope; 0 is the
            # "no particular subscription" value, and Python rechecks every active
            # subscription on wakeup regardless of which one the Signal named.
            targets: list[tuple[int, int | None]] = [
                (
                    wait_id,
                    await backend.current_park_generation(self._stream_key, wait_id),
                )
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
        except WakeNotAcknowledgedError:
            raise
        except (Exception, asyncio.CancelledError) as err:
            # `pending` is empty and `restart` is set: nothing was composed, so
            # there is nothing to re-send and `retry_wake` would be a no-op that
            # looked like recovery. Calling `wake()` again is the recovery.
            #
            # Cancellation is in here rather than re-raised ahead of it. Every
            # caller reaches this method after a durable append, so a
            # `CancelledError` escaping bare says nothing about the record that
            # already landed and cannot be told apart from cancellation before the
            # append -- the one distinction the caller needs in order to know
            # whether the value still has to be published (ADR-036). The
            # cancellation is not lost: it is what `cancelled` reports.
            raise WakeNotAcknowledgedError(
                "the record was appended but its wake could not be composed: "
                f"{err!r}. No Signal was sent. Call wake() again -- it re-observes "
                "the parked set, and a parked wake's request ID is derived from "
                "the generation, so a wake another producer already sent "
                "deduplicates against it.",
                pending=[],
                restart=True,
                cancelled=isinstance(err, asyncio.CancelledError),
            ) from err

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
            except (Exception, asyncio.CancelledError) as err:
                # Cancellation included, for the reason the coordination handler
                # above gives: the record is durable either way, and here the
                # wakes still owed are known, so the recovery is `retry_wake` with
                # `pending` rather than a fresh `wake()`.
                raise WakeNotAcknowledgedError(
                    f"the record was appended but its wake was not acknowledged: "
                    f"{err!r}. Retrying with the same request is safe -- it derives "
                    "the same request ID and the server deduplicates it.",
                    pending=requests[index:],
                    cancelled=isinstance(err, asyncio.CancelledError),
                ) from err
        return sent

    async def retry_wake(self, pending: list[WakeRequest]) -> list[str]:
        """Re-sends the wakes a failed attempt still owed.

        Takes the requests verbatim rather than recomputing them: recomputing
        would draw a fresh wake counter for an unparked wake, derive a different
        request ID, and defeat the deduplication that makes the retry safe.

        Refuses an empty list rather than returning quietly. A
        :class:`WakeNotAcknowledgedError` raised before any request was composed
        carries no pending wakes and sets ``restart``; a caller that fed that
        empty list to this method would get a successful-looking no-op while the
        record stayed durable and unannounced. There is exactly one recovery from
        that state and it is :meth:`wake`.
        """
        if not pending:
            raise ValueError(
                "retry_wake() was given no wakes to re-send. A "
                "WakeNotAcknowledgedError with an empty `pending` sets `restart`, "
                "which means no Signal was composed and there is nothing to "
                "re-send: call wake() again instead."
            )
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
            except (Exception, asyncio.CancelledError) as err:
                raise WakeNotAcknowledgedError(
                    f"the wake retry did not complete: {err!r}",
                    pending=pending[index:],
                    cancelled=isinstance(err, asyncio.CancelledError),
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

        **Waits for every earlier ``publish()`` on this stream to reach a durable
        append**, across every handle ``topic()`` has returned for the name. The
        claim is otherwise not one this method can make: ``publish()`` draws its
        sequence before awaiting the payload codec, so a publish invoked first
        can still be encoding when this one is called, and a fence that overtook
        it would park a consumer in front of data that had not been written --
        having spent, if the publish was a ``wake=False`` member of a batch, the
        one wake that would have moved it again.

        It does **not** wait for another concurrent fence. Each fence asserts
        durability of the publishes it was invoked after, and neither of two
        fences is inside the other's claim, so ordering them would only let one
        that never reached the backend refuse the other.

        Raises:
            PrecedingWriteFailedError: An earlier ``publish()`` on this stream,
                still in flight when this call was made, ended with no durable
                record -- including one whose outcome was unknown and which
                :meth:`resolve_append` then proved absent. Nothing was appended
                for the fence; see that class for what the two recoveries are.

            WakeNotAcknowledgedError: The fence is durable; the wake is not, on
                exactly the terms :meth:`publish` describes -- cancellation after
                the append included. Retrying ``finish_writing()`` appends a
                *second* fence, for the same reason retrying ``publish()`` appends
                a second record, and a fence that reads back twice is a producer
                session that ended twice.

            AppendNotAcknowledgedError: The fence's append reported no outcome,
                again on exactly :meth:`publish`'s terms. :meth:`resolve_append`
                takes ``.record`` and settles it, leaving one fence whichever way
                the interrupted attempt went. **Also raised for an earlier
                append on this stream whose outcome is unknown**, carrying that
                operation's record rather than a fence: the record may be durable,
                so the fence may not go in front of it, and the stream takes no
                further append until it is settled (ADR-038).
        """
        # Drawn before the append for the same reason as in `publish`, though a
        # fence encodes nothing and so cannot be reordered by a codec: what the
        # two share is that the number belongs to the *call*, so a retry that
        # makes the same calls in the same order derives the same keys.
        self._refuse_while_unresolved()
        sequence = self._producer._next_sequence()
        # Read in the same uninterrupted step as the draw, and *not* registered
        # alongside them: a fence is not a write anything else waits behind, so
        # it reads the order without joining it (see `_StreamOperation`).
        preceding = self._preceding_publishes()
        # The sequence identity is this call's, but the *append* is ordered: the
        # fence enters the backend only once every earlier publish on this stream
        # has one.
        await self._await_preceding_appends(preceding)
        fence = StreamRecord(
            kind=RecordKind.WRITE_FENCE,
            payload=b"",
            producer_session_id=self._producer.session_id,
            sequence=sequence,
        )
        placed = await self._append(fence, wake=wake, lease=lease, operation=None)
        # A fence is the record most likely to find the Workflow parked -- it is
        # what a producer appends when it has nothing more to say -- so an
        # unsignalled one strands the Workflow for its whole idle timeout at
        # exactly the moment it was waiting to be told.
        return await self._wake_for(placed, wake=wake, lease=lease)


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
