"""The Workflow-facing API (P9).

.. code-block:: python

    streams = external_stream.with_options(idle_timeout=timedelta(seconds=1))
    tokens = streams.topic("tokens", backend="tokens-redis", type=str)

    async for token in tokens.subscribe():
        process(token)

Workflow code **names** a backend; it never constructs or imports one. Provider
instances hold connections and credentials, live on the Worker outside the
sandbox, and are reached only through an opaque handle.

Everything here is a mirror image of the shipped
:py:mod:`temporalio.contrib.workflow_streams`, not a second implementation of
it, so no name may collide -- and in particular no name here may begin with
``__temporal_workflow_stream``.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Any, Generic, Protocol

import temporalio.workflow
from temporalio.contrib.external_workflow_streams._backend import StreamKey
from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec
from temporalio.contrib.external_workflow_streams._errors import (
    StreamError,
    classify_read_failure,
)
from temporalio.contrib.external_workflow_streams._record import (
    StreamRecord,
)
from temporalio.types import AnyType

__all__ = [
    "merge",
    "ExternalStreamOptions",
    "ExternalStreamSubscription",
    "ExternalStreamTopic",
    "external_stream",
]

DEFAULT_IDLE_TIMEOUT = timedelta(seconds=1)
"""How long the complete blocked set waits before parking.

A property of the **set**, not of one subscription: one idle stream must not
park a Workflow Task another stream is still driving.
"""

MAX_RECORDS_PER_ACTIVATION = 256
"""How many records one activation may hand to Workflow code.

Without a cap, a producer that keeps the buffer non-empty makes one
``activate()`` call never return: the iterator re-fills before every record and
always finds one. Activations run on a thread-pool executor under a **2-second
deadlock timeout**, so the Workflow Task fails -- and every retry hits the same
producer, so the Workflow is stuck permanently rather than merely slowed. Driving
the real iterator against a never-empty buffer consumed 316,086 records in two
seconds and blocked zero times.

A **record count, never a duration**. A time-based cap would be nondeterministic,
and because the boundary each activation stopped at is recorded in the
annotation, a replay of the same records would cut the segments somewhere else
and diverge from the live run.
"""

#: Where per-Run subscription state hangs off the Workflow instance. Reserved,
#: and deliberately not in the `__temporal_workflow_stream*` namespace the
#: shipped contrib feature already owns.
_RUN_STATE_ATTR = "__temporal_external_stream_state"


class ExternalStreamRuntime(Protocol):
    """What Workflow code needs from the Worker, and nothing more.

    An opaque handle across the sandbox boundary: it resolves a backend *name*
    and registers a wait. Workflow code never sees a provider instance.
    """

    def stream_key(self, stream_name: str) -> StreamKey:
        """The full stream identity for a name, from the running Run's chain."""
        ...

    def register(
        self,
        *,
        wait_id: int,
        stream_key: StreamKey,
        backend_name: str,
        idle_timeout: timedelta,
    ) -> None:
        """Registers a wait with the Worker's subscription manager.

        ``idle_timeout`` is the *configured* value for this one subscription.
        The runtime reduces the quiescent set's values to one; this side has no
        business doing that reduction, because it can only see one member.
        """
        ...

    def drain(self, wait_id: int, max_records: int | None = None) -> list[StreamRecord]:
        """Pops buffered records. Performs no I/O."""
        ...

    def delivery_budget_remaining(self) -> int:
        """How many more records this activation may hand to Workflow code.

        Lives on the runtime rather than on a subscription because the budget is
        an *activation* budget: :py:func:`merge` consumes from several
        subscriptions in one activation, and a per-subscription counter would let
        *n* streams run *n* times as long.
        """
        ...

    def codec_for(self, value_type: type | None) -> StreamPayloadCodec[Any]:
        """The Workflow's DataConverter, bound to a topic's declared type."""
        ...

    def new_readiness_future(self) -> asyncio.Future[None]:
        """A future the readiness activation handler will resolve.

        Created by the runtime rather than here because it must belong to the
        Workflow's own deterministic event loop, which this module has no
        business reaching into.
        """
        ...

    def record_delivery(self, wait_id: int, record: StreamRecord) -> None:
        """Notes one record reaching the Workflow, in observed global order.

        Called for control records too. They are never yielded, but they occupy
        offsets inside a run, so the annotation has to know about them.
        """
        ...

    def note_blocked(self, wait_id: int, blocked: bool) -> None:
        """Records whether Workflow code is currently waiting on this wait."""
        ...

    def unsubscribe(self, wait_id: int) -> None:
        """Ends a wait and stops the Worker serving it.

        Keeps the wait's recorded state -- replay and a Continue-As-New
        successor both still need it -- and only makes it unblockable.
        """
        ...

    def register_pending(self, wait_id: int, future: asyncio.Future[None]) -> None:
        """Registers the future the readiness activation will resolve."""
        ...

    def discard_pending(self, wait_id: int) -> None:
        """Forgets a future that is no longer being awaited."""
        ...


@dataclass
class _RunState:
    """Per-Run subscription bookkeeping.

    Lives on the Workflow instance rather than in a module global, so it shares
    the instance's lifetime exactly -- a module global would outlive an evicted
    Run and hand its wait ids to the next one.
    """

    runtime: ExternalStreamRuntime | None = None
    next_wait_id: int = 1
    #: `wait_id -> Future`, resolved by the readiness activation handler.
    pending: dict[int, Any] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.pending is None:
            self.pending = {}


def _run_state() -> _RunState:
    instance = temporalio.workflow.instance()
    state = getattr(instance, _RUN_STATE_ATTR, None)
    if state is None:
        state = _RunState()
        setattr(instance, _RUN_STATE_ATTR, state)
    return state


def _install_runtime(instance: Any, runtime: ExternalStreamRuntime) -> None:
    """Gives a Workflow instance its handle to the Worker's manager.

    Called by the Worker when it creates the instance.
    """
    state = getattr(instance, _RUN_STATE_ATTR, None)
    if state is None:
        state = _RunState()
        setattr(instance, _RUN_STATE_ATTR, state)
    state.runtime = runtime


@dataclass(frozen=True)
class ExternalStreamOptions:
    """The entry point, and the options every topic under it inherits."""

    idle_timeout: timedelta = DEFAULT_IDLE_TIMEOUT

    def with_options(
        self, *, idle_timeout: timedelta | None = None
    ) -> ExternalStreamOptions:
        """A copy with the given options replaced.

        Args:
            idle_timeout: How long the complete blocked set waits with no record
                on any active subscription before parking.

        Raises:
            ValueError: The timeout is not positive. Rejected rather than
                coerced -- zero would park instantly and a negative value means
                nothing at all, so neither can be what a caller intended.
        """
        if idle_timeout is not None and idle_timeout <= timedelta(0):
            raise ValueError(
                f"idle_timeout must be positive, got {idle_timeout}. A "
                "non-positive timeout is a configuration error rather than a "
                "request to park immediately."
            )
        return replace(
            self,
            idle_timeout=self.idle_timeout if idle_timeout is None else idle_timeout,
        )

    def topic(
        self, name: str, *, backend: str, type: type[AnyType] | None = None
    ) -> ExternalStreamTopic[Any]:
        """A handle for one stream.

        Args:
            name: The stream name. The only place it appears.
            backend: The **name** of a backend registered on the Worker with
                ``external_stream_backends={...}``. Not a provider instance:
                Workflow code may not hold one.
            type: The value type, used as the decode hint.
        """
        if not name:
            raise ValueError("a topic needs a non-empty name")
        if not backend:
            raise ValueError(
                "a topic needs the name of a backend registered on the Worker; "
                "Workflow code names a backend rather than constructing one"
            )
        return ExternalStreamTopic(
            name=name, backend_name=backend, value_type=type, options=self
        )


@dataclass(frozen=True)
class ExternalStreamTopic(Generic[AnyType]):
    """One stream, from the Workflow's side.

    Has ``subscribe`` and no ``publish``: the consumer and producer handles are
    distinct types, not one object passed across a process boundary.
    """

    name: str
    backend_name: str
    value_type: type[AnyType] | None
    options: ExternalStreamOptions

    def subscribe(self) -> ExternalStreamSubscription[AnyType]:
        """Starts a new subscription and returns its async iterator.

        Each call is an **independent** subscription with its own ``wait_id``,
        cursor, and park intent -- even two calls naming the same stream.
        Delivery is broadcast, so each sees every record from its own cursor.

        ``wait_id`` comes from a per-Run counter in call order, which puts it in
        the same hazard class as timers and activities: inserting, removing, or
        reordering a ``subscribe()`` call renumbers every later wait in the Run
        and must be gated behind ``workflow.patched()``.
        """
        state = _run_state()
        if state.runtime is None:
            raise RuntimeError(
                "external streams are not configured on this Worker; pass "
                "external_stream_backends={...} to the Worker"
            )
        wait_id = state.next_wait_id
        state.next_wait_id += 1

        stream_key = state.runtime.stream_key(self.name)
        state.runtime.register(
            wait_id=wait_id,
            stream_key=stream_key,
            backend_name=self.backend_name,
            # Passed on registration rather than read back from the subscription
            # later, because the runtime is what holds the quiescent set and
            # reduces it with `min`. Leaving it out is not a smaller default --
            # it silently substitutes DEFAULT_IDLE_TIMEOUT for whatever
            # `with_options` was given, so no configured value can ever reach
            # the reduction and every set parks after one second.
            idle_timeout=self.options.idle_timeout,
        )
        # Registering a wait is not blocking on one. The quiescent snapshot is a
        # request to Core to retain the Workflow Task and, once the idle timer
        # expires, to park it; a subscription Workflow code has not begun
        # iterating has no coroutine waiting on it, so including it would ask
        # Core to hold a Workflow Task open -- and eventually park it -- for a
        # wait nothing will ever resolve. The first `_await_readiness` is what
        # enters the blocked state, and that transition is what the wait
        # generation counts.
        state.runtime.note_blocked(wait_id, False)
        return ExternalStreamSubscription(
            topic=self, wait_id=wait_id, stream_key=stream_key, state=state
        )


async def merge(
    *subscriptions: ExternalStreamSubscription[Any],
) -> AsyncIterator[tuple[ExternalStreamSubscription[Any], Any]]:
    """Iterates several subscriptions as one wait, in delivery order.

    Yields ``(subscription, value)`` rather than bare values: the streams may
    carry different types, and a merged value whose origin had to be guessed
    from its shape would be unusable for anything but logging.

    The subscriptions are already one wait set -- every subscription a Run holds
    is -- so this adds no coordination of its own. What it adds is the ability to
    *wait on all of them at once*: iterating them one at a time would block on
    the first while records piled up on the second, and the idle timer covering
    the set would then fire against a Workflow that was not actually idle.

    Each pass takes **at most one record from each subscription**, in ``wait_id``
    order, resuming after the subscription that last took one. All three of
    those are load-bearing:

    - *In ``wait_id`` order*, which is what makes the interleaving reproduce.
      Records that arrived in one batch across two streams have no inherent
      order between them, so an order that depended on dict iteration, on
      arrival time, or on which watcher happened to run first would replay
      differently than it ran. ``wait_id`` comes from a per-Run counter in
      ``subscribe()`` call order, so replay reconstructs the same total order
      from the Workflow code itself, and the pass then depends only on which
      waits have a record ready -- which replay reconstructs from the recorded
      segments.
    - *At most one record*, which is what makes it a merge rather than a
      priority order. Draining one subscription's whole ready list first lets
      the lowest ``wait_id`` spend the entire
      :py:data:`MAX_RECORDS_PER_ACTIVATION` budget by itself, and the next
      activation starts the same pass in the same order, so a continuously
      backlogged first stream starves every later one forever.
    - *Resuming after the last take*, which is what makes "at most one each" add
      up to fairness across activations rather than only within a pass. The
      budget covers the merged set, so a pass can be cut anywhere inside it, and
      a pass that always restarted at the lowest wait id would be cut in the
      same place every activation and re-privilege the same prefix forever. With
      257 ready subscriptions the 257th is never asked at all -- ``_fill``
      returns on the spent budget before it reaches the manager, so that wait is
      not merely served nothing, it is never enquired after. With 100 the first
      56 take one record per activation more than the rest, and the gap grows
      without bound. Rotating the start is what makes the skew between any two
      continuously ready streams what it is claimed to be: a single record.

    A control record spends the subscription's turn: it is consumed, because it
    occupies an offset inside a run, and the pass moves on. Filling one record
    at a time is also what keeps the budget exact -- a fill that took the whole
    remaining budget into one subscription's ready list would let the rest of
    that list be consumed after the budget was spent, and would strand it there,
    since the completion path re-arms readiness from the *manager's* buffers and
    knows nothing about records already popped out of them.

    The recorded delivery schedule follows: alternating records across two
    streams encode as one run per delivery, because a run is a maximal
    consecutive stretch from a single wait.

    The delivery budget covers the whole set rather than each member, so a merge
    over *n* never-empty streams still returns after
    :py:data:`MAX_RECORDS_PER_ACTIVATION` records. A per-subscription budget
    would let the activation run *n* times as long, which is the same deadlock
    with a larger constant in front of it.
    """
    ordered = sorted(subscriptions, key=lambda s: s.wait_id)
    if not ordered:
        raise ValueError("merge() needs at least one subscription")
    if len({s.wait_id for s in ordered}) != len(ordered):
        raise ValueError(
            "merge() was given the same subscription twice; each one is a "
            "separate wait with its own cursor, and merging one with itself "
            "would deliver every record to it twice"
        )

    # The wait that last took a record, so the next pass resumes after it rather
    # than restarting at the lowest wait id.
    #
    # Local to this generator, recorded nowhere, and that is what makes it
    # replay-safe rather than merely convenient. Replay serves a drain from the
    # **front** of the recorded segment and only while that front belongs to the
    # asking wait, so a wait asked out of turn gets nothing and the record stays
    # for whoever asks next. Every active wait is still asked exactly once per
    # pass, so the yielded sequence is the recorded global order whatever
    # position the pass starts at. Under replay the budget is unbounded, so
    # passes are not cut where they were cut live and this cursor generally ends
    # up somewhere else than it did -- which steers nothing but *later live*
    # fairness, and no live schedule was ever recorded for History to contradict.
    resume_after = 0
    while True:
        # A closed subscription leaves the set rather than blocking it: it has
        # no coroutine behind it, so including it in the wait would ask Core to
        # retain the Workflow Task for a wait nothing can resolve.
        active = [s for s in ordered if not s._finished]
        if not active:
            return
        # The first wait past the last take, wrapping to the front when there is
        # none -- which is also what happens when the wait the cursor named has
        # since closed and left the set.
        start = next((i for i, s in enumerate(active) if s.wait_id > resume_after), 0)
        delivered_any = False
        for step in range(len(active)):
            subscription = active[(start + step) % len(active)]
            subscription._fill(1)
            record = subscription._peek()
            if record is None:
                continue
            delivered_any = True
            # Advanced only where a record was actually taken, control records
            # included, because those spend the turn and the budget too. A wait
            # that had nothing has not had its turn, and moving the cursor past
            # it would cost it the turn it never got -- which is the starvation
            # this exists to end, reintroduced from the other side.
            resume_after = subscription.wait_id
            if record.is_control:
                subscription._commit(record)
                continue
            # Decoded before consumption is committed, for the same reason as in
            # `_iterate`: a decode that raises must leave the record where a
            # later pass can still find it.
            value = subscription._decode(record)
            subscription._commit(record)
            yield subscription, value
        if not delivered_any:
            # Nothing anywhere: block on all of them at once. Whichever wait
            # Core resolves first wakes this, and the next pass picks it up.
            if await _await_any_readiness(active):
                continue


async def _await_any_readiness(
    subscriptions: Sequence[ExternalStreamSubscription[Any]],
) -> bool:
    """Blocks until any one of the waits is resolved. Returns whether it skipped.

    Every wait is marked blocked, because the quiescent snapshot must name the
    **complete** set the Workflow is waiting on: a set missing one member would
    let Core park the Workflow Task while that member was still live.
    """
    runtime = subscriptions[0]._state.runtime
    assert runtime is not None
    futures = []
    for subscription in subscriptions:
        runtime.note_blocked(subscription.wait_id, True)
        future = runtime.new_readiness_future()
        runtime.register_pending(subscription.wait_id, future)
        # Also on the subscription, so `close()` can resume a merge that is
        # sitting on this wait: the runtime's map is keyed for the side that
        # resolves readiness, and closing is neither that side nor this one.
        subscription._pending_future = future
        futures.append(future)
    try:
        # The same last look the single-subscription path takes, for the same
        # reason: a record buffered before these waits were registered had its
        # readiness reported to nobody. And for the same reason as there, it
        # goes through `_fill` and so finds nothing once the budget is spent --
        # otherwise a merge over a busy stream would resume immediately and the
        # activation would never end.
        #
        # One record, matching the pass above: a fill that took the whole
        # remaining budget here would put records into a ready list the budget
        # can no longer pay for, and the completion path re-arms readiness from
        # the manager's buffers, which no longer hold them.
        for subscription in subscriptions:
            subscription._fill(1)
            if subscription._ready:
                return True
        await asyncio.wait(futures, return_when=asyncio.FIRST_COMPLETED)
        return False
    except BaseException:
        # Abandoned rather than resolved -- cancellation, most often because
        # this merge lost a race against a timer. Nothing is awaiting these
        # waits any more, so they must leave the blocked set; leaving them in it
        # asks Core to retain and eventually park the Workflow Task for a
        # coroutine that no longer exists.
        for subscription in subscriptions:
            runtime.note_blocked(subscription.wait_id, False)
        raise
    finally:
        for subscription in subscriptions:
            subscription._pending_future = None
            runtime.discard_pending(subscription.wait_id)
        for future in futures:
            if not future.done():
                future.cancel()


class ExternalStreamSubscription(Generic[AnyType]):
    """One subscription's async iterator over decoded values."""

    def __init__(
        self,
        *,
        topic: ExternalStreamTopic[AnyType],
        wait_id: int,
        stream_key: StreamKey,
        state: _RunState,
    ) -> None:
        self._topic = topic
        self._wait_id = wait_id
        self._stream_key = stream_key
        self._state = state
        self._ready: list[StreamRecord] = []
        #: Set by :meth:`close`, and the only thing that ends iteration.
        self._finished = False
        #: The readiness future currently being awaited, if any. Held here as
        #: well as on the runtime because :meth:`close` has to be able to resume
        #: the coroutine sitting on it, and the runtime's map is keyed by wait
        #: id for the *resolving* side rather than for this one.
        self._pending_future: asyncio.Future[None] | None = None

    @property
    def wait_id(self) -> int:
        return self._wait_id

    @property
    def stream_key(self) -> StreamKey:
        return self._stream_key

    @property
    def idle_timeout(self) -> timedelta:
        return self._topic.options.idle_timeout

    def __aiter__(self) -> AsyncIterator[AnyType]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[AnyType]:
        while not self._finished:
            # Re-filled before *every* record rather than once per batch. A
            # record buffered while Workflow code was doing something else -- a
            # timer, an activity, another stream -- has already had its readiness
            # reported and consumed, so nothing will report it again. Blocking
            # without looking would wait forever on a record that is already
            # here. Filling is a buffer pop and costs nothing.
            self._fill()
            record = self._peek()
            if record is not None:
                if record.is_control:
                    self._commit(record)
                    continue
                # Decoded first, committed second. Consumption is a claim that
                # Workflow code *received* this record, and the claim is only
                # true once a value exists: a decode that raises out of a
                # mismatched converter never yields anything. Committing first
                # makes the claim false in exactly that case -- the record
                # leaves the ready list, the buffer it came from is already
                # empty, and the consumption cursor a Continue-As-New successor
                # resumes from has stepped over a record nothing ever saw. Left
                # uncommitted it stays at the head of the ready list and the
                # next `__anext__` retries it.
                #
                # Synchronous, so there is no longer a point *inside* decoding
                # at which cancellation can land at all: the record either
                # becomes a value or raises, and neither outcome can leave the
                # ready list half-consumed.
                value = self._decode(record)
                self._commit(record)
                yield value
                continue
            await self._await_readiness()

    def _fill(self, limit: int | None = None) -> None:
        """Moves whatever is buffered into this subscription's ready list.

        Draining is a buffer pop and never touches the backend -- the record is
        already here or it is not, and if it is not, only Core can say when to
        look again.

        Bounded by the activation's remaining delivery budget. A drain that took
        the whole buffer would be handed straight back by a producer that keeps
        refilling it, and this activation would never return.

        ``limit`` bounds it further, for a caller that will consume fewer records
        than the budget allows. :py:func:`merge` passes 1: it takes one record
        per subscription per pass, and anything it pulled out of the manager's
        buffer and did not take would be stranded there -- the completion path
        re-arms readiness from the manager's buffers, so a record already popped
        out of one has nothing left to announce it.
        """
        if self._ready:
            return
        assert self._state.runtime is not None
        budget = self._state.runtime.delivery_budget_remaining()
        if limit is not None:
            budget = min(budget, limit)
        if budget <= 0:
            # Nothing is taken even though records are sitting right here, so the
            # caller blocks and the activation ends. The records are not lost:
            # the completion path re-reports readiness for every buffer that is
            # still non-empty, which is what brings the next activation in.
            return
        drained = self._state.runtime.drain(self._wait_id, budget)
        for record in drained:
            # Recorded for *every* record, control ones included: they occupy
            # offsets inside a run, so a run's count includes them and their
            # positions go in `control_positions`. Omitting them would make
            # replay's range read find more records than the marker claims.
            self._state.runtime.record_delivery(self._wait_id, record)
        # Control records stay in the list rather than being filtered out here,
        # so that consumption advances past them in order. A filtered control
        # record would be neither consumed nor left behind, and the continuation
        # cursor would step over it.
        self._ready = list(drained)

    def _peek(self) -> StreamRecord | None:
        """The next ready record, or ``None``. Commits nothing.

        Leaving this wait's blocked state is right here rather than at commit
        time: a record is in hand, so no coroutine is waiting on this wait, and a
        quiescent snapshot taken while the value is being decoded must not name
        it.
        """
        if not self._ready:
            return None
        assert self._state.runtime is not None
        self._state.runtime.note_blocked(self._wait_id, False)
        return self._ready[0]

    def _commit(self, record: StreamRecord) -> None:
        """Pops the record and records that Workflow code now has it.

        Consumption is not delivery. A batch is delivered whole, but a Workflow
        that stops iterating part-way through has consumed only its prefix, and
        a successor Run resuming from the delivery cursor would step over the
        rest.

        Called only once the record has actually become a value -- or, for a
        control record, once it has been skipped, which is the whole of what
        receiving one means.
        """
        assert self._state.runtime is not None
        assert self._ready and self._ready[0] is record
        self._ready.pop(0)
        self._state.runtime.record_consumption(self._wait_id, record)

    async def _await_readiness(self) -> None:
        """Blocks until the readiness activation resolves this wait.

        The future is resolved from the ``ResolveExternalStreamWaits`` branch of
        the activation dispatch, which is the only thing that knows a record
        arrived.
        """
        assert self._state.runtime is not None
        # Entering the blocked state is what the wait generation counts, and is
        # what later makes a readiness notification for *this* block
        # distinguishable from one for a block already resolved.
        self._state.runtime.note_blocked(self._wait_id, True)
        future = self._state.runtime.new_readiness_future()
        self._state.runtime.register_pending(self._wait_id, future)
        self._pending_future = future
        try:
            # Look once more, now that this wait is registered. A record buffered
            # between the last drain and this registration had its readiness
            # reported while nothing was waiting for it, and no second
            # notification is coming -- the watcher has already moved its
            # prefetch cursor past it. Checking after registering is what closes
            # that window: anything earlier is found here, anything later
            # resolves the future.
            #
            # It goes through `_fill`, so it observes the delivery budget like
            # every other drain. A refill that ignored the budget would hand this
            # wait a record the instant the budget stopped it, and the activation
            # would go right back to never returning -- the double-check would
            # undo the cap rather than merely coexist with it.
            self._fill()
            if self._ready:
                return
            await future
        except BaseException:
            # Abandoned rather than resolved: cancellation, most often because
            # Workflow code raced this stream against a timer and cancelled the
            # loser. Nothing is awaiting this wait any more, so it must leave the
            # blocked set -- a wait left in it is named by the quiescent
            # snapshot, and Core would retain and eventually park the Workflow
            # Task for a coroutine that no longer exists.
            self._state.runtime.note_blocked(self._wait_id, False)
            raise
        finally:
            self._pending_future = None
            self._state.runtime.discard_pending(self._wait_id)

    def _decode(self, record: StreamRecord) -> AnyType:
        """This record as a value of the topic's declared type.

        **Synchronous, and that is the point.** The Workflow thread runs one
        half of ``DataConverter.decode`` -- ``from_payloads``, which needs the
        topic's type and performs no I/O. The other half, external-payload
        retrieval and the user's ``PayloadCodec``, already ran on the Worker's
        loop before this record was buffered, exactly as it does for every other
        payload an activation carries. Awaiting a codec here would perform real
        I/O inside a deterministic event loop, synthesize Workflow commands out
        of the codec's internals, and put arbitrary user work under the
        2-second deadlock timeout.

        Failures of *either* half surface from here, because here is where the
        record would have become a value. A preparation error carried over from
        the Worker's loop is raised at the delivery it belongs to rather than
        where it happened: the watcher has no Workflow to tell, and a record
        that is never delivered must not fail anything.
        """
        assert self._state.runtime is not None
        codec = self._state.runtime.codec_for(self._topic.value_type)
        prepared = getattr(record, "prepared_payload", None)
        failed = getattr(record, "prepare_error", None)
        try:
            if failed is not None:
                raise failed
            if prepared is None:
                prepared = codec.parse_unprepared(record.payload)
            return codec.convert(prepared)
        except StreamError:
            # Already classified -- a storage failure reaching the payload store
            # stays row one rather than being relabelled as the consumer's
            # converter mismatch.
            raise
        except Exception as err:
            # `range_validated=True` for both delivery paths, and for the same
            # reason. Replay validated the recorded range before this record was
            # prepared; live delivery read the record out of the backend and the
            # provider guarantees a record's bytes cannot change once written
            # (ADR-003). Either way the bytes are the bytes that were written,
            # so what failed is the consumer's converter -- row three -- and not
            # the stream (ADR-015).
            raise classify_read_failure(range_validated=True, cause=err) from err

    def close(self) -> None:
        """Ends this subscription: iteration stops and the wait goes away.

        Synchronous and idempotent, because the ordinary shape is a ``finally``
        that cannot know whether the iterator already ended.

        Three things happen, and each answers a distinct way an abandoned
        subscription is visible:

        - the wait leaves the **blocked set**, so no quiescent snapshot names it
          and Core is not asked to retain -- and eventually park -- the Workflow
          Task for it;
        - any coroutine sitting on this wait's readiness future is **resumed**
          rather than cancelled, so ``_iterate`` reaches its loop condition and
          the iteration ends with ``StopAsyncIteration`` instead of raising
          ``CancelledError`` into Workflow code that merely closed a stream;
        - records already drained but never handed over are **dropped without
          being consumed**, so the consumption cursor stops short of them and a
          Continue-As-New successor receives them.

        The Worker-side half -- stopping this wait's watcher and removing its
        park intent -- is the manager's, and is reached through the runtime
        rather than from here, because it has to be scheduled onto the Worker's
        loop: this runs on the Workflow thread, where creating a task is
        silently ineffective.

        What closing does **not** do is forget the subscription. Its recorded
        binding is what replay needs to know what this wait was reading, and its
        cursor is what stops a Continue-As-New successor restarting the stream
        from the beginning.
        """
        if self._finished:
            return
        self._finished = True
        runtime = self._state.runtime
        if runtime is None:
            return
        runtime.note_blocked(self._wait_id, False)
        runtime.discard_pending(self._wait_id)
        self._ready.clear()
        future = self._pending_future
        self._pending_future = None
        if future is not None and not future.done():
            future.set_result(None)
        runtime.unsubscribe(self._wait_id)


#: The entry point Workflow code uses.
external_stream = ExternalStreamOptions()
