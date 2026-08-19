"""The per-Worker subscription and watcher manager (P8).

**Any design in which ``_apply`` reads the backend is wrong by construction, not
merely slow.** ``_WorkflowInstanceImpl.activate()`` is synchronous, runs on a
thread-pool executor under a 2-second deadlock timeout, and drives a custom
deterministic event loop in which real network I/O cannot be awaited at all.

So the manager lives out here, on the Worker's own asyncio loop, owns every
backend connection and watcher task, and communicates with the Workflow thread
through a **bounded, thread-safe buffer per subscription**:

===========  ======================  =====================================
Step         Where                   What
===========  ======================  =====================================
Register     Workflow thread         Records the wait. Non-blocking.
Prefetch     Manager loop            Reads ahead into the bounded buffer.
Readiness    Manager loop            Reports **only after** a record is
                                     buffered -- never on a bare socket
                                     event.
Drain        Workflow thread         Pops from the buffer. Nothing else.
===========  ======================  =====================================

That "only after buffered" rule is what makes the resulting activation
guaranteed non-blocking. Readiness for an unbuffered record would produce an
activation whose drain must block, reintroducing exactly the deadlock hazard
this structure exists to remove.

Three cursors, and conflating them is how a speculative read becomes a durable
claim:

===================  ===================  ===========  ==================
Cursor               Advances on          Owner        Survives eviction
===================  ===================  ===========  ==================
``committed``        marker commit only   the marker   yes
``delivery``         hand-off to workflow the instance no
``prefetch``         buffering            the manager  no
===================  ===================  ===========  ==================

``prefetch`` is speculative: reading a record is not consuming it, and consuming
it is not committing it. The manager may only move it *backwards* to
``committed``, never forwards past what a marker has committed.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
from collections import deque
from collections.abc import Awaitable, Callable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import temporalio.converter
from temporalio.contrib.external_workflow_streams._annotation import StreamBinding
from temporalio.contrib.external_workflow_streams._backend import (
    ParkIntent,
    StreamBackend,
    StreamKey,
)
from temporalio.contrib.external_workflow_streams._errors import (
    StreamError,
    StreamStorageError,
)
from temporalio.contrib.external_workflow_streams._replay import (
    ReplayPlan,
    ReplaySegment,
    build_replay_plan,
)
from temporalio.contrib.external_workflow_streams._record import (
    AFTER,
    BEGINNING,
    Cursor,
    StreamRecord,
)
from temporalio.contrib.external_workflow_streams._wake import new_sender_identity

__all__ = [
    "PreparedRecord",
    "ReadinessResult",
    "StreamSubscriptionManager",
    "Subscription",
]

logger = logging.getLogger(__name__)


@contextlib.contextmanager
def _as_storage_failure(what: str) -> Iterator[None]:
    """Reports a backend failure on an activation path as row one.

    The park handshake is a backend transaction an activation is waiting on, and
    a backend that is unreachable or erroring is the taxonomy's *transient*
    row: nothing for an operator to do, and it clears when the backend
    recovers. Left as whatever the provider raised, it reaches the server as an
    anonymous Workflow Task failure with no cause and no counter,
    indistinguishable from a bug in the Workflow's own code.

    Not used for the intent removal a resolve performs, which deliberately logs
    rather than raises: that one is cleanup on the delivery path, and failing a
    Workflow Task for it would trade a stale intent for a repeated Workflow
    Task.

    Anything already classified passes through untouched, so an integrity
    failure is never relabelled as a transient one.
    """
    try:
        yield
    except asyncio.CancelledError:
        raise
    except StreamError:
        raise
    except Exception as err:
        raise StreamStorageError(f"{what} failed: {err}") from err


DEFAULT_BUFFER_SIZE = 256
"""How many records one subscription may hold ahead of the Workflow.

Backpressure *is* this bound: a full buffer stops prefetch. It never drops a
record and never blocks the Workflow thread.
"""

DEFAULT_WATCH_BLOCK = timedelta(seconds=5)


class ReadinessResult:
    """The subset of Core's readiness answers the manager reacts to.

    Mirrors :py:class:`temporalio.bridge.worker.ExternalStreamReadyResult` but
    is matched structurally, so the manager can be driven in tests without a
    Core worker behind it.
    """

    ACCEPTED = "Accepted"
    STALE = "Stale"
    PARKED = "Parked"
    NO_OPEN_WORKFLOW_TASK = "NoOpenWorkflowTask"
    RUN_NOT_FOUND = "RunNotFound"


class RunStatus:
    """The read-only answer to "what state is this Run's wait set in?".

    Mirrors :py:class:`temporalio.bridge.worker.ExternalStreamRunStatus` and is
    matched structurally for the same reason :class:`ReadinessResult` is.
    """

    WFT_OPEN = "WftOpen"
    PARKED = "Parked"
    NO_OPEN_WORKFLOW_TASK = "NoOpenWorkflowTask"
    RUN_NOT_FOUND = "RunNotFound"


READINESS_ATTEMPTS = 3
"""How many times a failing readiness report is retried before a wake is owed.

A raising notifier means the record is buffered and Core has not been told. That
is indistinguishable, from here, from a Run with no open Workflow Task -- so
after these attempts it is treated as exactly that.
"""

READINESS_RETRY_DELAY = timedelta(milliseconds=200)

STALE_REPORT_ATTEMPTS = 3
"""How many times a stale readiness report is re-sent against a newer generation.

Bounded rather than open-ended: a generation that keeps moving is a Workflow
consuming records happily, and owing a wake at the end of that costs one empty
Workflow Task, where giving up silently costs the record.
"""

STALE_RETRY_DELAY = timedelta(milliseconds=50)

SHUTDOWN_WAKE_ATTEMPTS = 3
"""How many times one owed wake is attempted before it is reported.

More than one because a Worker shutting down is often shutting down *because*
something is unhealthy, so the first attempt is the one most likely to land in
the middle of it. Bounded because the alternative to giving up is holding
shutdown open, and the metric exists precisely so that giving up is visible.
"""

SHUTDOWN_WAKE_RETRY_DELAY = timedelta(milliseconds=200)
"""Short enough that three attempts fit comfortably inside the grace period."""

DEFAULT_SHUTDOWN_GRACE = timedelta(seconds=10)
"""How long the sweep may hold shutdown open.

Bounded on purpose: a Worker that could not reach the server would otherwise
hang on the way out, and a wake that has not been acknowledged by now is better
reported than waited on indefinitely.
"""

DEFAULT_PROBE_GRACE = timedelta(seconds=2)
"""How long the probe phase may delay Core's stop-polling step.

Much shorter than the sweep's grace, and for a different reason: the probe is
purely local -- one message on Core's own serialized input lane per Run -- so it
is either quick or wedged, and everything it delays is a Worker that has already
been asked to stop. A Run it does not reach is not lost; it is probed again by
the sweep, on whatever Core has left to say.
"""


def _status_value(status: Any) -> str:
    return getattr(status, "value", status)


#: What the manager calls to tell Core a record is buffered. Returns one of the
#: five readiness results as a plain value (or an enum whose ``value`` is one).
ReadinessNotifier = Callable[[str, int, int], Awaitable[Any]]

#: What the manager calls when local readiness could not be delivered. Filled in
#: by the producer wake-signal path (P14); until then a subscription simply
#: records that a wake was owed.
WakeSender = Callable[["Subscription"], Awaitable[None]]

#: The read-only Run-status probe (C4), returning one of the four run statuses.
RunStatusProbe = Callable[[str], Awaitable[Any]]


def _result_value(result: Any) -> str:
    return getattr(result, "value", result)


@dataclass
class Subscription:
    """One Workflow subscription, as the manager sees it."""

    run_id: str
    wait_id: int
    stream_key: StreamKey
    backend: StreamBackend
    buffer_size: int = DEFAULT_BUFFER_SIZE

    committed_cursor: Cursor = BEGINNING
    """Advances only when a marker commits. Reconstructed from History."""

    delivery_cursor: Cursor = BEGINNING
    """Advances when a record is handed to Workflow code. Rebuilt by replay."""

    prefetch_cursor: Cursor = BEGINNING
    """Advances on buffering. Speculative, and discarded outright on eviction."""

    wait_generation: int = 0
    """Increments each time this wait re-enters the blocked state.

    Written by :meth:`note_wait_generation` from the Workflow thread and read by
    the watcher on the Worker's loop, so both go through ``_lock``. Only the
    runtime knows when a wait re-blocks, so nothing out here can derive it.
    """

    #: Records read ahead but not yet delivered. Appended by the manager loop,
    #: popped by the Workflow thread, so every touch is under `_lock`.
    _buffer: deque[StreamRecord] = field(default_factory=deque, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _has_room: asyncio.Event = field(default_factory=asyncio.Event, repr=False)
    _watcher: asyncio.Task[None] | None = field(default=None, repr=False)
    _cancelled: bool = field(default=False, repr=False)
    #: Bumped whenever every speculative read is discarded. A `read_after`
    #: already in flight started from the cursor being discarded, so its records
    #: are speculative too; the watcher compares this across the await and drops
    #: them rather than appending them on top of the reset -- which would put the
    #: buffer straight back where it was and re-advance `prefetch_cursor`.
    _prefetch_epoch: int = field(default=0, repr=False)

    #: Wakes owed because local readiness could not be delivered. Counted rather
    #: than merely logged, so a test can tell "no wake was needed" from "a wake
    #: was needed and dropped".
    wakes_owed: int = 0

    installed_park_generation: int | None = None
    """The generation of the park intent this manager has in the backend, if any.

    The manager's mirror of one piece of backend state, kept so the invariant
    *an intent exists only while that park is outstanding* is enforceable from
    here: the manager is the only installer, so it is the only thing that can
    know an intent is owed a removal. ``None`` means no intent of ours is
    installed, and the removal path is then free -- which matters because a
    resolve activation is the ordinary live-delivery path, not a rare one.
    """

    def __post_init__(self) -> None:
        self._has_room.set()

    # --- the Workflow thread's half (synchronous, never blocks) -------------

    @property
    def buffered(self) -> int:
        with self._lock:
            return len(self._buffer)

    def drain(self, max_records: int | None = None) -> list[StreamRecord]:
        """Pops buffered records. **The only thing ``_apply`` may call.**

        Bounded, non-blocking, and performs no I/O -- which is what makes the
        activation it serves safe under the 2-second deadlock timeout.
        """
        with self._lock:
            limit = len(self._buffer) if max_records is None else max_records
            popped = [
                self._buffer.popleft() for _ in range(min(limit, len(self._buffer)))
            ]
        if popped:
            last = popped[-1].offset
            assert last is not None
            self.delivery_cursor = AFTER(last)
        return popped

    def note_wait_generation(self, generation: int) -> None:
        """Takes the wait generation the runtime just moved to.

        Called from the Workflow thread the moment the wait re-enters the
        blocked state. Without it this stays 0 for the life of the
        subscription, every readiness report after the first block names a
        generation Core has already left behind, and Core answers ``Stale`` --
        which the watcher treats as "re-probe later" while its prefetch cursor
        is already past the record. The append is then never re-announced and
        the Workflow is never woken.
        """
        with self._lock:
            self.wait_generation = generation

    def current_wait_generation(self) -> int:
        """The generation to report readiness under, read on the Worker's loop."""
        with self._lock:
            return self.wait_generation

    def blocked_cursor(self) -> Cursor:
        """Where this subscription's deliveries stopped.

        The terminal's boundary. Fixed the moment the last activation returned,
        so it is never refreshed against the backend -- doing that could name a
        position replay must not reproduce.
        """
        return self.delivery_cursor

    # --- the manager loop's half --------------------------------------------

    def _append(self, records: list[StreamRecord]) -> None:
        with self._lock:
            self._buffer.extend(records)
            full = len(self._buffer) >= self.buffer_size
        last = records[-1].offset
        assert last is not None
        self.prefetch_cursor = AFTER(last)
        if full:
            self._has_room.clear()

    def _room(self) -> int:
        with self._lock:
            return max(0, self.buffer_size - len(self._buffer))

    def note_drained(self) -> None:
        """Tells the watcher there is room again.

        Called from the Workflow thread through the manager, which hops it onto
        the loop -- an asyncio.Event is not thread-safe to set directly.
        """
        self._has_room.set()

    def reset_to_committed(self) -> None:
        """Discards every speculative read, as eviction and failure require.

        Reading is not consuming and consuming is not committing, so nothing
        here was ever a claim. This is exactly why "no cursor advances unless
        the marker commits" is safe to state.
        """
        with self._lock:
            self._buffer.clear()
            self._prefetch_epoch += 1
        self.delivery_cursor = self.committed_cursor
        self.prefetch_cursor = self.committed_cursor
        self._has_room.set()

    def commit(self, cursor: Cursor) -> None:
        """Advances the committed cursor when a marker commits."""
        self.committed_cursor = cursor

    def reposition_to(self, cursor: Cursor) -> None:
        """Commits a marker's boundary and restarts every read from it.

        What replay needs afterwards. While replay was handing this Workflow the
        records the marker recorded, the watcher was independently reading the
        *same* records from the subscription's start cursor into this buffer --
        nothing had told it the marker exists. Leaving it there hands Workflow
        code every replayed record a second time the moment the next live drain
        happens.

        The boundary comes from the marker rather than from what the buffer
        happens to hold, because the marker is the only durable statement of
        where consumption reached.
        """
        self.commit(cursor)
        self.reset_to_committed()


class PreparedRecord(StreamRecord):
    """A record whose payload has already had retrieval and codec applied.

    A subclass rather than a field on :py:class:`StreamRecord`, because being
    prepared is not a property of a record -- it is a property of *this
    Worker's* handling of one, and a record read by a producer, written by a
    backend, or compared for idempotency has no such half-state. Everything that
    reads a record reads the same fields; only the delivery path looks for the
    two added here.

    ``payload`` is deliberately left as the backend's bytes. Replacing it would
    make the record no longer equal to what the stream holds, which is what
    idempotency comparison and integrity validation are expressed in.
    """

    #: The payload as the payload converter will see it, or ``None`` if
    #: preparing it raised.
    prepared_payload: Any = None

    #: What preparing raised, carried rather than raised on the Worker's loop.
    #: The watcher has no Workflow Task to fail and no Workflow to tell, and a
    #: record that is never delivered must not fail anything at all -- so the
    #: error travels with the record and is raised by the delivery that would
    #: have yielded its value.
    prepare_error: BaseException | None = None

    def __eq__(self, other: object) -> bool:
        """Equal to the record it was built from, and to any equal record.

        A frozen dataclass's generated ``__eq__`` compares classes exactly, so
        without this a prepared record would be unequal to the identical
        ``StreamRecord`` a caller built to compare against -- and being prepared
        is a property of this Worker's handling, not of the record. Preparation
        is deliberately not part of the comparison for the same reason.
        """
        if isinstance(other, StreamRecord):
            return self._fields() == PreparedRecord._fields(other)
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._fields())

    def _fields(self) -> tuple[Any, ...]:
        return (
            self.kind,
            self.payload,
            self.producer_session_id,
            self.sequence,
            self.offset,
        )

    @classmethod
    def of(
        cls,
        record: StreamRecord,
        prepared: Any,
        error: BaseException | None,
    ) -> PreparedRecord:
        out = cls(
            kind=record.kind,
            payload=record.payload,
            producer_session_id=record.producer_session_id,
            sequence=record.sequence,
            offset=record.offset,
        )
        # `StreamRecord` is frozen, and these two are this subclass's own
        # fields rather than dataclass fields, so they are set the same way a
        # frozen dataclass sets anything.
        object.__setattr__(out, "prepared_payload", prepared)
        object.__setattr__(out, "prepare_error", error)
        return out


class StreamSubscriptionManager:
    """Every subscription on one Worker, keyed by Run.

    Keying by Run ID is what keeps a stale Run from leaking connections: the
    teardown path for eviction, Workflow Task failure, and shutdown is the same
    one, and it is reached by Run.
    """

    def __init__(
        self,
        *,
        backends: Mapping[str, StreamBackend],
        notify_ready: ReadinessNotifier,
        send_wake: WakeSender | None = None,
        run_status: Callable[[str], Awaitable[Any]] | None = None,
        shutdown_wake_failed_metric: Callable[[Any], None] | None = None,
        client_identity: str = "",
        data_converter: temporalio.converter.DataConverter | None = None,
        buffer_size: int = DEFAULT_BUFFER_SIZE,
        watch_block: timedelta = DEFAULT_WATCH_BLOCK,
    ) -> None:
        self._backends = backends
        #: The Worker's own converter, used for the **asynchronous half** of
        #: decoding only -- retrieval and codec, never the payload converter,
        #: which needs a type only Workflow code knows. Optional so a manager
        #: can be constructed in isolation; a Worker always supplies it, and a
        #: record that reaches the Workflow thread unprepared under a converter
        #: that had something to do is refused there rather than decoded late.
        self._data_converter = data_converter
        self._notify_ready = notify_ready
        self._send_wake = send_wake
        self._run_status = run_status
        self._metric = shutdown_wake_failed_metric
        #: This Worker's sender identity for unparked wakes. Drawn once here
        #: rather than taken from the client identity, because two Workers in
        #: one process share a `Client`: with the client identity their first
        #: unparked wakes derive the same request ID, the server deduplicates
        #: the second, and the Run that the surviving Worker picked up never
        #: gets a Workflow Task. Fixed for this manager's lifetime, so the
        #: shutdown sweep's retry stays the same wake rather than a new one.
        self.wake_sender_identity = new_sender_identity(client_identity)
        #: Wakes the shutdown sweep could not get acknowledged. Reported through
        #: `external_stream_shutdown_wake_failed`; kept here so a test can tell
        #: "no wake was needed" from "a wake was needed and lost".
        self.shutdown_wake_failures = 0
        self._buffer_size = buffer_size
        self._watch_block = watch_block
        self._runs: dict[str, dict[int, Subscription]] = {}
        #: Replay plans read and validated ahead of the delivering activation.
        self._replay_plans: dict[str, ReplayPlan] = {}
        # The loop the watchers run on, captured at construction because that is
        # the Worker's loop. `register` is called from the *Workflow executor
        # thread*, which has no loop of its own and must never touch this one
        # directly.
        self._loop = asyncio.get_event_loop()
        self._shutting_down = False
        #: Whether the sweep has already run, so `shutdown` can tell "nobody
        #: swept" -- which it must then do itself -- from "already swept".
        self._swept = False
        #: What the probe phase heard, per Run. Recorded rather than acted on,
        #: because the two halves of the sweep belong at different points of the
        #: Worker's shutdown -- see `probe_runs`.
        self._probed: dict[str, str] = {}
        #: One Run's park-intent work, serialized. See `_park_lock`.
        self._park_locks: dict[str, asyncio.Lock] = {}
        #: Strong references to the in-flight registration-time reconciliations.
        self._reconciliations: set[asyncio.Task[None]] = set()

    # --- registration -------------------------------------------------------

    def register(
        self,
        *,
        run_id: str,
        wait_id: int,
        stream_key: StreamKey,
        backend_name: str,
        start_cursor: Cursor = BEGINNING,
    ) -> Subscription:
        """Registers a wait and starts its watcher. Non-blocking.

        Called from the Workflow thread, so it must not await: the watcher task
        is scheduled onto the manager's loop rather than started here.
        """
        backend = self._backends[backend_name]
        subscription = Subscription(
            run_id=run_id,
            wait_id=wait_id,
            stream_key=stream_key,
            backend=backend,
            buffer_size=self._buffer_size,
            committed_cursor=start_cursor,
            delivery_cursor=start_cursor,
            prefetch_cursor=start_cursor,
        )
        replaced = self._runs.setdefault(run_id, {}).get(wait_id)
        if replaced is not None:
            # A wait re-registered under a key that already has one. The old
            # subscription is unreachable from here on, and its watcher would go
            # on polling the backend for the life of the process -- a leak that
            # only shows up as a connection that never closes.
            replaced._cancelled = True
            self._loop.call_soon_threadsafe(self._cancel_watcher, replaced)
        self._runs[run_id][wait_id] = subscription
        # `create_task` is not thread-safe, and this runs on the Workflow
        # executor thread. Scheduling the start onto the manager's loop is the
        # difference between a watcher that runs and one that is silently never
        # scheduled -- which looks exactly like a stream that never delivers.
        self._loop.call_soon_threadsafe(self._start_watcher, subscription)
        return subscription

    def _cancel_watcher(self, subscription: Subscription) -> None:
        watcher = subscription._watcher
        if watcher is not None and not watcher.done():
            watcher.cancel()

    def _start_watcher(self, subscription: Subscription) -> None:
        """Starts a watcher, and reconciles the park state it inherited.

        Both on the manager's own loop, because both are `create_task` calls and
        `register` runs on the Workflow executor thread.
        """
        if subscription._cancelled or subscription._watcher is not None:
            return
        reconcile = self._loop.create_task(self._reconcile_inherited_park(subscription))
        # Held, because the loop keeps only a weak reference to a running task:
        # an unreferenced one can be collected mid-await, and this one's whole
        # job is a backend round trip.
        self._reconciliations.add(reconcile)
        reconcile.add_done_callback(self._reconciliations.discard)
        subscription._watcher = self._loop.create_task(self._watch(subscription))

    async def _reconcile_inherited_park(self, subscription: Subscription) -> None:
        """Removes a park intent this Worker inherited rather than installed.

        `installed_park_generation` is a *mirror* of backend state, and it lives
        on the Worker that installed the park. The intent it mirrors is durable:
        it survives eviction, a Workflow Task that moved to another Worker, and
        shutdown, all of which take the mirror with them. Removal keyed on the
        mirror alone therefore cannot reach the one class of intent that most
        needs reaching -- the intents whose installer is gone -- and no later
        resolve can repair it, because the resolve looks at the same empty
        mirror.

        Registration is where a Worker learns such an intent exists, and it is
        also the moment its status is unambiguous. A subscription is registered
        by user Workflow code running, and no user code runs inside a park
        (`wft-lifecycle.md`), so an intent found here belongs to a park that is
        over: the Core that confirmed it has either moved on or gone with the
        Worker that held it. What leaving it costs is the invariant's whole
        point -- `current_park_generation` keeps answering a generation Core has
        discarded, every producer wake names that generation and Core discards
        it as stale, and because a parked wake's request ID ignores sender
        identity the second such wake is byte-identical to the first and the
        server deduplicates it away. The Workflow then waits forever on a record
        that is durably present.

        Best-effort for the same reason :meth:`resolve_park` is: this is the
        registration path of a Run that is already running, and a momentary
        backend failure here must not fail a Workflow Task. The next
        registration -- the next eviction and pick-up -- asks again.
        """
        async with self._park_lock(subscription.run_id):
            if (
                subscription._cancelled
                or subscription.installed_park_generation is not None
            ):
                # A park confirmed since this was scheduled is *this* manager's,
                # and `resolve_park` owns it. Removing it here would take the
                # intent out from under a park that really is outstanding.
                return
            try:
                inherited = await subscription.backend.park_intent(
                    subscription.stream_key, subscription.wait_id
                )
                if inherited is None:
                    # The ordinary case, and the reason this is a read before it
                    # is a write: a Run that never parked owes the backend
                    # nothing.
                    return
                if (
                    subscription._cancelled
                    or subscription.installed_park_generation is not None
                ):
                    # Asked again across the read, because this Run's state can
                    # have moved on entirely while it was in flight: evicted and
                    # picked up again, with a *new* park confirmed for the same
                    # key. Removing what was found before that would strand the
                    # park that replaced it.
                    return
                logger.info(
                    "Removing an inherited external stream park intent for %s "
                    "wait %s: park generation %s was confirmed by a Worker that "
                    "no longer holds this Run",
                    subscription.stream_key,
                    subscription.wait_id,
                    inherited.park_generation,
                )
                await subscription.backend.remove_park_intent(
                    subscription.stream_key, subscription.wait_id
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Failed removing an inherited park intent for %s wait %s; "
                    "it will be retried the next time this wait is registered",
                    subscription.stream_key,
                    subscription.wait_id,
                )

    def _park_lock(self, run_id: str) -> asyncio.Lock:
        """Serializes one Run's park-intent work on the manager's loop.

        The install/recheck handshake, the resolve, and the reconciliation above
        all read-then-write the same `(stream key, wait_id)` objects, and the
        reconciliation is scheduled from another thread, so their interleaving
        is not otherwise constrained. Without this, a reconciliation that
        overlapped a confirming park could remove the intent that park had just
        installed -- an unwakeable Run produced by the very code that exists to
        prevent one.
        """
        lock = self._park_locks.get(run_id)
        if lock is None:
            lock = self._park_locks[run_id] = asyncio.Lock()
        return lock

    def subscription(self, run_id: str, wait_id: int) -> Subscription | None:
        return self._runs.get(run_id, {}).get(wait_id)

    def subscriptions(self, run_id: str) -> list[Subscription]:
        return list(self._runs.get(run_id, {}).values())

    def runs_with_subscriptions(self) -> list[str]:
        return [run_id for run_id, subs in self._runs.items() if subs]

    # --- the Workflow thread's entry points ---------------------------------

    def drain(
        self, run_id: str, wait_id: int, max_records: int | None = None
    ) -> list[StreamRecord]:
        """Pops from one subscription's buffer. Performs no I/O."""
        subscription = self.subscription(run_id, wait_id)
        if subscription is None:
            return []
        popped = subscription.drain(max_records)
        if popped:
            # Freeing room restarts prefetch. Hopped onto the loop because an
            # asyncio.Event may not be set from another thread.
            self._loop.call_soon_threadsafe(subscription.note_drained)
        return popped

    def note_wait_generation(self, run_id: str, wait_id: int, generation: int) -> None:
        """Hands one wait's current generation to the subscription reporting it.

        Called from the Workflow thread, and deliberately *not* hopped onto the
        manager's loop: the next readiness report may be issued by a watcher
        before a queued callback would run, and it would then name the stale
        generation this exists to replace. The write is a single guarded field
        assignment, so it is safe to make directly.
        """
        subscription = self.subscription(run_id, wait_id)
        if subscription is not None:
            subscription.note_wait_generation(generation)

    def rearm_ready(self, run_id: str) -> None:
        """Re-reports readiness for every buffer this Run left non-empty.

        The per-activation delivery budget stops an iterator with records still
        buffered. Nothing else will announce them: readiness is reported once,
        when the watcher buffers a record, and that watcher has long since moved
        its prefetch cursor past these. Without this the Workflow blocks forever
        on records already sitting in front of it -- and worse, it blocks
        *quiescently*, so Core would start the idle timer and eventually park a
        Workflow Task whose data had already arrived.

        Called from the Workflow thread at activation completion, so the work is
        hopped onto the manager's loop: `create_task` is not thread-safe, and a
        task created from the Workflow executor thread is silently never
        scheduled -- indistinguishable from a stream that never delivers.
        """
        self._loop.call_soon_threadsafe(self._rearm_ready, run_id)

    def _rearm_ready(self, run_id: str) -> None:
        """The manager-loop half of :meth:`rearm_ready`."""
        for subscription in self.subscriptions(run_id):
            if subscription._cancelled or not subscription.buffered:
                continue
            self._loop.create_task(self._report_ready(subscription))

    def reposition_to_committed(
        self, run_id: str, cursors: Mapping[int, Cursor]
    ) -> None:
        """Moves each named wait to the boundary a replayed marker committed.

        Called from the Workflow thread once replay has delivered a marker's
        recorded ranges, so the work is hopped onto the manager's loop for the
        same reason :meth:`rearm_ready` hops: an ``asyncio.Event`` may not be
        set from another thread, and a watcher task created from the Workflow
        executor thread is silently never scheduled.
        """
        self._loop.call_soon_threadsafe(
            self._reposition_to_committed, run_id, dict(cursors)
        )

    def _reposition_to_committed(
        self, run_id: str, cursors: Mapping[int, Cursor]
    ) -> None:
        """The manager-loop half of :meth:`reposition_to_committed`."""
        for wait_id, cursor in cursors.items():
            subscription = self.subscription(run_id, wait_id)
            if subscription is None or subscription._cancelled:
                # A wait the marker records but this Run no longer holds. Replay
                # itself reports that as nondeterminism; there is nothing to
                # reposition here and nothing to say about it.
                continue
            subscription.reposition_to(cursor)

    def blocked_snapshot(self, run_id: str) -> dict[int, Cursor]:
        """Where every active subscription's deliveries stopped.

        Read from manager state alone -- no provider call, no backend read.
        The boundary is not "wherever the stream is now"; it is where this
        Workflow Task's deliveries stopped, which is already fixed.
        """
        return {
            wait_id: sub.blocked_cursor()
            for wait_id, sub in sorted(self._runs.get(run_id, {}).items())
        }

    # --- watchers -----------------------------------------------------------

    async def _watch(self, subscription: Subscription) -> None:
        """Prefetches into the buffer and reports readiness once buffered.

        Survives Workflow Task completion. Torn down only on cancellation, Run
        eviction, or Worker shutdown -- because the window between tasks is
        exactly when an append most needs someone watching for it.
        """
        try:
            while not subscription._cancelled:
                room = subscription._room()
                if room == 0:
                    # Backpressure. Stop reading; drop nothing, block nobody.
                    subscription._has_room.clear()
                    await subscription._has_room.wait()
                    continue

                # Captured before the read, so that a reposition landing while
                # it is in flight is detected afterwards. The read started from
                # a cursor that no longer describes this subscription, and
                # appending its result would undo the reposition rather than
                # merely race it.
                epoch = subscription._prefetch_epoch
                try:
                    records = await subscription.backend.read_after(
                        subscription.stream_key,
                        subscription.prefetch_cursor,
                        max_records=room,
                        block=self._watch_block,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # Watcher failures are retried within provider policy. The
                    # Workflow Task is not failed from out here -- it may not
                    # even exist.
                    logger.exception(
                        "External stream watcher failed for %s wait %s",
                        subscription.stream_key,
                        subscription.wait_id,
                    )
                    await asyncio.sleep(0.05)
                    continue

                if not records:
                    continue

                # Prepared **before** buffering, so that what the Workflow
                # thread pops is already a value waiting to be typed. This is
                # the whole reason the manager holds a converter: a codec
                # awaited on the Workflow thread performs I/O in a
                # deterministic event loop.
                #
                # And before the epoch check rather than after it, because
                # preparing awaits: a reposition landing while a user's codec
                # runs would otherwise slip past a check that had already
                # passed, and the retracted records would go into the buffer
                # anyway. The check stays the last thing before the append.
                prepared = await self._prepare(records)

                if subscription._prefetch_epoch != epoch:
                    # Discarded rather than buffered: these were read from a
                    # position a reposition has since retracted, so they are
                    # records the marker already accounts for. The next pass
                    # reads from the new cursor.
                    continue

                subscription._append(prepared)
                await self._report_ready(subscription)
        except asyncio.CancelledError:
            pass

    async def _prepare(self, records: list[StreamRecord]) -> list[StreamRecord]:
        """Runs the DataConverter's asynchronous half, here on the Worker's loop.

        Every record is prepared, including ones the Workflow may never take:
        this is the same bargain the Worker already makes for an activation's
        payloads, which are decoded in full before the executor sees any of
        them. The cost of preparing a record that is later discarded is one
        codec call; the cost of *not* preparing it is that the Workflow thread
        has to, and the Workflow thread cannot.

        **Nothing raises out of here.** A failure is carried on the record and
        raised by the delivery that would have yielded its value, which is the
        only point at which a Workflow exists to be told. Raising here would
        kill the watcher for the Run -- taking every later record with it -- and
        would report a failure for a record the Workflow might never have asked
        for.

        Control records carry no payload by construction, so they pass through.
        """
        if self._data_converter is None:
            return records
        from temporalio.contrib.external_workflow_streams._codec import (
            StreamPayloadCodec,
        )

        # No type: the type belongs to the topic, and the topic belongs to
        # Workflow code. Nothing out here needs it, because nothing out here
        # runs the payload converter.
        codec: StreamPayloadCodec[Any] = StreamPayloadCodec(self._data_converter, None)
        prepared: list[StreamRecord] = []
        for record in records:
            if record.is_control:
                prepared.append(record)
                continue
            try:
                prepared.append(
                    PreparedRecord.of(record, await codec.prepare(record.payload), None)
                )
            except asyncio.CancelledError:
                raise
            except Exception as err:
                logger.debug(
                    "Preparing external stream record at %s failed; the error "
                    "travels with the record",
                    record.offset,
                    exc_info=True,
                )
                prepared.append(PreparedRecord.of(record, None, err))
        return prepared

    async def _report_ready(self, subscription: Subscription) -> None:
        """Tells Core a record is buffered, and acts on which answer comes back.

        **Total, except for cancellation.** Nothing but `CancelledError` may
        leave this method. The watcher calls it in its loop, so an exception
        escaping ends that watcher for good -- the subscription stays registered,
        its buffer keeps its records, and nothing ever announces them again.
        `_rearm_ready` also launches it with `create_task`, where an exception
        becomes a task result nobody retrieves and the failure is not even
        logged.
        """
        result = await self._notify_ready_with_retries(subscription)

        if result == ReadinessResult.ACCEPTED:
            # Core will activate; the record is announced and this is done.
            return

        if result == ReadinessResult.STALE:
            # Core is holding a newer generation for this wait than the one just
            # named, so the report was for a block that has already been
            # resolved. Re-report against the current generation rather than
            # returning: the watcher only calls back here after a *new*
            # non-empty read, and `prefetch_cursor` is already past the record
            # in the buffer, so nothing would announce it a second time. A
            # record announced to nobody is a Workflow blocked forever on data
            # it is already holding.
            if await self._retry_stale(subscription):
                return

        # Everything left means local readiness could not be delivered, so a
        # Signal is owed. They differ in what happens to the watcher afterwards.
        subscription.wakes_owed += 1
        if self._send_wake is not None:
            try:
                await self._send_wake(subscription)
            except asyncio.CancelledError:
                raise
            except Exception:
                # The wake sender reports failure by raising, because a wake
                # counted as delivered when it was not is the failure this whole
                # path exists to prevent. `wakes_owed` stays incremented and the
                # shutdown sweep remains the backstop.
                logger.exception(
                    "External stream wake failed for %s wait %s",
                    subscription.stream_key,
                    subscription.wait_id,
                )

        if result == ReadinessResult.RUN_NOT_FOUND:
            # The Run is gone from this Worker. Nothing here can serve it again.
            subscription._cancelled = True
            self._runs.get(subscription.run_id, {}).pop(subscription.wait_id, None)
        # PARKED and NO_OPEN_WORKFLOW_TASK both *keep* the watcher: the Run is
        # still cached and this is the normal window between Workflow Tasks.

    async def _notify_ready_with_retries(self, subscription: Subscription) -> str:
        """Reports readiness, retrying a failing call a bounded number of times.

        A raising notifier is a transport problem, not an answer. Letting it
        escape kills the watcher; swallowing it and returning would claim the
        record was announced. Retrying and then falling through to the
        wake-owed branch treats an unreachable Core as what it is: local
        readiness could not be delivered, which is the sixth case the five
        answers do not name.
        """
        for attempt in range(READINESS_ATTEMPTS):
            try:
                return _result_value(
                    await self._notify_ready(
                        subscription.run_id,
                        subscription.wait_id,
                        subscription.current_wait_generation(),
                    )
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    "External stream readiness report %s/%s failed for %s wait %s",
                    attempt + 1,
                    READINESS_ATTEMPTS,
                    subscription.stream_key,
                    subscription.wait_id,
                    exc_info=True,
                )
                if attempt + 1 < READINESS_ATTEMPTS:
                    await asyncio.sleep(READINESS_RETRY_DELAY.total_seconds())
        return ReadinessResult.NO_OPEN_WORKFLOW_TASK

    async def _retry_stale(self, subscription: Subscription) -> bool:
        """Re-reports a stale readiness against the generation Core now holds.

        Returns whether the record ended up announced. A generation moves when
        the wait re-enters the blocked state, which is Workflow code coming back
        around to it -- so the report that raced it is answered by the next one.
        Bounded, because a wait that keeps moving is a Workflow consuming
        happily, and a wake owed at the end of that costs one empty Workflow
        Task rather than a silent stall.
        """
        for _ in range(STALE_REPORT_ATTEMPTS):
            await asyncio.sleep(STALE_RETRY_DELAY.total_seconds())
            if subscription._cancelled or not subscription.buffered:
                return True
            if await self._notify_ready_with_retries(subscription) == (
                ReadinessResult.ACCEPTED
            ):
                return True
        return False

    # --- the runtime-only jobs' backend work (P19) --------------------------

    async def prepare_park(
        self, run_id: str, park_generation: int, blocked: Mapping[int, Cursor]
    ) -> bool:
        """Installs park intents, then rechecks every stream.

        Returns ``True`` if any stream became ready, which abandons this parking
        generation.

        ``blocked`` **is the park set**, not a lookup table beside one: its keys
        are the waits Core asked to park, taken from
        ``PrepareExternalStreamPark.waits``, and its values are their cursor
        boundaries. The manager's own registration list is a superset -- a
        subscription that delivered a record and was not awaited again is
        registered and not blocked -- and parking the superset is wrong in both
        directions. A recheck for a wait outside the set finds records nothing
        is waiting on and aborts a legitimate park, which then runs again on the
        next idle timeout and aborts again; and an intent installed for a wait
        outside the set is an intent with no park behind it, which is exactly
        what `backend-contract.md` forbids leaving in a backend.

        The order is what closes the append/park race: a producer appends its
        record *before* it observes the park generation, so an append is either
        seen by the recheck below or paired with a wake Signal. Rechecking
        before all the intents were installed would leave a window where it is
        neither.
        """
        async with self._park_lock(run_id):
            with _as_storage_failure("installing external stream park intents"):
                return await self._prepare_park(run_id, park_generation, blocked)

    async def _prepare_park(
        self, run_id: str, park_generation: int, blocked: Mapping[int, Cursor]
    ) -> bool:
        """The handshake itself, under this Run's park lock."""
        subscriptions = [
            subscription
            for subscription in self.subscriptions(run_id)
            if subscription.wait_id in blocked
        ]
        missing = set(blocked) - {s.wait_id for s in subscriptions}
        if missing:
            # Core is parking a wait this manager no longer holds. Nothing can
            # be installed for it and there is nothing to recheck; a producer on
            # that stream finds no intent and sends the unparked wake, which
            # Core accepts as a recheck request (ADR-023).
            logger.warning(
                "Core asked run %s to park waits %s, which this Worker does not "
                "hold; they are left out of the park set",
                run_id,
                sorted(missing),
            )

        installed: list[Subscription] = []
        try:
            for subscription in subscriptions:
                await subscription.backend.install_park_intent(
                    subscription.stream_key,
                    ParkIntent(
                        wait_id=subscription.wait_id,
                        cursor=blocked[subscription.wait_id],
                        park_generation=park_generation,
                        run_id=run_id,
                    ),
                )
                subscription.installed_park_generation = park_generation
                installed.append(subscription)

            became_ready = False
            for subscription in subscriptions:
                if await subscription.backend.recheck(
                    subscription.stream_key, subscription.wait_id
                ):
                    became_ready = True
                    break
        except asyncio.CancelledError:
            raise
        except Exception:
            # A park that failed part-way through is still a park every producer
            # can see. The activation fails and Core confirms nothing, so those
            # intents describe a park that does not exist -- and an eviction then
            # takes the local mirror away while they stay. Rolling back is what
            # keeps "all-or-nothing across the set" true of the failure path too.
            # Removal failures are swallowed: the storage error that got here is
            # the one worth reporting, and anything still installed stays owed
            # through `installed_park_generation` for the next resolve to retry.
            await self._withdraw_park(installed)
            raise

        if became_ready:
            # All-or-nothing: a park confirmed for a set with a ready member
            # would lose that member's record until a producer happened to
            # signal, so every intent installed above comes back out.
            failures = await self._withdraw_park(installed)
            if failures:
                # Nothing may report `became_ready` while an intent for that
                # generation is still installed. Failing the activation retries
                # the whole Workflow Task, which commits no cursor and loses no
                # record; the removal stays owed either way.
                raise failures[0]
        return became_ready

    async def _withdraw_park(
        self, subscriptions: list[Subscription]
    ) -> list[Exception]:
        """Takes every intent one attempted park installed back out.

        Total: every subscription is attempted whatever the others do, because a
        rollback that stopped at its first failure would leave behind exactly
        the orphans it exists to prevent. The failures are returned rather than
        raised so each caller can decide which error is the one worth reporting.
        """
        failures: list[Exception] = []
        for subscription in subscriptions:
            try:
                await self._remove_park_intent(subscription)
            except asyncio.CancelledError:
                raise
            except Exception as err:
                logger.exception(
                    "Failed removing the park intent for %s wait %s while "
                    "withdrawing an unconfirmed park; it stays owed and the "
                    "next resolve retries it",
                    subscription.stream_key,
                    subscription.wait_id,
                )
                failures.append(err)
        return failures

    async def resolve_park(self, run_id: str) -> None:
        """Removes the intents of a park this Run is no longer sitting in.

        The other half of :meth:`prepare_park`, and the reason the invariant is
        stated as *an intent exists only while that park is outstanding* rather
        than as "an aborted park cleans up after itself". An aborted park was
        only ever the cheaper half: a **confirmed** park ends too, when a wake
        Signal or a fresh quiescent snapshot clears Core's ``park_generation``,
        and Core's own state moving on is not something the backend can observe.

        A left-behind intent is not inert, because it is the answer
        ``current_park_generation`` gives to everyone who asks:

        - a **producer** reads it to decide what its wake Signal names, and a
          dead generation is precisely the claim Core is designed to discard as
          stale -- so the producer appends, signals, and the Workflow is never
          woken by it;
        - the **shutdown sweep** reads it through the same call, and would send a
          parked wake where P20 requires the unparked one (ADR-023). Worse than
          useless there: a parked wake's request ID deliberately ignores sender
          identity, so it comes out identical to the wake that already resolved
          that generation and the server deduplicates it away.

        This is the half for parks *this* manager installed, and it is only
        half. An intent whose installer is gone -- evicted, moved to another
        Worker, shut down -- leaves no mirror here to remove it by, and is taken
        out by :meth:`_reconcile_inherited_park` when the wait is registered
        again instead.

        Driven by ``ResolveExternalStreamWaits``, which is Core telling this
        Worker the wait set has moved on -- the one event that covers both ways a
        confirmed park ends, and the aborted-in-Core-but-confirmed-in-lang case
        the recheck cannot see either.

        Failure is logged rather than raised. The removal is cleanup, and turning
        a momentary backend blip into a failed Workflow Task on the *delivery*
        path would trade a stale intent for a repeated Workflow Task. The
        generation stays recorded, so the next resolve retries it.
        """
        async with self._park_lock(run_id):
            for subscription in self.subscriptions(run_id):
                try:
                    await self._remove_park_intent(subscription)
                except Exception:
                    logger.exception(
                        "Failed removing the resolved park intent for %s wait %s; "
                        "it will be retried when this Run's waits next resolve",
                        subscription.stream_key,
                        subscription.wait_id,
                    )

    async def _remove_park_intent(self, subscription: Subscription) -> None:
        """Removes one installed intent, and forgets it only once it is gone."""
        if subscription.installed_park_generation is None:
            return
        await subscription.backend.remove_park_intent(
            subscription.stream_key, subscription.wait_id
        )
        subscription.installed_park_generation = None

    async def prepare_replay(self, run_id: str, replay_annotation: bytes) -> ReplayPlan:
        """Reads and validates every recorded range, before any delivery.

        Runs here rather than in ``_apply`` because an inclusive range read over
        a whole recorded batch is exactly the multi-second operation the
        Workflow thread's deadlock timeout cannot accommodate -- and it gets
        worse the more records the marker recorded.

        Subscriptions may not exist yet: on replay the Workflow has not run far
        enough to call ``subscribe()``. The annotation's own header carries the
        stream key, the backend name, and the provider identity for each wait,
        which is why it records them.
        """
        from temporalio.contrib.external_workflow_streams._annotation import (
            decode_annotation,
        )

        annotation = decode_annotation(replay_annotation)
        backends: dict[int, StreamBackend] = {}
        stream_keys: dict[int, StreamKey] = {}
        for wait_id, binding in annotation.header.streams.items():
            stream_keys[wait_id] = binding.stream_key
            resolved = self._replay_backend(wait_id, binding)
            if resolved is not None:
                backends[wait_id] = resolved

        plan = await build_replay_plan(replay_annotation, backends, stream_keys)
        # Replay delivers through the same drain the live path does, so it has
        # to arrive in the same condition: prepared. This is the only chance to
        # do it -- by the time the segments are delivered, the Workflow thread
        # is running, and the ranges have already been validated here, so a
        # decode failure from now on is a converter mismatch rather than
        # integrity loss (ADR-015).
        for index, segment in enumerate(plan.segments):
            plan.segments[index] = ReplaySegment(
                tuple(
                    (wait_id, prepared)
                    for (wait_id, _), prepared in zip(
                        segment.deliveries,
                        await self._prepare([r for _, r in segment.deliveries]),
                    )
                )
            )
        self._replay_plans[run_id] = plan
        return plan

    def _replay_backend(
        self, wait_id: int, binding: StreamBinding
    ) -> StreamBackend | None:
        """The one backend a recorded wait may be read from.

        Selection is by the **name the Workflow itself named**, never by a
        search for something that declares the recorded provider id. A provider
        id names an implementation, not a store: two Redis instances -- separate
        clusters, or one cluster with separate key prefixes -- declare the same
        id and hold entirely different records, so picking the first match reads
        one wait's recorded range out of a store that never held it. That does
        not fail cleanly either; the range simply is not there, and it surfaces
        as integrity loss against a backend nothing is wrong with.

        Returning ``None`` leaves the wait unresolved, which
        :func:`build_replay_plan` reports as nondeterminism if the annotation
        recorded any records for it. That is the right reading of a name the
        Workflow no longer subscribes: the name is Workflow code.
        """
        backend = self._backends.get(binding.backend_name)
        if backend is None:
            return None

        # Whether the name still resolves to the *same implementation* is a
        # deployment question, not a Workflow one: the Workflow is unchanged and
        # the backend is undamaged, so neither nondeterminism nor integrity loss
        # describes it. It is reported as a storage failure -- retried, and it
        # clears when a Worker carrying the recorded implementation picks the
        # task up -- and it is raised before any read, so an incompatible
        # implementation never gets to interpret the recorded offsets at all.
        declared_id = type(backend).provider_id
        if declared_id != binding.provider_id:
            raise StreamStorageError(
                f"external stream wait {wait_id} was recorded against provider "
                f"{binding.provider_id!r}, but the backend registered on this "
                f"Worker as {binding.backend_name!r} declares {declared_id!r}. "
                "This Worker cannot read what that marker recorded; register the "
                "recorded provider under that name."
            )
        declared_version = type(backend).provider_format_version
        if declared_version != binding.provider_format_version:
            raise StreamStorageError(
                f"external stream wait {wait_id} was recorded by provider "
                f"{binding.provider_id!r} format version "
                f"{binding.provider_format_version}, but the backend registered "
                f"as {binding.backend_name!r} implements format version "
                f"{declared_version}. Reading it would interpret the recorded "
                "offsets under a format they were not written in."
            )
        return backend

    def take_replay_plan(self, run_id: str) -> ReplayPlan | None:
        """The prepared plan, consumed once by the delivering activation."""
        return self._replay_plans.pop(run_id, None)

    # --- teardown -----------------------------------------------------------

    def cancel_from_workflow_thread(self, run_id: str, wait_id: int) -> None:
        """Schedules :meth:`cancel` from the Workflow thread.

        Workflow code closes a subscription inside the synchronous
        ``activate()``, on the executor thread, where creating a task is not
        merely unsafe but silently ineffective: the task is never scheduled, and
        a watcher that was supposed to stop keeps running with nothing to say so.
        """
        self._loop.call_soon_threadsafe(
            lambda: self._loop.create_task(self.cancel(run_id, wait_id))
        )

    async def cancel(self, run_id: str, wait_id: int) -> None:
        """Cancels one subscription: remove its intent, drop its buffer, stop it.

        The intent is removed **here**, not left to the resolve path. That path
        can afford to log a failure and try again because the subscription stays
        registered; this one drops it, so nothing later can retry and an intent
        left behind is left behind for good. A stale intent is what
        `current_park_generation` answers to every producer that asks, and a
        producer naming a generation Core has discarded sends a wake Core
        ignores as stale -- the record is appended, the Signal is sent, and the
        Workflow is never woken.
        """
        subscription = self._runs.get(run_id, {}).pop(wait_id, None)
        if subscription is None:
            return
        async with self._park_lock(run_id):
            try:
                await self._remove_park_intent(subscription)
            except Exception:
                logger.exception(
                    "Could not remove the park intent for %s wait %s while "
                    "cancelling it; it will be reconciled if this Run is "
                    "registered again",
                    subscription.stream_key,
                    subscription.wait_id,
                )
        await self._stop(subscription)

    async def evict_run(self, run_id: str) -> None:
        """Tears down every subscription for a Run.

        The same path serves eviction, Workflow Task failure mid-batch, and
        shutdown, because all three leave exactly the same thing behind:
        speculative reads that were never committed.
        """
        self._replay_plans.pop(run_id, None)
        self._park_locks.pop(run_id, None)
        for subscription in self._runs.pop(run_id, {}).values():
            await self._stop(subscription)

    async def probe_runs(self, *, grace: timedelta = DEFAULT_PROBE_GRACE) -> None:
        """Records what state Core says each Run is in. The sweep's first half.

        Separated from the wakes because the two halves have opposite timing
        requirements, and running them together means one of them is wrong:

        - the **probe** has to be asked while Core can still answer it. Its whole
          value is telling ``WftOpen``, ``Parked`` and ``NoOpenWorkflowTask``
          apart, and all three are statements about a Run Core still holds. Core
          keeps them only until the Worker's shutdown is initiated: an idle
          cached Run has no pending work, so ``shutdown_done`` is satisfied by
          the very first input after the shutdown token is cancelled and the
          whole workflow-state lane ends. Every probe after that answers
          ``RunNotFound`` -- which still owes a wake, so the sweep goes on
          looking correct while its ``Parked`` branch (a Run that needs no wake
          at all) and its ``WftOpen`` branch (a Run that must be left to C15b
          rather than raced) have silently stopped being reachable.
        - the **wakes** must not run there. Sending them before the pollers stop
          would offer the Run to a task queue this Worker is still polling, which
          is the opposite of a hand-off, and would hold the stop-polling step
          open for as long as the server takes to acknowledge them.

        So this runs immediately *before* shutdown is initiated and does nothing
        but ask and remember, and :meth:`sweep` acts on the answers afterwards.
        The one thing that costs: a Run recorded as ``NoOpenWorkflowTask`` here
        may still pick up a Workflow Task from an in-flight poll before the
        pollers stop, and would then get both C15b's forced replacement and this
        sweep's wake. That is one extra empty Workflow Task, which this design
        permits, and it is the cheaper side of the trade -- the alternative is
        never distinguishing the states at all.

        Bounded by ``grace`` because nothing may make stopping the pollers wait
        indefinitely. Every Run left unprobed is simply probed again by the
        sweep, where the answer is whatever Core has left to say.
        """
        probe = self._run_status
        if probe is None:
            return
        try:
            await asyncio.wait_for(self._probe_runs(probe), grace.total_seconds())
        except asyncio.TimeoutError:
            logger.warning(
                "External stream shutdown probe did not finish within %s; the "
                "Runs it did not reach are swept on whatever Core can still say",
                grace,
            )

    async def _probe_runs(self, probe: RunStatusProbe) -> None:
        for run_id in list(self._runs):
            if not self._runs.get(run_id):
                continue
            try:
                self._probed[run_id] = _status_value(await probe(run_id))
            except Exception:
                logger.exception(
                    "External stream shutdown probe failed for run %s", run_id
                )

    async def sweep(self, *, grace: timedelta = DEFAULT_SHUTDOWN_GRACE) -> None:
        """Sends the wakes the probe found owed. The sweep's second half.

        Separate from :meth:`shutdown` so teardown can stay where it belongs --
        after every activation has been answered, so per-Run teardown remains
        driven by ``RemoveFromCache`` and a ``FinalizeExternalStreams`` in flight
        is answered before the Run's state disappears.

        Never blocked past ``grace``. A wake that could not be acknowledged in
        time is reported rather than dropped, and never counted as delivered.
        """
        if self._swept:
            return
        self._swept = True
        self._shutting_down = True
        try:
            await asyncio.wait_for(self._sweep(), grace.total_seconds())
        except asyncio.TimeoutError:
            logger.warning(
                "External stream shutdown sweep did not finish within %s; "
                "tearing down anyway",
                grace,
            )

    async def shutdown(self, *, grace: timedelta = DEFAULT_SHUTDOWN_GRACE) -> None:
        """Sweeps every Run that still holds subscriptions, then tears down.

        Per-Run teardown is normally eviction's job, and this is the backstop for
        Runs eviction never reaches -- an idle cached Run gets no eviction
        activation at shutdown at all, which is exactly the Run that most needs
        the sweep: its records are buffered in a process that is about to exit,
        and nothing else will ever tell the Workflow they arrived.
        """
        await self.sweep(grace=grace)
        for run_id in list(self._runs):
            await self.evict_run(run_id)

    async def _sweep(self) -> None:
        """Acts on each Run's state and owes a wake only where one is owed.

        The probe is deliberately **not** the readiness call. Readiness means "a
        record is buffered", so probing with it would assert something false and
        manufacture a spurious Workflow Task on the way out of a Worker that is
        shutting down.
        """
        if self._run_status is None:
            return
        for run_id in list(self._runs):
            subscriptions = list(self._runs.get(run_id, {}).values())
            if not subscriptions:
                continue
            status = self._probed.pop(run_id, None)
            if status is None:
                # Not probed while Core could still answer -- either nothing ran
                # the probe phase, or this Run was cached from an in-flight poll
                # after it. Ask anyway rather than guessing: the answer is
                # whatever Core has left to say, and both answers it can still
                # give owe a wake.
                try:
                    status = _status_value(await self._run_status(run_id))
                except Exception:
                    logger.exception(
                        "External stream shutdown probe failed for run %s", run_id
                    )
                    continue

            if status == RunStatus.WFT_OPEN:
                # A Workflow Task is open and Core owns what happens to it
                # (C15b). A wake here would race that transition and produce a
                # second task for a Run already being attended to.
                continue
            if status == RunStatus.PARKED:
                # Already parked, so a producer's append will wake it through the
                # ordinary path. Nothing is owed.
                continue

            # NoOpenWorkflowTask and RunNotFound both mean local readiness has
            # nowhere to go. They differ in what happens to the Run afterwards,
            # not in what is owed now.
            for subscription in subscriptions:
                await self._sweep_wake(subscription)

    async def _sweep_wake(self, subscription: Subscription) -> None:
        """Sends one unparked wake, retrying within the grace period.

        Awaited rather than fired: an unacknowledged wake is the case this whole
        sweep exists to prevent, and reporting shutdown as clean while a record
        sits unannounced in a stream would make the wakeup-durability boundary
        false.

        Retried because the common failure here is a momentary one -- a Worker
        shutting down is often shutting down *because* something is unhealthy,
        and the first attempt lands in the middle of it. The retry is safe and is
        not a second wake: the request ID is derived from the wake's identity, so
        the server deduplicates it against the attempt that may in fact have
        arrived. The grace period bounds the whole sweep, so this cannot extend
        shutdown past it.
        """
        subscription.wakes_owed += 1
        if self._send_wake is None:
            self._record_shutdown_wake_failure(subscription)
            return

        for attempt in range(SHUTDOWN_WAKE_ATTEMPTS):
            try:
                await self._send_wake(subscription)
                return
            except Exception:
                logger.warning(
                    "External stream shutdown wake attempt %s/%s failed for %s wait %s",
                    attempt + 1,
                    SHUTDOWN_WAKE_ATTEMPTS,
                    subscription.stream_key,
                    subscription.wait_id,
                    exc_info=True,
                )
                if attempt + 1 < SHUTDOWN_WAKE_ATTEMPTS:
                    await asyncio.sleep(SHUTDOWN_WAKE_RETRY_DELAY.total_seconds())
        self._record_shutdown_wake_failure(subscription)

    def _record_shutdown_wake_failure(self, subscription: Subscription) -> None:
        """Surfaces the wake that could not be acknowledged.

        Counted rather than only logged: a dropped wake is silent by nature --
        the Workflow simply waits, and nothing distinguishes that from a producer
        having nothing to say.
        """
        self.shutdown_wake_failures += 1
        if self._metric is not None:
            self._metric(subscription)

    async def _stop(self, subscription: Subscription) -> None:
        subscription._cancelled = True
        subscription.reset_to_committed()
        watcher = subscription._watcher
        if watcher is not None and not watcher.done():
            watcher.cancel()
            try:
                await watcher
            except asyncio.CancelledError:
                pass
