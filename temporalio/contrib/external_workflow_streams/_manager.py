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
import logging
import threading
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from temporalio.contrib.external_workflow_streams._backend import (
    ParkIntent,
    StreamBackend,
    StreamKey,
)
from temporalio.contrib.external_workflow_streams._record import (
    AFTER,
    BEGINNING,
    Cursor,
    StreamRecord,
)

__all__ = ["ReadinessResult", "StreamSubscriptionManager", "Subscription"]

logger = logging.getLogger(__name__)

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


#: What the manager calls to tell Core a record is buffered. Returns one of the
#: five readiness results as a plain value (or an enum whose ``value`` is one).
ReadinessNotifier = Callable[[str, int, int], Awaitable[Any]]

#: What the manager calls when local readiness could not be delivered. Filled in
#: by the producer wake-signal path (P14); until then a subscription simply
#: records that a wake was owed.
WakeSender = Callable[["Subscription"], Awaitable[None]]


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
    """Increments each time this wait re-enters the blocked state."""

    #: Records read ahead but not yet delivered. Appended by the manager loop,
    #: popped by the Workflow thread, so every touch is under `_lock`.
    _buffer: deque[StreamRecord] = field(default_factory=deque, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _has_room: asyncio.Event = field(default_factory=asyncio.Event, repr=False)
    _watcher: asyncio.Task[None] | None = field(default=None, repr=False)
    _cancelled: bool = field(default=False, repr=False)

    #: Wakes owed because local readiness could not be delivered. Counted rather
    #: than merely logged, so a test can tell "no wake was needed" from "a wake
    #: was needed and dropped".
    wakes_owed: int = 0

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
        self.delivery_cursor = self.committed_cursor
        self.prefetch_cursor = self.committed_cursor
        self._has_room.set()

    def commit(self, cursor: Cursor) -> None:
        """Advances the committed cursor when a marker commits."""
        self.committed_cursor = cursor


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
        buffer_size: int = DEFAULT_BUFFER_SIZE,
        watch_block: timedelta = DEFAULT_WATCH_BLOCK,
    ) -> None:
        self._backends = backends
        self._notify_ready = notify_ready
        self._send_wake = send_wake
        self._buffer_size = buffer_size
        self._watch_block = watch_block
        self._runs: dict[str, dict[int, Subscription]] = {}
        # The loop the watchers run on, captured at construction because that is
        # the Worker's loop. `register` is called from the *Workflow executor
        # thread*, which has no loop of its own and must never touch this one
        # directly.
        self._loop = asyncio.get_event_loop()
        self._shutting_down = False

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
        self._runs.setdefault(run_id, {})[wait_id] = subscription
        # `create_task` is not thread-safe, and this runs on the Workflow
        # executor thread. Scheduling the start onto the manager's loop is the
        # difference between a watcher that runs and one that is silently never
        # scheduled -- which looks exactly like a stream that never delivers.
        self._loop.call_soon_threadsafe(self._start_watcher, subscription)
        return subscription

    def _start_watcher(self, subscription: Subscription) -> None:
        """Starts a watcher on the manager's own loop."""
        if subscription._cancelled or subscription._watcher is not None:
            return
        subscription._watcher = self._loop.create_task(self._watch(subscription))

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

                subscription._append(records)
                await self._report_ready(subscription)
        except asyncio.CancelledError:
            pass

    async def _report_ready(self, subscription: Subscription) -> None:
        """Tells Core a record is buffered, and acts on which answer comes back."""
        result = _result_value(
            await self._notify_ready(
                subscription.run_id, subscription.wait_id, subscription.wait_generation
            )
        )

        if result in (ReadinessResult.ACCEPTED, ReadinessResult.STALE):
            # Accepted: Core will activate. Stale: re-probe on the next loop.
            return

        # The other three all mean local readiness could not be delivered, so a
        # Signal is owed. They differ in what happens to the watcher afterwards.
        subscription.wakes_owed += 1
        if self._send_wake is not None:
            await self._send_wake(subscription)

        if result == ReadinessResult.RUN_NOT_FOUND:
            # The Run is gone from this Worker. Nothing here can serve it again.
            subscription._cancelled = True
            self._runs.get(subscription.run_id, {}).pop(subscription.wait_id, None)
        # PARKED and NO_OPEN_WORKFLOW_TASK both *keep* the watcher: the Run is
        # still cached and this is the normal window between Workflow Tasks.

    # --- the runtime-only jobs' backend work (P19) --------------------------

    async def prepare_park(
        self, run_id: str, park_generation: int, blocked: Mapping[int, Cursor]
    ) -> bool:
        """Installs park intents, then rechecks every stream.

        Returns ``True`` if any stream became ready, which abandons this parking
        generation.

        The order is what closes the append/park race: a producer appends its
        record *before* it observes the park generation, so an append is either
        seen by the recheck below or paired with a wake Signal. Rechecking
        before all the intents were installed would leave a window where it is
        neither.
        """
        subscriptions = self.subscriptions(run_id)
        for subscription in subscriptions:
            await subscription.backend.install_park_intent(
                subscription.stream_key,
                ParkIntent(
                    wait_id=subscription.wait_id,
                    cursor=blocked.get(
                        subscription.wait_id, subscription.delivery_cursor
                    ),
                    park_generation=park_generation,
                    run_id=run_id,
                ),
            )

        became_ready = False
        for subscription in subscriptions:
            if await subscription.backend.recheck(
                subscription.stream_key, subscription.wait_id
            ):
                became_ready = True
                break

        if became_ready:
            # All-or-nothing: a park confirmed for a set with a ready member
            # would lose that member's record until a producer happened to
            # signal, so every intent installed above comes back out.
            for subscription in subscriptions:
                await subscription.backend.remove_park_intent(
                    subscription.stream_key, subscription.wait_id
                )
        return became_ready

    async def prepare_replay(self, run_id: str, replay_annotation: bytes) -> None:
        """Fills and validates the recorded ranges before delivery.

        Left to P13, which owns the read path and the four range checks. The
        job is still partitioned here rather than in ``_apply`` because that is
        where the reads must happen whatever they turn out to be -- an inclusive
        range read over a whole recorded batch is exactly the multi-second
        operation the deadlock timeout cannot accommodate.
        """
        raise NotImplementedError(
            "the external stream replay read path is not implemented yet (P13)"
        )

    # --- teardown -----------------------------------------------------------

    async def cancel(self, run_id: str, wait_id: int) -> None:
        """Cancels one subscription: drop its buffer, stop its watcher."""
        subscription = self._runs.get(run_id, {}).pop(wait_id, None)
        if subscription is None:
            return
        await self._stop(subscription)

    async def evict_run(self, run_id: str) -> None:
        """Tears down every subscription for a Run.

        The same path serves eviction, Workflow Task failure mid-batch, and
        shutdown, because all three leave exactly the same thing behind:
        speculative reads that were never committed.
        """
        for subscription in self._runs.pop(run_id, {}).values():
            await self._stop(subscription)

    async def shutdown(self) -> None:
        """Tears down every Run. Per-Run teardown is normally eviction's job.

        This is the backstop for Runs eviction never reaches -- an idle cached
        Run gets no eviction activation at shutdown at all.
        """
        self._shutting_down = True
        for run_id in list(self._runs):
            await self.evict_run(run_id)

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
