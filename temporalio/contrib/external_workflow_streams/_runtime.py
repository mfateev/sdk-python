"""The per-Workflow-instance runtime handle (P10a, P10b).

One of these is created per Run and handed to Workflow code as an opaque handle.
It is the only thing that crosses the sandbox boundary, and it exposes exactly
three capabilities: resolve a backend *name*, register a wait, and pop from that
wait's buffer. No provider instance is reachable through it.

It also builds the **observation delta** -- the thing that makes replay possible
-- because it is the only component that sees deliveries in the order Workflow
code actually received them. Neither the manager (which sees buffering order)
nor Core (which is annotation-blind) can reconstruct that.

Two rules here are easy to get subtly wrong:

- **Emission is not conditional on records having been consumed.** A
  subscription's first observation, an activation that drained nothing, and the
  boundary an activation returned on *all* produce deltas. A subscription to an
  empty stream must still record its provider, stream key, and start cursor, or
  replay has no starting point at all.
- **Control records are recorded even though they are never yielded.** They
  occupy offsets inside a run, so a run's ``count`` includes them and
  ``control_positions`` marks which relative indices they were. Leaving them out
  would make every replay range read find more records than the marker claims.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Mapping

import temporalio.converter
import temporalio.workflow
from temporalio.contrib.external_workflow_streams._annotation import (
    MAX_ANNOTATION_BYTES,
    AnnotationAccumulator,
    AnnotationHeader,
    Run,
    Segment,
    SegmentEndReason,
    StreamBinding,
)
from temporalio.contrib.external_workflow_streams._api import (
    MAX_RECORDS_PER_ACTIVATION,
)
from temporalio.contrib.external_workflow_streams._backend import (
    StreamBackend,
    StreamKey,
)
from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec
from temporalio.contrib.external_workflow_streams._continuation import Continuation
from temporalio.contrib.external_workflow_streams._manager import (
    StreamSubscriptionManager,
)
from temporalio.contrib.external_workflow_streams._record import (
    AFTER,
    BEGINNING,
    Cursor,
    StreamRecord,
)
from temporalio.contrib.external_workflow_streams._replay import ReplayPlan

__all__ = ["QuiescentWait", "WorkflowStreamRuntime"]

_REPLAY_UNBOUNDED = 2**63 - 1
"""The budget reported while a recorded segment is being delivered.

A number rather than ``None`` so that every caller keeps one code path: a
segment is finite and already recorded, so "as many as the segment holds" and
"no limit" are the same answer.
"""


@dataclass(frozen=True)
class QuiescentWait:
    """One member of the complete blocked snapshot Core is asked to retain for."""

    wait_id: int
    generation: int
    immediately_parkable: bool


@dataclass
class _SubscriptionState:
    """What the runtime tracks per subscription, on the Workflow thread."""

    wait_id: int
    stream_key: StreamKey
    backend_name: str
    start_cursor: Cursor
    idle_timeout: timedelta

    delivery_cursor: Cursor = BEGINNING
    """How far records have been handed to the *subscription's* buffer.

    What the annotation records, and what replay must reproduce.
    """
    consumption_cursor: Cursor = BEGINNING
    """How far Workflow code has actually taken records.

    Trails :attr:`delivery_cursor` by whatever is still sitting unread in the
    buffer. That buffer dies with the Run, so this -- not delivery -- is what a
    successor Run must resume from.
    """
    generation: int = 0
    """Increments each time this wait re-enters the blocked state."""

    announced: bool = False
    """Whether the header has recorded this subscription yet.

    The first observation must carry provider identity, stream key, and start
    cursor even if no record was ever delivered -- otherwise replay of a
    subscription to an empty stream has nowhere to begin.
    """

    fence_reached: bool = False
    """Set by a write fence, cleared by any later record.

    A fence means only that *this* producer session's preceding writes are all
    appended. A later record does not violate it; it simply clears it.
    """

    blocked: bool = True
    """Whether Workflow code is currently waiting on this subscription."""


class WorkflowStreamRuntime:
    """The handle Workflow code holds, and the observation delta's author."""

    def __init__(
        self,
        *,
        manager: StreamSubscriptionManager,
        backends: Mapping[str, StreamBackend],
        run_id: str,
        namespace: str,
        workflow_id: str,
        first_execution_run_id: str,
        data_converter: temporalio.converter.DataConverter,
        default_idle_timeout: timedelta,
        max_annotation_bytes: int = MAX_ANNOTATION_BYTES,
        continuation: Continuation | None = None,
    ) -> None:
        self._manager = manager
        self._backends = backends
        self._run_id = run_id
        self._namespace = namespace
        self._workflow_id = workflow_id
        self._first_execution_run_id = first_execution_run_id
        self._data_converter = data_converter
        self._default_idle_timeout = default_idle_timeout
        self._max_annotation_bytes = max_annotation_bytes
        #: What the predecessor Run committed, or None on a first execution.
        #: Restored from History before any subscription is established, never
        #: read from the backend -- a cursor derived from mutable backend state
        #: would give replay whatever the stream holds now (ADR-022).
        self._continuation = continuation

        self._subscriptions: dict[int, _SubscriptionState] = {}
        #: Where the *current* annotation begins, per wait. Captured when the
        #: annotation begins rather than when its header is first needed --
        #: lazily reading the delivery cursor would let a record delivered
        #: before the first emission slip in front of the start cursor, and
        #: replay of that marker would then never deliver it.
        self._annotation_start: dict[int, Cursor] = {}
        self._accumulator: AnnotationAccumulator | None = None
        #: Whether this Workflow Task's annotation has already been closed with
        #: its terminal. Distinct from "no accumulator": one means nothing has
        #: been written yet and a terminal must create the header for it, the
        #: other that the annotation is finished and a second terminal would
        #: append a whole second annotation to the marker.
        self._annotation_closed = False
        self._pending_deltas: list[bytes] = []
        #: Runs recorded since the current segment opened, in delivery order.
        self._runs: list[Run] = []
        #: Set when a subscription is registered or a record delivered, so an
        #: activation that changed nothing at all emits nothing.
        self._observed_this_activation = False
        #: `wait_id -> Future`, awaited by Workflow code and resolved by the
        #: readiness activation. It lives here rather than on either side alone
        #: because the two halves are in different modules and a second map
        #: would mean the side that resolves is never the side that registered.
        self._pending: dict[int, asyncio.Future[None]] = {}
        #: Non-``None`` only while a recorded segment is being delivered.
        self._replay_ready: list[tuple[int, StreamRecord]] | None = None
        #: The bindings of the marker currently being replayed. Non-``None``
        #: only for the length of one replay job, which is what makes a
        #: registration made during it checkable against what was recorded.
        self._replay_bindings: dict[int, StreamBinding] | None = None
        #: Records handed to Workflow code since this activation began. Counted
        #: here rather than per subscription because the cap is an *activation*
        #: budget: `merge()` consumes from several subscriptions inside one
        #: activation, and a per-subscription counter would let n streams run n
        #: times as long.
        self._delivered_this_activation = 0

    # --- the per-activation delivery budget ---------------------------------

    def begin_activation(self) -> None:
        """Resets the delivery budget. Called once per activation.

        The budget is per activation because that is the unit the deadlock
        timeout applies to: what must be bounded is how long one ``activate()``
        call can run, not how much a Run receives over its life.
        """
        self._delivered_this_activation = 0

    def delivery_budget_remaining(self) -> int:
        """How many more records this activation may hand to Workflow code.

        Unbounded during replay. Delivery then comes from the recorded segments
        rather than a live producer, so it is already finite, and the recorded
        boundaries already say how many records each activation received --
        re-cutting them here would deliver a different schedule than the one in
        History.
        """
        if self._replay_ready is not None:
            return _REPLAY_UNBOUNDED
        return max(0, MAX_RECORDS_PER_ACTIVATION - self._delivered_this_activation)

    def delivery_budget_exhausted(self) -> bool:
        """Whether this activation stopped delivering because of the budget.

        The completion path asks, because records left buffered by the budget
        have no readiness notification coming: the watcher moved its prefetch
        cursor past them when it buffered them. Their readiness has to be
        re-reported or the Workflow waits forever on records already in front of
        it.
        """
        return (
            self._replay_ready is None
            and self._delivered_this_activation >= MAX_RECORDS_PER_ACTIVATION
        )

    def rearm_readiness(self) -> None:
        """Re-reports readiness for every buffer this Run left non-empty.

        Called on the Workflow thread, so the hop onto the manager's loop happens
        inside the manager -- the same reason ``register`` hops.
        """
        self._manager.rearm_ready(self._run_id)

    # --- the ExternalStreamRuntime protocol ---------------------------------

    def stream_key(self, stream_name: str) -> StreamKey:
        return StreamKey(
            namespace=self._namespace,
            workflow_id=self._workflow_id,
            first_execution_run_id=self._first_execution_run_id,
            stream_name=stream_name,
        )

    def register(
        self,
        *,
        wait_id: int,
        stream_key: StreamKey,
        backend_name: str,
        idle_timeout: timedelta | None = None,
        start_cursor: Cursor | None = None,
    ) -> None:
        """Registers a wait with the Worker's manager. Non-blocking, no I/O.

        The start cursor defaults to what the predecessor Run committed for this
        ``wait_id``, so a chain resumes where it left off without the Workflow
        code saying anything about it -- and a first execution gets ``BEGINNING``
        from the same path.
        """
        if start_cursor is None:
            start_cursor = self.restored_start(wait_id, stream_key.stream_name)
        if backend_name not in self._backends:
            known = ", ".join(sorted(self._backends)) or "<none>"
            raise KeyError(
                f"no external stream backend named {backend_name!r} is registered on "
                f"this Worker; registered backends are: {known}"
            )
        if self._replay_bindings is not None:
            # A subscription made while a marker is being replayed -- which is
            # every subscription, on the activation that both starts the
            # Workflow and replays its first marker. Checked before the state
            # exists, so a mismatch is reported before a single recorded record
            # can be handed through the wrong subscription.
            binding = self._replay_bindings.get(wait_id)
            if binding is not None:
                self._verify_binding(wait_id, stream_key, backend_name, binding)
        self._subscriptions[wait_id] = _SubscriptionState(
            wait_id=wait_id,
            stream_key=stream_key,
            backend_name=backend_name,
            start_cursor=start_cursor,
            delivery_cursor=start_cursor,
            consumption_cursor=start_cursor,
            idle_timeout=idle_timeout or self._default_idle_timeout,
        )
        # A subscription created part-way through an annotation begins at its
        # own start cursor, not at wherever the others happen to be.
        self._annotation_start[wait_id] = start_cursor
        self._manager.register(
            run_id=self._run_id,
            wait_id=wait_id,
            stream_key=stream_key,
            backend_name=backend_name,
            start_cursor=start_cursor,
        )
        # A registration alone is replay-visible: it is what puts the stream in
        # the annotation header, without which replay cannot start.
        self._observed_this_activation = True

    def drain(self, wait_id: int, max_records: int | None = None) -> list[StreamRecord]:
        """Pops buffered records. Performs no I/O and never blocks.

        During replay the records come from the segment currently being
        delivered rather than from the live buffer. ``_apply`` cannot tell the
        difference, which is the point: replay and live delivery run the same
        Workflow code down the same path.
        """
        if self._replay_ready is not None:
            # Deliberately *not* the live buffer, even if the manager happens to
            # hold something: a watcher that ran before the Run was evicted may
            # have prefetched past where the marker stops, and delivering that
            # would replay records this Workflow Task never saw.
            taken: list[StreamRecord] = []
            remaining: list[tuple[int, StreamRecord]] = []
            for entry_wait_id, record in self._replay_ready:
                if entry_wait_id == wait_id and (
                    max_records is None or len(taken) < max_records
                ):
                    taken.append(record)
                else:
                    remaining.append((entry_wait_id, record))
            self._replay_ready = remaining
            return taken
        return self._manager.drain(self._run_id, wait_id, max_records)

    # --- replay delivery ----------------------------------------------------

    def take_replay_plan(self) -> ReplayPlan | None:
        """The plan prepared for this Run, if a replay job is being delivered."""
        return self._manager.take_replay_plan(self._run_id)

    def begin_replay(self, bindings: Mapping[int, StreamBinding]) -> None:
        """Holds the marker's bindings open, and checks the ones already made.

        Delivery joins a recorded run to a subscription by ``wait_id`` alone,
        and an integer is not an identity: it says nothing about *what* the wait
        was subscribed to. Without this the same wait number pointing at a
        different stream quietly delivers the recorded stream's bytes through
        the new subscription -- the failure taxonomy's row four turned into a
        silently different stream result, which is the one outcome the design
        says must never happen.

        Checked here as well as in :meth:`register` because the two moments are
        different Workflow Tasks' worth of history: a marker replayed after the
        Workflow has already run past its ``subscribe()`` calls finds the
        subscriptions in place, and one replayed in the same activation that
        starts the Workflow finds none of them yet.
        """
        self._replay_bindings = dict(bindings)
        for wait_id, state in sorted(self._subscriptions.items()):
            binding = self._replay_bindings.get(wait_id)
            if binding is not None:
                self._verify_binding(
                    wait_id, state.stream_key, state.backend_name, binding
                )

    def _verify_binding(
        self,
        wait_id: int,
        stream_key: StreamKey,
        backend_name: str,
        binding: StreamBinding,
    ) -> None:
        """Row four, and deliberately not integrity loss.

        Both fields compared here were chosen by Workflow code -- the stream the
        topic names and the backend it names it on -- so a difference means the
        code moved, not that anything is wrong with the backend. Reporting it as
        integrity loss would send an operator to repair a store that is fine.

        Only the **stream name** is compared, not the whole key. The other three
        components -- namespace, Workflow id, first execution Run id -- are the
        Run's identity rather than anything the code chose, and a replay harness
        legitimately supplies its own: `Replayer` runs under `ReplayNamespace`,
        so comparing the full key would report every replayed history as
        nondeterministic. The key is still *recorded* whole, because replay has
        to read the ranges it names.

        The start cursor is **not** compared. It is the position the wait stood
        at when that Workflow Task opened, not a property of the subscription,
        so for every marker after a Run's first it legitimately differs from the
        cursor the subscription was registered with. What guards the cursor
        across Runs is ADR-022's continuation check in :meth:`restored_start`.
        """
        if stream_key.stream_name != binding.stream_key.stream_name:
            raise temporalio.workflow.NondeterminismError(
                f"the marker records external stream wait {wait_id} on stream "
                f"{binding.stream_key.stream_name!r}, but this Workflow "
                f"subscribes it to {stream_key.stream_name!r}. A subscribe() "
                "call was inserted, removed, or reordered, which renumbers "
                "every later wait; gate the change behind workflow.patched() "
                "exactly as an inserted timer would be."
            )
        if backend_name != binding.backend_name:
            raise temporalio.workflow.NondeterminismError(
                f"the marker records external stream wait {wait_id} against "
                f"backend {binding.backend_name!r}, but this Workflow subscribes "
                f"it against {backend_name!r}. The recorded records live in the "
                "backend that wrote them; gate the change behind "
                "workflow.patched() exactly as an inserted timer would be."
            )

    def begin_replay_segment(
        self, deliveries: Sequence[tuple[int, StreamRecord]]
    ) -> None:
        """Makes one recorded segment the only thing a drain can see."""
        self._verify_replay_consumed()
        self._replay_ready = list(deliveries)

    def verify_replay_consumed(self) -> None:
        """Every recorded delivery must have reached Workflow code.

        A record in a run was handed to Workflow code during the activation the
        run was recorded in, so a replay that leaves one behind is running
        different code. The common way to get here is a removed ``subscribe()``
        call: its wait is never registered, nothing ever drains it, and the
        marker's records for it would otherwise be discarded in silence -- the
        Workflow reaching its next command having consumed less than History
        says it consumed.
        """
        self._verify_replay_consumed()

    def _verify_replay_consumed(self) -> None:
        if not self._replay_ready:
            return
        wait_ids = sorted({wait_id for wait_id, _ in self._replay_ready})
        raise temporalio.workflow.NondeterminismError(
            f"the marker records {len(self._replay_ready)} delivery(s) for "
            f"external stream wait(s) {wait_ids} that this Workflow never took. "
            "A subscribe() call was removed, reordered, or is no longer "
            "consumed, which leaves recorded records undelivered; gate the "
            "change behind workflow.patched() exactly as a removed timer would "
            "be."
        )

    def reposition_after_replay(self, boundaries: Mapping[int, Cursor]) -> None:
        """Moves the manager's cursors to what the replayed marker committed.

        Replay delivers from the annotation, not from the manager's buffer --
        but the watcher has been filling that buffer from the subscription's
        start cursor the whole time, because nothing out there knows a marker
        for these records exists. Without this the first live drain after a
        replay hands Workflow code every replayed record a second time; the
        symptom is a Workflow that received ``['alpha', 'alpha', 'beta']``.

        The boundaries come from the marker rather than from wherever the
        replay's deliveries happened to stop -- the marker is History's own
        statement of what was committed -- and only the waits it recorded
        something for are named.

        Hopped onto the manager's loop inside the manager, for the same reason
        :meth:`rearm_readiness` hops: this runs on the Workflow thread.
        """
        self._manager.reposition_to_committed(self._run_id, boundaries)

    def end_replay(self) -> None:
        """Hands drains back to the live buffer.

        Called from a ``finally``: a partial replay that left this set would
        make every later drain on this Run return nothing at all, turning one
        marker's failure into a Workflow that silently never receives again.
        """
        self._replay_ready = None
        self._replay_bindings = None

    def codec_for(self, value_type: type | None) -> StreamPayloadCodec[Any]:
        return StreamPayloadCodec(self._data_converter, value_type)

    def new_readiness_future(self) -> asyncio.Future[None]:
        """A future belonging to the Workflow's own deterministic event loop."""
        return asyncio.get_event_loop().create_future()

    def register_pending(self, wait_id: int, future: asyncio.Future[None]) -> None:
        self._pending[wait_id] = future

    def discard_pending(self, wait_id: int) -> None:
        self._pending.pop(wait_id, None)

    def resolve_all_pending(self) -> int:
        """Resumes every waiting subscription. Returns how many were resumed.

        *Every* one, not only those a readiness job named: the job's hints are
        hints, not an exhaustive availability claim, so each subscription is
        left to find out for itself whether anything arrived. Resolving only the
        hinted ones would strand a record whose notification was coalesced away.
        """
        resumed = 0
        for wait_id in sorted(self._pending):
            future = self._pending.pop(wait_id, None)
            if future is not None and not future.done():
                future.set_result(None)
                resumed += 1
        return resumed

    def clear_pending(self) -> None:
        """Drops every waiting future, as eviction requires."""
        self._pending.clear()

    # --- recording what Workflow code observed -------------------------------

    def record_delivery(self, wait_id: int, record: StreamRecord) -> None:
        """Notes one record reaching the runtime, in observed global order.

        Called for **every** record including control records: they occupy
        offsets inside a run, so a run's count includes them and their relative
        indices go in ``control_positions``. Omitting them would make replay's
        range read find more records than the marker claims and fail as
        integrity loss.
        """
        state = self._subscriptions.get(wait_id)
        if state is None or record.offset is None:
            return

        state.delivery_cursor = AFTER(record.offset)
        state.fence_reached = record.is_control

        if self._replay_ready is not None:
            # Replay re-delivers records the marker already recorded. Advancing
            # the cursors is right -- the runtime has to end up in the state the
            # live run was in -- but accumulating them into a *new* annotation is
            # not: Core would be asked to write a second marker for observations
            # already in History, and the command would be matched against the
            # very event it was read from.
            return

        self._observed_this_activation = True

        # Extend the open run when this is another consecutive delivery from the
        # same stream. A run is *maximal*, which is what makes a single-stream
        # batch of 100,000 records cost one run rather than 100,000.
        if self._runs and self._runs[-1].wait_id == wait_id:
            previous = self._runs[-1]
            positions = previous.control_positions
            if record.is_control:
                positions = (*positions, previous.count)
            self._runs[-1] = Run(
                wait_id=wait_id,
                first_offset=previous.first_offset,
                last_offset=record.offset,
                count=previous.count + 1,
                control_positions=positions,
            )
            return

        self._runs.append(
            Run(
                wait_id=wait_id,
                first_offset=record.offset,
                last_offset=record.offset,
                count=1,
                control_positions=(0,) if record.is_control else (),
            )
        )

    def note_blocked(self, wait_id: int, blocked: bool) -> None:
        """Records whether Workflow code is waiting on this subscription."""
        state = self._subscriptions.get(wait_id)
        if state is None:
            return
        if blocked and not state.blocked:
            # Re-entering the blocked state is what the wait generation counts,
            # and it is what makes a readiness notification for the previous
            # block recognisable as stale.
            state.generation += 1
            # Pushed to the manager immediately. The manager is what reports
            # readiness to Core, and Core compares the generation it is given
            # against the one this runtime put in the quiescent snapshot. A
            # manager left at the generation it registered with reports every
            # block after the first under a stale number, Core answers `Stale`,
            # and the watcher -- whose prefetch cursor is already past the
            # record -- never re-announces it. The Workflow then waits forever
            # on an append that did arrive.
            self._manager.note_wait_generation(self._run_id, wait_id, state.generation)
        state.blocked = blocked

    # --- the observation delta (P10b) ----------------------------------------

    def close_segment(self, reason: SegmentEndReason | None = None) -> None:
        """Closes this activation's segment, whether or not it saw anything.

        An empty segment is meaningful: an activation that drained and found
        nothing still ran one event-loop drain, and replay must reproduce that
        drain or ``wait_condition`` predicates fire a different number of times.
        """
        if not self._observed_this_activation:
            return
        if reason is None:
            reason = self._segment_end_reason()
        accumulator = self._ensure_accumulator()
        self._pending_deltas.append(
            accumulator.add_segment(Segment(tuple(self._runs), reason))
        )
        self._runs = []

    def _segment_end_reason(self) -> SegmentEndReason:
        """Why this activation stopped delivering.

        Decided here rather than passed in, because the reason has to be right
        whether the caller remembered it or not: it is durable, and a reader of
        the annotation has nothing else to tell it why the activation ended.

        ``BATCH_LIMIT`` outranks the other two. Both of them assert that nothing
        more was available -- ``FENCE_REACHED`` additionally that the set is
        immediately parkable -- and both are simply false when the runtime
        stopped with records still sitting in the buffer. Recording
        ``NO_DATA_AVAILABLE`` for a budget cut-off would put a claim in History
        that the stream ran dry when it did not.
        """
        if self.delivery_budget_exhausted():
            return SegmentEndReason.BATCH_LIMIT
        if self._subscriptions and all(
            s.fence_reached for s in self._subscriptions.values()
        ):
            return SegmentEndReason.FENCE_REACHED
        return SegmentEndReason.NO_DATA_AVAILABLE

    def take_observation_delta(self) -> bytes | None:
        """The bytes to put on this completion's `WorkflowStreamProgress`.

        ``None`` means nothing replay-visible changed, which is the only case
        where a completion legitimately carries no progress command -- and a
        replay delivery is exactly that case, since everything it delivered is
        already recorded in the marker being replayed.
        """
        if self._replay_ready is not None:
            return None
        self.close_segment()
        if not self._pending_deltas:
            return None
        delta = b"".join(self._pending_deltas)
        self._pending_deltas = []
        self._observed_this_activation = False
        return delta

    def add_terminal(self) -> bytes:
        """Encodes the blocked snapshot that closes this Workflow Task.

        Read from in-memory state alone: the boundary is not "wherever the
        stream is now", it is where this task's deliveries stopped, which was
        fixed the moment the last activation returned. Refreshing it against the
        backend could name a position replay must not reproduce.

        Returns **no bytes at all** when this Workflow Task's annotation is
        already closed and nothing has been accumulated since. Core can decide a
        boundary for a task whose last completion had already ended one -- a
        rollover deadline or a shutdown landing on a completion that left no
        subscription blocked -- and answering with a second header and terminal
        would append a whole second annotation to the one Core is about to
        write. The marker would then decode as far as the first terminal and
        fail on the frame after it.
        """
        if self._annotation_closed and self._accumulator is None:
            return b""
        accumulator = self._ensure_accumulator()
        terminal = accumulator.add_terminal(
            {
                wait_id: state.delivery_cursor
                for wait_id, state in sorted(self._subscriptions.items())
            }
        )
        # Anything still pending goes out with the terminal. Usually there is
        # nothing -- each activation flushes its own delta -- but a Workflow Task
        # that observed *nothing* creates its accumulator right here, and the
        # header rides that creation. Returning the terminal alone would produce
        # an annotation that begins at a terminal frame and cannot be decoded.
        delta = b"".join([*self._pending_deltas, terminal])

        # The annotation ends at the terminal. A later activation on this Run --
        # the Continue-As-New's own, say -- opens a fresh annotation rather than
        # appending past it: Core writes one marker per finalized annotation, and
        # a segment recorded after the terminal could never be read back.
        self.start_new_annotation()
        self._annotation_closed = True
        return delta

    @property
    def annotation_started(self) -> bool:
        """Whether Core is holding accumulated bytes for the current annotation.

        True from the moment the header is emitted, which is what makes it the
        right question to ask before closing an annotation with its terminal: an
        annotation nobody has begun has nothing to terminate, and asking for a
        terminal anyway would create a header and a terminal for a Workflow Task
        that never touched a stream.
        """
        return self._accumulator is not None

    @property
    def request_rollover(self) -> bool:
        """Whether the annotation has passed its byte-budget high-water mark."""
        return self._accumulator is not None and self._accumulator.request_rollover

    def start_new_annotation(self) -> None:
        """Begins a fresh annotation, as the next Workflow Task requires.

        Its header records the *current* cursors rather than the original start
        cursors, so consumption continues uninterrupted across a rollover or any
        other Workflow Task boundary.
        """
        self._accumulator = None
        self._annotation_closed = False
        self._pending_deltas = []
        self._runs = []
        self._observed_this_activation = False
        self._annotation_start = {
            wait_id: state.delivery_cursor
            for wait_id, state in self._subscriptions.items()
        }

    def record_consumption(self, wait_id: int, record: StreamRecord) -> None:
        """Notes that Workflow code has been handed this record.

        Distinct from delivery, which advances by whole drained batches. The
        difference is exactly the records sitting in a subscription's buffer that
        the Workflow never asked for -- which die with the Run, and which a
        successor must therefore still receive.

        Also where the activation's delivery budget is spent, because this is
        called exactly once per record actually handed over -- unlike delivery,
        which moves in whole batches, and unlike decoding, which control records
        skip. Counted before the guards below so that a record whose subscription
        has already gone still costs its budget: the cap has to bound the
        activation whatever the bookkeeping says.
        """
        if self._replay_ready is None:
            # Replayed records are deliberately not counted, not merely not
            # capped. Counting them would leave the budget spent for any live
            # delivery later in the same activation, and would make the
            # completion re-arm readiness on a purely replayed Workflow Task.
            self._delivered_this_activation += 1
        state = self._subscriptions.get(wait_id)
        if state is None or record.offset is None:
            return
        state.consumption_cursor = AFTER(record.offset)

    def continuation(self) -> Continuation:
        """Where each subscription had got to, for the successor Run.

        The **consumption** cursor, which is the only one of the four positions
        a successor may resume from.

        Not prefetch, which is speculative. Not delivery: a batch is delivered
        whole, but a Workflow that stops iterating part-way through has taken
        only its prefix, and the buffer holding the difference dies with this
        Run -- so a successor starting from delivery would step silently over
        records nothing ever showed to Workflow code. Not the committed cursor
        either: by the time this is read the terminal command is being built,
        and the observation delta covering these deliveries commits on that same
        path, so a continuation taken from the last *marker* would restart the
        successor at a stale cursor and lose the final segment.
        """
        return Continuation(
            cursors={
                wait_id: state.consumption_cursor
                for wait_id, state in self._subscriptions.items()
            },
            stream_names={
                wait_id: state.stream_key.stream_name
                for wait_id, state in self._subscriptions.items()
            },
        )

    def restored_start(self, wait_id: int, stream_name: str) -> Cursor:
        """The start cursor for a subscription, from the predecessor Run if any.

        ``BEGINNING`` on a first execution -- the same field, filled the same
        way, so replay reads an explicit boundary in either case.
        """
        if self._continuation is None:
            return BEGINNING
        restored = self._continuation.cursors.get(wait_id)
        if restored is None:
            # A subscription the predecessor did not have. Safe: adding one on a
            # path the chain has not reached yet is the supported change.
            return BEGINNING
        recorded = self._continuation.stream_names.get(wait_id, "")
        if recorded and recorded != stream_name:
            # Row four of the taxonomy, not integrity loss: the cursor is
            # exactly what the predecessor committed, and it is the Workflow
            # code that moved.
            raise temporalio.workflow.NondeterminismError(
                f"the predecessor Run recorded external stream wait {wait_id} "
                f"on stream {recorded!r}, but this Run subscribes it to "
                f"{stream_name!r}. A subscribe() call was inserted, removed, or "
                "reordered, which renumbers every later wait; gate the change "
                "behind workflow.patched() exactly as an inserted timer would be."
            )
        return restored

    def _ensure_accumulator(self) -> AnnotationAccumulator:
        if self._accumulator is None:
            self._accumulator = AnnotationAccumulator(
                self._header(), max_bytes=self._max_annotation_bytes
            )
            # The header has to ride the *first delta*. Core accumulates by byte
            # append and never parses, so anything the runtime holds but does
            # not emit simply never reaches the marker -- and a marker whose
            # annotation starts at a segment frame cannot be decoded at all.
            self._pending_deltas.append(self._accumulator.accumulated())
        return self._accumulator

    def _header(self) -> AnnotationHeader:
        """One binding per subscription, each naming **its own** backend.

        The provider identity is read from the backend this subscription is
        actually registered against rather than from a single annotation-wide
        one. The API lets every topic name a different backend, so a shared
        label would be right for at most one wait and would send replay to the
        wrong store for all the others.
        """
        for state in self._subscriptions.values():
            state.announced = True
        return AnnotationHeader(
            streams={
                wait_id: self._binding(state)
                for wait_id, state in sorted(self._subscriptions.items())
            },
        )

    def _binding(self, state: _SubscriptionState) -> StreamBinding:
        backend = self._backends.get(state.backend_name)
        # `register` refuses an unregistered name, so a missing backend here
        # would mean the Worker's registration changed under a live Run. Its
        # provider identity is unknowable rather than empty, so record what the
        # Workflow named and let replay's own check report the mismatch.
        return StreamBinding(
            stream_key=state.stream_key,
            start_cursor=self._annotation_start.get(state.wait_id, state.start_cursor),
            backend_name=state.backend_name,
            provider_id=type(backend).provider_id if backend is not None else "",
            provider_format_version=(
                type(backend).provider_format_version if backend is not None else 1
            ),
        )

    # --- quiescence (P10a) ----------------------------------------------------

    def quiescent_snapshot(self) -> list[QuiescentWait] | None:
        """The **complete** set of waits Workflow code is blocked on.

        ``None`` when nothing is blocked, which is the signal not to ask for
        retention at all. A *partial* set would be worse than none: it would let
        one idle stream park a Workflow Task another stream is still driving.
        """
        blocked = [s for s in self._subscriptions.values() if s.blocked]
        if not blocked:
            return None
        # A set the delivery budget stopped is blocked but not quiescent: records
        # are still sitting in the local buffer, and the only reason nobody is
        # reading them is that this activation ran out of budget. Calling any of
        # it immediately parkable would ask Core to park a Workflow Task whose
        # data has already arrived at the Worker.
        exhausted = self.delivery_budget_exhausted()
        return [
            QuiescentWait(
                wait_id=s.wait_id,
                generation=s.generation,
                immediately_parkable=s.fence_reached and not exhausted,
            )
            for s in sorted(blocked, key=lambda s: s.wait_id)
        ]

    def effective_idle_timeout(self) -> timedelta:
        """The quiescent set's timeouts reduced to one value.

        The reduction is ``min``, applied in ``wait_id`` order over the blocked
        set and nothing else, so the result is deterministic and reproduces on
        replay. It has to reduce at all because the timeout is a property of the
        *set*: subscriptions configured differently still share one timer.
        """
        blocked = sorted(
            (s for s in self._subscriptions.values() if s.blocked),
            key=lambda s: s.wait_id,
        )
        if not blocked:
            return self._default_idle_timeout
        reduced = blocked[0].idle_timeout
        for state in blocked[1:]:
            reduced = min(reduced, state.idle_timeout)
        return reduced

    # --- teardown -------------------------------------------------------------

    def subscriptions(self) -> list[int]:
        return sorted(self._subscriptions)

    def blocked_snapshot(self) -> dict[int, Cursor]:
        return {
            wait_id: state.delivery_cursor
            for wait_id, state in sorted(self._subscriptions.items())
        }
