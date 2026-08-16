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
from temporalio.contrib.external_workflow_streams._annotation import (
    MAX_ANNOTATION_BYTES,
    AnnotationAccumulator,
    AnnotationHeader,
    Run,
    Segment,
    SegmentEndReason,
    StreamBinding,
)
from temporalio.contrib.external_workflow_streams._backend import (
    StreamBackend,
    StreamKey,
)
from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec
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

        self._subscriptions: dict[int, _SubscriptionState] = {}
        #: Where the *current* annotation begins, per wait. Captured when the
        #: annotation begins rather than when its header is first needed --
        #: lazily reading the delivery cursor would let a record delivered
        #: before the first emission slip in front of the start cursor, and
        #: replay of that marker would then never deliver it.
        self._annotation_start: dict[int, Cursor] = {}
        self._accumulator: AnnotationAccumulator | None = None
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
        start_cursor: Cursor = BEGINNING,
    ) -> None:
        """Registers a wait with the Worker's manager. Non-blocking, no I/O."""
        if backend_name not in self._backends:
            known = ", ".join(sorted(self._backends)) or "<none>"
            raise KeyError(
                f"no external stream backend named {backend_name!r} is registered on "
                f"this Worker; registered backends are: {known}"
            )
        self._subscriptions[wait_id] = _SubscriptionState(
            wait_id=wait_id,
            stream_key=stream_key,
            backend_name=backend_name,
            start_cursor=start_cursor,
            delivery_cursor=start_cursor,
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

    def begin_replay_segment(
        self, deliveries: Sequence[tuple[int, StreamRecord]]
    ) -> None:
        """Makes one recorded segment the only thing a drain can see."""
        self._replay_ready = list(deliveries)

    def end_replay(self) -> None:
        """Hands drains back to the live buffer.

        Called from a ``finally``: a partial replay that left this set would
        make every later drain on this Run return nothing at all, turning one
        marker's failure into a Workflow that silently never receives again.
        """
        self._replay_ready = None

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
            reason = (
                SegmentEndReason.FENCE_REACHED
                if self._subscriptions
                and all(s.fence_reached for s in self._subscriptions.values())
                else SegmentEndReason.NO_DATA_AVAILABLE
            )
        accumulator = self._ensure_accumulator()
        self._pending_deltas.append(
            accumulator.add_segment(Segment(tuple(self._runs), reason))
        )
        self._runs = []

    def take_observation_delta(self) -> bytes | None:
        """The bytes to put on this completion's `WorkflowStreamProgress`.

        ``None`` means nothing replay-visible changed, which is the only case
        where a completion legitimately carries no progress command.
        """
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
        """
        accumulator = self._ensure_accumulator()
        return accumulator.add_terminal(
            {
                wait_id: state.delivery_cursor
                for wait_id, state in sorted(self._subscriptions.items())
            }
        )

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
        self._pending_deltas = []
        self._runs = []
        self._observed_this_activation = False
        self._annotation_start = {
            wait_id: state.delivery_cursor
            for wait_id, state in self._subscriptions.items()
        }

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
        provider = self._provider()
        for state in self._subscriptions.values():
            state.announced = True
        return AnnotationHeader(
            provider_id=type(provider).provider_id if provider else "",
            provider_format_version=(
                type(provider).provider_format_version if provider else 1
            ),
            streams={
                wait_id: StreamBinding(
                    state.stream_key,
                    self._annotation_start.get(wait_id, state.start_cursor),
                )
                for wait_id, state in sorted(self._subscriptions.items())
            },
        )

    def _provider(self) -> StreamBackend | None:
        for state in self._subscriptions.values():
            backend = self._backends.get(state.backend_name)
            if backend is not None:
                return backend
        return None

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
        return [
            QuiescentWait(
                wait_id=s.wait_id,
                generation=s.generation,
                immediately_parkable=s.fence_reached,
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
