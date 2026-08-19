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
    encode_bindings,
    encode_header,
    encode_segment,
    encode_terminal,
    encoded_run_size,
    encoded_segment_size,
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
from temporalio.contrib.external_workflow_streams._errors import (
    ExternalStreamCapacityError,
)
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

_RUN_COST_FLOOR = 64
"""The per-record annotation cost assumed until a real run has been measured.

Pessimistic on purpose, and only ever a *starting* price: the largest run the
current annotation has actually encoded replaces it as soon as there is one, and
that measurement survives the segment it was taken in. A floor alone is not a
bound -- a provider chooses how long its offsets are, and a run costs two of
them -- which is why :data:`_SEGMENT_SPILL_CAP` exists behind it.

Well above what a typical provider's two offsets cost (a Redis stream ID is
fifteen bytes), so in the ordinary case this bounds nothing: the per-activation
record cap is reached long first.
"""

_SEGMENT_SPILL_CAP = 4096
"""The largest margin a segment frame may overrun the affordability line into.

The runtime prices a record it has not seen yet, so it can misprice one -- and a
segment frame cannot be moved to the next annotation, because its deliveries
happened in this Workflow Task and that task's marker is where replay has to find
them. The margin is what makes a misprice a *rollover* instead of a refused
frame: the segment spills into it, the same completion asks Core to end the
Workflow Task, and the terminal is still reserved behind it.

Capped rather than a plain fraction so that a large budget does not hold back
kilobytes it will never need; floored by the fraction so that a small budget --
which only tests use -- still has a margin at all.
"""

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
    """Whether the *current* annotation has carried this subscription's binding.

    The first observation must carry provider identity, stream key, and start
    cursor even if no record was ever delivered -- otherwise replay of a
    subscription to an empty stream has nowhere to begin.

    Read rather than merely set, because ``register`` accepts a subscription at
    any activation of a retained Workflow Task and the header frame for that
    task has usually already gone to Core. A wait that joined after it is bound
    by its own bindings frame, and this is what says which waits still need
    one.
    """

    fence_reached: bool = False
    """Set by a write fence, cleared by any later record.

    A fence means only that *this* producer session's preceding writes are all
    appended. A later record does not violate it; it simply clears it.
    """

    blocked: bool = True
    """Whether Workflow code is currently waiting on this subscription."""

    ready_records: int = 0
    """Records delivered to this subscription's ready list and not yet consumed.

    What separates :attr:`delivery_cursor` from :attr:`consumption_cursor`, as a
    count rather than as two boundaries -- until a close drops the ready list,
    which zeroes this while leaving both cursors where they were. Kept
    because the per-activation delivery budget has to be charged for records
    *already in Workflow code's hands* at the start of an activation as well as
    for the ones it delivers during it: a ready list carried across an activation
    boundary is consumed with no drain and therefore no budget check, and n
    subscriptions each carrying one would otherwise let one activation run for n
    times the cap.
    """

    closed: bool = False
    """Whether Workflow code has ended this subscription.

    A closed wait keeps its binding and its cursor -- replay and a successor Run
    both still need them -- but can never be blocked again, so it can never put
    Core back in the position of retaining a Workflow Task for a wait nothing is
    awaiting.
    """


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
        #: Already bound to this Workflow's `WorkflowSerializationContext` by
        #: the Worker that built this runtime, so `codec_for` hands Workflow
        #: code a converter carrying the same context every other payload in the
        #: activation was converted with. Bound out there rather than in here
        #: because `with_context` runs user code, and this object lives on the
        #: far side of the sandbox boundary.
        self._data_converter = data_converter
        self._default_idle_timeout = default_idle_timeout
        self._max_annotation_bytes = max_annotation_bytes
        #: The margin a segment frame may overrun the affordability line into.
        #: See :data:`_SEGMENT_SPILL_CAP`.
        self._spill_bytes = min(_SEGMENT_SPILL_CAP, max(64, max_annotation_bytes // 8))
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
        #: What each of those runs costs encoded, parallel to `_runs`. Kept
        #: because the open segment's frame size is what decides whether another
        #: record can be *delivered* at all: a segment frame that no longer fits
        #: the annotation cannot be deferred to the next one -- its deliveries
        #: happened in this Workflow Task and that task's marker is where replay
        #: has to find them -- so the only place left to act is before the record
        #: that would grow it is handed over. Measured, never estimated: a run's
        #: cost is two provider-supplied offset strings whose length this side
        #: does not choose.
        self._run_sizes: list[int] = []
        #: The largest run *this annotation* has encoded, in bytes. Unlike
        #: `_run_sizes` this survives `close_segment`, which is the whole point:
        #: a record is priced before it is delivered, and pricing the first record
        #: of each activation at the bare floor -- as a per-segment maximum
        #: does -- hands over a record the closing segment then cannot record.
        self._max_run_bytes = 0
        #: How many segments this annotation already holds. Zero means the next
        #: record would be its first, which is the one case delivery is never
        #: refused for: a fresh annotation is the most room there will ever be, so
        #: refusing there rolls over to an annotation that refuses identically.
        self._segments_in_annotation = 0
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
        #: Records this activation has put into Workflow code's hands, whether by
        #: delivering them or by starting with them already in a ready list.
        #: Counted here rather than per subscription because the cap is an
        #: *activation* budget: `merge()` consumes from several subscriptions
        #: inside one activation, and a per-subscription counter would let n
        #: streams run n times as long.
        self._delivered_this_activation = 0

    # --- the per-activation delivery budget ---------------------------------

    def begin_activation(self) -> None:
        """Resets the delivery budget. Called once per activation.

        The budget is per activation because that is the unit the deadlock
        timeout applies to: what must be bounded is how long one ``activate()``
        call can run, not how much a Run receives over its life.

        **Reset to what is already in Workflow code's hands, not to zero.** A
        batch is delivered whole and consumed one record at a time, so an
        activation that stops iterating part-way through leaves the rest in the
        subscription's ready list -- where the *next* activation consumes it with
        no drain, and so with no budget check of any kind. Zeroing here would make
        that carried-over remainder free, and it accumulates: one subscription can
        drain a full batch, consume one record and block elsewhere on every
        activation in turn, so n subscriptions arrive at an activation holding
        roughly n times the cap between them and hand all of it over in one
        `activate()` call. Starting the count at the carry-over makes what an
        activation may hand over -- carried-over plus newly delivered -- exactly
        the cap, whatever the schedule.
        """
        self._delivered_this_activation = sum(
            state.ready_records for state in self._subscriptions.values()
        )

    def delivery_budget_remaining(self) -> int:
        """How many more records this activation may hand to Workflow code.

        Two budgets, and the smaller wins. The record cap bounds how long one
        ``activate()`` call can run; the **annotation budget** bounds what the
        marker for this Workflow Task can record. The second belongs here for the
        same reason as the first: a record handed to Workflow code has to be
        recorded, a segment frame that no longer fits cannot be moved to the next
        annotation -- its deliveries happened in *this* Workflow Task and that
        task's marker is where replay must find them -- and there is no third
        option. So the budget is spent before the record is delivered rather than
        checked after the segment is built.

        Unbounded during replay. Delivery then comes from the recorded segments
        rather than a live producer, so it is already finite, the recorded
        boundaries already say how many records each activation received --
        re-cutting them here would deliver a different schedule than the one in
        History -- and nothing is being written to a new annotation at all.
        """
        if self._replay_ready is not None:
            return _REPLAY_UNBOUNDED
        return min(
            max(0, MAX_RECORDS_PER_ACTIVATION - self._delivered_this_activation),
            self._annotation_records_affordable(),
        )

    def delivery_budget_exhausted(self) -> bool:
        """Whether this activation stopped delivering because of a budget.

        The completion path asks, because records left buffered by a budget have
        no readiness notification coming: the watcher moved its prefetch cursor
        past them when it buffered them. Their readiness has to be re-reported or
        the Workflow waits forever on records already in front of it.

        Either budget counts. The annotation one leaves records buffered in
        exactly the same way, and its rollover ends the Workflow Task rather than
        the activation -- so the successor task has to be told the buffer is not
        empty just as the next activation would have been.

        **Conservative rather than exact**, in both of the ways the count can
        reach the cap: an activation whose last drain happened to empty the buffer
        did not stop *because* of the budget, and an activation pre-charged for a
        carried-over ready list may not have tried to deliver at all. Both
        over-report, and over-reporting is the safe direction -- a re-arm for an
        empty buffer is skipped by the manager, and a wait wrongly withheld from
        immediate parkability is retained for its idle timeout instead of parked.
        Under-reporting loses records: nothing else announces what a budget left
        behind.
        """
        return self._replay_ready is None and (
            self._delivered_this_activation >= MAX_RECORDS_PER_ACTIVATION
            or self.annotation_budget_exhausted
        )

    # --- the annotation byte budget (ADR-007) --------------------------------

    @property
    def annotation_budget_exhausted(self) -> bool:
        """Whether the annotation can no longer afford another record.

        Read by the completion path, which turns it into a rollover request, and
        by :meth:`_segment_end_reason`, which records it as the reason this
        segment ended. Not a failure: approaching the budget is a runtime event
        and rollover is the mechanism for it (ADR-007).

        Deliberately **not** conditioned on this activation having observed
        anything. That is a property of the activation and this is a property of
        the annotation, and conflating the two wedges the Workflow: the completion
        path reads this *after* ``take_observation_delta`` -- which it must, since
        that call is what closes the crossing segment -- and that call clears the
        observed flag. The rollover would then go unrequested, the next activation
        would begin against the same full annotation, and
        :meth:`delivery_budget_remaining` would hand it a budget of zero. Nothing
        delivered means nothing observed, which means no rollover, forever.
        """
        return self._replay_ready is None and self._annotation_records_affordable() <= 0

    def _annotation_records_affordable(self) -> int:
        """How many more records this activation's segment can afford to record.

        Bytes converted into records by the most expensive run **this annotation**
        has encoded, floored at :data:`_RUN_COST_FLOOR`. Annotation-wide and not
        per-segment, which is the difference between a price and a guess: the open
        segment is emptied at the end of every activation, so a per-segment
        maximum prices the first record of *every* activation at the bare floor,
        and a real run costs two provider-chosen offset strings. Delivering on
        that price hands over a record the closing segment then cannot record.

        Still only a price, never a proof: nothing here has seen the offsets of
        the record it is pricing. What makes the arithmetic safe is that a
        misprice spills into :data:`_SEGMENT_SPILL_CAP` and becomes a rollover,
        and that the reserve behind the margin keeps the terminal affordable
        regardless.

        **One record is always affordable while the annotation holds no segment.**
        A fresh annotation is the most room there will ever be, so refusing there
        would roll over to an annotation that refuses identically -- a Workflow
        that delivers nothing, observes nothing, and therefore never even asks for
        the rollover that was supposed to save it.
        """
        headroom = self._annotation_headroom() - self._segment_bytes()
        price = max(_RUN_COST_FLOOR, self._max_run_bytes)
        if headroom >= price:
            return headroom // price
        if self._segments_in_annotation == 0 and not self._runs:
            return 1
        return 0

    def _annotation_headroom(self) -> int:
        """Bytes this annotation still has for frames that are not its closers.

        The reserve is re-priced here rather than read off the accumulator, which
        holds whatever it was told last. A terminal entry costs one byte while a
        wait sits at ``BEGINNING`` and more once it has a cursor, so the figure
        the accumulator is carrying goes stale on the first delivery -- and a
        stale reserve on this path is an *under*-reserve, which is the direction
        that matters.

        When there is no accumulator yet, the first observation is about to create
        one and its header comes out of the same budget, so the header is priced
        here rather than discovered to be unaffordable after the fact.
        """
        size = (
            self._accumulator.size
            if self._accumulator is not None
            else len(encode_header(self._header_preview()))
        )
        return max(
            0,
            self._max_annotation_bytes
            - size
            - self._reserve_bytes()
            - self._spill_bytes,
        )

    def _segment_bytes(self) -> int:
        """What the open segment would cost as a frame, right now."""
        if not self._run_sizes:
            return 0
        return encoded_segment_size(self._run_sizes)

    def _reserve_bytes(self) -> int:
        """What closing this annotation will cost: the terminal, plus bindings.

        Held back rather than checked, because neither frame may ever be refused:
        both record something that already happened, and an annotation Core
        writes without a terminal is durable and cannot be decoded past the frame
        after it. See :attr:`AnnotationAccumulator.reserved`.
        """
        reserve = len(
            encode_terminal(
                {
                    wait_id: state.delivery_cursor
                    for wait_id, state in sorted(self._subscriptions.items())
                }
            )
        )
        late = {
            wait_id: self._binding(state)
            for wait_id, state in sorted(self._subscriptions.items())
            if not state.announced
        }
        if late and self._accumulator is not None:
            # Only with an accumulator: without one the header is about to carry
            # every one of these, and `_annotation_headroom` prices it there.
            reserve += len(encode_bindings(late))
        return reserve

    def _update_reserve(self) -> None:
        """Re-prices the closing frames on the accumulator that holds them."""
        if self._accumulator is not None:
            self._accumulator.reserve(self._reserve_bytes(), spill=self._spill_bytes)

    def _check_segment_recordable(self) -> None:
        """Refuses a segment that has grown past even the spill margin.

        The last line, and the one thing the arithmetic above cannot rule out: a
        record is priced before its offsets are seen, so a provider whose offsets
        are far longer than anything measured can make one run cost more than the
        whole budget has left. Rollover does not help -- a fresh annotation still
        has to carry this run -- so the boundary is genuinely unrecordable.

        Raised **here**, where the record has been drained but not yet handed to
        Workflow code, and as the same non-retryable capacity error
        ``subscribe()`` raises. Two things follow, and both are the point:
        ``AnnotationBudgetExceeded`` is not what surfaces, so the message names
        the provider's offsets rather than an internal byte budget; and the
        Workflow fails rather than its Workflow Task, so the encoding that cannot
        fit is not retried forever (ADR-007).
        """
        # Headroom plus the margin: what a segment may still spend, counting the
        # header and every earlier frame this annotation already holds. Measured
        # against the reserve alone it would miss the header entirely, and the
        # header is the largest frame most annotations carry.
        emittable = self._annotation_headroom() + self._spill_bytes
        segment = self._segment_bytes()
        if segment <= emittable:
            return
        largest = max(self._run_sizes) if self._run_sizes else 0
        raise ExternalStreamCapacityError(
            f"this Workflow Task's external stream deliveries no longer fit the "
            f"replay annotation: the segment recording them needs {segment} bytes "
            f"against {emittable} available, and its largest single run costs "
            f"{largest}. A run is two provider-supplied offsets, so this means the "
            "backend's offsets are far longer than the marker format is sized for. "
            "Rolling the Workflow Task over cannot help -- the next annotation has "
            "to carry the same run. Use a backend with shorter offsets, or "
            "subscribe to fewer streams from one Workflow."
        )

    def _check_annotation_capacity(self, wait_id: int) -> None:
        """Refuses a subscription set no annotation could ever record.

        A header is one indivisible frame, and a binding carries four
        caller-chosen strings -- namespace, Workflow ID, first-execution Run ID,
        stream name -- plus the backend name and provider id. Enough
        subscriptions, or long enough valid names, and the header alone is larger
        than the whole budget. Nothing downstream can recover from that: rollover
        writes a *fresh* header, so the next annotation is the same size and the
        one after that too, and the Workflow Task fails identically on every
        retry with no marker ever written. ADR-007 exists to keep the budget from
        producing exactly that.

        So the capacity question is asked where the answer is still actionable:
        inside the Workflow's ``subscribe()`` call. It is deterministic -- the
        same subscriptions in the same order give the same answer -- so replay
        reproduces the refusal rather than diverging on it.

        Priced against an **empty** annotation, and against everything such an
        annotation is nonetheless obliged to carry: its header, its terminal, one
        segment frame -- an activation that drained and observed nothing still
        encodes one, and it is meaningful (ADR-018) -- and the spill margin a
        mispriced record overruns into. Leaving any of those out accepts a
        subscription set that clears the check and then cannot encode its very
        first completion, which is the failure this exists to prevent rather than
        to relocate.
        """
        floor = (
            len(encode_header(self._header_preview()))
            + len(
                encode_terminal(
                    {
                        other: state.delivery_cursor
                        for other, state in sorted(self._subscriptions.items())
                    }
                )
            )
            + len(encode_segment(Segment((), SegmentEndReason.NO_DATA_AVAILABLE)))
            + self._spill_bytes
        )
        if floor <= self._max_annotation_bytes:
            return
        raise ExternalStreamCapacityError(
            f"subscribing external stream wait {wait_id} would make this "
            f"Workflow's replay annotation need {floor} bytes before recording a "
            f"single record -- its header, its terminal, one segment frame, and "
            f"the {self._spill_bytes}-byte margin -- past the "
            f"{self._max_annotation_bytes}-byte budget. "
            "Rolling the Workflow Task over cannot help: every annotation begins "
            "with a header of this size. Subscribe to fewer streams from one "
            "Workflow, or shorten the stream and backend names."
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

        Raises:
            ExternalStreamCapacityError: This subscription set cannot be recorded
                in an annotation at all -- see
                :meth:`_check_annotation_capacity`. Raised *here*, out of the
                Workflow's own ``subscribe()`` call, because that is the only
                point at which the answer is still "do not make this
                subscription" rather than "this Workflow Task cannot be
                completed".
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
        state = _SubscriptionState(
            wait_id=wait_id,
            stream_key=stream_key,
            backend_name=backend_name,
            start_cursor=start_cursor,
            delivery_cursor=start_cursor,
            consumption_cursor=start_cursor,
            idle_timeout=idle_timeout or self._default_idle_timeout,
        )
        self._subscriptions[wait_id] = state
        # A subscription created part-way through an annotation begins at its
        # own start cursor, not at wherever the others happen to be.
        self._annotation_start[wait_id] = start_cursor
        try:
            self._check_annotation_capacity(wait_id)
        except Exception:
            # Rolled back so the refusal leaves no half-registered wait behind:
            # a state entry with no manager registration would reach the next
            # annotation's header as a binding for a wait nothing is watching.
            del self._subscriptions[wait_id]
            del self._annotation_start[wait_id]
            raise
        self._update_reserve()
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
            #
            # Taken from the **front**, and only while the front belongs to this
            # wait. A segment is the recorded global order across every stream --
            # the order `record_delivery` saw, which is the order Workflow code
            # took records in -- so a drain that searched past a record belonging
            # to another wait would hand this one a record that came *after* it
            # live. A segment recorded as (wait 2, wait 1) replays through
            # `merge` as (wait 1, wait 2) under that reading, because `merge`
            # asks in `wait_id` order and the search obliges every time.
            #
            # Live, that drain would simply have found nothing: the record was
            # not in this wait's buffer yet. Returning nothing here is the same
            # answer, and it leaves the record where the drain that recorded it
            # will find it.
            taken: list[StreamRecord] = []
            while (
                self._replay_ready
                and self._replay_ready[0][0] == wait_id
                and (max_records is None or len(taken) < max_records)
            ):
                taken.append(self._replay_ready.pop(0)[1])
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
        """Everything the marker recorded must have happened again.

        Two things, checked once the last segment has been delivered.

        A record in a run was handed to Workflow code during the activation the
        run was recorded in, so a replay that leaves one behind is running
        different code. The common way to get here is a removed ``subscribe()``
        call: its wait is never registered, nothing ever drains it, and the
        marker's records for it would otherwise be discarded in silence -- the
        Workflow reaching its next command having consumed less than History
        says it consumed.

        And every wait the marker *bound* must have been recreated, which is a
        strictly larger claim: a binding is written for a subscription whether
        or not anything was ever delivered through it. The first observation has
        to carry provider identity, stream key, and start cursor even for a
        stream that stayed quiet for the whole Workflow Task, so a binding with
        no runs behind it is the ordinary shape of a quiet subscription rather
        than an exotic one -- and it is invisible to every check that reasons
        from deliveries.
        """
        self._verify_replay_consumed()
        # The one check that reads in the other direction. Every other one walks
        # what the code *did*: :meth:`begin_replay` iterates the subscriptions
        # that exist and verifies only those the marker also names,
        # :meth:`register` verifies a wait only when it is registered at all,
        # and :meth:`_verify_replay_consumed` has nothing to report unless
        # records were left undelivered. A recorded wait that the code no longer
        # creates and that the marker holds no records for is therefore reached
        # by none of them.
        #
        # Two removals escape without this, and the second is the worse one:
        #
        # - the **last** ``subscribe()`` removed, on a stream that was quiet.
        #   Nothing renumbers, no records go undelivered, and the replay is
        #   accepted although the Workflow now holds one subscription fewer than
        #   History says it did -- with its own live reads never made.
        # - a **middle** ``subscribe()`` removed where the later waits name the
        #   same stream and backend. Every survivor renumbers down by one, so
        #   the binding comparison compares equal for all of them, and the
        #   records the marker recorded for wait *k* are consumed by what was
        #   subscription *k+1*: a different cursor, a different consumer, and
        #   nothing left over for the delivery check to notice.
        #
        # Deliberately **not** symmetric, and deliberately not in
        # :meth:`_verify_replay_consumed`, which :meth:`begin_replay_segment`
        # also calls before each segment. "every bound wait was registered" is
        # the invariant; the converse is not, because replay runs the Workflow
        # forward past the Workflow Task the marker covers, and the
        # subscriptions it makes there belong to the *next* marker's header. An
        # added ``subscribe()`` at the end is a supported change and must stay
        # one.
        missing = sorted(set(self._replay_bindings or {}) - set(self._subscriptions))
        if missing:
            # Row four, not integrity loss: the recorded ranges are exactly
            # where they were written, and it is the Workflow code that moved.
            raise temporalio.workflow.NondeterminismError(
                f"the marker records external stream wait(s) {missing} that this "
                "Workflow never created. A subscribe() call was removed or "
                "reordered, which renumbers every later wait and can hand one "
                "wait's recorded records to another wait's subscription; gate "
                "the change behind workflow.patched() exactly as a removed "
                "timer would be."
            )

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

        Performed **synchronously**, before this returns: the drain that
        follows replay runs on this same thread, so a reposition merely posted
        to the manager's loop would still have the marker-covered records in the
        buffer when that drain reaches it. Only the watcher's wakeup is hopped;
        see :meth:`StreamSubscriptionManager.reposition_to_committed`.
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

        **Also where the activation's delivery budget is spent.** Delivery is the
        moment a record leaves the manager's buffer for a subscription's private
        ready list, and that is what has to be charged rather than the later
        consumption of it, for two reasons that point the same way. The budget is
        a *reservation*: a drain that checked the budget and charged nothing let
        the next subscription's drain see the same room and take it again, so two
        subscriptions consumed independently -- no `merge()` involved -- delivered
        twice the cap in one activation, and n of them n times it. And delivery is
        what the annotation records, so a cap charged at consumption bounded a
        different quantity than the segment it is supposed to bound: the recorded
        segment could hold more records than the cap replay will divide it by.
        """
        state = self._subscriptions.get(wait_id)
        if self._replay_ready is None:
            # Counted before the guards below so that a record whose subscription
            # has already gone still costs its budget: the cap has to bound the
            # activation whatever the bookkeeping says.
            #
            # Replayed records are deliberately not counted, not merely not
            # capped. Counting them would leave the budget spent for any live
            # delivery later in the same activation, and would make the completion
            # re-arm readiness on a purely replayed Workflow Task.
            self._delivered_this_activation += 1
        if state is None or record.offset is None:
            return

        state.delivery_cursor = AFTER(record.offset)
        state.ready_records += 1
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
            # Only the run that changed is re-measured. Extending one is cheap;
            # re-encoding the whole segment once per record would not be.
            self._run_sizes[-1] = encoded_run_size(self._runs[-1])
            self._max_run_bytes = max(self._max_run_bytes, self._run_sizes[-1])
            self._check_segment_recordable()
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
        self._run_sizes.append(encoded_run_size(self._runs[-1]))
        self._max_run_bytes = max(self._max_run_bytes, self._run_sizes[-1])
        self._check_segment_recordable()

    def unsubscribe(self, wait_id: int) -> None:
        """Ends a wait, and tells the Worker to stop serving it.

        The subscription's state is **kept**, not removed. Two things are built
        from it after a wait ends: the annotation header, whose binding replay
        needs in order to know what the closed wait was reading -- without it a
        replay of unchanged code fails as though the Workflow never created that
        wait -- and the Continue-As-New continuation, whose cursor is what stops
        a successor Run restarting that stream from the beginning. Closing
        changes exactly one thing about the state: the wait can never be blocked
        again.
        """
        state = self._subscriptions.get(wait_id)
        if state is None or state.closed:
            return
        state.closed = True
        state.blocked = False
        # Whatever was drained but never handed over is dropped rather than
        # consumed -- that is what leaves the consumption cursor short of it, so a
        # Continue-As-New successor receives it. It also stops being carry-over the
        # next activation's budget has to pay for: those records are gone, and a
        # count left standing would shrink every later activation's budget by the
        # size of a ready list nobody can consume.
        state.ready_records = 0
        self._manager.cancel_from_workflow_thread(self._run_id, wait_id)

    def note_blocked(self, wait_id: int, blocked: bool) -> None:
        """Records whether Workflow code is waiting on this subscription."""
        state = self._subscriptions.get(wait_id)
        if state is None or (state.closed and blocked):
            # A closed wait may leave the blocked set but never re-enter it: the
            # coroutine that was awaiting it is gone, so retaining a Workflow
            # Task for it would be retaining for nobody.
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
        self._run_sizes = []
        self._segments_in_annotation += 1

    def _segment_end_reason(self) -> SegmentEndReason:
        """Why this activation stopped delivering.

        Decided here rather than passed in, because the reason has to be right
        whether the caller remembered it or not: it is durable, and a reader of
        the annotation has nothing else to tell it why the activation ended.

        ``BATCH_LIMIT`` outranks the two availability reasons. Both of them
        assert that nothing more was available -- ``FENCE_REACHED`` additionally
        that the set is immediately parkable -- and both are simply false when the
        runtime stopped with records still sitting in the buffer. Recording
        ``NO_DATA_AVAILABLE`` for a budget cut-off would put a claim in History
        that the stream ran dry when it did not.

        ``BUDGET_ROLLOVER`` outranks even that, because it says something the
        others do not: the batch continues in the *following marker*. A rollover
        recorded as ``BATCH_LIMIT`` would leave replay treating this segment as
        the end of the consumption.
        """
        if self.annotation_budget_exhausted:
            # Ranked first: it is the only one of the four that says the batch
            # continues in the *next marker*, which is what tells replay to
            # reassemble across the rollover boundary rather than treat this
            # segment as the end of the consumption.
            return SegmentEndReason.BUDGET_ROLLOVER
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
        """Whether Core should be asked to end this Workflow Task's annotation.

        Two conditions, and the second is not redundant. The high-water mark is a
        *fraction* of the budget, and it only becomes true once a frame that
        crossed it has been emitted -- but an indivisible frame can be larger than
        the fraction that was left, so waiting for the mark alone is not a bound.
        The second condition is the runtime having stopped delivering because the
        annotation could no longer afford to record another record: that one is
        true in the same activation, before anything has overflowed, and it is
        what makes "an annotation can never exceed the budget" (ADR-007) hold for
        a frame of any size.
        """
        if self.annotation_budget_exhausted:
            return True
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
        self._run_sizes = []
        self._max_run_bytes = 0
        self._segments_in_annotation = 0
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

        Not where the delivery budget is spent -- :meth:`record_delivery` is,
        because the budget has to be reserved by the drain that moves a whole
        batch into a ready list rather than charged one record at a time
        afterwards. What this does spend is the *carry-over* the next
        :meth:`begin_activation` starts its count from: a record consumed here is
        one the next activation no longer has in hand.
        """
        state = self._subscriptions.get(wait_id)
        if state is None or record.offset is None:
            return
        state.consumption_cursor = AFTER(record.offset)
        state.ready_records = max(0, state.ready_records - 1)

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
        else:
            self._announce_late_subscriptions()
        # Re-priced after any late binding went out, so what is held back is what
        # is still owed rather than what was owed when the accumulator was made.
        self._update_reserve()
        return self._accumulator

    def _announce_late_subscriptions(self) -> None:
        """Binds every subscription the emitted header does not already carry.

        ``register`` accepts a subscription at any activation of a retained
        Workflow Task, and only the first of those activations gets to write the
        header -- Core appends the deltas it is handed and never rewrites what it
        already holds, so the header cannot be extended in place. A wait that
        joined later is bound by its own frame instead.

        Called from :meth:`_ensure_accumulator`, which is on the path of both
        things that can follow a registration: the segment that records the
        wait's first run, and the terminal that records where it stopped. The
        binding therefore always precedes both, and no wait can reach the marker
        as a run or a terminal entry with no stream key, no backend, and no start
        cursor -- which replay reports as a wait "this Workflow did not create"
        even when the code is unchanged.
        """
        assert self._accumulator is not None
        late = {
            wait_id: state
            for wait_id, state in sorted(self._subscriptions.items())
            if not state.announced
        }
        if not late:
            return
        for state in late.values():
            state.announced = True
        self._pending_deltas.append(
            self._accumulator.add_bindings(
                {wait_id: self._binding(state) for wait_id, state in late.items()}
            )
        )

    def _header(self) -> AnnotationHeader:
        """One binding per subscription, each naming **its own** backend.

        Marks every subscription announced, which is also how the flag is reset
        for the *next* annotation: a header is written from scratch whenever an
        accumulator is created, so whatever exists when it is carries its
        binding there rather than in a bindings frame.

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

    def _header_preview(self) -> AnnotationHeader:
        """The header a fresh annotation would carry, without claiming it.

        :meth:`_header` marks every subscription announced, which is right when
        the header is actually being emitted and wrong when the question is only
        what it would cost.
        """
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
