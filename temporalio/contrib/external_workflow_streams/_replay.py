"""The replay read path (P13).

Replay reads the **exact ranges the marker recorded** and delivers them from
memory in the recorded order. It never asks the backend what comes next: the
answer is already in the annotation, and consulting current stream timing would
be reproducing something other than what happened.

Three kinds of waiting exist, and only one of them occurs here:

===============================================  ==========================
Waiting for *new* records -- watchers, idle      Never. Core starts no
timer, park handshake                            timers and no watcher runs.
Reading *recorded* offsets                       Occurs. Ordinary blocking
                                                 provider I/O.
Blocking on backend latency or unavailability    Not a determinism failure --
                                                 a transient storage failure.
===============================================  ==========================

**The four range checks are the whole of validation**, and they are sufficient
precisely because every provider guarantees a record's bytes cannot change
(ADR-003). Given that, the only damage replay has to detect is a record that is
no longer *there* -- and deletion, trimming, and retention expiry are all caught
by one of the four:

1. both endpoints present,
2. the range contains exactly ``count`` records,
3. ordering is strictly increasing under the provider's comparator,
4. ``control_positions`` match.

A first, middle, or last deletion each fails a different one of them.

What these checks are **not** for is a marker that names a subscription this
Workflow never created. That is row four of the taxonomy -- ordinary
nondeterminism -- because nothing is wrong with the backend: the ranges are
exactly where they were written and the Workflow code changed underneath them.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import temporalio.workflow
from temporalio.contrib.external_workflow_streams._annotation import (
    Annotation,
    Run,
    decode_annotation,
)
from temporalio.contrib.external_workflow_streams._backend import (
    StreamBackend,
    StreamKey,
)
from temporalio.contrib.external_workflow_streams._errors import (
    StreamIntegrityError,
    StreamStorageError,
)
from temporalio.contrib.external_workflow_streams._record import (
    AFTER,
    Cursor,
    StreamRecord,
)

__all__ = ["ReplayPlan", "ReplaySegment", "build_replay_plan", "validate_run"]


@dataclass(frozen=True)
class ReplaySegment:
    """One original activation's deliveries, in the order they happened.

    Segments are kept apart rather than concatenated because each one was a
    separate event-loop drain. Collapsing them would reproduce the record order
    while changing how many drains occurred, and ``wait_condition`` predicates
    would then fire a different number of times (ADR-018).
    """

    deliveries: tuple[tuple[int, StreamRecord], ...]


@dataclass
class ReplayPlan:
    """Everything one marker's replay needs, already read and validated."""

    annotation: Annotation
    segments: list[ReplaySegment] = field(default_factory=list)

    @property
    def total_records(self) -> int:
        return sum(len(s.deliveries) for s in self.segments)

    @property
    def committed_boundaries(self) -> dict[int, Cursor]:
        """Where this marker left each wait it recorded anything for.

        What live delivery has to resume from once replay has handed the
        recorded ranges over. The terminal is the authority, and every marker
        has one: Core asks for a terminal on the boundaries it decides, and the
        completion path supplies one on the boundaries Python decides -- a
        Workflow Task that ended carrying server-bound commands included
        (ADR-008).

        The fallback to the last recorded delivery per wait is kept for a marker
        that somehow arrives without one, and it is not an approximation: both
        name the last record the Workflow Task handed over. What only the
        terminal can say is where a wait that received *nothing* in its final
        activation stopped.

        A wait the marker recorded nothing for is deliberately absent rather
        than mapped to its start cursor. Nothing was delivered for it, so there
        is nothing to resume past.
        """
        boundaries: dict[int, Cursor] = {}
        for segment in self.annotation.segments:
            for run in segment.runs:
                boundaries[run.wait_id] = AFTER(run.last_offset)
        # The terminal wins wherever it exists: it is where deliveries stopped
        # at the end of the Workflow Task, which can be past the last run for a
        # wait whose final activation delivered nothing.
        for wait_id, cursor in (self.annotation.terminal or {}).items():
            boundaries[wait_id] = cursor
        return boundaries


def validate_run(run: Run, records: list[StreamRecord], backend: StreamBackend) -> None:
    """Applies the four range checks. Raises :class:`StreamIntegrityError`.

    **Integrity loss must never resolve to an alternate stream result.** Every
    check below reports what is wrong rather than repairing it, because
    substituting a later record for a deleted one would hand Workflow code a
    different history than the one its commands were derived from -- and that
    would surface much later as an unrelated nondeterminism error.
    """
    offsets = [r.offset for r in records]
    if any(o is None for o in offsets):
        raise StreamIntegrityError(
            f"the provider returned a record with no offset for range "
            f"[{run.first_offset}, {run.last_offset}]"
        )

    # 1. Both endpoints present. A first- or last-record deletion fails here,
    # and nothing else would catch it: the count check alone cannot tell a
    # missing endpoint from a missing interior record.
    if not records or records[0].offset != run.first_offset:
        raise StreamIntegrityError(
            f"the record at {run.first_offset} is missing from the stream; the "
            f"marker records it as the first of a run of {run.count}"
        )
    if records[-1].offset != run.last_offset:
        raise StreamIntegrityError(
            f"the record at {run.last_offset} is missing from the stream; the "
            f"marker records it as the last of a run of {run.count}"
        )

    # 2. Exact count. An interior deletion fails here.
    if len(records) != run.count:
        raise StreamIntegrityError(
            f"range [{run.first_offset}, {run.last_offset}] contains "
            f"{len(records)} record(s) but the marker records {run.count}; "
            "records have been deleted, trimmed, or have expired"
        )

    # 3. Strictly increasing under the *provider's* comparator, never lexically.
    if not backend.strictly_increasing(offsets):  # type: ignore[arg-type]
        raise StreamIntegrityError(
            f"range [{run.first_offset}, {run.last_offset}] did not read back in "
            "strictly increasing offset order"
        )

    # 4. Control positions. A control record replaced by a data record at the
    # same position would otherwise be delivered to Workflow code, which never
    # saw it the first time.
    actual = tuple(i for i, record in enumerate(records) if record.is_control)
    if actual != tuple(run.control_positions):
        raise StreamIntegrityError(
            f"range [{run.first_offset}, {run.last_offset}] has control records at "
            f"{actual} but the marker records them at {tuple(run.control_positions)}"
        )


async def build_replay_plan(
    annotation_bytes: bytes,
    backends: dict[int, StreamBackend],
    stream_keys: dict[int, StreamKey],
) -> ReplayPlan:
    """Reads and validates every recorded range, up front.

    All of it before any delivery, deliberately: a range that fails validation
    must stop replay before Workflow code has seen anything from the marker,
    rather than part-way through with some records already delivered.

    Args:
        annotation_bytes: The marker's opaque annotation.
        backends: The provider for each ``wait_id`` in the annotation.
        stream_keys: The stream each ``wait_id`` was subscribed to.

    Raises:
        StreamStorageError: The backend was unreachable or errored. Transient;
            it clears when the backend recovers.
        StreamIntegrityError: A recorded range no longer reads back as written.
            Keeps failing until an operator repairs the backend.
    """
    annotation = decode_annotation(annotation_bytes)
    plan = ReplayPlan(annotation=annotation)

    # One read per run rather than one per record: replay I/O cost is a function
    # of the consumed range, not of how the batch was segmented.
    for segment in annotation.segments:
        deliveries: list[tuple[int, StreamRecord]] = []
        for run in segment.runs:
            backend = backends.get(run.wait_id)
            key = stream_keys.get(run.wait_id)
            if key is not None and backend is None:
                # The wait is in the annotation, so the Workflow did create it;
                # what did not resolve is the backend name the marker recorded.
                # Also row four: the backend a topic names is Workflow code, and
                # the recorded records live in the backend that wrote them.
                raise temporalio.workflow.NondeterminismError(
                    f"the marker records external stream wait {run.wait_id} "
                    "against a backend name that is not registered on this "
                    "Worker. Either the Workflow now names a different backend "
                    "-- gate that behind workflow.patched() exactly as an "
                    "inserted timer would be -- or this Worker is missing the "
                    "backend registration the recorded Run had."
                )
            if backend is None or key is None:
                # Row four of the failure taxonomy, and **not** integrity loss.
                # Nothing is wrong with the backend: the recorded ranges are
                # exactly where they were written, and the Workflow code has
                # changed underneath them. Reporting this as integrity loss
                # would send an operator to repair a backend that is fine, when
                # the fix is to version the Workflow code.
                #
                # Reported rather than skipped either way, because skipping
                # would silently deliver a different stream result.
                raise temporalio.workflow.NondeterminismError(
                    f"the marker records external stream wait {run.wait_id}, "
                    "which this Workflow did not create. A subscribe() call was "
                    "inserted, removed, or reordered, which renumbers every "
                    "later wait; gate the change behind workflow.patched() "
                    "exactly as an inserted timer would be."
                )
            records = await _read_range(backend, key, run)
            validate_run(run, records, backend)
            deliveries.extend((run.wait_id, record) for record in records)
        plan.segments.append(ReplaySegment(tuple(deliveries)))

    return plan


async def _read_range(
    backend: StreamBackend, key: StreamKey, run: Run
) -> list[StreamRecord]:
    """The inclusive read, with provider failures classified as transient.

    ``read_range`` and not ``read_after``: the range is closed on both ends and
    the marker already names it. An exclusive read here would silently drop each
    run's first record, and nothing before the first replay would notice.
    """
    try:
        return await backend.read_range(key, run.first_offset, run.last_offset)
    except StreamStorageError:
        raise
    except Exception as err:
        # Not integrity loss: nothing has been shown to be missing, only
        # unreadable right now. An operator sent to repair a backend that was
        # merely unreachable would find nothing wrong with it.
        raise StreamStorageError(
            f"could not read recorded range [{run.first_offset}, {run.last_offset}] "
            f"from {key}: {err}"
        ) from err
