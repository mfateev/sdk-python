"""Replay through the real path: a live run, then its own history replayed.

The unit tests drive the replay driver directly, which proves the mechanism but
not that a history a Worker actually produced can be fed back through it. These
run a Workflow against a live server, fetch the history it wrote, and replay it
with the real ``Replayer`` -- the same tool a user reaches for to check new code
against old histories.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import pytest

import temporalio.converter
from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.external_workflow_streams._annotation import decode_annotation
from temporalio.contrib.external_workflow_streams._backend import (
    DEFAULT_WATCH_BLOCK,
    StreamKey,
)
from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec
from temporalio.contrib.external_workflow_streams._record import (
    BEGINNING,
    Cursor,
    Offset,
    RecordKind,
    StreamRecord,
)
from temporalio.worker import Replayer, Worker
from tests.contrib.external_workflow_streams.memory_backend import MemoryStreamBackend

with workflow.unsafe.imports_passed_through():
    from temporalio.contrib.external_workflow_streams._api import external_stream
    from tests.contrib.external_workflow_streams import observations


EXTERNAL_STREAM_MARKER = "core_external_stream"
"""The marker name Core writes a replay annotation under."""


# Unsandboxed on purpose: the observation sink below has to be the *same*
# object in Workflow code as in the test. A sandboxed Workflow re-imports every
# module it touches, including a passed-through one, so it would write its
# observations into a copy and the comparison would silently compare nothing.
# Nothing here depends on the sandbox; the sandbox itself is covered elsewhere.
@workflow.defn(sandboxed=False)
class ConditionWorkflow:
    """Consumes records while a ``wait_condition`` watches the same state.

    The condition is the point. It is evaluated once per event-loop drain, so it
    is a direct probe of activation segmentation: if replay collapses several
    recorded segments into one, the predicate sees a different sequence of
    states than it did live even though the records arrive in the same order.
    """

    def __init__(self) -> None:
        self._seen: list[str] = []
        self._states_observed: list[int] = []

    @workflow.run
    async def run(self, expected: int) -> list[int]:
        tokens = external_stream.with_options(idle_timeout=timedelta(seconds=30)).topic(
            "tokens", backend="tokens-memory", type=str
        )

        async def watch() -> None:
            # Records what the predicate saw on every evaluation. Returning this
            # rather than the records is what makes a collapsed replay visible:
            # the record order would look identical either way.
            await workflow.wait_condition(self._observe)

        watcher = asyncio.ensure_future(watch())
        async for token in tokens.subscribe():
            self._seen.append(token)
            if len(self._seen) >= expected:
                break
        await watcher
        # Recorded outside the Workflow as well as returned: the return value is
        # only visible for the live run, and the whole question here is whether
        # the *replay* saw the same thing.
        observations.record(workflow.info().run_id, self._states_observed)
        return self._states_observed

    def _observe(self) -> bool:
        self._states_observed.append(len(self._seen))
        return len(self._seen) >= 2


@workflow.defn
class EmptyStreamWorkflow:
    """Subscribes to a stream nothing is ever written to, then gives up.

    The case an implicit "start wherever" would leave unrecorded: the marker has
    to carry an explicit start cursor even though no record was ever delivered,
    or replay has no boundary to reproduce.
    """

    @workflow.run
    async def run(self) -> str:
        tokens = external_stream.with_options(idle_timeout=timedelta(seconds=1)).topic(
            "tokens", backend="tokens-memory", type=str
        )
        subscription = tokens.subscribe()
        iterator = subscription.__aiter__()
        try:
            await asyncio.wait_for(iterator.__anext__(), 3)
        except asyncio.TimeoutError:
            return "nothing arrived"
        return "unexpected record"


@pytest.fixture
def backend() -> MemoryStreamBackend:
    return MemoryStreamBackend()


#: Per-stream sequence, so successive publishes do not collide. `(session_id,
#: sequence)` is the idempotency key: restarting the count re-uses a key with
#: different content, which the backend contract rejects outright -- correctly,
#: and it is the test that is wrong when it happens.
_sequences: dict[StreamKey, int] = {}


async def publish(backend: MemoryStreamBackend, key: StreamKey, values: list[str]):
    codec = StreamPayloadCodec(temporalio.converter.DataConverter.default, str)
    start = _sequences.get(key, 0)
    for i, value in enumerate(values, start=start):
        await backend.append(
            key,
            StreamRecord(RecordKind.DATA, await codec.encode(value), "producer", i),
        )
    _sequences[key] = start + len(values)


async def stream_key_for(client: Client, handle, name: str) -> StreamKey:  # type: ignore[no-untyped-def]
    description = await handle.describe()
    return StreamKey(
        client.namespace,
        handle.id,
        description.raw_description.workflow_execution_info.first_run_id,
        name,
    )


async def test_replaying_a_stream_history_reproduces_the_same_observations(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """The end-to-end form of ADR-018, through the tool users actually run.

    A collapsed replay would deliver the same records in the same order and
    still be wrong: the predicate would fire a different number of times, and
    any Workflow whose control flow depends on a condition would diverge.

    So "replay did not fail" is not the assertion. Replay is only obliged to
    match the *commands* in History, and this Workflow's commands say nothing
    about how many times its predicate ran -- a replay that delivered both
    records in one drain would produce the same completion and pass a
    failure-only check. What is compared instead is the sequence the predicate
    itself observed, live against replayed, for a marker that spans several
    activations.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[ConditionWorkflow],
        external_stream_backends={"tokens-memory": backend},
    ):
        handle = await client.start_workflow(
            ConditionWorkflow.run,
            2,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        key = await stream_key_for(client, handle, "tokens")
        await asyncio.sleep(1)
        # Two separate arrivals, so the marker spans more than one activation.
        await publish(backend, key, ["alpha"])
        await asyncio.sleep(0.5)
        await publish(backend, key, ["beta"])

        live = await asyncio.wait_for(handle.result(), 60)
        history = await handle.fetch_history()
        run_id = handle.first_execution_run_id or handle.result_run_id

    markers = [
        e
        for e in history.events
        if e.HasField("marker_recorded_event_attributes")
        and e.marker_recorded_event_attributes.marker_name == EXTERNAL_STREAM_MARKER
    ]
    assert markers, "no stream marker was written, so there is nothing to replay"

    replayer = Replayer(
        workflows=[ConditionWorkflow],
        external_stream_backends={"tokens-memory": backend},
    )
    result = await replayer.replay_workflow(history)

    assert result.replay_failure is None, (
        f"replaying the history the Worker just wrote failed: {result.replay_failure}"
    )
    assert live, "the live run observed nothing, so this proves nothing"
    assert len(live) > 1, (
        "the predicate ran only once live, so a collapsed replay would look "
        f"identical and this proves nothing: {live}"
    )

    assert run_id is not None
    runs = observations.executions(run_id)
    assert len(runs) == 2, (
        "expected exactly two executions to be recorded -- the live run and the "
        f"replay -- got {len(runs)}"
    )
    live_observed, replayed_observed = runs
    assert live_observed == live, (
        "the live run recorded something other than it returned"
    )
    assert replayed_observed == live_observed, (
        "the replayed run's predicate saw a different sequence of states than "
        "the live one did. The records arrive in the same order either way, so "
        "this is the segmentation itself diverging: recorded activation "
        f"boundaries were collapsed or re-cut. live={live_observed} "
        f"replayed={replayed_observed}"
    )


async def test_a_history_with_stream_markers_needs_its_backends_to_replay(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """Replay re-reads the recorded ranges, so it needs the provider.

    Without the option there is no way to supply one, and the replay fails the
    way a Worker with no backends would -- correct, but a configuration error
    rather than a finding about the history. Asserted so the option cannot
    quietly stop being threaded through.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[ConditionWorkflow],
        external_stream_backends={"tokens-memory": backend},
    ):
        handle = await client.start_workflow(
            ConditionWorkflow.run,
            2,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        key = await stream_key_for(client, handle, "tokens")
        await asyncio.sleep(1)
        await publish(backend, key, ["alpha", "beta"])
        await asyncio.wait_for(handle.result(), 60)
        history = await handle.fetch_history()

    without = await Replayer(workflows=[ConditionWorkflow]).replay_workflow(
        history, raise_on_replay_failure=False
    )

    assert without.replay_failure is not None
    assert "external_stream_backends" in str(without.replay_failure), (
        "the failure must name the missing option rather than surface as an "
        f"attribute error: {without.replay_failure}"
    )


async def test_an_empty_stream_replays_from_its_recorded_boundary(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """A subscription that never received a record still has a boundary.

    This is the case the explicit start cursor exists for: the marker records
    where the subscription began even though nothing was delivered, so replay
    reproduces an empty observation rather than resolving a position from
    whatever the stream holds by then.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[EmptyStreamWorkflow],
        external_stream_backends={"tokens-memory": backend},
    ):
        handle = await client.start_workflow(
            EmptyStreamWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        assert await asyncio.wait_for(handle.result(), 60) == "nothing arrived"
        history = await handle.fetch_history()
        key = await stream_key_for(client, handle, "tokens")

    # Records land *after* the Run finished. Replay must not see them: the
    # marker's boundary is where the subscription was, not where the stream got
    # to afterwards.
    await publish(backend, key, ["late-one", "late-two"])

    result = await Replayer(
        workflows=[EmptyStreamWorkflow],
        external_stream_backends={"tokens-memory": backend},
    ).replay_workflow(history, raise_on_replay_failure=False)

    assert result.replay_failure is None, (
        "replay resolved a position from live stream state rather than from the "
        f"recorded boundary: {result.replay_failure}"
    )


#: ``run_id -> [whether each execution of the run body began while replaying]``.
#:
#: A Run that was evicted and came back executes its body from the top a second
#: time, replaying; one that merely stayed cached never does. Written from
#: Workflow code, which is why the Workflow below is unsandboxed: a sandboxed
#: one would write into its own copy of this module and the count would stay at
#: one however many times the Run was rebuilt.
STARTS: dict[str, list[bool]] = {}


@workflow.defn(sandboxed=False)
class EmptyStreamParkWorkflow:
    """Subscribes to an empty stream, parks on it, and consumes what arrives later.

    Nothing else is pending -- no timer, no other command -- so the first
    Workflow Task is retained until the idle timeout parks it. Both halves of
    that matter here: the park is what writes the empty marker, and it is also
    what leaves the Run resumable *only* by a wake Signal, so the records
    published while it is evicted cannot be delivered by a watcher that happened
    to survive.
    """

    def __init__(self) -> None:
        self._seen: list[str] = []

    @workflow.run
    async def run(self, expected: int) -> list[str]:
        STARTS.setdefault(workflow.info().run_id, []).append(
            workflow.unsafe.is_replaying()
        )
        tokens = external_stream.with_options(idle_timeout=timedelta(seconds=1)).topic(
            "tokens", backend="tokens-memory", type=str
        )
        async for token in tokens.subscribe():
            self._seen.append(token)
            if len(self._seen) >= expected:
                break
        # Recorded outside the Workflow as well as returned: the return value
        # exists only for the live run, and what the *replays* observed is half
        # the question here.
        observations.record(workflow.info().run_id, self._seen)
        return self._seen


@workflow.defn
class CacheFillerWorkflow:
    """Occupies the one cache slot, which is what evicts the Run under test."""

    @workflow.run
    async def run(self) -> str:
        return "done"


class RecordedRangesOnlyBackend(MemoryStreamBackend):
    """Serves recorded ranges faithfully and poisons every live read.

    Replay is obliged to read the ranges its markers name and nothing else, so
    those ranges read back exactly as written while the *live* view of the same
    stream answers with records no marker ever recorded. Any position or
    delivery replay took from live state then shows up as a record the live Run
    never saw, rather than as nothing -- which is the difference that matters
    for an empty marker, whose live-resolved position would otherwise hand the
    Workflow the very records it was about to receive anyway and look correct.

    The poison is served once. A live view that answered forever would fill the
    buffer and say nothing more about it than the first answer already does.

    A watcher still runs during replay -- the Run goes on living once the last
    marker is exhausted, and something has to be watching for that -- so
    ``read_after`` is answered rather than refused. Writes and the park
    handshake are recorded rather than refused for a reason worth keeping: an
    exception raised inside a watcher is caught by the manager's own retry loop
    and logged, so a refusal there would be asserted into a log line nobody
    reads.
    """

    def __init__(self, source: MemoryStreamBackend, poison: list[StreamRecord]) -> None:
        super().__init__()
        # The same storage the live Run wrote, so the recorded ranges -- and
        # only those -- still read back through `read_range`.
        self._records = source._records
        self._by_key = source._by_key
        self._poison = poison
        self._poison_served = False
        #: Every provider call replay made that was not a recorded-range read.
        self.live_calls: list[str] = []

    async def read_after(  # type: ignore[override]
        self,
        key: StreamKey,
        after: Cursor,
        *,
        max_records: int,
        block: timedelta | None = DEFAULT_WATCH_BLOCK,
    ) -> list[StreamRecord]:
        self.live_calls.append("read_after")
        if not self._poison_served:
            self._poison_served = True
            return list(self._poison[:max_records])
        if block is not None:
            # Blocked rather than returned empty immediately: a watcher whose
            # read returns instantly spins, and the busy loop would be this
            # test's own doing.
            await asyncio.sleep(block.total_seconds())
        return []

    async def append(self, key: StreamKey, record: StreamRecord) -> StreamRecord:
        self.live_calls.append("append")
        raise AssertionError("replay must never append to the stream")

    async def install_park_intent(self, key, intent) -> None:  # type: ignore[no-untyped-def]
        self.live_calls.append("install_park_intent")

    async def recheck(self, key: StreamKey, wait_id: int) -> bool:
        self.live_calls.append("recheck")
        return False


def stream_markers(events) -> list:  # type: ignore[no-untyped-def]
    return [
        e
        for e in events
        if e.HasField("marker_recorded_event_attributes")
        and e.marker_recorded_event_attributes.marker_name == EXTERNAL_STREAM_MARKER
    ]


def marker_annotation(marker_event):  # type: ignore[no-untyped-def]
    """The decoded annotation a marker carries."""
    from temporalio.bridge.proto.external_data import ExternalStreamMarkerData

    data = ExternalStreamMarkerData()
    data.ParseFromString(
        marker_event.marker_recorded_event_attributes.details["external_stream"]
        .payloads[0]
        .data
    )
    return decode_annotation(data.replay_annotation)


async def wait_for_markers(
    handle, count: int, message: str, timeout: float = 30
) -> None:  # type: ignore[no-untyped-def]
    """Polls until History holds ``count`` stream markers.

    Polled rather than slept: a marker is written when the Workflow Task ends,
    and waiting a fixed time instead would make every assertion after it a
    statement about how fast this machine is.
    """
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if (
            len(stream_markers([e async for e in handle.fetch_history_events()]))
            >= count
        ):
            return
        await asyncio.sleep(0.3)
    raise AssertionError(message)


async def wait_until(predicate, message: str, timeout: float = 15) -> None:  # type: ignore[no-untyped-def]
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.1)
    raise AssertionError(message)


async def test_an_empty_stream_parked_and_evicted_replays_from_the_recorded_cursor(
    client: Client, backend: MemoryStreamBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The empty boundary, carried across a park, an eviction, and two replays.

    Four things happen in order, and each is asserted rather than assumed --
    which is the whole difficulty of this case, because a test that quietly
    skipped the park or the eviction would still pass on a Run that simply sat
    in cache:

    1. **Subscribe to an empty stream.** Nothing is ever published before the
       eviction, so the subscription's entire first Workflow Task observes
       nothing.
    2. **Park.** Asserted in the backend, where the park handshake installs its
       intent, and not by waiting out the idle timeout and hoping: the intent's
       own cursor is the empty boundary, ``BEGINNING``.
    3. **Evict.** One cache slot, so the filler Workflow pushes the Run out; the
       manager's eviction of the Run is observed directly, and History is
       checked for the Workflow Task failure that is the only other thing that
       would rebuild the Run.
    4. **Replay.** The records are published while the Run is *gone*, so the
       cursor the resumed Run starts from cannot have come from anything still
       running on this Worker. It starts where the marker says -- ``BEGINNING``,
       the boundary the empty subscription recorded -- so it receives both, and
       a start resolved from live backend state would have started at the tail
       and received neither.

    Then the marker itself is decoded, and the finished history is replayed
    twice: once against the real backend with records appended *after* the Run
    finished, which replay must not see, and once against a backend whose only
    working operation is the recorded-range read, which nothing but the marker's
    own ranges can satisfy.
    """
    from temporalio.contrib.external_workflow_streams._manager import (
        StreamSubscriptionManager,
    )
    from temporalio.contrib.external_workflow_streams._wake import (
        UNPARKED_WAKE_GENERATION,
        WakeRequest,
        send_wake_signal,
    )

    #: Which Runs the Worker tore down. `RemoveFromCache` is the only thing that
    #: reaches this while a Worker is running, so it is the eviction itself
    #: rather than a symptom of one.
    evicted: list[str] = []
    evict_run = StreamSubscriptionManager.evict_run

    async def spy_on_eviction(self, run_id: str) -> None:  # type: ignore[no-untyped-def]
        evicted.append(run_id)
        await evict_run(self, run_id)

    monkeypatch.setattr(StreamSubscriptionManager, "evict_run", spy_on_eviction)

    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[EmptyStreamParkWorkflow, CacheFillerWorkflow],
        external_stream_backends={"tokens-memory": backend},
        # One slot, so running anything else evicts the Run under test.
        max_cached_workflows=1,
        max_concurrent_workflow_tasks=2,
    ):
        handle = await client.start_workflow(
            EmptyStreamParkWorkflow.run,
            2,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        key = await stream_key_for(client, handle, "tokens")
        run_id = key.first_execution_run_id

        # --- 1 and 2: an empty subscription, parked ------------------------
        await wait_for_markers(
            handle,
            1,
            "the Run never ended a Workflow Task, so it cannot have parked on "
            "the empty stream",
        )
        parked = await backend.parked_wait_ids(key)
        assert parked, (
            "no park intent was installed, so the Run never actually parked -- "
            "it ended its Workflow Task some other way and the eviction below "
            "would be evicting a Run in the wrong state"
        )
        intent = await backend.park_intent(key, parked[0])
        assert intent is not None
        assert intent.cursor == BEGINNING, (
            "the subscription parked at "
            f"{intent.cursor} rather than at the beginning of the stream, so "
            "the boundary it recorded is not the empty one this case is about"
        )
        assert not backend.all_records(key), (
            "the stream was not empty while the Run was subscribed to it, so "
            "this is no longer the empty-boundary case"
        )

        # --- 3: eviction ---------------------------------------------------
        await client.execute_workflow(
            CacheFillerWorkflow.run,
            id=f"filler-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        await wait_until(
            lambda: run_id in evicted,
            "the filler Workflow did not evict the Run: it is still cached, so "
            "what follows would be a live resume rather than a replay",
        )

        # --- 4: published while the Run is gone, then woken ----------------
        # Nothing on this Worker is watching for these: the Run is parked and
        # evicted, its watcher torn down with it. The wake Signal is the only
        # thing that can create a Workflow Task, and it carries no records.
        await publish(backend, key, ["alpha", "beta"])
        # The unparked generation rather than the one the intent names, which is
        # not the shape it looks like. A wake that names a park generation is
        # identified *by* that generation -- its request ID deliberately ignores
        # the sender, so every producer racing to wake one park is deduplicated
        # to a single Workflow Task. That is correct for the producer's own wake
        # and fatal for the Worker's follow-up: this Run's records land in the
        # buffer just after the replayed Workflow Task closes, the Worker is
        # answered `NoOpenWorkflowTask`, and the wake it then owes re-derives the
        # very request ID the wake above already used. The server deduplicates
        # it, no Workflow Task is created, and the Run waits forever on records
        # it is already holding. Generation 0 keeps that follow-up wake
        # distinguishable, which leaves this test about the cursor rather than
        # about the wake protocol (ADR-023).
        await send_wake_signal(
            client,
            WakeRequest(
                namespace=client.namespace,
                workflow_id=handle.id,
                first_execution_run_id=run_id,
                stream_name="tokens",
                wait_id=intent.wait_id,
                park_generation=UNPARKED_WAKE_GENERATION,
                sender_identity=f"test-{uuid.uuid4()}",
                wake_counter=1,
            ),
        )

        try:
            live = await asyncio.wait_for(handle.result(), 45)
        except asyncio.TimeoutError:
            # The same finding as the assertion below, which a Run that never
            # finishes would otherwise report as a bare timeout.
            raise AssertionError(
                "the resumed Run never received the records published while it "
                "was evicted. They were in the stream before it came back and "
                "its marker's boundary is the beginning of that stream, so a "
                "Run starting where the marker says receives both; one that "
                "resolved its position from live backend state starts at the "
                "tail and waits for records that are already behind it."
            ) from None
        history = await handle.fetch_history()

    assert live == ["alpha", "beta"], (
        "the resumed Run did not start from the boundary its empty marker "
        "recorded. Both records were published before it came back, so a Run "
        "starting at BEGINNING sees both; one that resolved its position from "
        f"live backend state starts at the tail and sees neither. Got {live}"
    )

    starts = STARTS.get(run_id, [])
    assert len(starts) >= 2, (
        "the Workflow body ran only once, so the Run was never rebuilt and "
        "nothing was replayed -- the eviction did not take effect"
    )
    assert starts[0] is False and starts[1] is True, (
        "the second execution of the Workflow body did not begin in replay, so "
        f"the Run came back some way other than by replaying its history: {starts}"
    )
    assert not [
        e for e in history.events if e.HasField("workflow_task_failed_event_attributes")
    ], (
        "a Workflow Task failed, which rebuilds the Run on its own -- the "
        "second execution above is then not evidence of the eviction"
    )

    # --- the recorded empty boundary -----------------------------------------
    markers = stream_markers(history.events)
    assert len(markers) >= 2, (
        "expected a marker for the parked Workflow Task and one for the "
        f"Workflow Task that consumed, got {len(markers)}"
    )
    empty = marker_annotation(markers[0])
    binding = empty.header.streams.get(intent.wait_id)
    assert binding is not None, (
        "the empty subscription's marker records no stream in its header, so "
        "replay of it has nothing to start from"
    )
    assert binding.stream_key == key
    assert binding.start_cursor == BEGINNING, (
        "the marker for a subscription that received nothing must still carry "
        "an explicit start cursor, or replay resolves a position from whatever "
        f"the stream holds by then; got {binding.start_cursor}"
    )
    assert not [run for segment in empty.segments for run in segment.runs], (
        "the marker written for the empty subscription records deliveries, so "
        "it is not the empty boundary"
    )
    assert empty.terminal == {intent.wait_id: BEGINNING}, (
        "the parked Workflow Task's terminal must say the subscription stopped "
        f"at the beginning of the stream; got {empty.terminal}"
    )

    # --- replay 1: the real backend, with records appended afterwards --------
    # They land after the Run finished. Replay must not see them: the marker's
    # boundary is where the subscription was, not where the stream got to.
    await publish(backend, key, ["late-one", "late-two"])
    against_live = await Replayer(
        workflows=[EmptyStreamParkWorkflow],
        external_stream_backends={"tokens-memory": backend},
    ).replay_workflow(history, raise_on_replay_failure=False)
    assert against_live.replay_failure is None, (
        f"replaying the history the Worker wrote failed: {against_live.replay_failure}"
    )

    # --- replay 2: recorded ranges and nothing else --------------------------
    # The late records are the poison: they are in the stream, they are in no
    # recorded range, and the live Run never saw one. A replay that took a
    # position or a delivery from live state hands them to Workflow code, and
    # the observation below is then not the one the live Run recorded.
    late = backend.all_records(key)[-2:]
    recorded_only = RecordedRangesOnlyBackend(backend, late)
    against_recorded = await Replayer(
        workflows=[EmptyStreamParkWorkflow],
        external_stream_backends={"tokens-memory": recorded_only},
    ).replay_workflow(history, raise_on_replay_failure=False)
    assert against_recorded.replay_failure is None, (
        "replay could not reproduce the Run from the ranges its markers name, "
        "so something in it was reading the stream live: "
        f"{against_recorded.replay_failure}"
    )
    assert [call for call in recorded_only.live_calls if call != "read_after"] == [], (
        "replay wrote to the stream or ran the park handshake, neither of which "
        f"it may do: {recorded_only.live_calls}"
    )
    expected_ranges: list[tuple[Offset, Offset]] = [
        (run.first_offset, run.last_offset)
        for marker in markers
        for segment in marker_annotation(marker).segments
        for run in segment.runs
    ]
    assert recorded_only.range_reads == expected_ranges, (
        "replay read ranges the markers do not name -- the empty marker names "
        "none at all, so anything read for it came from live stream state: "
        f"{recorded_only.range_reads} against {expected_ranges}"
    )

    runs = observations.executions(run_id)
    assert len(runs) == 3, (
        "expected the live Run and both replays to record what they observed, "
        f"got {len(runs)}"
    )
    assert runs == [live, live, live], (
        "a replay observed something other than the live Run did. Records were "
        "appended after the Run finished and both replays could reach them "
        f"through the same provider: {runs}"
    )
