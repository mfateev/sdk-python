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
from temporalio.contrib.external_workflow_streams._backend import StreamKey
from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec
from temporalio.contrib.external_workflow_streams._record import (
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
