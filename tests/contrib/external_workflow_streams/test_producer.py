"""P6a/P6 — producer binding, and append plus write fence."""

from __future__ import annotations

import asyncio
import dataclasses
import uuid

import pytest

import temporalio.converter
from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.contrib.external_workflow_streams._backend import (
    AppendConflictError,
    StreamKey,
)
from temporalio.contrib.external_workflow_streams._producer import (
    AppendNotAcknowledgedError,
    ChainKeyMismatchError,
    ExternalStreamProducer,
    PrecedingWriteFailedError,
    WorkflowChainKey,
    _default_session_id,
)
from temporalio.contrib.external_workflow_streams._record import BEGINNING, RecordKind
from temporalio.testing import ActivityEnvironment
from temporalio.worker import Worker
from tests.contrib.external_workflow_streams.memory_backend import MemoryStreamBackend


@workflow.defn
class ChainWorkflow:
    """Runs long enough for a producer to describe it."""

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: False)


@pytest.fixture
def backend() -> MemoryStreamBackend:
    return MemoryStreamBackend()


# --- the chain key ----------------------------------------------------------


@pytest.mark.parametrize(
    "missing", ["namespace", "workflow_id", "first_execution_run_id"]
)
def test_every_part_of_the_chain_key_is_required(missing: str) -> None:
    parts = {
        "namespace": "ns",
        "workflow_id": "wf",
        "first_execution_run_id": "run-1",
    }
    parts[missing] = ""

    with pytest.raises(ValueError, match=missing):
        WorkflowChainKey(**parts)


def test_the_stream_name_appears_only_in_topic() -> None:
    """No two arguments can disagree about the stream name.

    ``connect()`` takes the chain key and ``topic(name)`` completes it, so
    there is exactly one place the name is written.
    """
    key = WorkflowChainKey("ns", "wf", "run-1")

    assert key.stream_key("tokens") == StreamKey("ns", "wf", "run-1", "tokens")
    assert key.stream_key("tokens") != key.stream_key("tool-events")
    assert not hasattr(key, "stream_name")


# --- session IDs ------------------------------------------------------------


def test_a_plain_process_must_supply_a_session_id() -> None:
    """A random default would look correct until the first retry.

    Idempotency is on ``(session_id, sequence)``, so a fresh session per attempt
    means every record appended twice -- and the failure shows up as duplicate
    delivery to the Workflow, a long way from here.
    """
    with pytest.raises(ValueError, match="no default outside an Activity"):
        _default_session_id()


async def test_an_activity_derives_a_session_id_stable_across_attempts() -> None:
    env = ActivityEnvironment()
    base = dataclasses.replace(
        ActivityEnvironment.default_info(),
        activity_id="publish-tokens",
        workflow_run_id="run-abc",
    )

    seen = []
    for attempt in (1, 2, 3):
        env.info = dataclasses.replace(base, attempt=attempt)

        @activity.defn
        async def capture() -> str:
            return _default_session_id()

        seen.append(await env.run(capture))

    assert len(set(seen)) == 1, (
        f"the session id must not vary by attempt, got {seen}; a retried attempt "
        "must reuse its predecessor's idempotency keys"
    )
    assert "run-abc" in seen[0] and "publish-tokens" in seen[0]


# --- binding and verification -----------------------------------------------


async def test_a_wrong_first_execution_run_id_fails_loudly(
    client: Client, backend: MemoryStreamBackend
) -> None:
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(client, task_queue=task_queue, workflows=[ChainWorkflow]):
        handle = await client.start_workflow(
            ChainWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        try:
            with pytest.raises(ChainKeyMismatchError, match="first execution Run ID"):
                await ExternalStreamProducer.connect(
                    backend=backend,
                    workflow=WorkflowChainKey(
                        client.namespace, handle.id, "definitely-not-the-run-id"
                    ),
                    client=client,
                )
        finally:
            await handle.terminate()


async def test_a_wrong_namespace_fails_before_any_server_call(
    client: Client, backend: MemoryStreamBackend
) -> None:
    with pytest.raises(ChainKeyMismatchError, match="namespace"):
        await ExternalStreamProducer.connect(
            backend=backend,
            workflow=WorkflowChainKey("some-other-namespace", "wf", "run-1"),
            client=client,
            session_id="s",
        )


async def test_an_activity_and_a_plain_process_publish_under_one_verified_key(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """Two very different producers, one stream, one verified chain key.

    The Activity gets its session id for free from its own identity; the plain
    process supplies one. Neither can derive the chain key -- ``activity.Info``
    has no first execution Run ID -- so both are handed it by the Workflow.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(client, task_queue=task_queue, workflows=[ChainWorkflow]):
        handle = await client.start_workflow(
            ChainWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        try:
            description = await handle.describe()
            chain = WorkflowChainKey(
                client.namespace,
                handle.id,
                description.raw_description.workflow_execution_info.first_run_id,
            )

            @activity.defn
            async def publish_from_activity() -> str:
                producer = await ExternalStreamProducer.connect(
                    backend=backend, workflow=chain, client=client
                )
                tokens = producer.topic("tokens", type=str)
                await tokens.publish("from-activity", wake=False)
                await tokens.finish_writing(wake=False)
                return producer.session_id

            env = ActivityEnvironment()
            env.info = dataclasses.replace(
                ActivityEnvironment.default_info(),
                activity_id="publish-tokens",
                workflow_run_id=description.run_id,
            )
            activity_session = await env.run(publish_from_activity)
            assert activity_session.startswith("activity:"), (
                "an Activity must derive its session id from its own identity"
            )

            # And a plain, non-Temporal-hosted process against the same key.
            plain = await ExternalStreamProducer.connect(
                backend=backend,
                workflow=chain,
                client=client,
                session_id="standalone-script",
            )
            await plain.topic("tokens", type=str).publish("from-script", wake=False)

            records = backend.all_records(chain.stream_key("tokens"))
            assert [r.kind for r in records] == [
                RecordKind.DATA,
                RecordKind.WRITE_FENCE,
                RecordKind.DATA,
            ]
            assert {r.producer_session_id for r in records} == {
                activity_session,
                "standalone-script",
            }
        finally:
            await handle.terminate()


# --- append and the fence, without a server ---------------------------------


def _offline_producer(backend: MemoryStreamBackend, session: str = "s"):  # type: ignore[no-untyped-def]
    """A producer built directly, skipping the verification a bind performs.

    Verification is P6a's and is covered above against a real server; these
    cases are about what append and the fence do once a producer is bound.
    """
    import temporalio.converter

    return ExternalStreamProducer(
        backend=backend,
        workflow=WorkflowChainKey("ns", "wf", "run-1"),
        data_converter=temporalio.converter.DataConverter.default,
        session_id=session,
    )


async def test_records_are_appended_in_order(backend: MemoryStreamBackend) -> None:
    tokens = _offline_producer(backend).topic("tokens", type=str)

    for value in ["a", "b", "c"]:
        await tokens.publish(value, wake=False)

    records = backend.all_records(StreamKey("ns", "wf", "run-1", "tokens"))
    assert [r.sequence for r in records] == [0, 1, 2]
    assert backend.strictly_increasing([r.offset for r in records])  # type: ignore[arg-type]


async def test_a_fence_takes_its_place_in_the_sequence(
    backend: MemoryStreamBackend,
) -> None:
    tokens = _offline_producer(backend).topic("tokens", type=str)

    await tokens.publish("a", wake=False)
    await tokens.finish_writing(wake=False)
    await tokens.publish("b", wake=False)

    records = backend.all_records(StreamKey("ns", "wf", "run-1", "tokens"))
    assert [r.kind for r in records] == [
        RecordKind.DATA,
        RecordKind.WRITE_FENCE,
        RecordKind.DATA,
    ]


async def test_a_retried_attempt_appends_no_duplicate(
    backend: MemoryStreamBackend,
) -> None:
    """The whole reason the session id must be stable across attempts."""
    key = StreamKey("ns", "wf", "run-1", "tokens")

    first = _offline_producer(backend, "attempt-stable").topic("tokens", type=str)
    await first.publish("a", wake=False)
    await first.publish("b", wake=False)

    # The Activity is retried: a fresh producer, the *same* session id, and the
    # same values published again from the top.
    retried = _offline_producer(backend, "attempt-stable").topic("tokens", type=str)
    await retried.publish("a", wake=False)
    await retried.publish("b", wake=False)

    assert len(backend.all_records(key)) == 2


class GatedPayloadCodec(temporalio.converter.PayloadCodec):
    """A codec whose ``encode`` completes only when the test releases that value.

    Stands in for what a real one does: an external payload store or a KMS round
    trip, which is arbitrary I/O and completes in whatever order the service
    answers. That order is not stable across Activity attempts, which is the whole
    point -- so the test drives it explicitly instead of racing it.
    """

    def __init__(self) -> None:
        self.arrived: dict[str, asyncio.Event] = {}
        self.gates: dict[str, asyncio.Event] = {}

    def _for(self, marker: str) -> tuple[asyncio.Event, asyncio.Event]:
        self.arrived.setdefault(marker, asyncio.Event())
        self.gates.setdefault(marker, asyncio.Event())
        return self.arrived[marker], self.gates[marker]

    async def wait_inside(self, marker: str) -> None:
        """Blocks until this value's encode has started."""
        arrived, _ = self._for(marker)
        await asyncio.wait_for(arrived.wait(), 2)

    def release(self, marker: str) -> None:
        _, gate = self._for(marker)
        gate.set()

    async def encode(self, payloads):  # type: ignore[no-untyped-def]
        # The default converter renders a `str` as JSON, so the value is legible
        # in the payload's bytes and needs no side channel to identify it.
        marker = payloads[0].data.decode()
        arrived, gate = self._for(marker)
        arrived.set()
        await gate.wait()
        return list(payloads)

    async def decode(self, payloads):  # type: ignore[no-untyped-def]
        return list(payloads)


def _gated_producer(
    backend: MemoryStreamBackend, codec: GatedPayloadCodec, session: str
):  # type: ignore[no-untyped-def]
    import temporalio.converter

    return ExternalStreamProducer(
        backend=backend,
        workflow=WorkflowChainKey("ns", "wf", "run-1"),
        data_converter=dataclasses.replace(
            temporalio.converter.DataConverter.default, payload_codec=codec
        ),
        session_id=session,
    )


async def test_concurrent_publishes_take_their_sequence_in_invocation_order(
    backend: MemoryStreamBackend,
) -> None:
    """Idempotency may not depend on the order a payload codec answers in.

    ``(session_id, sequence)`` is the idempotency key and a retried Activity
    reuses the session id on purpose, so the sequence a call draws has to be a
    property of *the call*. Drawing it after awaiting the encode makes it a
    property of the encode's completion order instead -- and a codec is allowed to
    do real I/O, so that order is not stable across attempts. Two concurrent
    publishes then exchange sequence numbers whenever the store answers the other
    way round, the backend sees each stable key reused with different bytes, and
    the retry raises ``AppendConflictError`` on both calls: a valid concurrent
    Activity made permanently non-retryable by timing alone.

    The two attempts here are byte-identical in what they do and differ only in
    which encode finishes first.
    """
    key = StreamKey("ns", "wf", "run-1", "tokens")

    codec = GatedPayloadCodec()
    topic = _gated_producer(backend, codec, "attempt-stable").topic("tokens", type=str)
    first = asyncio.ensure_future(topic.publish("a", wake=False))
    second = asyncio.ensure_future(topic.publish("b", wake=False))
    await codec.wait_inside('"a"')
    await codec.wait_inside('"b"')
    codec.release('"a"')
    codec.release('"b"')
    offsets = [await first, await second]

    # The Activity is retried: a fresh producer, the *same* session id, the same
    # calls in the same order -- and the store answers in the other order.
    retry_codec = GatedPayloadCodec()
    retried = _gated_producer(backend, retry_codec, "attempt-stable").topic(
        "tokens", type=str
    )
    first_again = asyncio.ensure_future(retried.publish("a", wake=False))
    second_again = asyncio.ensure_future(retried.publish("b", wake=False))
    await retry_codec.wait_inside('"a"')
    await retry_codec.wait_inside('"b"')
    retry_codec.release('"b"')
    retry_codec.release('"a"')

    assert [await first_again, await second_again] == offsets, (
        "the retry appended the same values at different offsets, so the two "
        "attempts disagree about what landed where"
    )
    records = backend.all_records(key)
    assert [r.sequence for r in records] == [0, 1]
    assert len(records) == 2, "the retry was not idempotent; it appended duplicates"


async def test_reordered_encodes_cannot_duplicate_across_two_topics(
    backend: MemoryStreamBackend,
) -> None:
    """The same defect on the other side of the deduplication boundary.

    Deduplication is scoped by stream key, so two publishes to *different* topics
    that exchange sequence numbers do not collide -- they each land under a key
    the other stream has never seen, and the retry appends a second record
    instead of raising. The conflict is the loud failure mode; this one is silent.
    """
    tokens_key = StreamKey("ns", "wf", "run-1", "tokens")
    events_key = StreamKey("ns", "wf", "run-1", "tool-events")

    codec = GatedPayloadCodec()
    producer = _gated_producer(backend, codec, "attempt-stable")
    tokens = producer.topic("tokens", type=str)
    events = producer.topic("tool-events", type=str)
    first = asyncio.ensure_future(tokens.publish("a", wake=False))
    second = asyncio.ensure_future(events.publish("b", wake=False))
    await codec.wait_inside('"a"')
    await codec.wait_inside('"b"')
    codec.release('"a"')
    codec.release('"b"')
    await first
    await second

    retry_codec = GatedPayloadCodec()
    retried = _gated_producer(backend, retry_codec, "attempt-stable")
    retried_tokens = retried.topic("tokens", type=str)
    retried_events = retried.topic("tool-events", type=str)
    first_again = asyncio.ensure_future(retried_tokens.publish("a", wake=False))
    second_again = asyncio.ensure_future(retried_events.publish("b", wake=False))
    await retry_codec.wait_inside('"a"')
    await retry_codec.wait_inside('"b"')
    retry_codec.release('"b"')
    retry_codec.release('"a"')
    await first_again
    await second_again

    assert len(backend.all_records(tokens_key)) == 1
    assert len(backend.all_records(events_key)) == 1, (
        "reversing the encode order appended a duplicate on a topic where the "
        "swapped key could not collide, so nothing raised"
    )


# --- the fence's ordering claim ---------------------------------------------


TOKENS = StreamKey("ns", "wf", "run-1", "tokens")
EVENTS = StreamKey("ns", "wf", "run-1", "tool-events")


async def _let_every_ready_task_run() -> None:
    """Runs every task that can run without waiting for anything.

    What makes "the fence has not appended" an assertion rather than a race.
    Nothing here sleeps in wall-clock terms: an unordered ``finish_writing()``
    reaches the backend without awaiting anything that yields, so one turn of
    the loop is enough for it to have finished, and the extra turns only make
    that margin obvious.
    """
    for _ in range(10):
        await asyncio.sleep(0)


class GatedFailingCodec(GatedPayloadCodec):
    """A gated codec whose release makes the encode *fail* rather than finish.

    The payload store rejecting a value, at the point where a concurrent fence
    is already waiting behind the publish.
    """

    async def encode(self, payloads):  # type: ignore[no-untyped-def]
        await super().encode(payloads)
        raise RuntimeError("the payload store rejected the value")


class CommittingThenFailingBackend(MemoryStreamBackend):
    """Commits an append and then loses the answer, on cue.

    The unknown outcome the producer cannot see through: the record is durable
    and its offset reached nobody, so it may not be treated as absent -- and a
    fence appended in front of it would claim durability for a write whose place
    in the stream is not yet fixed (ADR-038).
    """

    def __init__(self) -> None:
        super().__init__()
        self.committed = asyncio.Event()
        self.lose_the_answer = asyncio.Event()
        self.losing = True

    async def append(self, key, record):  # type: ignore[no-untyped-def]
        placed = await super().append(key, record)
        if self.losing:
            self.losing = False
            self.committed.set()
            await self.lose_the_answer.wait()
            raise ConnectionError("connection reset")
        return placed


async def test_a_fence_waits_for_a_publish_still_inside_its_codec(
    backend: MemoryStreamBackend,
) -> None:
    """The fence's claim is about invocation order, so it must hold under one.

    ``publish()`` draws its sequence before awaiting the codec -- which is what
    keeps idempotency keys stable across attempts -- so an earlier publish can
    still be encoding when ``finish_writing()`` is called. A fence appended there
    says every preceding write in this session is in the stream while the first
    one has not been sent yet, and a consumer that drains through it may park on
    exactly that. The unsafe shape is this test's: a ``wake=False`` publish
    batched behind a fence that carries the batch's only wake, where the wake is
    spent before the data exists.
    """
    codec = GatedPayloadCodec()
    tokens = _gated_producer(backend, codec, "batch").topic("tokens", type=str)

    publishing = asyncio.ensure_future(tokens.publish("a", wake=False))
    await codec.wait_inside('"a"')
    fencing = asyncio.ensure_future(tokens.finish_writing(wake=False))
    await _let_every_ready_task_run()

    assert not fencing.done(), (
        "the fence completed while the publish was still encoding"
    )
    assert backend.all_records(TOKENS) == [], (
        "the fence reached the backend ahead of a publish invoked before it, so "
        "its durability claim was false when a consumer could act on it"
    )

    codec.release('"a"')
    await publishing
    await fencing

    records = backend.all_records(TOKENS)
    assert [(r.sequence, r.kind) for r in records] == [
        (0, RecordKind.DATA),
        (1, RecordKind.WRITE_FENCE),
    ], "backend order and invocation order disagree"
    assert backend.strictly_increasing([r.offset for r in records])  # type: ignore[arg-type]


async def test_two_handles_for_one_topic_share_the_fence_order(
    backend: MemoryStreamBackend,
) -> None:
    """``topic()`` returns a fresh handle per call; the stream is still one.

    So the order cannot live on the handle. A producer that publishes through
    one handle and fences through another -- the shape a helper that takes a
    topic name rather than a handle produces -- is the same stream and the same
    claim.
    """
    codec = GatedPayloadCodec()
    producer = _gated_producer(backend, codec, "batch")
    writing = producer.topic("tokens", type=str)
    finishing = producer.topic("tokens", type=str)
    assert writing is not finishing

    publishing = asyncio.ensure_future(writing.publish("a", wake=False))
    await codec.wait_inside('"a"')
    fencing = asyncio.ensure_future(finishing.finish_writing(wake=False))
    await _let_every_ready_task_run()

    assert not fencing.done() and backend.all_records(TOKENS) == [], (
        "the second handle fenced without waiting for the first handle's publish"
    )

    codec.release('"a"')
    await publishing
    await fencing

    assert [r.kind for r in backend.all_records(TOKENS)] == [
        RecordKind.DATA,
        RecordKind.WRITE_FENCE,
    ]


async def test_a_fence_does_not_wait_for_another_stream(
    backend: MemoryStreamBackend,
) -> None:
    """The claim is per stream, so the ordering must be too.

    One connection serves several topics, and a publish blocked in a codec on
    one of them says nothing about a fence on another -- stalling it there would
    make an unrelated slow payload store hold up a finished topic.
    """
    codec = GatedPayloadCodec()
    producer = _gated_producer(backend, codec, "batch")
    tokens = producer.topic("tokens", type=str)
    events = producer.topic("tool-events", type=str)

    publishing = asyncio.ensure_future(tokens.publish("a", wake=False))
    await codec.wait_inside('"a"')

    await asyncio.wait_for(events.finish_writing(wake=False), 2)

    assert [r.kind for r in backend.all_records(EVENTS)] == [RecordKind.WRITE_FENCE]
    assert backend.all_records(TOKENS) == []

    codec.release('"a"')
    await publishing
    assert [r.kind for r in backend.all_records(TOKENS)] == [RecordKind.DATA]


async def test_a_fence_will_not_overtake_an_append_of_unknown_outcome() -> None:
    """An append with no answer may still be in front of the fence.

    The record is durable and its offset reached nobody, so the one thing that
    cannot be assumed is that it is absent. The fence is refused with *that*
    operation's error rather than its own, which is the refusal that already
    governs the stream while an append is unsettled: settle it with
    ``resolve_append()``, then fence.
    """
    backend = CommittingThenFailingBackend()
    tokens = _offline_producer(backend, "batch").topic("tokens", type=str)

    publishing = asyncio.ensure_future(tokens.publish("a", wake=False))
    await asyncio.wait_for(backend.committed.wait(), 2)
    fencing = asyncio.ensure_future(tokens.finish_writing(wake=False))
    await _let_every_ready_task_run()

    assert not fencing.done()
    assert [r.sequence for r in backend.all_records(TOKENS)] == [0], (
        "the fixture must actually have committed, or this asserts nothing"
    )

    backend.lose_the_answer.set()
    with pytest.raises(AppendNotAcknowledgedError) as lost:
        await publishing
    with pytest.raises(AppendNotAcknowledgedError) as refused:
        await fencing

    assert refused.value.record.idempotency_key == lost.value.record.idempotency_key, (
        "the fence must report the unsettled operation's recovery, not invent one "
        "for a fence that was never appended"
    )
    assert [r.kind for r in backend.all_records(TOKENS)] == [RecordKind.DATA], (
        "a fence was appended in front of a record whose position was not settled"
    )

    await tokens.resolve_append(lost.value.record, wake=False)
    await tokens.finish_writing(wake=False)

    records = backend.all_records(TOKENS)
    assert [(r.sequence, r.kind) for r in records] == [
        (0, RecordKind.DATA),
        (2, RecordKind.WRITE_FENCE),
    ], "the settled record must be in the stream once, ahead of the fence"


async def test_a_fence_refuses_to_stand_in_for_a_failed_publish(
    backend: MemoryStreamBackend,
) -> None:
    """A failed earlier write is propagated, not passed over.

    The caller asked for that write before it asked for the fence, and a fence
    appended over the gap tells a consumer the batch is complete when it is
    short a record. Nothing is appended for the refused fence, so once the
    failure has been dealt with -- by republishing the value or by accepting the
    batch without it -- ``finish_writing()`` again appends one.
    """
    codec = GatedFailingCodec()
    tokens = _gated_producer(backend, codec, "batch").topic("tokens", type=str)

    publishing = asyncio.ensure_future(tokens.publish("a", wake=False))
    await codec.wait_inside('"a"')
    fencing = asyncio.ensure_future(tokens.finish_writing(wake=False))
    await _let_every_ready_task_run()
    codec.release('"a"')

    with pytest.raises(RuntimeError):
        await publishing
    with pytest.raises(PrecedingWriteFailedError) as caught:
        await fencing

    assert caught.value.sequence == 0 and caught.value.stream_key == TOKENS
    assert isinstance(caught.value.__cause__, RuntimeError), (
        "the fence must carry what actually failed, not describe it"
    )
    assert backend.all_records(TOKENS) == []

    await tokens.finish_writing(wake=False)
    assert [(r.sequence, r.kind) for r in backend.all_records(TOKENS)] == [
        (2, RecordKind.WRITE_FENCE)
    ], "the failed write is no longer outstanding, so a fence must go in now"


async def test_a_cancelled_fence_does_not_refuse_a_later_fence(
    backend: MemoryStreamBackend,
) -> None:
    """A failed *fence* is not a failed write, so it is not propagated as one.

    Two concurrent fences make independent claims about the publishes each was
    invoked after, and neither is inside the other's claim. Holding them in one
    append order made a fence that never reached the backend -- cancelled here,
    but a refusal does the same -- look to a later fence exactly like a publish
    whose record went missing, so a fence with every preceding write durable
    behind it was refused with ``PrecedingWriteFailedError``. That error is
    documented as reporting a failed ``publish()``, and nothing else.
    """
    codec = GatedPayloadCodec()
    tokens = _gated_producer(backend, codec, "batch").topic("tokens", type=str)

    publishing = asyncio.ensure_future(tokens.publish("a", wake=False))
    await codec.wait_inside('"a"')
    abandoned = asyncio.ensure_future(tokens.finish_writing(wake=False))
    await _let_every_ready_task_run()
    fencing = asyncio.ensure_future(tokens.finish_writing(wake=False))
    await _let_every_ready_task_run()

    abandoned.cancel()
    with pytest.raises(asyncio.CancelledError):
        await abandoned
    assert not fencing.done(), (
        "the second fence appended while the publish it covers was still encoding"
    )

    codec.release('"a"')
    await publishing
    await fencing

    assert [(r.sequence, r.kind) for r in backend.all_records(TOKENS)] == [
        (0, RecordKind.DATA),
        (2, RecordKind.WRITE_FENCE),
    ], (
        "the surviving fence must append behind the data, and the cancelled one "
        "must leave nothing in the stream"
    )


class RefusingRecoveryBackend(MemoryStreamBackend):
    """Loses the answer to a data append, then refuses its recovery.

    Both halves are contract behaviour rather than a broken store: a backend
    commits before it answers, so the answer can be lost, and
    ``AppendConflictError`` is the one definite refusal the contract defines --
    the key it names holds different bytes, so the record being recovered did
    not land and never will. Together they are the case where "no longer
    unresolved" and "durable" are not the same thing, which is reachable from a
    session-id collision or a retry that reused the sequence for other bytes.
    """

    def __init__(self) -> None:
        super().__init__()
        self.reached = asyncio.Event()
        self.answer = asyncio.Event()
        self.data_appends = 0

    async def append(self, key, record):  # type: ignore[no-untyped-def]
        if record.kind is not RecordKind.DATA:
            return await super().append(key, record)
        self.data_appends += 1
        if self.data_appends == 1:
            self.reached.set()
            await self.answer.wait()
            raise ConnectionError("connection reset")
        # Refused without suspending, so nothing runs between the unresolved
        # entry disappearing and the waiting fence reading the outcome. That is
        # the window in which a stale "unknown" looked like durability.
        raise AppendConflictError(record.idempotency_key)


async def test_a_fence_is_refused_when_recovery_proves_the_write_absent() -> None:
    """A resolved append is not a durable one, so the fence may not infer it.

    An append whose answer was lost is unknown, not failed -- and the fence
    waits rather than refusing outright. What ends the wait is
    ``resolve_append()``, and it ends it *two* ways: the record is durable, or
    the backend refuses the key and the record demonstrably never landed.
    Deciding between them by whether the record is still in the producer's
    unresolved set cannot work, because both outcomes remove it, and the fence
    then appended over a hole while claiming the batch complete.
    """
    backend = RefusingRecoveryBackend()
    tokens = _offline_producer(backend, "batch").topic("tokens", type=str)

    async def publish_then_recover() -> None:
        with pytest.raises(AppendNotAcknowledgedError) as lost:
            await tokens.publish("a", wake=False)
        with pytest.raises(AppendConflictError):
            await tokens.resolve_append(lost.value.record, wake=False)

    publishing = asyncio.ensure_future(publish_then_recover())
    await asyncio.wait_for(backend.reached.wait(), 2)
    fencing = asyncio.ensure_future(tokens.finish_writing(wake=False))
    await _let_every_ready_task_run()

    assert not fencing.done(), "the fence overtook an append of unknown outcome"

    backend.answer.set()
    await publishing
    with pytest.raises(PrecedingWriteFailedError) as refused:
        await fencing

    assert refused.value.sequence == 0 and refused.value.stream_key == TOKENS
    assert isinstance(refused.value.__cause__, AppendConflictError), (
        "the fence must carry the refusal recovery learned, not the unknown "
        "outcome the interrupted call reported"
    )
    assert backend.all_records(TOKENS) == [], (
        "a fence was appended claiming a write that recovery proved absent"
    )


async def test_a_fence_goes_in_once_recovery_makes_the_write_durable() -> None:
    """The other half of the same decision, and the one that must not refuse.

    A recovery that finds the record already there resolves the unknown outcome
    to *durable*, and the write the fence was waiting for is now in the stream
    ahead of it. The fence has to be released, not refused: reporting
    ``PrecedingWriteFailedError`` for a record that landed sends the caller to
    republish a value the stream already holds.
    """
    backend = CommittingThenFailingBackend()
    tokens = _offline_producer(backend, "batch").topic("tokens", type=str)

    async def publish_then_recover() -> None:
        with pytest.raises(AppendNotAcknowledgedError) as lost:
            await tokens.publish("a", wake=False)
        # The record did commit, so re-appending byte-identical content is the
        # no-op the recovery depends on -- and it returns without suspending, so
        # the fence's next turn is the first one after the resolution.
        await tokens.resolve_append(lost.value.record, wake=False)

    publishing = asyncio.ensure_future(publish_then_recover())
    await asyncio.wait_for(backend.committed.wait(), 2)
    fencing = asyncio.ensure_future(tokens.finish_writing(wake=False))
    await _let_every_ready_task_run()

    assert not fencing.done()

    backend.lose_the_answer.set()
    await publishing
    await fencing

    records = backend.all_records(TOKENS)
    assert [(r.sequence, r.kind) for r in records] == [
        (0, RecordKind.DATA),
        (1, RecordKind.WRITE_FENCE),
    ], "the recovered record must be in the stream once, ahead of the fence"


class LosesOneAnswerThenRefusesIt(MemoryStreamBackend):
    """Loses sequence 0's answer, refuses its recovery, and holds sequence 1.

    Two data writes precede one fence, and only the *second* is still in flight
    when the first is recovered -- which is the schedule that decides whether the
    fence reads an outcome or a memory of one.
    """

    def __init__(self) -> None:
        super().__init__()
        self.reached = asyncio.Event()
        self.answer = asyncio.Event()
        self.release_second = asyncio.Event()

    async def append(self, key, record):  # type: ignore[no-untyped-def]
        if record.kind is RecordKind.DATA and record.sequence == 0:
            if not self.reached.is_set():
                self.reached.set()
                await self.answer.wait()
                raise ConnectionError("connection reset")
            raise AppendConflictError(record.idempotency_key)
        if record.kind is RecordKind.DATA and record.sequence == 1:
            await self.release_second.wait()
        return await super().append(key, record)


async def test_a_resolution_reached_while_the_fence_waits_on_a_later_write() -> None:
    """An outcome read at wait time can still change; one read after cannot.

    The fence waits on each earlier publish in turn, so it can pass the first
    one -- reading "unknown", which is not yet a failure -- and then sit on the
    second while ``resolve_append()`` proves the first absent. An outcome
    captured on the way past is the stale one, and stale here means permissive:
    the fence skips a write recovery has refused and appends over the hole.
    """
    backend = LosesOneAnswerThenRefusesIt()
    tokens = _offline_producer(backend, "batch").topic("tokens", type=str)

    first = asyncio.ensure_future(tokens.publish("a", wake=False))
    await asyncio.wait_for(backend.reached.wait(), 2)
    second = asyncio.ensure_future(tokens.publish("b", wake=False))
    await _let_every_ready_task_run()
    fencing = asyncio.ensure_future(tokens.finish_writing(wake=False))
    await _let_every_ready_task_run()

    assert not fencing.done()

    backend.answer.set()
    with pytest.raises(AppendNotAcknowledgedError) as lost:
        await first
    # Lets the fence take its turn on the *first* write and park on the second,
    # which is the state this case is about.
    await _let_every_ready_task_run()
    assert not fencing.done(), "the fence must still be held by the second write"

    with pytest.raises(AppendConflictError):
        await tokens.resolve_append(lost.value.record, wake=False)

    backend.release_second.set()
    await second

    with pytest.raises(PrecedingWriteFailedError) as refused:
        await fencing

    assert refused.value.sequence == 0, (
        "the fence must report the write recovery refused, not the one that landed"
    )
    assert isinstance(refused.value.__cause__, AppendConflictError)
    assert [(r.sequence, r.kind) for r in backend.all_records(TOKENS)] == [
        (1, RecordKind.DATA)
    ], "only the second write may be in the stream, and no fence behind it"


async def test_republishing_different_content_under_one_key_is_an_error(
    backend: MemoryStreamBackend,
) -> None:
    """Silently accepting it would rewrite what a consumer may have delivered."""
    first = _offline_producer(backend, "same-session").topic("tokens", type=str)
    await first.publish("a", wake=False)

    retried = _offline_producer(backend, "same-session").topic("tokens", type=str)
    with pytest.raises(AppendConflictError):
        await retried.publish("changed", wake=False)


async def test_sequences_are_per_connection_not_per_topic(
    backend: MemoryStreamBackend,
) -> None:
    """Two topics' first records must not collide on ``(session, 0)``."""
    producer = _offline_producer(backend)
    tokens = producer.topic("tokens", type=str)
    events = producer.topic("tool-events", type=str)

    await tokens.publish("a", wake=False)
    await events.publish("b", wake=False)

    (token_record,) = backend.all_records(StreamKey("ns", "wf", "run-1", "tokens"))
    (event_record,) = backend.all_records(StreamKey("ns", "wf", "run-1", "tool-events"))
    assert token_record.sequence != event_record.sequence


async def test_a_topic_needs_a_name(backend: MemoryStreamBackend) -> None:
    with pytest.raises(ValueError, match="non-empty name"):
        _offline_producer(backend).topic("")


async def test_the_producer_handle_has_no_subscribe(
    backend: MemoryStreamBackend,
) -> None:
    """The two sides are mirror images, not one type used twice."""
    tokens = _offline_producer(backend).topic("tokens", type=str)

    assert not hasattr(tokens, "subscribe")
    assert hasattr(tokens, "publish")
    assert hasattr(tokens, "finish_writing")


async def test_a_standalone_read_back_sees_what_was_published(
    backend: MemoryStreamBackend,
) -> None:
    """The P6 criterion, without a Temporal server in the picture."""
    tokens = _offline_producer(backend).topic("tokens", type=str)
    for value in ["a", "b", "c"]:
        await tokens.publish(value, wake=False)
    await tokens.finish_writing(wake=False)

    key = StreamKey("ns", "wf", "run-1", "tokens")
    read_back = await backend.read_after(key, BEGINNING, max_records=100, block=None)

    import temporalio.converter
    from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec

    codec = StreamPayloadCodec(temporalio.converter.DataConverter.default, str)
    values = [await codec.decode(r.payload) for r in read_back if not r.is_control]
    assert values == ["a", "b", "c"]
