"""P6a/P6 — producer binding, and append plus write fence."""

from __future__ import annotations

import dataclasses
import uuid

import pytest

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.contrib.external_workflow_streams._backend import (
    AppendConflictError,
    StreamKey,
)
from temporalio.contrib.external_workflow_streams._producer import (
    ChainKeyMismatchError,
    ExternalStreamProducer,
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
                await tokens.publish("from-activity")
                await tokens.finish_writing()
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
            await plain.topic("tokens", type=str).publish("from-script")

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
        await tokens.publish(value)

    records = backend.all_records(StreamKey("ns", "wf", "run-1", "tokens"))
    assert [r.sequence for r in records] == [0, 1, 2]
    assert backend.strictly_increasing([r.offset for r in records])  # type: ignore[arg-type]


async def test_a_fence_takes_its_place_in_the_sequence(
    backend: MemoryStreamBackend,
) -> None:
    tokens = _offline_producer(backend).topic("tokens", type=str)

    await tokens.publish("a")
    await tokens.finish_writing()
    await tokens.publish("b")

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
    await first.publish("a")
    await first.publish("b")

    # The Activity is retried: a fresh producer, the *same* session id, and the
    # same values published again from the top.
    retried = _offline_producer(backend, "attempt-stable").topic("tokens", type=str)
    await retried.publish("a")
    await retried.publish("b")

    assert len(backend.all_records(key)) == 2


async def test_republishing_different_content_under_one_key_is_an_error(
    backend: MemoryStreamBackend,
) -> None:
    """Silently accepting it would rewrite what a consumer may have delivered."""
    first = _offline_producer(backend, "same-session").topic("tokens", type=str)
    await first.publish("a")

    retried = _offline_producer(backend, "same-session").topic("tokens", type=str)
    with pytest.raises(AppendConflictError):
        await retried.publish("changed")


async def test_sequences_are_per_connection_not_per_topic(
    backend: MemoryStreamBackend,
) -> None:
    """Two topics' first records must not collide on ``(session, 0)``."""
    producer = _offline_producer(backend)
    tokens = producer.topic("tokens", type=str)
    events = producer.topic("tool-events", type=str)

    await tokens.publish("a")
    await events.publish("b")

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
        await tokens.publish(value)
    await tokens.finish_writing()

    key = StreamKey("ns", "wf", "run-1", "tokens")
    read_back = await backend.read_after(key, BEGINNING, max_records=100, block=None)

    import temporalio.converter
    from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec

    codec = StreamPayloadCodec(temporalio.converter.DataConverter.default, str)
    values = [await codec.decode(r.payload) for r in read_back if not r.is_control]
    assert values == ["a", "b", "c"]
