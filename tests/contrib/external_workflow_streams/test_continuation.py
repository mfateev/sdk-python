"""P15 — the Continue-As-New cursor.

A stream spans a whole chain, so a new Run must resume where its predecessor
stopped. The position travels in a reserved internal header and is restored from
History, because a cursor derived from mutable backend state would give replay
whatever the stream holds *now* rather than what the Run started from -- and two
replays of one history could then diverge (ADR-022).
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import pytest

import temporalio.converter
from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.external_workflow_streams._annotation import (
    AnnotationDecodeError,
)
from temporalio.contrib.external_workflow_streams._backend import StreamKey
from temporalio.contrib.external_workflow_streams._continuation import (
    CONTINUATION_HEADER,
    Continuation,
    decode_continuation,
    encode_continuation,
    read_continuation_header,
    write_continuation_header,
)
from temporalio.contrib.external_workflow_streams._manager import (
    ReadinessResult,
)
from temporalio.contrib.external_workflow_streams._record import (
    AFTER,
    BEGINNING,
    Offset,
    RecordKind,
    StreamRecord,
)
from temporalio.contrib.external_workflow_streams._runtime import WorkflowStreamRuntime
from temporalio.worker import Worker
from tests.contrib.external_workflow_streams.memory_backend import MemoryStreamBackend

with workflow.unsafe.imports_passed_through():
    from temporalio.contrib.external_workflow_streams._api import external_stream


async def _notify(run_id: str, wait_id: int, generation: int) -> str:
    return ReadinessResult.ACCEPTED


def make_runtime(manager, backend, continuation=None):  # type: ignore[no-untyped-def]
    return WorkflowStreamRuntime(
        manager=manager,
        backends={"tokens": backend},
        run_id="run-2",
        namespace="ns",
        workflow_id="wf",
        first_execution_run_id="first-run",
        data_converter=temporalio.converter.DataConverter.default,
        default_idle_timeout=timedelta(seconds=1),
        continuation=continuation,
    )


@pytest.fixture
def backend() -> MemoryStreamBackend:
    return MemoryStreamBackend()


class StubManager:
    def cancel_from_workflow_thread(self, run_id, wait_id):  # type: ignore[no-untyped-def]
        pass

    """Records registrations and starts no watcher.

    What these tests are about is the cursor a subscription is registered *with*,
    which is decided before any watching happens. A real manager here would spawn
    prefetch loops against a backend nothing is writing to, and its failures
    would be noise rather than signal.
    """

    def __init__(self) -> None:
        self.registered: list[tuple[int, object]] = []

    def register(self, *, run_id, wait_id, stream_key, backend_name, start_cursor):  # type: ignore[no-untyped-def]
        self.registered.append((wait_id, start_cursor))

    def note_wait_generation(self, run_id, wait_id, generation) -> None:  # type: ignore[no-untyped-def]
        pass


@pytest.fixture
def manager() -> StubManager:
    return StubManager()


# --- the header's encoding ----------------------------------------------------


def test_a_continuation_round_trips() -> None:
    original = Continuation(
        cursors={1: AFTER(Offset("100-3")), 2: BEGINNING},
        stream_names={1: "tokens", 2: "tool-events"},
    )

    assert decode_continuation(encode_continuation(original)) == original


def test_the_encoding_is_stable_for_one_state() -> None:
    """It rides on a command whose payload replay compares against History.

    Encoding the same state two different ways -- by iterating a dict in
    insertion order, say -- would make an otherwise identical Continue-As-New
    command mismatch on replay.
    """
    forwards = Continuation({1: BEGINNING, 2: BEGINNING}, {1: "a", 2: "b"})
    backwards = Continuation({2: BEGINNING, 1: BEGINNING}, {2: "b", 1: "a"})

    assert encode_continuation(forwards) == encode_continuation(backwards)


def test_a_newer_schema_version_is_reported_not_guessed_at() -> None:
    """Silently starting at BEGINNING would redeliver the whole stream."""
    future = bytearray(encode_continuation(Continuation({1: BEGINNING}, {1: "a"})))
    future[0] = 99

    with pytest.raises(AnnotationDecodeError, match="schema version 99"):
        decode_continuation(bytes(future))


def test_the_header_bypasses_the_user_data_converter() -> None:
    """One Run writes it and the next reads it, possibly on different config.

    A chain whose Runs were deployed with different converter configuration
    would otherwise restart at an unreadable cursor -- silently, since an
    unreadable cursor looks exactly like no cursor at all.
    """
    payload = write_continuation_header(Continuation({1: BEGINNING}, {1: "a"}))

    assert payload.metadata["encoding"] == b"binary/plain"
    assert decode_continuation(payload.data).cursors == {1: BEGINNING}


def test_the_header_name_is_in_this_features_namespace() -> None:
    assert CONTINUATION_HEADER.startswith("__temporal_external_stream")
    assert not CONTINUATION_HEADER.startswith("__temporal_workflow_stream")


def test_no_header_is_a_first_execution_not_a_failure() -> None:
    assert read_continuation_header(None) is None
    assert read_continuation_header({}) is None
    assert (
        read_continuation_header(
            {"other": write_continuation_header(Continuation({1: BEGINNING}, {1: "a"}))}
        )
        is None
    )


# --- restoration --------------------------------------------------------------


def test_a_first_execution_starts_at_the_beginning(
    manager: StubManager, backend: MemoryStreamBackend
) -> None:
    """The same field, filled the same way -- replay reads a boundary either way.

    Including when the stream was empty for the subscription's entire life,
    which is the case an implicit "start wherever" would leave unrecorded.
    """
    runtime = make_runtime(manager, backend)

    runtime.register(
        wait_id=1, stream_key=runtime.stream_key("tokens"), backend_name="tokens"
    )

    assert runtime._subscriptions[1].start_cursor == BEGINNING


def test_a_successor_run_restores_its_predecessors_cursor(
    manager: StubManager, backend: MemoryStreamBackend
) -> None:
    runtime = make_runtime(
        manager,
        backend,
        Continuation({1: AFTER(Offset("100-3"))}, {1: "tokens"}),
    )

    runtime.register(
        wait_id=1, stream_key=runtime.stream_key("tokens"), backend_name="tokens"
    )

    assert runtime._subscriptions[1].start_cursor == AFTER(Offset("100-3"))


def test_restoration_reads_nothing_from_the_backend(
    manager: StubManager, backend: MemoryStreamBackend
) -> None:
    """The criterion, and the reason the header exists at all.

    A cursor read from the backend is whatever the stream holds now, so two
    replays of one history could restore different positions and diverge.
    """
    runtime = make_runtime(
        manager, backend, Continuation({1: AFTER(Offset("100-3"))}, {1: "tokens"})
    )

    runtime.register(
        wait_id=1, stream_key=runtime.stream_key("tokens"), backend_name="tokens"
    )

    assert backend.range_reads == [], "restoration must not read the stream"


def test_two_same_stream_subscriptions_restore_independently(
    manager: StubManager, backend: MemoryStreamBackend
) -> None:
    """They are separate waits with their own cursors.

    A stream-keyed continuation would restart one of them at the other's
    position, replaying records it had already consumed or skipping records it
    had not.
    """
    runtime = make_runtime(
        manager,
        backend,
        Continuation(
            {1: AFTER(Offset("100-0")), 2: AFTER(Offset("500-0"))},
            {1: "tokens", 2: "tokens"},
        ),
    )
    key = runtime.stream_key("tokens")

    runtime.register(wait_id=1, stream_key=key, backend_name="tokens")
    runtime.register(wait_id=2, stream_key=key, backend_name="tokens")

    assert runtime._subscriptions[1].start_cursor == AFTER(Offset("100-0"))
    assert runtime._subscriptions[2].start_cursor == AFTER(Offset("500-0"))


def test_a_subscription_the_predecessor_lacked_starts_at_the_beginning(
    manager: StubManager, backend: MemoryStreamBackend
) -> None:
    """Adding one on a path the chain has not reached yet is a supported change."""
    runtime = make_runtime(
        manager, backend, Continuation({1: AFTER(Offset("100-0"))}, {1: "tokens"})
    )

    runtime.register(
        wait_id=2, stream_key=runtime.stream_key("later"), backend_name="tokens"
    )

    assert runtime._subscriptions[2].start_cursor == BEGINNING


def test_a_renumbered_subscription_is_caught_rather_than_resumed_wrong(
    manager: StubManager, backend: MemoryStreamBackend
) -> None:
    """Restoring a cursor onto a differently-numbered wait resumes the wrong stream.

    The backend would accept the offset and no later check would catch it, so
    this is reported here -- as nondeterminism, with the same remedy an inserted
    timer needs. Not as integrity loss: the cursor is exactly what the
    predecessor committed, and it is the Workflow code that moved.
    """
    runtime = make_runtime(
        manager, backend, Continuation({1: AFTER(Offset("100-0"))}, {1: "tokens"})
    )

    with pytest.raises(
        temporalio.workflow.NondeterminismError, match="workflow.patched"
    ):
        runtime.register(
            wait_id=1,
            stream_key=runtime.stream_key("tool-events"),
            backend_name="tokens",
        )


# --- what gets committed ------------------------------------------------------


def test_the_continuation_reports_what_was_consumed_not_what_was_delivered(
    manager: StubManager, backend: MemoryStreamBackend
) -> None:
    """The successor must not start past records the Workflow never took.

    Delivery advances by whole drained batches, because that is what the
    annotation records and what replay must reproduce. A Workflow that stops
    iterating part-way through a batch has consumed only its prefix, and the
    rest dies with the Run's buffer -- so a continuation taken from the delivery
    cursor would step over records nothing had ever shown to Workflow code.
    """
    runtime = make_runtime(manager, backend)
    runtime.register(
        wait_id=1, stream_key=runtime.stream_key("tokens"), backend_name="tokens"
    )
    batch = [
        StreamRecord(RecordKind.DATA, b"a", "s", i).placed_at(Offset(f"10-{i}"))
        for i in range(3)
    ]
    for record in batch:
        runtime.record_delivery(1, record)
    # The Workflow took only the first of the three, then stopped iterating.
    runtime.record_consumption(1, batch[0])

    assert runtime.continuation().cursors[1] == AFTER(Offset("10-0")), (
        "the successor must resume at the first record this Run did not consume"
    )


def test_a_control_record_still_advances_consumption(
    manager: StubManager, backend: MemoryStreamBackend
) -> None:
    """It is never yielded, but it is finished with.

    Leaving it behind would restart the successor at a fence it cannot act on,
    and it would be redelivered on every Run of the chain.
    """
    runtime = make_runtime(manager, backend)
    runtime.register(
        wait_id=1, stream_key=runtime.stream_key("tokens"), backend_name="tokens"
    )
    fence = StreamRecord(RecordKind.WRITE_FENCE, b"", "s", 0).placed_at(Offset("10-0"))

    runtime.record_consumption(1, fence)

    assert runtime.continuation().cursors[1] == AFTER(Offset("10-0"))


def test_a_run_that_delivered_nothing_still_reports_its_start(
    manager: StubManager, backend: MemoryStreamBackend
) -> None:
    """Otherwise the successor would restart at BEGINNING and redeliver."""
    runtime = make_runtime(
        manager, backend, Continuation({1: AFTER(Offset("100-0"))}, {1: "tokens"})
    )
    runtime.register(
        wait_id=1, stream_key=runtime.stream_key("tokens"), backend_name="tokens"
    )

    assert runtime.continuation().cursors[1] == AFTER(Offset("100-0"))


# --- through a real chain -----------------------------------------------------


@workflow.defn
class ChainedConsumerWorkflow:
    """Consumes two records, continues as new, consumes two more.

    The whole point is the second Run must not see the first Run's records. A
    Workflow that returned everything it saw would hide that: only counting what
    the *successor* observed makes a redelivery visible.
    """

    @workflow.run
    async def run(self, remaining: int) -> list[str]:
        tokens = external_stream.with_options(idle_timeout=timedelta(seconds=30)).topic(
            "tokens", backend="tokens-memory", type=str
        )
        seen: list[str] = []
        async for token in tokens.subscribe():
            seen.append(token)
            if len(seen) >= 2:
                break
        if remaining > 1:
            workflow.continue_as_new(remaining - 1)
        return seen


async def publish(backend: MemoryStreamBackend, key: StreamKey, values: list[str]):
    from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec

    codec = StreamPayloadCodec(temporalio.converter.DataConverter.default, str)
    for i, value in enumerate(values):
        await backend.append(
            key,
            StreamRecord(RecordKind.DATA, await codec.encode(value), "producer", i),
        )


async def test_a_chain_resumes_where_its_predecessor_stopped(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """The successor consumes the records the first Run did not, and no others."""
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[ChainedConsumerWorkflow],
        external_stream_backends={"tokens-memory": backend},
    ):
        handle = await client.start_workflow(
            ChainedConsumerWorkflow.run,
            2,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        description = await handle.describe()
        key = StreamKey(
            client.namespace,
            handle.id,
            description.raw_description.workflow_execution_info.first_run_id,
            "tokens",
        )
        await asyncio.sleep(1)
        await publish(backend, key, ["a", "b", "c", "d"])

        assert await asyncio.wait_for(handle.result(), 60) == ["c", "d"], (
            "the successor Run redelivered records its predecessor had already "
            "consumed, so the continuation cursor did not survive Continue-As-New"
        )
