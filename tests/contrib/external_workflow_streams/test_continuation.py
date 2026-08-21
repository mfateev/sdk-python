"""P15 — the Continue-As-New cursor.

A stream spans a whole chain, so a new Run must resume where its predecessor
stopped. The position travels in a reserved internal header and is restored from
History, because a cursor derived from mutable backend state would give replay
whatever the stream holds *now* rather than what the Run started from -- and two
replays of one history could then diverge (ADR-022).
"""

from __future__ import annotations

import asyncio
import dataclasses
import uuid
from datetime import timedelta
from typing import Any

import pytest

import temporalio.converter
from temporalio import workflow
from temporalio.client import Client, WorkflowHandle
from temporalio.contrib.external_workflow_streams._annotation import (
    AnnotationDecodeError,
    _Reader,
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
from temporalio.contrib.external_workflow_streams._errors import StreamStorageError
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
from temporalio.exceptions import ApplicationError
from temporalio.worker import Replayer, Worker
from tests.contrib.external_workflow_streams.memory_backend import MemoryStreamBackend

with workflow.unsafe.imports_passed_through():
    from temporalio.contrib.external_workflow_streams._api import external_stream


async def _notify(run_id: str, wait_id: int, generation: int) -> str:
    return ReadinessResult.ACCEPTED


def make_runtime(manager, backend, continuation=None, backends=None):  # type: ignore[no-untyped-def]
    return WorkflowStreamRuntime(
        manager=manager,
        backends={"tokens": backend} if backends is None else backends,
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


class OtherProviderBackend(MemoryStreamBackend):
    """The same name, mapped to a different implementation.

    A deployment can do this without touching Workflow code, which is exactly
    why the continuation has to carry what the cursor was produced by rather
    than only what it was produced for.
    """

    provider_id = "other-memory"


class NewerFormatBackend(MemoryStreamBackend):
    """The recorded provider, at a format version that reads offsets differently."""

    provider_format_version = 2


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
    """Binding included: a cursor without one cannot be checked on restoration."""
    original = Continuation(
        cursors={1: AFTER(Offset("100-3")), 2: BEGINNING},
        stream_names={1: "tokens", 2: "tool-events"},
        backend_names={1: "store-a", 2: "store-b"},
        provider_ids={1: "memory", 2: "redis"},
        provider_format_versions={1: 1, 2: 3},
    )

    assert decode_continuation(encode_continuation(original)) == original


def test_the_encoding_is_stable_for_one_state() -> None:
    """The header is re-derived from live state every time the command is built.

    Encoding the same state two different ways -- by iterating a dict in
    insertion order, say -- would hand the successor a different cursor on a
    Workflow Task the server retried.
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

    with pytest.raises(workflow.NondeterminismError, match="workflow.patched"):
        runtime.register(
            wait_id=1,
            stream_key=runtime.stream_key("tool-events"),
            backend_name="tokens",
        )


def test_a_changed_backend_is_caught_rather_than_resumed_wrong(
    manager: StubManager, backend: MemoryStreamBackend
) -> None:
    """Two stores, one offset syntax: the new store accepts a foreign boundary.

    The wait number and the stream name are both unchanged, so nothing before
    this looks wrong -- and the records the new store holds below the restored
    boundary would simply never be delivered. Nondeterminism rather than
    integrity loss, for the same reason :meth:`_verify_binding` says so: the
    backend a topic names is Workflow code.
    """
    other_store = MemoryStreamBackend()
    runtime = make_runtime(
        manager,
        backend,
        Continuation({1: AFTER(Offset("200-0"))}, {1: "tokens"}, {1: "old-store"}),
        backends={"old-store": backend, "new-store": other_store},
    )

    with pytest.raises(workflow.NondeterminismError, match="workflow.patched"):
        runtime.register(
            wait_id=1,
            stream_key=runtime.stream_key("tokens"),
            backend_name="new-store",
        )

    assert manager.registered == [], "the cursor must not reach the manager"
    assert other_store.range_reads == [], "the new store must not be read at all"


def test_a_backend_name_pointing_at_another_provider_is_a_storage_failure(
    manager: StubManager,
) -> None:
    """The Workflow is unchanged and neither store is damaged.

    Marker replay reports this against a recorded range; here it is reported
    against a restored cursor, which is the same question one Workflow Task
    earlier -- and before any read, so the wrong implementation never gets to
    interpret the boundary at all.
    """
    backend = OtherProviderBackend()
    runtime = make_runtime(
        manager,
        backend,
        Continuation(
            {1: AFTER(Offset("200-0"))},
            {1: "tokens"},
            {1: "tokens"},
            {1: "memory"},
            {1: 1},
        ),
    )

    with pytest.raises(StreamStorageError, match="'other-memory'"):
        runtime.register(
            wait_id=1, stream_key=runtime.stream_key("tokens"), backend_name="tokens"
        )

    assert manager.registered == []
    assert backend.range_reads == [], "raised before any backend read"


def test_a_newer_provider_format_version_is_a_storage_failure(
    manager: StubManager,
) -> None:
    """The implementation is the recorded one but reads offsets differently.

    Resuming would interpret a boundary under a format it was not written in,
    which is not something the offset itself can reveal.
    """
    backend = NewerFormatBackend()
    runtime = make_runtime(
        manager,
        backend,
        Continuation(
            {1: AFTER(Offset("200-0"))},
            {1: "tokens"},
            {1: "tokens"},
            {1: "memory"},
            {1: 1},
        ),
    )

    with pytest.raises(StreamStorageError, match="format version"):
        runtime.register(
            wait_id=1, stream_key=runtime.stream_key("tokens"), backend_name="tokens"
        )

    assert manager.registered == []
    assert backend.range_reads == [], "raised before any backend read"


def test_a_recorded_format_version_of_zero_is_still_compared(
    manager: StubManager,
) -> None:
    """Zero is a format version, not a "nothing was recorded" sentinel.

    The backend contract types ``provider_format_version`` as a plain integer
    and reserves no value, and the header represents zero exactly. Deciding
    whether a version was recorded by truthiness therefore skipped the
    comparison for the one value that looks falsey, which made Continue-As-New
    less safe than marker replay for the same binding: replay compares exactly.
    """
    backend = MemoryStreamBackend()
    assert type(backend).provider_format_version == 1, (
        "the fixture must declare a version the recorded zero disagrees with"
    )
    runtime = make_runtime(
        manager,
        backend,
        Continuation(
            {1: AFTER(Offset("200-0"))},
            {1: "tokens"},
            {1: "tokens"},
            {1: "memory"},
            {1: 0},
        ),
    )

    with pytest.raises(StreamStorageError, match="format version"):
        runtime.register(
            wait_id=1, stream_key=runtime.stream_key("tokens"), backend_name="tokens"
        )

    assert manager.registered == [], "the cursor must not reach the manager"
    assert backend.range_reads == [], "raised before any backend read"


def test_a_header_that_recorded_no_format_version_skips_the_check(
    manager: StubManager, backend: MemoryStreamBackend
) -> None:
    """The other half of reading that map by membership.

    A header written before the binding was carried has no entry at all, and
    there is nothing to compare against; refusing it would strand every chain
    that continued as new across the upgrade.
    """
    runtime = make_runtime(
        manager,
        backend,
        Continuation({1: AFTER(Offset("200-0"))}, {1: "tokens"}),
    )

    runtime.register(
        wait_id=1, stream_key=runtime.stream_key("tokens"), backend_name="tokens"
    )

    assert runtime._subscriptions[1].start_cursor == AFTER(Offset("200-0"))


# --- reading a header this Worker did not write -------------------------------


#: One wait at ``AFTER("100-3")`` on stream ``tokens``, as a Worker that only
#: knew schema version 1 wrote it. Written out rather than produced by this
#: module's encoder, so a change to the encoder cannot quietly redefine what a
#: history in flight is expected to contain.
VERSION_1_HEADER = bytes([1, 1, 1, 0x01, 5]) + b"100-3" + bytes([6]) + b"tokens"


def test_a_version_1_continuation_still_restores(
    manager: StubManager, backend: MemoryStreamBackend
) -> None:
    """A chain that continued as new before the binding was carried.

    Its successor's ``WorkflowExecutionStarted`` already holds those bytes, and
    that Run has to start where its predecessor stopped rather than at
    ``BEGINNING``. Nothing was recorded about the store, so the checks that need
    one are skipped -- not failed against nothing -- and re-encoding reproduces
    the bytes the header arrived as, since a header carrying only cursors has no
    extension to append.
    """
    restored = decode_continuation(VERSION_1_HEADER)

    assert restored.cursors == {1: AFTER(Offset("100-3"))}
    assert restored.backend_names == {}
    assert encode_continuation(restored) == VERSION_1_HEADER

    runtime = make_runtime(manager, backend, restored)
    runtime.register(
        wait_id=1, stream_key=runtime.stream_key("tokens"), backend_name="tokens"
    )

    assert runtime._subscriptions[1].start_cursor == AFTER(Offset("100-3"))


def _restore_as_a_worker_without_the_binding(raw: bytes, subscribed_stream: str) -> Any:
    """What a Worker deployed before the binding existed does with these bytes.

    Written out rather than imported, because the behaviour under test is a
    *shipped* one and is frozen. Both halves of it are: that decoder accepts
    schema version 1 and nothing else, reads the count and exactly that many
    entries, and returns without looking at what follows -- and its restoration
    then compares the **stream name alone**, because a backend name is not
    something it was ever given. Importing the current implementation would test
    this Worker against itself, which is the one pair that is never mixed.

    Returns the cursor that old Worker would hand its backend.
    """
    reader = _Reader(raw)
    version = reader.uvarint()
    if version != 1:
        raise AnnotationDecodeError(
            f"the Continue-As-New cursor header is schema version {version}, but "
            "this Worker understands 1. The previous Run of this chain was "
            "executed by a newer SDK."
        )
    cursors: dict[int, Any] = {}
    stream_names: dict[int, str] = {}
    for _ in range(reader.uvarint()):
        wait_id = reader.uvarint()
        cursors[wait_id] = reader.cursor()
        stream_names[wait_id] = reader.string()
    # The whole of the old restoration check.
    if stream_names.get(1, "") not in ("", subscribed_stream):
        raise AssertionError("this old Worker would have reported nondeterminism")
    return cursors.get(1)


LIVE_CONTINUATION = Continuation(
    cursors={1: AFTER(Offset("100-3")), 2: BEGINNING},
    stream_names={1: "tokens", 2: "tool-events"},
    backend_names={1: "store-a", 2: "store-b"},
    provider_ids={1: "memory", 2: "redis"},
    provider_format_versions={1: 1, 2: 3},
)


def test_a_live_header_is_refused_by_a_worker_without_the_binding() -> None:
    """The binding is must-understand data, so refusal is the required outcome.

    A Worker that cannot validate the binding must not start the successor Run,
    and the version number is the only thing that can make it one: no
    arrangement of bytes makes deployed code perform a check it has no code for.
    An envelope such a Worker *can* parse — the binding appended behind the
    entries it reads, say — is one it restores with the check skipped, and this
    test shows what that costs. The old restoration compares the stream name and
    nothing else, so a fleet where `tokens` resolves to another store on the old
    build hands it a cursor produced somewhere else, and every record below that
    boundary is skipped in silence.

    Refusing instead fails the successor's first Workflow Task until a
    binding-aware Worker takes it, which is a blocked Run rather than a wrong
    one — the direction ADR-014 takes throughout this feature.
    """
    raw = encode_continuation(LIVE_CONTINUATION)

    assert raw[0] == 2, (
        "a live header must announce a version that a Worker unable to check the "
        "binding refuses outright"
    )

    with pytest.raises(AnnotationDecodeError, match="schema version 2"):
        _restore_as_a_worker_without_the_binding(raw, subscribed_stream="tokens")

    # What that refusal prevents: the same old Worker, handed a header it *can*
    # parse, accepts the cursor on the strength of the stream name alone and
    # never learns which store produced it.
    version_1_shape = encode_continuation(
        dataclasses.replace(
            LIVE_CONTINUATION,
            backend_names={},
            provider_ids={},
            provider_format_versions={},
            schema_version=1,
        )
    )
    assert _restore_as_a_worker_without_the_binding(
        version_1_shape, subscribed_stream="tokens"
    ) == AFTER(Offset("100-3")), (
        "the old restoration must be shown accepting a cursor with no backend "
        "check, or the refusal above is not protecting anything"
    )


def test_a_header_with_the_binding_inlined_still_decodes() -> None:
    """Written only by a build between the binding landing and this envelope.

    A chain that continued as new under one has a successor whose start header
    holds those bytes, so they stay readable -- and re-encoding reproduces them
    rather than silently rewriting a header this Worker only read.
    """
    inlined = dataclasses.replace(LIVE_CONTINUATION, schema_version=2)
    raw = encode_continuation(inlined)

    assert raw[0] == 2
    decoded = decode_continuation(raw)

    assert decoded == inlined
    assert encode_continuation(decoded) == raw


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


# --- the boundary the header is taken at --------------------------------------
#
# Creating the Continue-As-New command does not end the activation. The event
# loop keeps draining what is already ready, and a stream consumer that runs in
# that tail consumes records the predecessor's final marker *does* record -- so a
# header taken when the command was created describes an earlier boundary than
# History does, and the successor is handed those records a second time.


@workflow.defn
class LateConsumerContinueAsNewWorkflow:
    """Schedules a consumer, then continues as new without yielding to it.

    The scheduled task takes a record that is already buffered, so it finishes
    without blocking -- it just does so after the terminal command exists.

    Only the successor's records are returned. A Run that reported everything it
    saw would hide the redelivery inside the predecessor's own list.
    """

    @workflow.run
    async def run(self, remaining: int) -> list[str]:
        tokens = external_stream.with_options(idle_timeout=timedelta(seconds=30)).topic(
            "tokens", backend="tokens-memory", type=str
        )
        subscription = tokens.subscribe()
        iterator = subscription.__aiter__()
        first = await iterator.__anext__()
        if remaining <= 1:
            return [first, await iterator.__anext__()]
        _require_a_buffered_record(subscription)

        async def take_one() -> None:
            await iterator.__anext__()

        # On the ready queue, and able to finish without blocking. `_run_once`
        # gives it its turn after the command below is created.
        asyncio.create_task(take_one())
        workflow.continue_as_new(remaining - 1)


@workflow.defn
class SignalContinueAsNewWorkflow:
    """A signal handler continues as new while a consumer is being unblocked.

    The consumer is parked on a condition the handler sets, and conditions are
    re-checked *after* the ready queue drains -- which is after the terminal
    command was created. So this reaches the same tail by the route ordinary
    application structure reaches it by, rather than by scheduling a task next
    to the Continue-As-New call.
    """

    def __init__(self) -> None:
        self._release = False
        self._staged = False

    @workflow.run
    async def run(self, remaining: int) -> list[str]:
        tokens = external_stream.with_options(idle_timeout=timedelta(seconds=30)).topic(
            "tokens", backend="tokens-memory", type=str
        )
        subscription = tokens.subscribe()
        iterator = subscription.__aiter__()
        first = await iterator.__anext__()
        if remaining <= 1:
            return [first, await iterator.__anext__()]
        _require_a_buffered_record(subscription)

        async def take_one() -> None:
            await workflow.wait_condition(lambda: self._release)
            await iterator.__anext__()

        asyncio.create_task(take_one())
        self._staged = True
        # The handler ends this Run; nothing else does.
        await workflow.wait_condition(lambda: False)
        raise AssertionError("unreachable")

    @workflow.query
    def staged(self) -> bool:
        """Whether the consumer is parked yet.

        Signalling before it is would continue as new with nothing else ready,
        and the test would pass without exercising anything.
        """
        return self._staged

    @workflow.signal
    async def wrap_up(self, remaining: int) -> None:
        self._release = True
        workflow.continue_as_new(remaining)


def _require_a_buffered_record(subscription: object) -> None:
    """Fails the Run rather than letting a thin test pass.

    The whole shape depends on a record still sitting in the subscription's
    ready list when the terminal command is created. If the batch arrived one
    record at a time there is nothing for the tail to consume, and the
    assertions below would hold for the wrong reason.
    """
    if not subscription._ready:  # type: ignore[attr-defined]
        raise ApplicationError(
            "no record was buffered when the terminal command was created, so "
            "nothing was consumed after it and this Run proves nothing",
            non_retryable=True,
        )


async def stage_stream(
    client: Client,
    backend: MemoryStreamBackend,
    handle: WorkflowHandle[Any, Any],
    values: list[str],
) -> StreamKey:
    """Puts the whole batch in the stream before any Worker can read it.

    A record that arrives an activation later is delivered to the successor by
    the ordinary path and says nothing about this boundary. Publishing before
    the Worker exists is what makes the Run's first drain take the batch whole.
    """
    description = await handle.describe()
    key = StreamKey(
        client.namespace,
        handle.id,
        description.raw_description.workflow_execution_info.first_run_id,
        "tokens",
    )
    await publish(backend, key, values)
    return key


async def _until_staged(handle: WorkflowHandle[Any, Any]) -> None:
    for _ in range(300):
        if await handle.query(SignalContinueAsNewWorkflow.staged):
            return
        await asyncio.sleep(0.1)
    raise AssertionError("the consumer never parked on its condition")


async def test_a_consumer_that_runs_after_the_terminal_command_is_still_consumed(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """The predecessor's marker and the successor's header are one boundary.

    Both records the predecessor took are recorded in its final marker. A
    continuation snapshot taken when the command was created holds only the
    first, and the successor then starts one record too early.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    handle = await client.start_workflow(
        LateConsumerContinueAsNewWorkflow.run,
        2,
        id=f"wf-{uuid.uuid4()}",
        task_queue=task_queue,
    )
    await stage_stream(client, backend, handle, ["a", "b", "c", "d"])

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[LateConsumerContinueAsNewWorkflow],
        external_stream_backends={"tokens-memory": backend},
    ):
        assert await asyncio.wait_for(handle.result(), 30) == ["c", "d"], (
            "the successor was handed a record its predecessor consumed after "
            "the Continue-As-New command was created, so the continuation "
            "header was taken before the activation had finished"
        )


async def test_a_signal_handler_continuing_as_new_waits_for_the_consumer(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """The same boundary, reached through a condition rather than a new task."""
    task_queue = f"tq-{uuid.uuid4()}"
    handle = await client.start_workflow(
        SignalContinueAsNewWorkflow.run,
        2,
        id=f"wf-{uuid.uuid4()}",
        task_queue=task_queue,
    )
    await stage_stream(client, backend, handle, ["a", "b", "c", "d"])

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[SignalContinueAsNewWorkflow],
        external_stream_backends={"tokens-memory": backend},
    ):
        await _until_staged(handle)
        await handle.signal(SignalContinueAsNewWorkflow.wrap_up, 1)

        assert await asyncio.wait_for(handle.result(), 30) == ["c", "d"], (
            "the consumer the signal handler unblocked consumed a record after "
            "the Continue-As-New command was created, and the successor "
            "received it again"
        )


async def test_a_history_written_at_the_earlier_boundary_still_replays(
    client: Client, backend: MemoryStreamBackend
) -> None:
    """A chain that continued as new before this Worker was deployed.

    Replay regenerates the header at the *later* boundary, so it no longer
    matches the recorded one. That is allowed and needs no compatibility flag:
    Core matches a Continue-As-New command to its
    ``WorkflowExecutionContinuedAsNew`` event by command type alone and never
    compares headers. The successor of such a chain is unaffected either way --
    it reads its cursor from its own ``WorkflowExecutionStarted``, which
    replaying the predecessor does not rewrite.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    handle = await client.start_workflow(
        LateConsumerContinueAsNewWorkflow.run,
        2,
        id=f"wf-{uuid.uuid4()}",
        task_queue=task_queue,
    )
    key = await stage_stream(client, backend, handle, ["a", "b", "c", "d"])
    first_run_id = handle.first_execution_run_id
    assert first_run_id is not None

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[LateConsumerContinueAsNewWorkflow],
        external_stream_backends={"tokens-memory": backend},
    ):
        await asyncio.wait_for(handle.result(), 30)
        history = await client.get_workflow_handle(
            handle.id, run_id=first_run_id
        ).fetch_history()

    continued = [
        e
        for e in history.events
        if e.HasField("workflow_execution_continued_as_new_event_attributes")
    ]
    assert continued, "the predecessor did not continue as new"
    attributes = continued[0].workflow_execution_continued_as_new_event_attributes
    recorded = decode_continuation(attributes.header.fields[CONTINUATION_HEADER].data)

    # What the earlier snapshot held: the first record, taken before the tail of
    # the activation consumed the second.
    first_offset = backend._records[key][0].offset
    assert first_offset is not None
    early = dataclasses.replace(
        recorded, cursors={wait_id: AFTER(first_offset) for wait_id in recorded.cursors}
    )
    assert early != recorded, (
        "the recorded header already holds the earlier boundary, so this "
        "history is not the pre-fix one it is meant to stand in for"
    )
    attributes.header.fields[CONTINUATION_HEADER].CopyFrom(
        write_continuation_header(early)
    )

    result = await Replayer(
        workflows=[LateConsumerContinueAsNewWorkflow],
        external_stream_backends={"tokens-memory": backend},
    ).replay_workflow(history, raise_on_replay_failure=False)

    assert result.replay_failure is None, (
        "a history whose Continue-As-New header holds the earlier boundary no "
        f"longer replays: {result.replay_failure}"
    )
