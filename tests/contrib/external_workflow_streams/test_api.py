"""P9 — the Workflow-facing API and its wait-id assignment."""

from __future__ import annotations

import asyncio
import inspect
from datetime import timedelta
from typing import Any

import pytest

import temporalio.converter
import temporalio.workflow
from temporalio.contrib.external_workflow_streams import _api
from temporalio.contrib.external_workflow_streams._api import (
    DEFAULT_IDLE_TIMEOUT,
    MAX_RECORDS_PER_ACTIVATION,
    ExternalStreamOptions,
    ExternalStreamSubscription,
    ExternalStreamTopic,
    _install_runtime,
    external_stream,
    merge,
)
from temporalio.contrib.external_workflow_streams._backend import StreamKey
from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec
from temporalio.contrib.external_workflow_streams._errors import (
    ConcurrentStreamConsumerError,
    StreamDecodeError,
)
from temporalio.contrib.external_workflow_streams._manager import PreparedRecord
from temporalio.contrib.external_workflow_streams._record import (
    RecordKind,
    StreamRecord,
)


class FakeRuntime:
    """The Worker's half, standing in for the real manager handle.

    Deliberately only offers what the protocol names -- no provider instance,
    no way to reach one -- because that is the point of the boundary.
    """

    def __init__(self, registered_backends: set[str] | None = None) -> None:
        self.registered = registered_backends or {"tokens-redis"}
        self.registrations: list[tuple[int, StreamKey, str]] = []
        #: `wait_id -> configured idle timeout`, so a test can see that the
        #: value `with_options` was given actually reached the Worker.
        self.idle_timeouts: dict[int, timedelta] = {}
        self.buffers: dict[int, list[StreamRecord]] = {}
        self.deliveries: list[tuple[int, StreamRecord]] = []
        self.consumed: list[tuple[int, StreamRecord]] = []
        self.blocked: list[tuple[int, bool]] = []
        #: The wait ids `close()` asked the Worker to stop serving.
        self.unsubscribed: list[int] = []
        self.pending: dict[int, asyncio.Future[None]] = {}
        self.budget = MAX_RECORDS_PER_ACTIVATION
        #: Overrides what `codec_for` hands back, so a test can control when --
        #: and whether -- decoding a record succeeds.
        self.codec: Any = None

    def stream_key(self, stream_name: str) -> StreamKey:
        return StreamKey("ns", "wf", "first-run", stream_name)

    def register(
        self,
        *,
        wait_id: int,
        stream_key: StreamKey,
        backend_name: str,
        idle_timeout: timedelta,
    ) -> None:
        if backend_name not in self.registered:
            raise KeyError(f"no external stream backend named {backend_name!r}")
        self.registrations.append((wait_id, stream_key, backend_name))
        self.idle_timeouts[wait_id] = idle_timeout

    def drain(self, wait_id: int, max_records: int | None = None) -> list[StreamRecord]:
        buffered = self.buffers.get(wait_id, [])
        if max_records is not None:
            buffered, self.buffers[wait_id] = (
                buffered[:max_records],
                buffered[max_records:],
            )
        else:
            self.buffers[wait_id] = []
        return buffered

    def delivery_budget_remaining(self) -> int:
        return self.budget

    def record_consumption(self, wait_id: int, record: StreamRecord) -> None:
        self.consumed.append((wait_id, record))

    def codec_for(
        self, value_type: type | None, wait_id: int
    ) -> StreamPayloadCodec[Any]:
        if self.codec is not None:
            return self.codec
        return StreamPayloadCodec(
            temporalio.converter.DataConverter.default, value_type
        )

    def new_readiness_future(self) -> asyncio.Future[None]:
        return asyncio.get_event_loop().create_future()

    def record_delivery(self, wait_id: int, record: StreamRecord) -> None:
        self.deliveries.append((wait_id, record))
        # Charged where the real runtime charges it: the drain that moves a record
        # into a ready list is the reservation, not the consumption that follows.
        self.budget = max(0, self.budget - 1)

    def note_blocked(self, wait_id: int, blocked: bool) -> None:
        self.blocked.append((wait_id, blocked))

    def unsubscribe(self, wait_id: int) -> None:
        self.unsubscribed.append(wait_id)

    def register_pending(self, wait_id: int, future: asyncio.Future[None]) -> None:
        self.pending[wait_id] = future

    def discard_pending(self, wait_id: int) -> None:
        self.pending.pop(wait_id, None)


class FakeInstance:
    """Stands in for the user's Workflow object, which holds the per-Run state."""


@pytest.fixture
def workflow_instance(monkeypatch: pytest.MonkeyPatch) -> FakeInstance:
    instance = FakeInstance()
    monkeypatch.setattr(temporalio.workflow, "instance", lambda: instance)
    return instance


@pytest.fixture
def runtime(workflow_instance: FakeInstance) -> FakeRuntime:
    runtime = FakeRuntime()
    _install_runtime(workflow_instance, runtime)
    return runtime


# --- options ------------------------------------------------------------------


def test_the_default_idle_timeout_is_one_second() -> None:
    assert external_stream.idle_timeout == DEFAULT_IDLE_TIMEOUT == timedelta(seconds=1)


def test_with_options_returns_a_copy() -> None:
    """The module-level entry point must not be mutated by configuring it."""
    configured = external_stream.with_options(idle_timeout=timedelta(seconds=5))

    assert configured.idle_timeout == timedelta(seconds=5)
    assert external_stream.idle_timeout == timedelta(seconds=1)
    assert isinstance(configured, ExternalStreamOptions)


@pytest.mark.parametrize("bad", [timedelta(0), timedelta(seconds=-1)])
def test_a_non_positive_idle_timeout_is_rejected(bad: timedelta) -> None:
    """A configuration error, not a request to park immediately."""
    with pytest.raises(ValueError, match="must be positive"):
        external_stream.with_options(idle_timeout=bad)


def test_topics_inherit_their_options(runtime: FakeRuntime) -> None:
    configured = external_stream.with_options(idle_timeout=timedelta(seconds=3))

    subscription = configured.topic("tokens", backend="tokens-redis").subscribe()

    assert subscription.idle_timeout == timedelta(seconds=3)


# --- naming a backend ---------------------------------------------------------


def test_a_workflow_names_a_backend_it_never_imports(runtime: FakeRuntime) -> None:
    """The criterion, and the reason the boundary exists.

    Nothing here is a provider instance: the Workflow supplies a name, and the
    Worker resolves it against its own registry, outside the sandbox.
    """
    topic = external_stream.topic("tokens", backend="tokens-redis", type=str)

    subscription = topic.subscribe()

    assert isinstance(subscription, ExternalStreamSubscription)
    assert runtime.registrations == [
        (1, StreamKey("ns", "wf", "first-run", "tokens"), "tokens-redis")
    ]
    assert isinstance(topic.backend_name, str)


def test_naming_an_unregistered_backend_fails(runtime: FakeRuntime) -> None:
    with pytest.raises(KeyError, match="no external stream backend"):
        external_stream.topic("tokens", backend="not-registered").subscribe()


@pytest.mark.parametrize(
    ("name", "backend", "reason"),
    [("", "tokens-redis", "non-empty name"), ("tokens", "", "names a backend")],
)
def test_a_topic_needs_both_a_name_and_a_backend(
    name: str, backend: str, reason: str
) -> None:
    with pytest.raises(ValueError, match=reason):
        external_stream.topic(name, backend=backend)


def test_subscribing_without_a_configured_worker_says_so(
    workflow_instance: FakeInstance,
) -> None:
    with pytest.raises(RuntimeError, match="external_stream_backends"):
        external_stream.topic("tokens", backend="tokens-redis").subscribe()


# --- wait id assignment -------------------------------------------------------


def test_wait_ids_are_assigned_from_one_in_subscribe_call_order(
    runtime: FakeRuntime,
) -> None:
    first = external_stream.topic("a", backend="tokens-redis").subscribe()
    second = external_stream.topic("b", backend="tokens-redis").subscribe()
    third = external_stream.topic("c", backend="tokens-redis").subscribe()

    assert [first.wait_id, second.wait_id, third.wait_id] == [1, 2, 3]
    assert [wait_id for wait_id, _, _ in runtime.registrations] == [1, 2, 3]


def test_wait_ids_reproduce_across_two_runs_of_the_same_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Replay depends on this exactly as it depends on timer sequence numbers.

    A renumbered wait produces an annotation mismatch rather than a silently
    different stream result -- but only because the numbering is reproducible
    in the first place.
    """

    def run_once() -> list[int]:
        instance = FakeInstance()
        monkeypatch.setattr(temporalio.workflow, "instance", lambda: instance)
        _install_runtime(instance, FakeRuntime())
        return [
            external_stream.topic(name, backend="tokens-redis").subscribe().wait_id
            for name in ("tokens", "tool-events", "tokens")
        ]

    assert run_once() == run_once() == [1, 2, 3]


def test_two_subscriptions_to_one_stream_get_distinct_wait_ids(
    runtime: FakeRuntime,
) -> None:
    """They are two independent waits, each with its own cursor and park intent.

    Delivery is broadcast, so each sees every record from its own position --
    which is only expressible if they are numbered apart.
    """
    topic = external_stream.topic("tokens", backend="tokens-redis")

    first = topic.subscribe()
    second = topic.subscribe()

    assert first.wait_id != second.wait_id
    assert first.stream_key == second.stream_key
    assert [wait_id for wait_id, _, _ in runtime.registrations] == [1, 2]


def test_a_second_run_restarts_the_counter(monkeypatch: pytest.MonkeyPatch) -> None:
    """Per-Run state lives on the instance, so an evicted Run takes it with it.

    A module global would outlive the Run and hand its wait ids to the next one.
    """
    first_instance = FakeInstance()
    monkeypatch.setattr(temporalio.workflow, "instance", lambda: first_instance)
    _install_runtime(first_instance, FakeRuntime())
    external_stream.topic("tokens", backend="tokens-redis").subscribe()

    second_instance = FakeInstance()
    monkeypatch.setattr(temporalio.workflow, "instance", lambda: second_instance)
    _install_runtime(second_instance, FakeRuntime())

    assert (
        external_stream.topic("tokens", backend="tokens-redis").subscribe().wait_id == 1
    )


# --- iteration ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_iteration_yields_decoded_values_from_the_buffer(
    runtime: FakeRuntime,
) -> None:
    codec = StreamPayloadCodec(temporalio.converter.DataConverter.default, str)
    subscription = external_stream.topic(
        "tokens", backend="tokens-redis", type=str
    ).subscribe()
    runtime.buffers[subscription.wait_id] = [
        StreamRecord(RecordKind.DATA, await codec.encode(v), "s", i)
        for i, v in enumerate(["a", "b"])
    ]

    seen = []
    async for value in subscription:
        seen.append(value)
        if len(seen) == 2:
            break

    assert seen == ["a", "b"]


@pytest.mark.asyncio
async def test_control_records_are_never_yielded(runtime: FakeRuntime) -> None:
    """A fence advances the cursor but is the runtime's, not the Workflow's."""
    codec = StreamPayloadCodec(temporalio.converter.DataConverter.default, str)
    subscription = external_stream.topic(
        "tokens", backend="tokens-redis", type=str
    ).subscribe()
    runtime.buffers[subscription.wait_id] = [
        StreamRecord(RecordKind.DATA, await codec.encode("a"), "s", 0),
        StreamRecord(RecordKind.WRITE_FENCE, b"", "s", 1),
        StreamRecord(RecordKind.DATA, await codec.encode("b"), "s", 2),
    ]

    seen = []
    async for value in subscription:
        seen.append(value)
        if len(seen) == 2:
            break

    assert seen == ["a", "b"]


@pytest.mark.asyncio
async def test_an_empty_buffer_blocks_on_a_readiness_future(
    runtime: FakeRuntime,
) -> None:
    """Iteration never polls the backend; only Core can say when to look again."""
    subscription = external_stream.topic(
        "tokens", backend="tokens-redis", type=str
    ).subscribe()

    iterator = subscription.__aiter__()
    pending = asyncio.ensure_future(iterator.__anext__())
    await asyncio.sleep(0.05)

    assert not pending.done(), "iteration must block rather than spin on the backend"
    pending.cancel()
    try:
        await pending
    except asyncio.CancelledError:
        pass


def test_closing_a_subscription_unsubscribes_it(runtime: FakeRuntime) -> None:
    """The Workflow side going quiet is only half of closing.

    Everything the Workflow itself can observe is already right without this
    call -- iteration ends, the wait leaves the blocked set, undelivered records
    stay unconsumed -- so nothing on this side of the boundary would notice it
    missing. What is left behind is on the Worker: a watcher still prefetching
    into a buffer nobody will drain, and a park intent still in the backend for
    every producer that asks.
    """
    subscription = external_stream.topic(
        "tokens", backend="tokens-redis", type=str
    ).subscribe()

    subscription.close()

    assert runtime.unsubscribed == [subscription.wait_id], (
        "closing left the Worker serving a wait no Workflow code is reading"
    )
    # Idempotent, like the rest of `close`: the ordinary shape is a `finally`
    # that cannot know whether the iterator already ended, and by a second call
    # the Worker has already dropped this wait.
    subscription.close()
    assert runtime.unsubscribed == [subscription.wait_id]


# --- one subscription, one consumer -------------------------------------------


@pytest.mark.asyncio
async def test_a_second_coroutine_waiting_on_one_subscription_is_refused(
    runtime: FakeRuntime,
) -> None:
    """Sharing a single-slot wait between two waiters strands one of them forever.

    ``__aiter__`` returns a new generator every time, but everything a blocked
    wait is found through is the subscription's: one ``_pending_future`` and one
    entry in the runtime's pending map, keyed by wait id. A second waiter
    therefore *replaced* the first in both places -- readiness resolved only the
    newer one, its ``finally`` removed the map entry, and the older future became
    unreachable by the readiness activation and by ``close()`` alike. Observed as
    a coroutine still pending after a record had been buffered, readiness
    resolved, and the subscription closed: permanently stuck, with the shared
    blocked flag saying the wait was not even blocked.
    """
    subscription = external_stream.topic(
        "tokens", backend="tokens-redis", type=str
    ).subscribe()

    first = asyncio.ensure_future(subscription.__aiter__().__anext__())
    await asyncio.sleep(0.05)
    assert subscription.wait_id in runtime.pending, (
        "the first consumer has to be registered and blocked before a second one "
        "can overwrite it"
    )

    with pytest.raises(ConcurrentStreamConsumerError, match="single consumer"):
        await subscription.__aiter__().__anext__()

    # The refusal changed nothing about the consumer that was already there.
    assert runtime.pending[subscription.wait_id] is subscription._pending_future
    codec = StreamPayloadCodec(temporalio.converter.DataConverter.default, str)
    runtime.buffers[subscription.wait_id] = [
        StreamRecord(RecordKind.DATA, await codec.encode("a"), "s", 0)
    ]
    runtime.pending[subscription.wait_id].set_result(None)

    assert await asyncio.wait_for(first, 1) == "a"
    assert not runtime.pending, "the resolved wait was left registered"
    assert subscription._pending_future is None


@pytest.mark.asyncio
async def test_iterating_again_after_the_first_consumer_stopped_is_allowed(
    runtime: FakeRuntime,
) -> None:
    """The shape the refusal must not catch, which is why it is not on the iterator.

    Taking a few records, doing something else, and coming back to the same
    subscription is ordinary code, and a ``break`` leaves the generator *suspended*
    rather than closed -- so a guard that claimed the subscription for an iterator
    could not tell that shape from two live consumers, and would refuse it.
    """
    codec = StreamPayloadCodec(temporalio.converter.DataConverter.default, str)
    subscription = external_stream.topic(
        "tokens", backend="tokens-redis", type=str
    ).subscribe()
    runtime.buffers[subscription.wait_id] = [
        StreamRecord(RecordKind.DATA, await codec.encode(value), "s", i)
        for i, value in enumerate(["a", "b"])
    ]

    async for value in subscription:
        assert value == "a"
        break

    # The abandoned generator is still suspended at its `yield`; nothing closed
    # it. A second pass must still work, and must resume rather than restart.
    async for value in subscription:
        assert value == "b"
        break


@pytest.mark.asyncio
async def test_a_merge_cannot_take_a_wait_another_consumer_is_blocked_on(
    runtime: FakeRuntime,
) -> None:
    """``merge()`` registers the same single slot, so it is the same defect.

    And it fails *before* registering anything, because a merge that refused
    half-way would leave its earlier waits blocked with no coroutine behind them
    -- which is the state that asks Core to retain a Workflow Task for nobody.
    """
    first = external_stream.topic("a", backend="tokens-redis", type=str).subscribe()
    second = external_stream.topic("b", backend="tokens-redis", type=str).subscribe()

    solo = asyncio.ensure_future(second.__aiter__().__anext__())
    await asyncio.sleep(0.05)
    assert set(runtime.pending) == {second.wait_id}

    async def consume_merged() -> None:
        async for _ in merge(first, second):
            pass

    with pytest.raises(ConcurrentStreamConsumerError):
        await consume_merged()

    assert set(runtime.pending) == {second.wait_id}, (
        "the refused merge left a wait registered, or took the one that was "
        "already blocked"
    )
    assert first._pending_future is None
    solo.cancel()
    try:
        await solo
    except asyncio.CancelledError:
        pass


# --- names --------------------------------------------------------------------


def test_no_exported_name_collides_with_the_shipped_contrib_feature() -> None:
    """The two coexist and are mirror images; nothing here may shadow that one."""
    import temporalio.contrib.external_workflow_streams as package

    for module in (package, _api):
        for name in dir(module):
            assert not name.startswith("__temporal_workflow_stream"), (
                f"{module.__name__}.{name} collides with the reserved namespace of "
                "temporalio.contrib.workflow_streams"
            )


def test_the_reserved_instance_attribute_is_in_this_features_namespace() -> None:
    assert _api._RUN_STATE_ATTR.startswith("__temporal_external_stream")
    assert not _api._RUN_STATE_ATTR.startswith("__temporal_workflow_stream")


def test_the_handle_types_are_named_as_the_design_says() -> None:
    assert ExternalStreamTopic.__name__ == "ExternalStreamTopic"
    assert not hasattr(ExternalStreamTopic, "publish")
    assert hasattr(ExternalStreamTopic, "subscribe")


def test_the_package_still_exports_nothing() -> None:
    """The public API lands with Milestone 1, not before (ADR-024)."""
    import temporalio.contrib.external_workflow_streams as package

    assert package.__all__ == []


@pytest.mark.asyncio
async def test_a_record_buffered_while_not_iterating_is_still_delivered(
    runtime: FakeRuntime,
) -> None:
    """The readiness for it was reported to nobody, and none is coming.

    A record that arrives while Workflow code is doing something else -- a
    timer, an activity, another stream -- is buffered and its readiness is
    reported and consumed immediately. By the time the Workflow comes back and
    asks for the next value, no further notification will ever be sent: the
    watcher has moved its prefetch cursor past that record. Blocking without
    looking at the buffer first strands the Workflow forever on a record that is
    already sitting in front of it.
    """
    codec = StreamPayloadCodec(temporalio.converter.DataConverter.default, str)
    subscription = external_stream.topic(
        "tokens", backend="tokens-redis", type=str
    ).subscribe()
    runtime.buffers[subscription.wait_id] = [
        StreamRecord(RecordKind.DATA, await codec.encode("first"), "s", 0)
    ]

    iterator = subscription.__aiter__()
    assert await iterator.__anext__() == "first"

    # Arrives while the Workflow is off doing something else. Nothing resolves a
    # readiness future for it, because nothing was waiting when it landed.
    runtime.buffers[subscription.wait_id] = [
        StreamRecord(RecordKind.DATA, await codec.encode("second"), "s", 1)
    ]

    assert await asyncio.wait_for(iterator.__anext__(), 1) == "second", (
        "the iterator blocked on a readiness notification that had already been "
        "spent, with the record buffered in front of it"
    )


@pytest.mark.asyncio
async def test_a_record_buffered_after_blocking_begins_still_resolves(
    runtime: FakeRuntime,
) -> None:
    """The other side of the window, which must keep working.

    Looking at the buffer before blocking must not replace the readiness path:
    a record that genuinely arrives later has no buffered copy to find, and the
    future is what delivers it.
    """
    codec = StreamPayloadCodec(temporalio.converter.DataConverter.default, str)
    subscription = external_stream.topic(
        "tokens", backend="tokens-redis", type=str
    ).subscribe()

    iterator = subscription.__aiter__()
    pending = asyncio.ensure_future(iterator.__anext__())
    await asyncio.sleep(0.05)
    assert not pending.done()

    runtime.buffers[subscription.wait_id] = [
        StreamRecord(RecordKind.DATA, await codec.encode("late"), "s", 0)
    ]
    runtime.pending[subscription.wait_id].set_result(None)

    assert await asyncio.wait_for(pending, 1) == "late"


# --- consumption is committed only once a value exists ------------------------


class FailingCodec:
    """A converter mismatch: the stream is fine, the configuration is not.

    Fails in :meth:`convert`, the half that runs on the Workflow thread, since
    that is where a type hint meets a payload the producer did not write.
    """

    def __init__(self, value_type: type | None = str) -> None:
        self._inner = StreamPayloadCodec(
            temporalio.converter.DataConverter.default, value_type
        )
        self.calls = 0

    def parse_unprepared(self, payload: bytes) -> Any:
        return self._inner.parse_unprepared(payload)

    def convert(self, prepared: Any) -> Any:
        self.calls += 1
        raise RuntimeError("this converter cannot read this payload")


@pytest.mark.asyncio
async def test_decoding_on_the_workflow_thread_cannot_suspend(
    runtime: FakeRuntime,
) -> None:
    """The Workflow thread converts; it does not await a converter.

    ``DataConverter.decode`` is retrieval, then a user ``PayloadCodec``, then
    ``from_payloads``. The first two are arbitrary asynchronous work -- a
    network fetch, a KMS round trip -- and the Worker awaits them on its own
    loop before a record is buffered, exactly as it does for every other payload
    an activation carries. What is left here is the third, which needs the
    topic's type and performs no I/O.

    Asserted structurally as well as behaviourally, because the property is
    structural: a coroutine on this path is a suspension point inside
    ``activate()``, and a suspension point is where I/O, a synthesized Workflow
    command, or a deadlock timeout can appear.
    """
    codec = StreamPayloadCodec(temporalio.converter.DataConverter.default, str)
    subscription = external_stream.topic(
        "tokens", backend="tokens-redis", type=str
    ).subscribe()
    runtime.buffers[subscription.wait_id] = [
        StreamRecord(RecordKind.DATA, await codec.encode("a"), "s", 0)
    ]

    assert not inspect.iscoroutinefunction(ExternalStreamSubscription._decode), (
        "decoding a record awaits on the Workflow thread, so a payload codec "
        "or an external payload fetch runs inside activate()"
    )

    record = runtime.buffers[subscription.wait_id][0]
    subscription._fill()
    assert subscription._decode(record) == "a"


@pytest.mark.asyncio
async def test_a_cancelled_delivery_leaves_the_record_unconsumed(
    runtime: FakeRuntime,
) -> None:
    """Consumption is a claim about what Workflow code received.

    Decoding no longer suspends, so the one place a cancellation can still land
    between a record arriving and Workflow code receiving it is the readiness
    wait. A cancellation there -- a stream raced against a timer, most often --
    must leave the record exactly where a later pass finds it: recording
    consumption for a value nothing yielded makes the claim false in the case
    that matters, because the consumption cursor is what a Continue-As-New
    successor resumes from.
    """
    codec = StreamPayloadCodec(temporalio.converter.DataConverter.default, str)
    subscription = external_stream.topic(
        "tokens", backend="tokens-redis", type=str
    ).subscribe()

    iterator = subscription.__aiter__()
    pending = asyncio.ensure_future(iterator.__anext__())
    # Blocked: nothing is buffered yet, and only Core can say when to look.
    await asyncio.sleep(0.05)
    assert not pending.done()

    # Buffered while the wait is outstanding, then cancelled before the
    # readiness that would have delivered it.
    runtime.buffers[subscription.wait_id] = [
        StreamRecord(RecordKind.DATA, await codec.encode("a"), "s", 0)
    ]
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending

    assert runtime.consumed == [], (
        "a cancelled wait recorded a consumption, so the cursor claims the "
        "Workflow received a record it never saw"
    )

    # And it is still there to be taken.
    assert await asyncio.wait_for(subscription.__aiter__().__anext__(), 1) == "a"
    assert [wait_id for wait_id, _ in runtime.consumed] == [subscription.wait_id]


@pytest.mark.asyncio
async def test_a_failed_decode_leaves_the_record_unconsumed(
    runtime: FakeRuntime,
) -> None:
    """The same ordering, reached by the failure a converter mismatch produces.

    The budget is spent on the record either way; the question is whether the
    Run also records having handed it over.
    """
    codec = StreamPayloadCodec(temporalio.converter.DataConverter.default, str)
    runtime.codec = FailingCodec()
    subscription = external_stream.topic(
        "tokens", backend="tokens-redis", type=str
    ).subscribe()
    runtime.buffers[subscription.wait_id] = [
        StreamRecord(RecordKind.DATA, await codec.encode("a"), "s", 0)
    ]

    with pytest.raises(StreamDecodeError, match="could not be decoded"):
        await subscription.__aiter__().__anext__()

    assert runtime.consumed == [], (
        "a record whose decode raised was marked consumed, so the value is lost "
        "and the cursor claims the Workflow received it"
    )


@pytest.mark.asyncio
async def test_a_preparation_failure_is_raised_where_the_record_would_arrive(
    runtime: FakeRuntime,
) -> None:
    """A codec that fails on the Worker's loop still fails *this* Workflow.

    Preparation happens in the watcher, which has no Workflow Task to fail and
    no Workflow to tell -- and which may be preparing a record the Workflow
    never asks for. So the failure travels with the record and is raised by the
    delivery that would have yielded its value, as the taxonomy's third row:
    the bytes are intact and the consumer's converter cannot read them.

    Raised, and *not* consumed: nothing was received, so the cursor must not say
    otherwise.
    """
    codec = StreamPayloadCodec(temporalio.converter.DataConverter.default, str)
    subscription = external_stream.topic(
        "tokens", backend="tokens-redis", type=str
    ).subscribe()
    runtime.buffers[subscription.wait_id] = [
        PreparedRecord.of(
            StreamRecord(RecordKind.DATA, await codec.encode("a"), "s", 0),
            None,
            RuntimeError("the codec could not decrypt this payload"),
        )
    ]

    with pytest.raises(StreamDecodeError, match="could not be decoded") as caught:
        await subscription.__aiter__().__anext__()
    assert isinstance(caught.value.__cause__, RuntimeError)

    assert runtime.consumed == [], (
        "a record whose preparation failed was marked consumed, so the cursor "
        "claims the Workflow received a value that never existed"
    )


@pytest.mark.asyncio
async def test_an_unprepared_record_is_refused_rather_than_decoded_late(
    runtime: FakeRuntime,
) -> None:
    """A codec-bearing converter has an asynchronous half that must have run.

    If a record reaches the Workflow thread without it, running it here is the
    defect this split exists to remove, and converting the raw bytes anyway
    would hand Workflow code whatever the codec's output happens to look like.
    Refused instead -- and refused as a decode failure, because that is what the
    Workflow can be told.
    """

    class NeverDecodes(temporalio.converter.PayloadCodec):
        async def encode(self, payloads: Any) -> Any:
            return list(payloads)

        async def decode(self, payloads: Any) -> Any:
            raise AssertionError("the Workflow thread ran a payload codec")

    plain = StreamPayloadCodec(temporalio.converter.DataConverter.default, str)
    runtime.codec = StreamPayloadCodec(
        temporalio.converter.DataConverter(payload_codec=NeverDecodes()), str
    )
    subscription = external_stream.topic(
        "tokens", backend="tokens-redis", type=str
    ).subscribe()
    runtime.buffers[subscription.wait_id] = [
        StreamRecord(RecordKind.DATA, await plain.encode("a"), "s", 0)
    ]

    with pytest.raises(StreamDecodeError, match="could not be decoded"):
        await subscription.__aiter__().__anext__()
    assert runtime.consumed == []
