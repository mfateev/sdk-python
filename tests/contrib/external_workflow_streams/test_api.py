"""P9 — the Workflow-facing API and its wait-id assignment."""

from __future__ import annotations

import asyncio
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
)
from temporalio.contrib.external_workflow_streams._backend import StreamKey
from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec
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
        self.buffers: dict[int, list[StreamRecord]] = {}
        self.deliveries: list[tuple[int, StreamRecord]] = []
        self.consumed: list[tuple[int, StreamRecord]] = []
        self.blocked: list[tuple[int, bool]] = []
        self.pending: dict[int, asyncio.Future[None]] = {}
        self.budget = MAX_RECORDS_PER_ACTIVATION

    def stream_key(self, stream_name: str) -> StreamKey:
        return StreamKey("ns", "wf", "first-run", stream_name)

    def register(
        self, *, wait_id: int, stream_key: StreamKey, backend_name: str
    ) -> None:
        if backend_name not in self.registered:
            raise KeyError(f"no external stream backend named {backend_name!r}")
        self.registrations.append((wait_id, stream_key, backend_name))

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
        self.budget = max(0, self.budget - 1)

    def codec_for(self, value_type: type | None) -> StreamPayloadCodec[Any]:
        return StreamPayloadCodec(
            temporalio.converter.DataConverter.default, value_type
        )

    def new_readiness_future(self) -> asyncio.Future[None]:
        return asyncio.get_event_loop().create_future()

    def record_delivery(self, wait_id: int, record: StreamRecord) -> None:
        self.deliveries.append((wait_id, record))

    def note_blocked(self, wait_id: int, blocked: bool) -> None:
        self.blocked.append((wait_id, blocked))

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
