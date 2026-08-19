"""P14 — the producer wake-signal path.

The wakeup used whenever no open Workflow Task can accept local readiness. Two of
its properties are the reason it does not reuse the public Signal API, and both
are asserted here rather than assumed: the envelope bypasses the user's
``DataConverter``, and the request ID is a pure function of the wake's identity.
"""

from __future__ import annotations

import asyncio
import pathlib
import re
import uuid
from datetime import timedelta

import pytest

import temporalio.api.common.v1
import temporalio.bridge
import temporalio.converter
from temporalio import workflow
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.bridge.proto.external_stream.external_stream_pb2 import WakeSignal
from temporalio.contrib.external_workflow_streams._backend import ParkIntent, StreamKey
from temporalio.contrib.external_workflow_streams._producer import (
    ExternalStreamProducer,
    WakeNotAcknowledgedError,
    WorkflowChainKey,
)
from temporalio.contrib.external_workflow_streams._record import (
    BEGINNING,
    RecordKind,
)
from temporalio.contrib.external_workflow_streams._wake import (
    UNPARKED_WAKE_GENERATION,
    send_wake_signal,
    WAKE_SIGNAL_ENCODING,
    WAKE_SIGNAL_ENVELOPE_VERSION,
    WAKE_SIGNAL_MESSAGE_TYPE,
    WAKE_SIGNAL_NAME,
    WakeRequest,
    build_signal_request,
    wake_request_id,
)
from tests.contrib.external_workflow_streams.memory_backend import MemoryStreamBackend

CHAIN = WorkflowChainKey("ns", "wf-1", "first-run-1")


def request(**overrides) -> WakeRequest:  # type: ignore[no-untyped-def]
    fields = dict(
        namespace="ns",
        workflow_id="wf-1",
        first_execution_run_id="first-run-1",
        stream_name="tokens",
        wait_id=1,
        park_generation=7,
    )
    fields.update(overrides)
    return WakeRequest(**fields)  # type: ignore[arg-type]


# --- the stable request ID ----------------------------------------------------


def test_two_producers_retrying_one_wake_derive_the_identical_request_id() -> None:
    """Which is what lets the server deduplicate them into a single wake.

    A generation is woken once. Producers racing to wake it must not each
    produce a Workflow Task, and the public Signal path -- which draws a fresh
    UUID per attempt -- would give exactly that.
    """
    first_producer = request()
    second_producer = request()

    assert wake_request_id(first_producer) == wake_request_id(second_producer)


def test_the_request_id_is_stable_across_processes() -> None:
    """Pinned, because a derivation that drifted would silently double every wake.

    Hashing anything process-local -- ``hash()``, an object id, a random seed --
    would still pass a same-process equality test and fail in production.
    """
    assert wake_request_id(request()) == "f5d6b411-3047-5b8e-a755-46cb74ad0ed8"


@pytest.mark.parametrize(
    "field",
    ["namespace", "workflow_id", "first_execution_run_id", "stream_name"],
)
def test_every_identity_field_changes_the_request_id(field: str) -> None:
    """Otherwise two different wakes would deduplicate into one, losing one."""
    assert wake_request_id(request()) != wake_request_id(request(**{field: "other"}))


def test_the_wait_id_and_generation_change_the_request_id() -> None:
    assert wake_request_id(request()) != wake_request_id(request(wait_id=2))
    assert wake_request_id(request()) != wake_request_id(request(park_generation=8))


def test_fields_cannot_be_confused_with_each_other() -> None:
    """A stream named with a separator must not collide with a different tuple.

    Delimiter-joining the identity would make ``("a", "b")`` and ``("a:b", "")``
    the same wake.
    """
    left = request(workflow_id="a", stream_name="b")
    right = request(workflow_id="a\x00b", stream_name="")

    assert wake_request_id(left) != wake_request_id(right)


# --- unparked wakes -----------------------------------------------------------


def test_two_unparked_wakes_from_different_senders_differ() -> None:
    """Generation 0 carries no attempt identity, so the sender must supply one.

    Without this, two Workers shutting down at different times would derive the
    same request ID and the server would deduplicate the second wake away --
    turning a correct retry mechanism into silent loss.
    """
    first = request(park_generation=0, sender_identity="worker-a", wake_counter=1)
    second = request(park_generation=0, sender_identity="worker-b", wake_counter=1)

    assert wake_request_id(first) != wake_request_id(second)


def test_one_senders_successive_unparked_wakes_differ() -> None:
    """They are two separate asks, not a retry of one."""
    first = request(park_generation=0, sender_identity="worker-a", wake_counter=1)
    second = request(park_generation=0, sender_identity="worker-a", wake_counter=2)

    assert wake_request_id(first) != wake_request_id(second)


def test_retrying_one_unparked_wake_keeps_its_request_id() -> None:
    """The counter is per attempt, not per send -- that is what makes it a retry."""
    attempt = request(park_generation=0, sender_identity="worker-a", wake_counter=3)

    assert wake_request_id(attempt) == wake_request_id(
        request(park_generation=0, sender_identity="worker-a", wake_counter=3)
    )


def test_an_unparked_wake_without_a_sender_identity_is_refused() -> None:
    """Refused at construction rather than producing a colliding request ID."""
    with pytest.raises(ValueError, match="sender identity"):
        request(park_generation=UNPARKED_WAKE_GENERATION)


def test_a_parked_wake_needs_no_sender_identity() -> None:
    """Its generation already identifies the attempt, for every sender alike."""
    assert wake_request_id(request(park_generation=7)) == wake_request_id(
        request(park_generation=7, sender_identity="", wake_counter=0)
    )


# --- the Worker's sender identity ---------------------------------------------


class _StubBackend:
    """Reports no confirmed park, so every wake here is an unparked one."""

    async def current_park_generation(self, stream_key, wait_id):  # type: ignore[no-untyped-def]
        return None


class _StubSubscription:
    def __init__(self, wakes_owed: int = 1) -> None:
        self.stream_key = StreamKey("ns", "wf-1", "first-run-1", "tokens")
        self.wait_id = 1
        self.wakes_owed = wakes_owed
        self.backend = _StubBackend()


def _worker_sending_wakes(client_identity: str):  # type: ignore[no-untyped-def]
    """One Worker's real wake sender, wired to a manager and nothing else.

    Built around the actual ``_WorkflowWorker`` method rather than a
    reimplementation of it, because the defect this guards was in *which*
    identity that method passed, not in the derivation it passed it to.
    """
    from temporalio.contrib.external_workflow_streams._manager import (
        StreamSubscriptionManager,
    )
    from temporalio.worker._workflow import _WorkflowWorker

    async def notify_ready(run_id: str, wait_id: int, generation: int) -> str:
        raise AssertionError("readiness is not part of this path")

    worker = object.__new__(_WorkflowWorker)
    worker._client = object()  # only ever handed to the patched sender
    worker._external_stream_manager = StreamSubscriptionManager(
        backends={},
        notify_ready=notify_ready,
        client_identity=client_identity,
    )
    return worker


@pytest.fixture
def sent_request_ids(monkeypatch):  # type: ignore[no-untyped-def]
    """Records the request ID each wake would actually be sent under."""
    import temporalio.contrib.external_workflow_streams._wake as wake_module

    recorded: list[str] = []

    async def fake_send(client, wake_request, *, producer_session_id: str = "") -> str:
        request_id = wake_request_id(wake_request)
        recorded.append(request_id)
        return request_id

    monkeypatch.setattr(wake_module, "send_wake_signal", fake_send)
    return recorded


@pytest.mark.asyncio
async def test_two_workers_sharing_one_client_derive_different_request_ids(
    sent_request_ids: list[str],
) -> None:
    """The client identity is shared; the sender identity must not be.

    Two Workers in one process share a ``Client`` and so share its identity, and
    each one's counter restarts at 1 -- so deriving from the client identity
    gives both first unparked wakes the same request ID. The server
    deduplicates the second, no Workflow Task is created, and the Run the
    surviving Worker picked up stalls.
    """
    first = _worker_sending_wakes("one-shared-client")
    second = _worker_sending_wakes("one-shared-client")

    await first._send_external_stream_wake(_StubSubscription(wakes_owed=1))
    await second._send_external_stream_wake(_StubSubscription(wakes_owed=1))

    assert sent_request_ids[0] != sent_request_ids[1], (
        "both Workers derived the same request ID, so the server would "
        "deduplicate the second wake away and the Run would never be woken"
    )


@pytest.mark.asyncio
async def test_one_workers_retry_of_an_unparked_wake_keeps_its_request_id(
    sent_request_ids: list[str],
) -> None:
    """The identity is fixed for the Worker's lifetime, which is what makes it a retry.

    The shutdown sweep re-sends one owed wake within its grace period. If the
    sender identity were redrawn per attempt the retry would ask for a second
    Workflow Task instead of resolving the attempt that may in fact have
    arrived.
    """
    worker = _worker_sending_wakes("one-shared-client")
    subscription = _StubSubscription(wakes_owed=1)

    await worker._send_external_stream_wake(subscription)
    await worker._send_external_stream_wake(subscription)

    assert sent_request_ids[0] == sent_request_ids[1], (
        "the retry derived a fresh request ID, so it would wake the Workflow a "
        "second time rather than deduplicate against the first attempt"
    )


@pytest.mark.asyncio
async def test_a_workers_sender_identity_still_names_its_client() -> None:
    """So a request ID stays traceable to a client in server-side logs.

    The per-instance part is what makes two Workers distinct; the client
    identity is what makes either of them identifiable afterwards.
    """
    worker = _worker_sending_wakes("client-identity-here")

    assert worker._external_stream_manager.wake_sender_identity.startswith(
        "client-identity-here"
    )


# --- the envelope -------------------------------------------------------------


def test_the_signal_bypasses_the_user_data_converter() -> None:
    """Core must read this Signal, and Core has no access to a user codec.

    Asserted structurally: the payload's encoding is the protocol's own, and its
    bytes parse as the protobuf envelope. A ``DataConverter`` that encrypted
    payloads would make it unreadable to the only reader that matters.
    """
    signal = build_signal_request(request(), identity="producer-1")

    (payload,) = signal.input.payloads
    assert payload.metadata["encoding"] == WAKE_SIGNAL_ENCODING
    assert payload.metadata["messageType"].decode() == WAKE_SIGNAL_MESSAGE_TYPE

    envelope = WakeSignal()
    envelope.ParseFromString(payload.data)
    assert envelope.envelope_version == WAKE_SIGNAL_ENVELOPE_VERSION
    assert envelope.stream_name == "tokens"
    assert envelope.wait_id == 1
    assert envelope.park_generation == 7
    assert envelope.first_execution_run_id == "first-run-1"


def test_the_signal_name_is_the_reserved_one_and_not_the_other_features() -> None:
    """``workflow_streams`` is a different feature that coexists with this one."""
    signal = build_signal_request(request(), identity="producer-1")

    assert signal.signal_name == WAKE_SIGNAL_NAME == "__temporal_external_stream_wake"
    assert not signal.signal_name.startswith("__temporal_workflow_stream")


def test_the_signal_carries_no_run_id() -> None:
    """So it always lands on the current Run of the chain.

    A wake sent while a Continue-As-New is in flight must reach the successor,
    not fail against a Run that has already closed. Chain identity travels inside
    the envelope instead, which is what lets Core reject a reused Workflow ID.
    """
    signal = build_signal_request(request(), identity="producer-1")

    assert signal.workflow_execution.workflow_id == "wf-1"
    assert signal.workflow_execution.run_id == ""


def test_the_envelope_carries_no_stream_payload() -> None:
    """The record's bytes stay in the backend; the Signal is pure identity."""
    signal = build_signal_request(
        request(), identity="producer-1", producer_session_id="session-1"
    )

    raw = signal.SerializeToString()
    envelope = WakeSignal()
    envelope.ParseFromString(signal.input.payloads[0].data)
    assert set(envelope.DESCRIPTOR.fields_by_name) == {
        "envelope_version",
        "stream_name",
        "wait_id",
        "park_generation",
        "first_execution_run_id",
        "producer_session_id",
    }, "a new envelope field must be reviewed for whether it can carry user data"
    assert b"session-1" in raw  # diagnostics only, and the only free-form string


# --- the producer send sequence -----------------------------------------------


class RecordingClient:
    """Captures the raw service call without a server.

    The wake path is a raw ``SignalWorkflowExecution``, so what needs asserting
    is the request itself -- not that some client method was reached.
    """

    def __init__(self, fail: bool = False) -> None:
        self.sent: list = []
        self.fail = fail
        self.namespace = "ns"
        self.data_converter = temporalio.converter.DataConverter.default
        self.service_client = self
        self.config = self
        self.identity = "producer-identity"
        self.workflow_service = self

    async def signal_workflow_execution(self, request) -> None:  # type: ignore[no-untyped-def]
        if self.fail:
            raise ConnectionError("service unavailable")
        self.sent.append(request)


def make_producer(backend, client) -> ExternalStreamProducer:  # type: ignore[no-untyped-def]
    return ExternalStreamProducer(
        backend=backend,
        workflow=CHAIN,
        data_converter=temporalio.converter.DataConverter.default,
        session_id="session-1",
        client=client,  # type: ignore[arg-type]
    )


async def park(backend: MemoryStreamBackend, key: StreamKey, wait_id: int, gen: int):
    await backend.install_park_intent(
        key,
        ParkIntent(
            wait_id=wait_id, cursor=BEGINNING, park_generation=gen, run_id="run-1"
        ),
    )


@pytest.mark.asyncio
async def test_a_producer_that_finds_a_wakeable_generation_sends_exactly_one_signal() -> (
    None
):
    backend = MemoryStreamBackend()
    client = RecordingClient()
    topic = make_producer(backend, client).topic("tokens", type=str)
    await park(backend, topic.stream_key, wait_id=1, gen=4)

    await topic.publish("a", wake=False)
    sent = await topic.wake()

    assert len(sent) == 1
    assert len(client.sent) == 1
    envelope = WakeSignal()
    envelope.ParseFromString(client.sent[0].input.payloads[0].data)
    assert envelope.park_generation == 4
    assert envelope.wait_id == 1


@pytest.mark.asyncio
async def test_a_producer_that_loses_the_claim_sends_the_same_wake_anyway() -> None:
    """A lost claim says someone *intends* to send. It is not evidence one did.

    The Signal goes out either way, and it costs no second Workflow Task: a
    parked wake's request ID is derived from the generation and ignores sender
    identity, so both producers issue byte-identical requests that the server
    deduplicates into one wake. Staying silent here is what strands a parked
    Workflow when the claim holder crashed between claiming and signalling.
    """
    backend = MemoryStreamBackend()
    key = StreamKey("ns", "wf-1", "first-run-1", "tokens")
    await park(backend, key, wait_id=1, gen=4)
    await backend.claim_park_generation(
        key, 1, 4, claimant="someone-else", lease=timedelta(seconds=30)
    )

    client = RecordingClient()
    topic = make_producer(backend, client).topic("tokens", type=str)
    await topic.publish("a", wake=False)

    assert len(await topic.wake()) == 1
    assert len(client.sent) == 1
    assert client.sent[0].request_id == wake_request_id(request(park_generation=4)), (
        "the claim holder's wake and this one must be the same request, or the "
        "duplicate the claim exists to avoid becomes a second Workflow Task"
    )


@pytest.mark.asyncio
async def test_an_expired_claim_is_taken_over_rather_than_stranding_the_wake() -> None:
    """A producer crashing between claim and Signal must not park the Workflow forever.

    Without lease expiry every other producer concludes the wake is already
    handled, and the Workflow waits with data sitting in the stream.
    """
    backend = MemoryStreamBackend()
    key = StreamKey("ns", "wf-1", "first-run-1", "tokens")
    await park(backend, key, wait_id=1, gen=4)
    # The crashed producer's claim, already expired.
    await backend.claim_park_generation(
        key, 1, 4, claimant="crashed", lease=timedelta(milliseconds=1)
    )
    await backend.expire_claims_for_test()

    client = RecordingClient()
    topic = make_producer(backend, client).topic("tokens", type=str)
    await topic.publish("a", wake=False)

    assert len(await topic.wake()) == 1, "an expired claim must be takeable"


@pytest.mark.asyncio
async def test_a_stream_with_no_park_intent_gets_an_unparked_wake() -> None:
    """Cached-with-no-open-task and evicted are invisible from out here (ADR-023).

    Staying silent because no park was observed loses the record until something
    else happens to wake the Workflow.
    """
    backend = MemoryStreamBackend()
    client = RecordingClient()
    topic = make_producer(backend, client).topic("tokens", type=str)

    await topic.publish("a", wake=False)
    assert len(await topic.wake()) == 1

    envelope = WakeSignal()
    envelope.ParseFromString(client.sent[0].input.payloads[0].data)
    assert envelope.park_generation == UNPARKED_WAKE_GENERATION


@pytest.mark.asyncio
async def test_two_subscriptions_to_one_stream_are_woken_independently() -> None:
    """They are two waits with their own cursors, generations, and claims.

    Waking only one leaves the other parked on a record it can already see.
    """
    backend = MemoryStreamBackend()
    client = RecordingClient()
    topic = make_producer(backend, client).topic("tokens", type=str)
    await park(backend, topic.stream_key, wait_id=1, gen=4)
    await park(backend, topic.stream_key, wait_id=2, gen=4)

    await topic.publish("a", wake=False)
    await topic.wake()

    woken = []
    for signal in client.sent:
        envelope = WakeSignal()
        envelope.ParseFromString(signal.input.payloads[0].data)
        woken.append(envelope.wait_id)
    assert sorted(woken) == [1, 2]


@pytest.mark.asyncio
async def test_a_failed_wake_reports_unacknowledged_and_the_retry_completes_it() -> (
    None
):
    """The record is durable, the wake is not, and only one of them may be retried.

    Reporting success here would make the "durable producer" row of the
    wakeup-durability boundary false.
    """
    backend = MemoryStreamBackend()
    client = RecordingClient(fail=True)
    topic = make_producer(backend, client).topic("tokens", type=str)
    await park(backend, topic.stream_key, wait_id=1, gen=4)
    offset = await topic.publish("a", wake=False)

    with pytest.raises(WakeNotAcknowledgedError) as caught:
        await topic.wake()

    assert caught.value.pending, "the retry must know what is still owed"

    # The record stayed put: the append is not what failed.
    assert await backend.read_range(topic.stream_key, offset, offset)

    client.fail = False
    assert len(await topic.retry_wake(caught.value.pending)) == 1


@pytest.mark.asyncio
async def test_a_retried_wake_reuses_the_original_request_id() -> None:
    """Recomputing it would draw a fresh counter and defeat the deduplication."""
    backend = MemoryStreamBackend()
    client = RecordingClient(fail=True)
    topic = make_producer(backend, client).topic("tokens", type=str)
    await topic.publish("a", wake=False)

    with pytest.raises(WakeNotAcknowledgedError) as caught:
        await topic.wake()
    expected = wake_request_id(caught.value.pending[0])

    client.fail = False
    assert await topic.retry_wake(caught.value.pending) == [expected]


class FlakyCoordinationBackend(MemoryStreamBackend):
    """A backend whose append works and whose coordination calls do not.

    One named method fails once. Appending keeps working throughout, which is the
    whole point: the record is durable before the failure happens, so what the
    caller has to be told is which half of ``publish()`` failed.
    """

    def __init__(self, failing: str) -> None:
        super().__init__()
        self.failing = failing
        self.failures = 0

    def _maybe_fail(self, name: str) -> None:
        if name == self.failing:
            self.failing = ""
            self.failures += 1
            raise ConnectionError(f"{name} is unavailable")

    async def parked_wait_ids(self, key):  # type: ignore[no-untyped-def]
        self._maybe_fail("parked_wait_ids")
        return await super().parked_wait_ids(key)

    async def current_park_generation(self, key, wait_id):  # type: ignore[no-untyped-def]
        self._maybe_fail("current_park_generation")
        return await super().current_park_generation(key, wait_id)

    async def claim_park_generation(self, key, wait_id, generation, **kwargs):  # type: ignore[no-untyped-def]
        self._maybe_fail("claim_park_generation")
        return await super().claim_park_generation(key, wait_id, generation, **kwargs)


@pytest.mark.parametrize(
    "failing",
    ["parked_wait_ids", "current_park_generation", "claim_park_generation"],
)
@pytest.mark.asyncio
async def test_a_coordination_failure_after_the_append_is_still_unacknowledged(
    failing: str,
) -> None:
    """The wake is three steps, and all three are after a durable append.

    Only the Signal used to be inside the guarantee. The observe and claim steps
    raised whatever the provider raised -- a bare ``ConnectionError`` -- which
    passed straight through ``publish()``, because ``publish()`` catches only the
    durable-but-unacknowledged error. The caller then had neither the offset nor
    any statement about what had already landed, and its only obvious move --
    retry ``publish()`` -- appends the record a *second* time, under a new
    sequence number and therefore a new idempotency key.
    """
    backend = FlakyCoordinationBackend(failing)
    client = RecordingClient()
    topic = make_producer(backend, client).topic("tokens", type=str)
    await park(backend, topic.stream_key, wait_id=1, gen=4)

    with pytest.raises(WakeNotAcknowledgedError) as caught:
        await topic.publish("a")

    assert backend.failures == 1, "the test did not exercise the failure it names"
    assert caught.value.offset is not None, (
        "the caller has no other way to learn that the append it must not retry "
        "did succeed"
    )
    assert caught.value.restart, (
        "no Signal was composed, so `pending` is empty and only a fresh wake() "
        "recovers -- a caller told to retry_wake([]) would get a no-op that "
        "looks like success"
    )
    assert not caught.value.pending
    with pytest.raises(ValueError, match="call wake\(\) again"):
        await topic.retry_wake(caught.value.pending)

    # Recovering the wake alone leaves exactly one record in the stream.
    assert await topic.wake()
    records = await backend.read_after(topic.stream_key, BEGINNING, max_records=10)
    assert [r.sequence for r in records if r.kind == RecordKind.DATA] == [0], (
        "the stream holds more than the one record that was published"
    )


@pytest.mark.asyncio
async def test_a_coordination_failure_after_a_fence_is_still_unacknowledged() -> None:
    """A fence is the record most likely to find the Workflow parked.

    Same guarantee as ``publish()``, and the duplicate it prevents is worse:
    retrying ``finish_writing()`` appends a second fence, which reads back as a
    producer session that ended twice.
    """
    backend = FlakyCoordinationBackend("parked_wait_ids")
    client = RecordingClient()
    topic = make_producer(backend, client).topic("tokens", type=str)
    await park(backend, topic.stream_key, wait_id=1, gen=4)

    with pytest.raises(WakeNotAcknowledgedError) as caught:
        await topic.finish_writing()

    assert caught.value.offset is not None
    assert caught.value.restart

    assert await topic.wake()
    records = await backend.read_after(topic.stream_key, BEGINNING, max_records=10)
    assert len([r for r in records if r.kind == RecordKind.WRITE_FENCE]) == 1, (
        "the stream holds more than the one fence that was written"
    )


@pytest.mark.asyncio
async def test_waking_without_a_client_says_so() -> None:
    backend = MemoryStreamBackend()
    producer = ExternalStreamProducer(
        backend=backend,
        workflow=CHAIN,
        data_converter=temporalio.converter.DataConverter.default,
        session_id="session-1",
    )

    with pytest.raises(RuntimeError, match="requires a Temporal client"):
        await producer.topic("tokens", type=str).wake()


# --- the two halves of the envelope live in different repos -------------------


def test_the_python_constants_match_cores() -> None:
    """Core is the reader; a drifted constant here is an unreadable Signal there.

    Nothing else would catch it: Python's own tests would still pass, Core's own
    tests would still pass, and the wake would simply stop working -- appearing
    as a Workflow that waits out its idle timeout for no visible reason.
    """
    core = (
        pathlib.Path(temporalio.bridge.__file__).parent
        / "sdk-core"
        / "crates"
        / "protos"
        / "src"
        / "protos"
        / "mod.rs"
    )
    if not core.exists():
        pytest.skip("the Core submodule is not checked out")
    source = core.read_text()

    for name, value in (
        ("WAKE_SIGNAL_NAME", f'"{WAKE_SIGNAL_NAME}"'),
        ("WAKE_SIGNAL_MESSAGE_TYPE", f'"{WAKE_SIGNAL_MESSAGE_TYPE}"'),
        ("WAKE_SIGNAL_ENVELOPE_VERSION", str(WAKE_SIGNAL_ENVELOPE_VERSION)),
        ("UNPARKED_WAKE_GENERATION", str(UNPARKED_WAKE_GENERATION)),
    ):
        assert re.search(rf"{name}:\s*\S+\s*=\s*{re.escape(value)}\s*;", source), (
            f"{name} is {value} in Python but Core declares something else"
        )


# --- P6b: publish()'s acknowledged-wake contract ------------------------------


@pytest.mark.asyncio
async def test_publish_completes_only_once_its_wake_is_acknowledged() -> None:
    """Returning means durable *and* signalled, which is the whole contract.

    An append that lands but is never signalled leaves the Workflow parked on
    data already sitting in the stream -- and a publish() that returned success
    there would be reporting a delivery that never happened.
    """
    backend = MemoryStreamBackend()
    client = RecordingClient()
    topic = make_producer(backend, client).topic("tokens", type=str)
    await park(backend, topic.stream_key, wait_id=1, gen=4)

    offset = await topic.publish("a")

    assert offset is not None
    assert len(client.sent) == 1, "publish must not return before it has signalled"


@pytest.mark.asyncio
async def test_publish_acknowledges_the_wake_even_when_another_producer_claimed_it() -> (
    None
):
    """A claim held by a producer that crashed must not report a wake nobody sent.

    The lease permits takeover *after* it expires; it schedules nobody to take
    over. If this publish is the last producer action -- and here it is the only
    one -- a claim-based silence leaves the Workflow parked on a durable record
    with a ``publish()`` that reported success.
    """
    backend = MemoryStreamBackend()
    key = StreamKey("ns", "wf-1", "first-run-1", "tokens")
    await park(backend, key, wait_id=1, gen=4)
    # A producer that claimed the generation and then died before signalling.
    # Its lease is unexpired, so the claim is not takeable.
    await backend.claim_park_generation(
        key, 1, 4, claimant="crashed", lease=timedelta(seconds=30)
    )

    client = RecordingClient()
    topic = make_producer(backend, client).topic("tokens", type=str)

    offset = await topic.publish("a")

    assert offset is not None
    assert len(client.sent) == 1, (
        "publish returned an acknowledged wake that nobody sent; the parked "
        "Workflow is stranded on a durable record"
    )
    envelope = WakeSignal()
    envelope.ParseFromString(client.sent[0].input.payloads[0].data)
    assert envelope.park_generation == 4
    assert client.sent[0].request_id == wake_request_id(request(park_generation=4)), (
        "the wake the crashed claimant owed and this one are the same request, "
        "so the server collapses them rather than creating a second task"
    )


@pytest.mark.asyncio
async def test_a_publish_whose_wake_fails_reports_unacknowledged() -> None:
    """Not success. This is the "durable producer" row of the boundary table."""
    backend = MemoryStreamBackend()
    client = RecordingClient(fail=True)
    topic = make_producer(backend, client).topic("tokens", type=str)
    await park(backend, topic.stream_key, wait_id=1, gen=4)

    with pytest.raises(WakeNotAcknowledgedError) as caught:
        await topic.publish("a")

    assert caught.value.offset is not None, (
        "the caller has no other way to learn that the append it must not retry "
        "did succeed"
    )
    assert caught.value.pending


@pytest.mark.asyncio
async def test_retrying_the_wake_completes_an_unacknowledged_publish() -> None:
    """And appends nothing: only the half that failed is retried."""
    backend = MemoryStreamBackend()
    client = RecordingClient(fail=True)
    topic = make_producer(backend, client).topic("tokens", type=str)
    await park(backend, topic.stream_key, wait_id=1, gen=4)

    with pytest.raises(WakeNotAcknowledgedError) as caught:
        await topic.publish("a")
    before = await backend.read_after(
        topic.stream_key, BEGINNING, max_records=100, block=None
    )

    client.fail = False
    assert len(await topic.retry_wake(caught.value.pending)) == 1

    after = await backend.read_after(
        topic.stream_key, BEGINNING, max_records=100, block=None
    )
    assert [r.offset for r in after] == [r.offset for r in before], (
        "the retry must complete the wake without re-appending the record"
    )


@pytest.mark.asyncio
async def test_the_unacknowledged_state_is_explicit_not_hidden() -> None:
    """``wake=False`` says "durable but un-signalled" in the call itself.

    The point is that a caller cannot end up in that state by accident -- it is
    reachable only by asking for it, or by a wake that failed loudly.
    """
    backend = MemoryStreamBackend()
    client = RecordingClient()
    topic = make_producer(backend, client).topic("tokens", type=str)
    await park(backend, topic.stream_key, wait_id=1, gen=4)

    await topic.publish("a", wake=False)
    assert client.sent == []

    await topic.wake()
    assert len(client.sent) == 1


@pytest.mark.asyncio
async def test_a_fence_is_signalled_too() -> None:
    """It is the record most likely to find the Workflow parked.

    A fence is what a producer appends when it has nothing more to say, so an
    unsignalled one strands the Workflow for its whole idle timeout at exactly
    the moment it was waiting to be told.
    """
    backend = MemoryStreamBackend()
    client = RecordingClient()
    topic = make_producer(backend, client).topic("tokens", type=str)
    await park(backend, topic.stream_key, wait_id=1, gen=4)

    await topic.finish_writing()

    assert len(client.sent) == 1


# --- deduplication, against a real server -------------------------------------


@workflow.defn
class WaitForeverWorkflow:
    """Exists to receive Signals. Never completes on its own."""

    @workflow.run
    async def run(self) -> None:
        await asyncio.Future()


async def _signalled_events(handle) -> int:  # type: ignore[no-untyped-def]
    return len(
        [
            e
            async for e in handle.fetch_history_events()
            if e.HasField("workflow_execution_signaled_event_attributes")
        ]
    )


async def _settled_signal_count(handle, expected: int) -> int:  # type: ignore[no-untyped-def]
    """Waits for the count to reach ``expected`` and then stay there.

    Counting once races persistence in both directions: too early and a wake
    that *was* recorded is missed, too eager and a duplicate that is about to
    appear is not. Waiting for the count to reach the expected value and hold
    across a further interval distinguishes "the server collapsed them" from
    "the second one has not landed yet".
    """
    for _ in range(40):
        if await _signalled_events(handle) >= expected:
            break
        await asyncio.sleep(0.1)
    await asyncio.sleep(1)
    return await _signalled_events(handle)


async def _start_waiter(client: Client, task_queue: str):  # type: ignore[no-untyped-def]
    handle = await client.start_workflow(
        WaitForeverWorkflow.run,
        id=f"wf-{uuid.uuid4()}",
        task_queue=task_queue,
    )
    description = await handle.describe()
    return handle, description.raw_description.workflow_execution_info.first_run_id


async def test_two_producers_retrying_one_wake_are_deduplicated_by_the_server(
    client: Client,
) -> None:
    """ID equality is only half the claim; this is the half that matters.

    Two producers racing to wake one generation must cost one Workflow Task, not
    two. Asserting that they derive the same string proves nothing about what
    the server does with it -- and the server is the component that has to
    collapse them.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(client, task_queue=task_queue, workflows=[WaitForeverWorkflow]):
        handle, first_run = await _start_waiter(client, task_queue)
        try:
            wake = request(
                namespace=client.namespace,
                workflow_id=handle.id,
                first_execution_run_id=first_run,
                park_generation=4,
            )

            # Two producers, one generation, sent independently.
            for _ in range(2):
                await send_wake_signal(client, wake)

            assert await _settled_signal_count(handle, 1) == 1, (
                "the server recorded both wakes; a producer retrying after an "
                "ambiguous failure would then cost a second Workflow Task"
            )
        finally:
            await handle.terminate()


async def test_two_workers_unparked_wakes_are_both_delivered(
    client: Client,
) -> None:
    """The opposite requirement, and why the counter exists.

    Two Workers shutting down at different times are two separate asks. If their
    request IDs collided the server would deduplicate the second away -- turning
    a correct retry mechanism into silent loss, which is the failure the sender
    identity and counter are there to prevent.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(client, task_queue=task_queue, workflows=[WaitForeverWorkflow]):
        handle, first_run = await _start_waiter(client, task_queue)
        try:
            for identity in ("worker-a", "worker-b"):
                await send_wake_signal(
                    client,
                    request(
                        namespace=client.namespace,
                        workflow_id=handle.id,
                        first_execution_run_id=first_run,
                        park_generation=0,
                        sender_identity=identity,
                        wake_counter=1,
                    ),
                )

            assert await _settled_signal_count(handle, 2) == 2, (
                "one of the two Workers' wakes was deduplicated away; both are "
                "separate asks and both must reach the Workflow"
            )
        finally:
            await handle.terminate()


async def test_one_senders_retry_stays_a_single_wake(client: Client) -> None:
    """The unparked case still deduplicates a genuine retry.

    The counter distinguishes *attempts*, not sends, so re-sending one attempt
    must collapse exactly as a parked wake does -- otherwise the shutdown
    sweep's retry would wake the Workflow twice.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(client, task_queue=task_queue, workflows=[WaitForeverWorkflow]):
        handle, first_run = await _start_waiter(client, task_queue)
        try:
            attempt = request(
                namespace=client.namespace,
                workflow_id=handle.id,
                first_execution_run_id=first_run,
                park_generation=0,
                sender_identity="worker-a",
                wake_counter=7,
            )
            await send_wake_signal(client, attempt)
            await send_wake_signal(client, attempt)

            assert await _settled_signal_count(handle, 1) == 1
        finally:
            await handle.terminate()
