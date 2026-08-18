"""P3/P3b — the Redis provider, against the conformance suite unmodified.

"Unmodified" is the point: if a check had to be relaxed for Redis, it would be
encoding this provider's behaviour rather than the contract, and the next
provider would inherit the exemption.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator
from datetime import timedelta

import pytest
import pytest_asyncio

from temporalio.contrib.external_workflow_streams._backend import ParkIntent, StreamKey
from temporalio.contrib.external_workflow_streams._record import (
    AFTER,
    BEGINNING,
    Offset,
    RecordKind,
    StreamRecord,
)
from temporalio.contrib.external_workflow_streams._redis import RedisStreamBackend
from tests.contrib.external_workflow_streams.conformance import (
    CONFORMANCE_CHECKS,
    PARKING_CONFORMANCE_CHECKS,
    Check,
)
from tests.contrib.external_workflow_streams.conftest import (
    KEY_NAMESPACE,
    redis_url,
)


@pytest_asyncio.fixture  # type: ignore[reportUntypedFunctionDecorator]
async def redis_backend(
    redis_worker_id: str,
) -> AsyncGenerator[RedisStreamBackend, None]:
    """A backend whose keys all sit under this test's own prefix.

    The prefix is the isolation: one Redis serves the whole suite, and two
    xdist workers running this module must not see each other's streams.
    """
    pytest.importorskip("redis.asyncio", reason="redis is not installed")
    backend = RedisStreamBackend(
        url=redis_url(),
        key_prefix=f"{KEY_NAMESPACE}:{redis_worker_id}:{uuid.uuid4().hex}",
    )
    try:
        await backend._client.ping()
    except Exception as err:
        await backend.aclose()
        pytest.skip(f"Redis is not reachable at {redis_url()}: {err}")

    try:
        yield backend
    finally:
        keys = [
            k async for k in backend._client.scan_iter(match=f"{backend._key_prefix}*")
        ]
        if keys:
            await backend._client.delete(*keys)
        await backend.aclose()


@pytest.fixture
def stream_key() -> StreamKey:
    return StreamKey("ns", "wf", uuid.uuid4().hex, "tokens")


# --- the conformance suites, unmodified -------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("check", CONFORMANCE_CHECKS, ids=lambda c: c.__name__)
async def test_redis_passes_the_core_conformance_suite(
    check: Check, redis_backend: RedisStreamBackend, stream_key: StreamKey
) -> None:
    await check(redis_backend, stream_key)


@pytest.mark.asyncio
@pytest.mark.parametrize("check", PARKING_CONFORMANCE_CHECKS, ids=lambda c: c.__name__)
async def test_redis_passes_the_parking_conformance_suite(
    check: Check, redis_backend: RedisStreamBackend, stream_key: StreamKey
) -> None:
    await check(redis_backend, stream_key)


# --- Redis specifics the suite cannot state ---------------------------------


@pytest.mark.asyncio
async def test_offsets_are_real_redis_ids(
    redis_backend: RedisStreamBackend, stream_key: StreamKey
) -> None:
    placed = await redis_backend.append(
        stream_key, StreamRecord(RecordKind.DATA, b"a", "s", 0)
    )

    assert placed.offset is not None
    ms, _, seq = placed.offset.token.partition("-")
    assert ms.isdigit() and seq.isdigit(), (
        f"a Redis offset is <ms>-<seq>, got {placed.offset.token!r}"
    )


@pytest.mark.asyncio
async def test_xrange_and_xread_disagree_exactly_as_the_contract_says(
    redis_backend: RedisStreamBackend, stream_key: StreamKey
) -> None:
    """The reason replay must not use XREAD, asserted against a real Redis.

    XRANGE includes the id it is given; XREAD starts strictly after it. A
    provider that reached for XREAD to serve the replay read would drop the
    first record of every recorded range -- and nothing before the first replay
    would notice.
    """
    first = await redis_backend.append(
        stream_key, StreamRecord(RecordKind.DATA, b"a", "s", 0)
    )
    second = await redis_backend.append(
        stream_key, StreamRecord(RecordKind.DATA, b"b", "s", 1)
    )
    assert first.offset is not None and second.offset is not None

    inclusive = await redis_backend.read_range(stream_key, first.offset, second.offset)
    exclusive = await redis_backend.read_after(
        stream_key, AFTER(first.offset), max_records=10, block=None
    )

    assert [r.payload for r in inclusive] == [b"a", b"b"]
    assert [r.payload for r in exclusive] == [b"b"]


@pytest.mark.asyncio
async def test_beginning_reads_from_the_zero_sentinel(
    redis_backend: RedisStreamBackend, stream_key: StreamKey
) -> None:
    """`BEGINNING` is a boundary; `0-0` is how Redis spells that boundary.

    They are not the same thing, which is why the cursor type keeps them apart
    -- but the provider has to know the translation.
    """
    await redis_backend.append(stream_key, StreamRecord(RecordKind.DATA, b"a", "s", 0))

    from_beginning = await redis_backend.read_after(
        stream_key, BEGINNING, max_records=10, block=None
    )
    assert [r.payload for r in from_beginning] == [b"a"]


@pytest.mark.asyncio
async def test_binary_payloads_survive_the_round_trip(
    redis_backend: RedisStreamBackend, stream_key: StreamKey
) -> None:
    """A decoding client would corrupt these, so the provider must not use one."""
    payload = bytes(range(256))
    placed = await redis_backend.append(
        stream_key, StreamRecord(RecordKind.DATA, payload, "s", 0)
    )

    assert placed.offset is not None
    (got,) = await redis_backend.read_range(stream_key, placed.offset, placed.offset)
    assert got.payload == payload


@pytest.mark.asyncio
async def test_an_idempotent_reappend_writes_nothing_to_the_stream(
    redis_backend: RedisStreamBackend, stream_key: StreamKey
) -> None:
    """Not merely "returns the same offset" -- no second entry may exist.

    Asserted at the Redis level because a provider could return the original
    offset while still having XADDed a duplicate, which replay would then find
    inside a recorded range and count as an integrity failure.
    """
    record = StreamRecord(RecordKind.DATA, b"payload", "retry", 0)
    await redis_backend.append(stream_key, record)
    await redis_backend.append(stream_key, record)

    length = await redis_backend._client.xlen(redis_backend.stream_key(stream_key))
    assert length == 1


@pytest.mark.asyncio
async def test_a_blocking_read_returns_empty_rather_than_hanging(
    redis_backend: RedisStreamBackend, stream_key: StreamKey
) -> None:
    """`XREAD BLOCK 0` blocks forever, so a zero timeout must omit BLOCK."""
    got = await redis_backend.read_after(
        stream_key, BEGINNING, max_records=10, block=timedelta(0)
    )
    assert got == []


@pytest.mark.asyncio
async def test_offsets_compare_numerically_not_lexically(
    redis_backend: RedisStreamBackend,
) -> None:
    """Real ids cross the width boundary; the comparator must not care."""
    assert redis_backend.compare_offsets(Offset("9-0"), Offset("10-0")) < 0
    assert (
        redis_backend.compare_offsets(
            Offset("1700000000000-1"), Offset("1700000000000-2")
        )
        < 0
    )
    assert redis_backend.compare_offsets(Offset("100-0"), Offset("100-0")) == 0


# --- retention loss against real Redis ---------------------------------------


@pytest.mark.asyncio
async def test_a_trimmed_range_is_integrity_loss_not_a_storage_failure(
    redis_backend: RedisStreamBackend, stream_key: StreamKey
) -> None:
    """`XTRIM MAXLEN` is how a recorded range actually disappears in production.

    Not a hypothetical: a stream with a retention policy trims its own head
    while a Workflow is still parked against it. That must read as integrity
    loss -- an operator has to restore or terminate -- and never as a transient
    storage failure, which would clear on its own and so would be retried
    forever against data that is not coming back.
    """
    from temporalio.contrib.external_workflow_streams._annotation import Run
    from temporalio.contrib.external_workflow_streams._errors import (
        StreamIntegrityError,
    )
    from temporalio.contrib.external_workflow_streams._replay import validate_run

    placed = []
    for i in range(5):
        placed.append(
            await redis_backend.append(
                stream_key, StreamRecord(RecordKind.DATA, b"x", "producer", i)
            )
        )
    run = Run(1, placed[0].offset, placed[-1].offset, 5)  # type: ignore[arg-type]

    # The head of the recorded range is trimmed away, exactly as a retention
    # policy would do it.
    await redis_backend._client.xtrim(
        redis_backend.stream_key(stream_key), maxlen=2, approximate=False
    )
    survivors = await redis_backend.read_range(
        stream_key,
        placed[0].offset,  # type: ignore[arg-type]
        placed[-1].offset,  # type: ignore[arg-type]
    )

    with pytest.raises(StreamIntegrityError) as caught:
        validate_run(run, survivors, redis_backend)

    assert "missing from the stream" in str(caught.value)


@pytest.mark.asyncio
async def test_a_deleted_write_fence_is_integrity_loss(
    redis_backend: RedisStreamBackend, stream_key: StreamKey
) -> None:
    """A control record occupies an offset inside a run, so losing one is loss.

    It is never yielded to Workflow code, which is exactly why it could be
    dismissed as harmless -- but the run's count includes it and its position is
    recorded, so a range missing it no longer reads back as written. Letting it
    pass would also shift every later control position by one.
    """
    from temporalio.contrib.external_workflow_streams._annotation import Run
    from temporalio.contrib.external_workflow_streams._errors import (
        StreamIntegrityError,
    )
    from temporalio.contrib.external_workflow_streams._replay import validate_run

    first = await redis_backend.append(
        stream_key, StreamRecord(RecordKind.DATA, b"a", "producer", 0)
    )
    fence = await redis_backend.append(
        stream_key, StreamRecord(RecordKind.WRITE_FENCE, b"", "producer", 1)
    )
    last = await redis_backend.append(
        stream_key, StreamRecord(RecordKind.DATA, b"b", "producer", 2)
    )
    run = Run(1, first.offset, last.offset, 3, control_positions=(1,))  # type: ignore[arg-type]

    await redis_backend.delete_for_test(stream_key, fence.offset)  # type: ignore[arg-type]
    survivors = await redis_backend.read_range(
        stream_key,
        first.offset,  # type: ignore[arg-type]
        last.offset,  # type: ignore[arg-type]
    )

    with pytest.raises(StreamIntegrityError) as caught:
        validate_run(run, survivors, redis_backend)

    assert "contains 2 record(s)" in str(caught.value), (
        f"a deleted fence must fail the count check, got: {caught.value}"
    )


# --- key injectivity ---------------------------------------------------------


def _colliding_identities() -> tuple[StreamKey, StreamKey]:
    """Two distinct identities a delimiter-joined key layout renders identically.

    Nothing here is exotic: a Workflow ID and a stream name are user-chosen
    strings, and `:` is an ordinary character in both.
    """
    first_run, second_run = uuid.uuid4().hex, uuid.uuid4().hex
    return (
        StreamKey("ns", "wf", first_run, f"{second_run}:tokens"),
        StreamKey("ns", f"wf:{first_run}", second_run, "tokens"),
    )


@pytest.mark.asyncio
async def test_distinct_identities_never_share_one_physical_key(
    redis_backend: RedisStreamBackend,
) -> None:
    """Every derivation, not only the stream: they all come from `stream_key`.

    Two `StreamKey`s that are not equal are two streams, and nothing they own --
    records, idempotency hashes, park intents, claims -- may land in one place.
    Two Workflows sharing one data structure is a cross-Workflow leak, and it
    reads as ordinary corruption rather than as a key-layout bug.
    """
    first, second = _colliding_identities()
    assert first != second

    for name, derive in (
        ("stream", redis_backend.stream_key),
        ("idempotency", redis_backend._idempotency_key),
        ("park intent", lambda k: redis_backend._intent_key(k, 1)),
        ("claim", lambda k: redis_backend._claim_key(k, 1)),
    ):
        assert derive(first) != derive(second), (
            f"the {name} key is not injective: {first} and {second} both "
            f"render as {derive(first)!r}"
        )


@pytest.mark.asyncio
async def test_colliding_identities_hold_isolated_records(
    redis_backend: RedisStreamBackend,
) -> None:
    """The consequence, asserted through the public operations rather than keys."""
    first, second = _colliding_identities()

    await redis_backend.append(
        first, StreamRecord(RecordKind.DATA, b"first", "producer-first", 0)
    )
    await redis_backend.append(
        second, StreamRecord(RecordKind.DATA, b"second", "producer-second", 0)
    )

    from_first = await redis_backend.read_after(
        first, BEGINNING, max_records=10, block=None
    )
    from_second = await redis_backend.read_after(
        second, BEGINNING, max_records=10, block=None
    )

    assert [r.payload for r in from_first] == [b"first"]
    assert [r.payload for r in from_second] == [b"second"]


@pytest.mark.asyncio
async def test_colliding_identities_hold_isolated_park_state(
    redis_backend: RedisStreamBackend,
) -> None:
    """An intent and a claim installed on one must be invisible on the other.

    A shared claim key is worse than a shared stream: the second Workflow's
    producer reads a claim it never made, concludes the wake is someone else's,
    and stays silent -- so the Run is never woken at all.
    """
    first, second = _colliding_identities()

    await redis_backend.install_park_intent(
        first, ParkIntent(wait_id=1, cursor=BEGINNING, park_generation=3, run_id="r")
    )

    assert await redis_backend.park_intent(second, 1) is None
    assert await redis_backend.current_park_generation(second, 1) is None
    assert await redis_backend.parked_wait_ids(second) == []
    assert await redis_backend.parked_wait_ids(first) == [1]

    lease = timedelta(seconds=30)
    assert await redis_backend.claim_park_generation(
        first, 1, 3, claimant="producer-first", lease=lease
    )
    assert await redis_backend.claim_park_generation(
        second, 1, 3, claimant="producer-second", lease=lease
    ), "a claim on one identity's generation must say nothing about another's"


@pytest.mark.parametrize(
    ("metacharacter", "impostor_name"),
    [
        # Each impostor name is a *different* stream whose rendering, read as a
        # `SCAN MATCH` glob, matches the victim's park keys. The lengths are
        # chosen so the suffix a naive implementation slices off still parses as
        # a wait id -- otherwise the leak hides behind an incidental filter.
        ("*", "token*"),
        ("?", "token?"),
        ("[", "toke[n]s"),
    ],
    ids=["star", "question", "bracket"],
)
@pytest.mark.asyncio
async def test_glob_metacharacters_cannot_enumerate_another_streams_intents(
    redis_backend: RedisStreamBackend,
    metacharacter: str,
    impostor_name: str,
) -> None:
    """`parked_wait_ids` must answer about one stream, whatever the name spells.

    The key goes into a `SCAN MATCH` pattern, so an unescaped `*`, `?` or `[` in
    a user-chosen stream name turns the enumeration into a wildcard over the
    keyspace -- and the producer then wakes, claims and rechecks waits that
    belong to a different Workflow.
    """
    assert metacharacter in impostor_name
    run_id = uuid.uuid4().hex
    victim = StreamKey("ns", "wf", run_id, "tokens")
    impostor = StreamKey("ns", "wf", run_id, impostor_name)

    for wait_id in (101, 202):
        await redis_backend.install_park_intent(
            victim,
            ParkIntent(
                wait_id=wait_id, cursor=BEGINNING, park_generation=1, run_id="r"
            ),
        )
    await redis_backend.install_park_intent(
        impostor, ParkIntent(wait_id=7, cursor=BEGINNING, park_generation=1, run_id="r")
    )

    assert await redis_backend.parked_wait_ids(impostor) == [7]
    assert await redis_backend.parked_wait_ids(victim) == [101, 202]


@pytest.mark.asyncio
async def test_a_glob_metacharacter_in_the_key_prefix_does_not_widen_the_scan(
    redis_backend: RedisStreamBackend,
) -> None:
    """The prefix is the operator's, not the identity's, so encoding cannot fix it.

    `key_prefix` separates deployments sharing one Redis and is passed through
    verbatim -- which is what makes it readable and what makes it the one place
    a `SCAN MATCH` pattern can still be widened. The two prefixes here are the
    same length so a leaked key's suffix still parses as a wait id; a shorter
    impostor would be hidden by that filter rather than by the escaping.
    """
    base = redis_backend._key_prefix  # cleaned up by the fixture's scan
    victim_backend = RedisStreamBackend(
        client=redis_backend._client, key_prefix=f"{base}:px"
    )
    impostor_backend = RedisStreamBackend(
        client=redis_backend._client, key_prefix=f"{base}:p*"
    )
    key = StreamKey("ns", "wf", uuid.uuid4().hex, "tokens")

    for wait_id in (101, 202):
        await victim_backend.install_park_intent(
            key,
            ParkIntent(
                wait_id=wait_id, cursor=BEGINNING, park_generation=1, run_id="r"
            ),
        )
    await impostor_backend.install_park_intent(
        key, ParkIntent(wait_id=7, cursor=BEGINNING, park_generation=1, run_id="r")
    )

    assert await impostor_backend.parked_wait_ids(key) == [7]
    assert await victim_backend.parked_wait_ids(key) == [101, 202]
