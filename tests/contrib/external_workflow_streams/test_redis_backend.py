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

from temporalio.contrib.external_workflow_streams._backend import StreamKey
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
