"""X2 — the Redis fixture itself, exercised through XADD/XREAD."""

from __future__ import annotations

import pytest

from tests.contrib.external_workflow_streams.conftest import (
    KEY_NAMESPACE,
    RedisKeyspace,
)


@pytest.mark.asyncio
async def test_xadd_and_xread_through_the_fixture(
    redis_keyspace: RedisKeyspace,
) -> None:
    stream = redis_keyspace.key("tokens")

    first = await redis_keyspace.client.xadd(stream, {"payload": "a"})  # type: ignore[attr-defined]
    second = await redis_keyspace.client.xadd(stream, {"payload": "b"})  # type: ignore[attr-defined]

    # XRANGE is inclusive of both endpoints -- this is the read replay uses.
    entries = await redis_keyspace.client.xrange(stream, first, second)  # type: ignore[attr-defined]
    assert [fields["payload"] for _, fields in entries] == ["a", "b"]

    # XREAD is exclusive of the supplied id -- this is the read watching uses.
    after_first = await redis_keyspace.client.xread({stream: first})  # type: ignore[attr-defined]
    assert [fields["payload"] for _, fields in after_first[0][1]] == ["b"]


@pytest.mark.asyncio
async def test_keys_are_namespaced_to_this_test(redis_keyspace: RedisKeyspace) -> None:
    assert redis_keyspace.key("tokens").startswith(f"{KEY_NAMESPACE}:")
    assert redis_keyspace.prefix.endswith(":")


@pytest.mark.asyncio
async def test_two_keyspaces_do_not_collide(
    redis_keyspace: RedisKeyspace, redis_client: object
) -> None:
    """A second keyspace over the same server sees none of the first's keys.

    This is the property xdist parallelism depends on; asserting it here means
    a regression shows up as a fixture failure rather than as a flaky suite.
    """
    other = RedisKeyspace(client=redis_client, prefix=f"{KEY_NAMESPACE}:other:x:")
    try:
        await redis_keyspace.client.xadd(redis_keyspace.key("s"), {"v": "1"})  # type: ignore[attr-defined]
        assert await redis_client.exists(other.key("s")) == 0  # type: ignore[attr-defined]
    finally:
        await other.cleanup()


@pytest.mark.asyncio
async def test_cleanup_removes_every_key(
    redis_keyspace: RedisKeyspace, redis_client: object
) -> None:
    stream = redis_keyspace.key("tokens")
    await redis_keyspace.client.xadd(stream, {"payload": "a"})  # type: ignore[attr-defined]

    assert await redis_keyspace.cleanup() == 1
    assert await redis_client.exists(stream) == 0  # type: ignore[attr-defined]
