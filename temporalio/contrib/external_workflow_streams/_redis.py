"""The Redis Streams provider (P3).

Redis Streams qualifies as a backend because an entry's fields cannot be
rewritten in place. It can be deleted by ``XDEL`` or removed by trimming --
which is exactly the damage replay's four range checks detect -- but never
altered, which is what makes those four checks sufficient.

The two reads are **different Redis commands**, not one with a flag:

- ``XRANGE first last`` is **inclusive of both endpoints**, and is the only
  command replay may use. The marker already names the range.
- ``XREAD BLOCK ... STREAMS key id`` returns entries **strictly after** ``id``,
  which is exactly the exclusive-after semantics a live watch needs. It cannot
  serve a replay read: it would silently drop the range's first record.

``BEGINNING`` maps to the sentinel ``0-0`` for watching, so a consumer that has
drained to the tail holds ``AFTER(<last id>)`` and never has to name the id of
a record nobody has written yet.

Offsets are Redis ids, compared as numeric ``(milliseconds, sequence)`` pairs.
String comparison is wrong the moment the millisecond component changes width.
"""

from __future__ import annotations

import hashlib
from datetime import timedelta
from typing import Any, Final
from urllib.parse import quote

from temporalio.contrib.external_workflow_streams._backend import (
    DEFAULT_WATCH_BLOCK,
    AppendConflictError,
    ParkIntent,
    StreamBackend,
    StreamKey,
)
from temporalio.contrib.external_workflow_streams._record import (
    Cursor,
    Offset,
    StreamRecord,
)

__all__ = ["RedisStreamBackend"]

DEFAULT_URL: Final = "redis://127.0.0.1:6379"
DEFAULT_KEY_PREFIX: Final = "temporal-external-stream"

#: Redis' beginning-of-stream sentinel for `XREAD`. Not an offset: no record
#: ever has this id, which is why `BEGINNING` is a distinct cursor form rather
#: than `AFTER(Offset("0-0"))`.
_BEGINNING_SENTINEL: Final = "0-0"


def _parse(offset: Offset) -> tuple[int, int]:
    """A Redis id as the ``(ms, seq)`` pair it actually is."""
    ms, _, seq = offset.token.partition("-")
    return int(ms), int(seq or 0)


def _content_hash(record: StreamRecord) -> str:
    """A stable digest of exactly what would be written.

    Excludes the offset, which the provider assigns -- so a re-append of
    identical content hashes identically rather than looking like a conflict.
    """
    digest = hashlib.sha256()
    for name, value in sorted(record.to_fields().items()):
        digest.update(name.encode())
        digest.update(b"\x00")
        digest.update(value)
        digest.update(b"\x00")
    return digest.hexdigest()


#: Append-if-new-or-identical, atomically.
#:
#: Split across two commands without a script, a crash between `XADD` and the
#: idempotency write would leave a record no retry could recognise as its own,
#: so the retry would append a duplicate.
_APPEND_LUA: Final = """
local existing = redis.call('HGET', KEYS[2], ARGV[1])
if existing then
  local sep = string.find(existing, '|')
  local offset = string.sub(existing, 1, sep - 1)
  local digest = string.sub(existing, sep + 1)
  if digest == ARGV[2] then
    return {'reused', offset}
  end
  return {'conflict', ''}
end
local id = redis.call('XADD', KEYS[1], '*', unpack(ARGV, 3))
redis.call('HSET', KEYS[2], ARGV[1], id .. '|' .. ARGV[2])
return {'appended', id}
"""


#: Compare-and-remove one park intent, atomically. The claim belongs to the
#: intent generation, so a successful removal retires both keys just like the
#: unconditional operation does.
_REMOVE_PARK_INTENT_IF_MATCHES_LUA: Final = """
local run_id = redis.call('HGET', KEYS[1], 'run_id')
local generation = redis.call('HGET', KEYS[1], 'generation')
if run_id == ARGV[1] and generation == ARGV[2] then
  redis.call('DEL', KEYS[1], KEYS[2])
  return 1
end
return 0
"""


class RedisStreamBackend(StreamBackend):
    """Redis Streams as an external workflow stream provider."""

    guarantees_immutability = True
    """`XADD` entries cannot be rewritten in place -- only deleted or trimmed."""

    provider_id = "redis-streams"
    provider_format_version = 1
    supports_leased_claims = True

    def __init__(
        self,
        *,
        url: str = DEFAULT_URL,
        client: Any | None = None,
        key_prefix: str = DEFAULT_KEY_PREFIX,
    ) -> None:
        """
        Args:
            url: Connection URL, used when ``client`` is not supplied.
            client: An existing ``redis.asyncio.Redis``. It **must** have been
                created with ``decode_responses=False`` -- payloads are
                arbitrary bytes, and a decoding client would corrupt them.
            key_prefix: Prepended to every key this backend creates, so one
                Redis can serve several deployments.
        """
        if client is None:
            import redis.asyncio  # imported lazily so redis stays optional

            client = redis.asyncio.from_url(url, decode_responses=False)
        self._client = client
        self._key_prefix = key_prefix
        self._append_script = client.register_script(_APPEND_LUA)
        self._remove_park_intent_if_matches_script = client.register_script(
            _REMOVE_PARK_INTENT_IF_MATCHES_LUA
        )

    # --- key layout ---------------------------------------------------------

    def stream_key(self, key: StreamKey) -> str:
        """The physical Redis key for one stream identity.

        Injective: distinct :class:`StreamKey` values always render as distinct
        Redis keys, because :func:`_escaped` removes the delimiter from every
        component before they are joined. See that function for why joining the
        raw fields is not merely untidy but a correctness failure.

        An identity with no reserved character in it renders byte-identically to
        the delimiter-joined layout this replaced, and the feature is private and
        unreleased, so there is nothing written under the old rendering worth
        migrating and no compatibility path here to maintain.
        """
        return f"{self._key_prefix}:{_escaped(key)}"

    def _idempotency_key(self, key: StreamKey) -> str:
        return f"{self.stream_key(key)}:idem"

    def _intent_key(self, key: StreamKey, wait_id: int) -> str:
        # `(stream key, wait_id)`, never the stream alone: two subscriptions to
        # one stream are two independent waits.
        return f"{self.stream_key(key)}:park:{wait_id}"

    def _claim_key(self, key: StreamKey, wait_id: int) -> str:
        return f"{self.stream_key(key)}:claim:{wait_id}"

    # --- required operations ------------------------------------------------

    async def append(self, key: StreamKey, record: StreamRecord) -> StreamRecord:
        fields = record.to_fields()
        args: list[Any] = [
            str(record.idempotency_key).encode(),
            _content_hash(record).encode(),
        ]
        for name, value in sorted(fields.items()):
            args.append(name.encode())
            args.append(value)

        outcome, offset = await self._append_script(
            keys=[self.stream_key(key), self._idempotency_key(key)],
            args=args,
        )
        if _text(outcome) == "conflict":
            raise AppendConflictError(record.idempotency_key)
        return record.placed_at(Offset(_text(offset)))

    async def read_range(
        self, key: StreamKey, first: Offset, last: Offset
    ) -> list[StreamRecord]:
        entries = await self._client.xrange(
            self.stream_key(key), first.serialize(), last.serialize()
        )
        return [_to_record(entry_id, fields) for entry_id, fields in entries]

    async def read_after(
        self,
        key: StreamKey,
        after: Cursor,
        *,
        max_records: int,
        block: timedelta | None = DEFAULT_WATCH_BLOCK,
    ) -> list[StreamRecord]:
        start = (
            _BEGINNING_SENTINEL if after.is_beginning else after.offset.serialize()  # type: ignore[union-attr]
        )
        block_ms = None if block is None else int(block.total_seconds() * 1000)
        if block_ms is not None and block_ms <= 0:
            # `XREAD BLOCK 0` blocks *forever*, which is the opposite of what a
            # zero timeout asks for, so a non-blocking read must omit BLOCK.
            block_ms = None

        streams = await self._client.xread(
            {self.stream_key(key): start}, count=max_records, block=block_ms
        )
        if not streams:
            return []
        _, entries = streams[0]
        return [_to_record(entry_id, fields) for entry_id, fields in entries]

    def compare_offsets(self, left: Offset, right: Offset) -> int:
        a, b = _parse(left), _parse(right)
        return (a > b) - (a < b)

    # --- parking (P3b) ------------------------------------------------------

    async def install_park_intent(self, key: StreamKey, intent: ParkIntent) -> None:
        await self._client.hset(
            self._intent_key(key, intent.wait_id),
            mapping={
                b"cursor": intent.cursor.serialize().encode(),
                b"generation": str(intent.park_generation).encode(),
                b"run_id": intent.run_id.encode(),
            },
        )

    async def remove_park_intent(self, key: StreamKey, wait_id: int) -> None:
        await self._client.delete(
            self._intent_key(key, wait_id), self._claim_key(key, wait_id)
        )

    async def remove_park_intent_if_matches(
        self,
        key: StreamKey,
        wait_id: int,
        *,
        run_id: str,
        park_generation: int,
    ) -> bool:
        removed = await self._remove_park_intent_if_matches_script(
            keys=[self._intent_key(key, wait_id), self._claim_key(key, wait_id)],
            args=[run_id, str(park_generation)],
        )
        return bool(removed)

    async def park_intent(self, key: StreamKey, wait_id: int) -> ParkIntent | None:
        stored = await self._client.hgetall(self._intent_key(key, wait_id))
        if not stored:
            return None
        stored = {_text(name): value for name, value in stored.items()}
        return ParkIntent(
            wait_id=wait_id,
            cursor=Cursor.deserialize(_text(stored["cursor"])),
            park_generation=int(_text(stored["generation"])),
            run_id=_text(stored["run_id"]),
        )

    async def recheck(self, key: StreamKey, wait_id: int) -> bool:
        intent = await self.park_intent(key, wait_id)
        if intent is None:
            return False
        found = await self.read_after(key, intent.cursor, max_records=1, block=None)
        return bool(found)

    async def claim_park_generation(
        self,
        key: StreamKey,
        wait_id: int,
        park_generation: int,
        *,
        claimant: str,
        lease: timedelta,
    ) -> bool:
        claim_key = self._claim_key(key, wait_id)
        value = f"{park_generation}|{claimant}".encode()
        lease_ms = max(1, int(lease.total_seconds() * 1000))

        # SET NX gives exclusion; the TTL is what makes a crashed producer's
        # claim recoverable instead of stranding the generation forever.
        if await self._client.set(claim_key, value, nx=True, px=lease_ms):
            return True

        held = await self._client.get(claim_key)
        if held is None:
            # It expired between the SET and the GET -- take it.
            return bool(await self._client.set(claim_key, value, nx=True, px=lease_ms))
        if held == value:
            # Renewal by the holder, which must not read as contention.
            await self._client.pexpire(claim_key, lease_ms)
            return True

        held_generation = _text(held).split("|", 1)[0]
        if held_generation != str(park_generation):
            # A claim for a different generation says nothing about this one.
            await self._client.set(claim_key, value, px=lease_ms)
            return True
        return False

    async def parked_wait_ids(self, key: StreamKey) -> list[int]:
        prefix = f"{self.stream_key(key)}:park:"
        found = []
        # SCAN rather than KEYS: this runs on the producer's hot path after every
        # append, and KEYS blocks the whole server for the length of the keyspace.
        #
        # The prefix is a *literal* here, so it is escaped before the trailing
        # `*` makes it a pattern. `_escaped` already leaves no glob character in
        # the identity half, but the operator-supplied `key_prefix` is not
        # escaped, and a pattern that quietly widened would enumerate another
        # stream's intents rather than fail.
        async for name in self._client.scan_iter(match=f"{_as_glob_literal(prefix)}*"):
            suffix = _text(name)[len(prefix) :]
            if suffix.isdigit():
                found.append(int(suffix))
        return sorted(found)

    async def current_park_generation(self, key: StreamKey, wait_id: int) -> int | None:
        intent = await self.park_intent(key, wait_id)
        return None if intent is None else intent.park_generation

    # --- lifecycle ----------------------------------------------------------

    async def aclose(self) -> None:
        await self._client.aclose()

    async def delete_for_test(self, key: StreamKey, offset: Offset) -> None:
        """Removes one record, standing in for trimming or retention expiry."""
        await self._client.xdel(self.stream_key(key), offset.serialize())


#: What Redis' glob matcher treats as more than itself. `]` and `^` are special
#: only inside a class, but escaping them too costs nothing and keeps the rule
#: one line long.
_GLOB_METACHARACTERS: Final = frozenset("*?[]\\")


def _as_glob_literal(text: str) -> str:
    """A literal string, made safe to embed in a `SCAN MATCH` pattern."""
    return "".join(
        f"\\{character}" if character in _GLOB_METACHARACTERS else character
        for character in text
    )


def _escaped(key: StreamKey) -> str:
    r"""One stream identity as a single, unambiguous key component.

    Percent-encoded per field, then joined -- **not** joined raw. A Workflow ID
    and a stream name are user-chosen strings in which `:` is an ordinary
    character, so joining the raw fields is not injective:

        ("ns", "wf", r1, f"{r2}:tokens")   and   ("ns", f"wf:{r1}", r2, "tokens")

    both render as `ns:wf:r1:r2:tokens`. Two unrelated Workflows would then share
    one stream, one idempotency hash, one park intent and one claim -- delivering
    each other's records, and each concluding the other's claim had already taken
    its wake. Same reasoning as `_wake.py`'s length-prefixed request-ID material,
    applied to a key rather than to a digest.

    Percent-encoding rather than length prefixes because a key is read by humans:
    an ordinary identity still renders verbatim in `redis-cli`, and only a field
    that actually contains a delimiter pays for it. It buys one property the
    length prefix does not -- the encoded form contains no `:`, `*`, `?`, `[` or
    `\`, so the derived `:idem`, `:park:<id>` and `:claim:<id>` suffixes stay
    unambiguous and `parked_wait_ids`' pattern cannot be widened by a stream name.

    Not reversible in practice, and not meant to be: `key_prefix` is
    operator-supplied and unescaped, so only the identity half round-trips.
    """
    return ":".join(
        quote(component, safe="")
        for component in (
            key.namespace,
            key.workflow_id,
            key.first_execution_run_id,
            key.stream_name,
        )
    )


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, (bytes, bytearray)) else str(value)


def _to_record(entry_id: Any, fields: Any) -> StreamRecord:
    return StreamRecord.from_fields(Offset(_text(entry_id)), fields)
