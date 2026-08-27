"""Redis output staging, terminal transitions, and read-barrier behavior."""

from __future__ import annotations

import dataclasses
import uuid
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio

from temporalio.contrib.external_workflow_streams._backend import (
    StreamDirection,
    StreamKey,
)
from temporalio.contrib.external_workflow_streams._errors import StreamIntegrityError
from temporalio.contrib.external_workflow_streams._output_backend import (
    OutputStageConflictError,
    OutputStageManifest,
    OutputStageResolutionError,
    OutputStageStatus,
    StagedOutputRecord,
)
from temporalio.contrib.external_workflow_streams._record import (
    AFTER,
    BEGINNING,
    RecordKind,
    StreamRecord,
)
from temporalio.contrib.external_workflow_streams._redis import RedisStreamBackend
from tests.contrib.external_workflow_streams.conftest import (
    KEY_NAMESPACE,
    redis_url,
)


@pytest_asyncio.fixture  # type: ignore[reportUntypedFunctionDecorator]
async def output_backend(
    redis_worker_id: str,
) -> AsyncGenerator[RedisStreamBackend, None]:
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
            key
            async for key in backend._client.scan_iter(match=f"{backend._key_prefix}*")
        ]
        if keys:
            await backend._client.delete(*keys)
        await backend.aclose()


@pytest.fixture
def output_key() -> StreamKey:
    return StreamKey(
        "ns",
        "wf",
        uuid.uuid4().hex,
        "events",
        StreamDirection.OUTPUT,
    )


def manifest(
    key: StreamKey,
    *,
    token: str = "stage-1",
    sub_batch_id: int = 0,
    fingerprint: bytes = b"f" * 32,
    record_count: int = 2,
) -> OutputStageManifest:
    return OutputStageManifest(
        stream_key=key,
        provider_id="redis-streams",
        provider_format_version=1,
        stage_token=token,
        run_id="run-1",
        history_floor_event_id=10,
        sub_batch_id=sub_batch_id,
        fingerprint_version=1,
        fingerprint=fingerprint,
        record_count=record_count,
        logical_byte_count=20,
    )


def staged(*payloads: bytes) -> tuple[StagedOutputRecord, ...]:
    return tuple(
        StagedOutputRecord(index, RecordKind.DATA, payload)
        for index, payload in enumerate(payloads)
    )


def direct(payload: bytes, *, sequence: int = 0) -> StreamRecord:
    return StreamRecord(
        RecordKind.DATA,
        payload,
        producer_session_id="activity",
        sequence=sequence,
    )


@pytest.mark.parametrize(
    ("terminal", "status"),
    [
        ("commit", OutputStageStatus.COMMITTED),
        ("abort", OutputStageStatus.ABORTED),
    ],
)
async def test_exact_stage_retry_returns_actual_status_after_terminal(
    output_backend: RedisStreamBackend,
    output_key: StreamKey,
    terminal: str,
    status: OutputStageStatus,
) -> None:
    batch = manifest(output_key)
    records = staged(b"one", b"two")

    first = await output_backend.stage_output(batch, records)
    repeated_pending = await output_backend.stage_output(batch, records)
    resolved = await getattr(output_backend, f"{terminal}_output")(batch)
    repeated_resolution = await getattr(output_backend, f"{terminal}_output")(batch)
    repeated_terminal = await output_backend.stage_output(
        batch,
        # Encoded bytes may vary under a randomized PayloadCodec. The immutable
        # logical manifest makes this the same operation, so Redis must retain
        # and return the first successfully staged bytes.
        staged(b"random-one", b"random-two"),
    )

    assert first.status is OutputStageStatus.PENDING
    assert repeated_pending == first
    assert resolved.status is status
    assert repeated_resolution == resolved
    assert repeated_terminal.status is status
    assert repeated_terminal.records == first.records
    assert [record.payload for record in repeated_terminal.records] == [
        b"one",
        b"two",
    ]
    assert await output_backend.output_stage(batch) == repeated_terminal


async def test_reusing_stage_identity_with_another_manifest_conflicts_atomically(
    output_backend: RedisStreamBackend,
    output_key: StreamKey,
) -> None:
    original = manifest(output_key)
    first = await output_backend.stage_output(original, staged(b"one", b"two"))
    conflicting = dataclasses.replace(original, fingerprint=b"x" * 32)

    with pytest.raises(OutputStageConflictError):
        await output_backend.stage_output(
            conflicting, staged(b"other-one", b"other-two")
        )

    assert await output_backend.output_stage(original) == first
    entries = await output_backend._client.xrange(output_backend.stream_key(output_key))
    assert entries is not None
    assert len(entries) == 2


async def test_pending_stage_blocks_later_direct_output_until_commit(
    output_backend: RedisStreamBackend,
    output_key: StreamKey,
) -> None:
    batch = manifest(output_key, record_count=1)
    pending = await output_backend.stage_output(batch, staged(b"workflow"))
    later = await output_backend.append_output(output_key, direct(b"activity"))

    blocked = await output_backend.read_output_after(
        output_key, BEGINNING, max_records=10, block=None
    )
    assert blocked.records == ()
    assert blocked.pending is not None
    assert blocked.pending.manifest == batch
    assert blocked.pending.offset == pending.records[0].offset
    assert await output_backend.output_tail(output_key) == BEGINNING

    await output_backend.commit_output(batch)
    visible = await output_backend.read_output_after(
        output_key, BEGINNING, max_records=10, block=None
    )
    assert [record.payload for record in visible.records] == [
        b"workflow",
        b"activity",
    ]
    assert visible.records[-1].offset == later.offset
    assert visible.pending is None


async def test_abort_skips_stage_and_cannot_be_reversed(
    output_backend: RedisStreamBackend,
    output_key: StreamKey,
) -> None:
    batch = manifest(output_key, record_count=1)
    staged_batch = await output_backend.stage_output(batch, staged(b"discarded"))
    later = await output_backend.append_output(output_key, direct(b"visible"))

    aborted = await output_backend.abort_output(batch)
    repeated = await output_backend.abort_output(batch)
    with pytest.raises(OutputStageResolutionError):
        await output_backend.commit_output(batch)

    visible = await output_backend.read_output_after(
        output_key, BEGINNING, max_records=10, block=None
    )
    assert aborted.status is OutputStageStatus.ABORTED
    assert repeated == aborted
    assert aborted.records == staged_batch.records
    assert [record.payload for record in visible.records] == [b"visible"]
    assert await output_backend.output_tail(output_key) == AFTER(later.offset)


async def test_missing_stage_metadata_is_an_integrity_failure(
    output_backend: RedisStreamBackend,
    output_key: StreamKey,
) -> None:
    batch = manifest(output_key, record_count=1)
    await output_backend.stage_output(batch, staged(b"workflow"))
    stage_id = f"{len(batch.stage_token)}:{batch.stage_token}:{batch.sub_batch_id}"
    await output_backend._client.hdel(
        output_backend._output_status_key(output_key), stage_id
    )

    with pytest.raises(StreamIntegrityError, match="no stage status"):
        await output_backend.read_output_after(
            output_key, BEGINNING, max_records=10, block=None
        )
    with pytest.raises(StreamIntegrityError, match="missing its status"):
        await output_backend.output_stage(batch)


async def test_missing_staged_record_is_an_integrity_failure(
    output_backend: RedisStreamBackend,
    output_key: StreamKey,
) -> None:
    batch = manifest(output_key, record_count=1)
    stage = await output_backend.stage_output(batch, staged(b"workflow"))
    await output_backend._client.xdel(
        output_backend.stream_key(output_key), stage.records[0].offset.serialize()
    )

    with pytest.raises(StreamIntegrityError, match="record.*missing"):
        await output_backend.output_stage(batch)


async def test_input_and_output_with_same_user_identity_are_physically_isolated(
    output_backend: RedisStreamBackend,
    output_key: StreamKey,
) -> None:
    input_key = dataclasses.replace(output_key, direction=StreamDirection.INPUT)
    input_record = direct(b"input")
    output_record = direct(b"output")

    placed_input = await output_backend.append(input_key, input_record)
    placed_output = await output_backend.append_output(output_key, output_record)

    input_records = await output_backend.read_after(
        input_key, BEGINNING, max_records=10, block=None
    )
    output_records = await output_backend.read_output_after(
        output_key, BEGINNING, max_records=10, block=None
    )
    assert output_backend.stream_key(input_key) != output_backend.stream_key(output_key)
    assert [record.payload for record in input_records] == [b"input"]
    assert placed_input.offset == input_records[0].offset
    assert [record.payload for record in output_records.records] == [b"output"]
    assert placed_output.offset == output_records.records[0].offset
    with pytest.raises(ValueError, match="OUTPUT"):
        await output_backend.append_output(
            input_key, direct(b"wrong-direction", sequence=1)
        )
