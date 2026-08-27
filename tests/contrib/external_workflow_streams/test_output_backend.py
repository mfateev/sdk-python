"""Provider-neutral output identity, records, and staging contract."""

from __future__ import annotations

import dataclasses

import pytest

from temporalio.contrib.external_workflow_streams._backend import (
    StreamBackend,
    StreamDirection,
    StreamKey,
)
from temporalio.contrib.external_workflow_streams._output_backend import (
    OutputReadResult,
    OutputStage,
    OutputStageManifest,
    OutputStageStatus,
    OutputStreamBackend,
    OutputStreamRecord,
    PendingOutputBarrier,
    StagedOutputRecord,
)
from temporalio.contrib.external_workflow_streams._producer import WorkflowChainKey
from temporalio.contrib.external_workflow_streams._record import (
    Offset,
    RecordKind,
    StreamRecord,
)


def output_key(name: str = "events") -> StreamKey:
    return StreamKey(
        "namespace",
        "workflow",
        "first-run",
        name,
        direction=StreamDirection.OUTPUT,
    )


def manifest(**changes: object) -> OutputStageManifest:
    values: dict[str, object] = {
        "stream_key": output_key(),
        "provider_id": "memory",
        "provider_format_version": 1,
        "stage_token": "stage-token",
        "run_id": "current-run",
        "history_floor_event_id": 7,
        "sub_batch_id": 0,
        "fingerprint_version": 1,
        "fingerprint": b"f" * 32,
        "record_count": 2,
        "logical_byte_count": 41,
    }
    values.update(changes)
    return OutputStageManifest(**values)  # type: ignore[arg-type]


def test_stream_direction_defaults_to_the_existing_input_identity() -> None:
    existing = StreamKey("namespace", "workflow", "first-run", "events")

    assert existing.direction is StreamDirection.INPUT
    assert existing == StreamKey(
        "namespace",
        "workflow",
        "first-run",
        "events",
        direction=StreamDirection.INPUT,
    )
    assert str(existing) == "namespace/workflow/first-run/input/events"


def test_input_and_output_topics_with_the_same_name_are_distinct() -> None:
    input_key = StreamKey("namespace", "workflow", "first-run", "events")
    output = output_key()

    assert input_key != output
    assert len({input_key, output}) == 2
    assert str(output) == "namespace/workflow/first-run/output/events"


def test_workflow_chain_key_builds_either_direction() -> None:
    chain = WorkflowChainKey("namespace", "workflow", "first-run")

    assert chain.stream_key("events") == StreamKey(
        "namespace", "workflow", "first-run", "events"
    )
    assert chain.stream_key("events", direction=StreamDirection.OUTPUT) == output_key()


def test_stream_key_rejects_a_raw_direction_string() -> None:
    with pytest.raises(TypeError, match="must be a StreamDirection"):
        StreamKey(
            "namespace",
            "workflow",
            "first-run",
            "events",
            direction="output",  # type: ignore[arg-type]
        )


def test_output_manifest_is_bound_to_an_output_key() -> None:
    with pytest.raises(ValueError, match="requires an OUTPUT stream key"):
        manifest(stream_key=StreamKey("namespace", "workflow", "first-run", "events"))


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("provider_id", "", "non-empty provider_id"),
        ("stage_token", "", "non-empty stage_token"),
        ("run_id", "", "non-empty run_id"),
        ("provider_format_version", 0, "positive provider_format_version"),
        ("fingerprint_version", 0, "positive fingerprint_version"),
        ("record_count", 0, "positive record_count"),
        ("history_floor_event_id", 0, "positive history_floor_event_id"),
        ("sub_batch_id", -1, "non-negative sub_batch_id"),
        ("logical_byte_count", -1, "non-negative logical_byte_count"),
        ("fingerprint", b"short", "must be 32 bytes"),
    ],
)
def test_output_manifest_rejects_incomplete_identity(
    field: str, value: object, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        manifest(**{field: value})


def test_staged_and_placed_control_records_carry_no_payload() -> None:
    staged = StagedOutputRecord(0, RecordKind.FINISH, b"")
    placed = OutputStreamRecord(RecordKind.FINISH, b"", Offset("1-0"))

    assert staged.kind is RecordKind.FINISH
    assert placed.kind.is_control
    with pytest.raises(ValueError, match="carries no payload"):
        StagedOutputRecord(0, RecordKind.FINISH, b"not-empty")
    with pytest.raises(ValueError, match="carries no payload"):
        OutputStreamRecord(RecordKind.WRITE_FENCE, b"not-empty", Offset("1-1"))


def test_finish_round_trips_through_provider_neutral_record_fields() -> None:
    finish = StreamRecord(RecordKind.FINISH, b"", "session", 3).placed_at(Offset("2-0"))

    assert finish.is_control
    assert StreamRecord.from_fields(Offset("2-0"), finish.to_fields()) == finish


def test_output_stage_record_count_must_match_its_manifest() -> None:
    records = (
        OutputStreamRecord(RecordKind.DATA, b"one", Offset("1-0")),
        OutputStreamRecord(RecordKind.DATA, b"two", Offset("1-1")),
    )

    stage = OutputStage(manifest(), records, OutputStageStatus.PENDING)
    assert stage.records == records
    with pytest.raises(ValueError, match="record count does not match"):
        OutputStage(manifest(record_count=1), records, OutputStageStatus.PENDING)


def test_output_models_are_frozen_and_use_immutable_record_collections() -> None:
    stage_manifest = manifest(record_count=1)
    record = OutputStreamRecord(RecordKind.DATA, b"data", Offset("1-0"))
    stage = OutputStage(stage_manifest, (record,), OutputStageStatus.PENDING)
    barrier = PendingOutputBarrier(stage_manifest, Offset("1-0"))
    result = OutputReadResult((record,), barrier)

    assert isinstance(stage.records, tuple)
    assert isinstance(result.records, tuple)
    with pytest.raises(dataclasses.FrozenInstanceError):
        result.pending = None  # type: ignore[misc]


def test_output_backend_is_an_additive_capability() -> None:
    assert not issubclass(OutputStreamBackend, StreamBackend)
    assert OutputStreamBackend.__abstractmethods__ == {
        "abort_output",
        "append_output",
        "commit_output",
        "compare_offsets",
        "output_stage",
        "output_tail",
        "read_output_after",
        "stage_output",
    }
