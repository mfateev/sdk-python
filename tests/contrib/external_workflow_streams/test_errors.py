"""P18 — the failure taxonomy, its metrics, and the classification rule."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

import temporalio.common
from temporalio.contrib.external_workflow_streams._errors import (
    METRIC_DECODE,
    METRIC_INTEGRITY,
    METRIC_SHUTDOWN_WAKE_FAILED,
    METRIC_STORAGE,
    StreamDecodeError,
    StreamError,
    StreamIntegrityError,
    StreamMetrics,
    StreamStorageError,
    classify_read_failure,
)


@dataclass
class RecordingCounter(temporalio.common.MetricCounter):
    counter_name: str
    added: list[int] = field(default_factory=list)

    @property
    def name(self) -> str:
        return self.counter_name

    @property
    def description(self) -> str | None:
        return None

    @property
    def unit(self) -> str | None:
        return None

    def add(
        self,
        value: int,
        additional_attributes: temporalio.common.MetricAttributes | None = None,
    ) -> None:
        self.added.append(value)

    def with_additional_attributes(
        self, additional_attributes: temporalio.common.MetricAttributes
    ) -> temporalio.common.MetricCounter:
        return self


class RecordingMeter(temporalio.common.MetricMeter):
    def __init__(self) -> None:
        self.counters: dict[str, RecordingCounter] = {}

    def create_counter(
        self, name: str, description: str | None = None, unit: str | None = None
    ) -> temporalio.common.MetricCounter:
        counter = RecordingCounter(name)
        self.counters[name] = counter
        return counter

    def create_histogram(self, name, description=None, unit=None):  # type: ignore[no-untyped-def]
        raise NotImplementedError

    def create_histogram_float(self, name, description=None, unit=None):  # type: ignore[no-untyped-def]
        raise NotImplementedError

    def create_histogram_timedelta(self, name, description=None, unit=None):  # type: ignore[no-untyped-def]
        raise NotImplementedError

    def create_gauge(self, name, description=None, unit=None):  # type: ignore[no-untyped-def]
        raise NotImplementedError

    def create_gauge_float(self, name, description=None, unit=None):  # type: ignore[no-untyped-def]
        raise NotImplementedError

    def with_additional_attributes(self, additional_attributes):  # type: ignore[no-untyped-def]
        return self


# --- the types --------------------------------------------------------------


def test_the_two_types_are_distinct_and_neither_subclasses_the_other() -> None:
    """Collapsing them would send an operator to repair an undamaged backend."""
    assert issubclass(StreamIntegrityError, StreamError)
    assert issubclass(StreamDecodeError, StreamError)
    assert not issubclass(StreamIntegrityError, StreamDecodeError)
    assert not issubclass(StreamDecodeError, StreamIntegrityError)


# --- the classification rule, in both directions ----------------------------


def test_a_failure_after_the_range_validated_is_a_decode_failure() -> None:
    cause = ValueError("bad JSON")

    err = classify_read_failure(range_validated=True, cause=cause)

    assert isinstance(err, StreamDecodeError)
    assert err.__cause__ is cause


def test_a_failure_before_the_range_validated_is_integrity_loss() -> None:
    cause = LookupError("offset 100-0 is missing")

    err = classify_read_failure(range_validated=False, cause=cause)

    assert isinstance(err, StreamIntegrityError)
    assert err.__cause__ is cause


def test_the_same_cause_classifies_differently_by_validation_alone() -> None:
    """The rule is mechanical: only `range_validated` decides which row it is."""
    cause = ValueError("undecodable")

    assert isinstance(
        classify_read_failure(range_validated=True, cause=cause), StreamDecodeError
    )
    assert isinstance(
        classify_read_failure(range_validated=False, cause=cause), StreamIntegrityError
    )


def test_a_storage_failure_is_never_reclassified() -> None:
    """There was no range to read, so validation has nothing to say about it."""
    cause = StreamStorageError("connection refused")

    for validated in (True, False):
        assert classify_read_failure(range_validated=validated, cause=cause) is cause


# --- metrics ----------------------------------------------------------------


def test_each_class_gets_its_own_counter() -> None:
    meter = RecordingMeter()
    metrics = StreamMetrics.create(meter)

    assert set(meter.counters) == {
        METRIC_INTEGRITY,
        METRIC_DECODE,
        METRIC_STORAGE,
        METRIC_SHUTDOWN_WAKE_FAILED,
    }
    # Separate series, not one counter with a `reason` attribute -- otherwise an
    # integrity alert is diluted by a backend outage sharing the series.
    assert len({id(c) for c in meter.counters.values()}) == 4
    assert metrics.integrity is not metrics.decode


def test_recording_increments_only_the_matching_counter() -> None:
    meter = RecordingMeter()
    metrics = StreamMetrics.create(meter)

    metrics.record(StreamIntegrityError("gone"))
    metrics.record(StreamDecodeError("garbled"))
    metrics.record(StreamDecodeError("garbled again"))
    metrics.record(StreamStorageError("timeout"))

    assert meter.counters[METRIC_INTEGRITY].added == [1]
    assert meter.counters[METRIC_DECODE].added == [1, 1]
    assert meter.counters[METRIC_STORAGE].added == [1]
    assert meter.counters[METRIC_SHUTDOWN_WAKE_FAILED].added == []


def test_an_unrelated_error_increments_nothing() -> None:
    meter = RecordingMeter()
    metrics = StreamMetrics.create(meter)

    metrics.record(RuntimeError("something else entirely"))

    assert all(not c.added for c in meter.counters.values())


# --- no terminal-failure opt-in ---------------------------------------------


@pytest.mark.parametrize(
    "err_type", [StreamIntegrityError, StreamDecodeError, StreamStorageError]
)
def test_no_stream_error_is_a_temporal_failure(err_type: type[StreamError]) -> None:
    """Integrity loss blocks a Workflow; it does not terminate one (ADR-014).

    A blocked Workflow can be resumed once the backend is repaired, and a
    failed one cannot. These stay plain exceptions rather than
    ``FailureError``s so that the SDK's failure conversion cannot turn one into
    a Workflow *execution* failure -- which is what a
    ``workflow_failure_exception_types`` registration would do.
    """
    import temporalio.exceptions

    assert not issubclass(err_type, temporalio.exceptions.FailureError)
    assert not issubclass(err_type, temporalio.exceptions.ApplicationError)
