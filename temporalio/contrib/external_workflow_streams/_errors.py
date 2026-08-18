"""The external stream failure taxonomy (P18).

Four outcomes, deliberately not collapsed, because each has a different operator
response. All four reach the server through the same channel -- a Workflow
activation completion is ``oneof status { Success | Failure }`` -- so what
distinguishes them is the **error type and the metric**, never the retry
behavior of the Workflow Task.

**There is no protocol-level "non-retryable Workflow Task failure": the server
retries Workflow Task failures regardless of cause.** Alerting must therefore
key on the metrics here rather than on Workflow Task failure counts, which a
transient backend outage also increments.

============================  ==========================  ====================
Condition                     Error type                  Operator response
============================  ==========================  ====================
Backend unreachable/erroring  :class:`StreamStorageError` None -- clears when
                                                          the backend recovers
Recorded offset missing,      :class:`StreamIntegrityError` Repair or restore
expired, reordered, or                                      the backend, or
miscounted                                                  terminate the Run
Bytes intact but undecodable  :class:`StreamDecodeError`  Align the consumer's
                                                          converter with the
                                                          producer's
Annotation does not match     ordinary nondeterminism     Fix or version the
the subscriptions made                                      Workflow code
============================  ==========================  ====================

There is deliberately **no** ``workflow_failure_exception_types`` registration
here: integrity loss *blocks* a Workflow rather than terminating it (ADR-014).
Retention loss is usually an operational error and usually repairable, and a
blocked Workflow can be resumed after repair where a failed one cannot.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import temporalio.common

__all__ = [
    "StreamDecodeError",
    "StreamError",
    "StreamIntegrityError",
    "StreamStorageError",
    "classify_read_failure",
]


class StreamError(Exception):
    """Base class for every external stream failure this module classifies."""


class StreamStorageError(StreamError):
    """The backend was unreachable, timed out, or returned an error.

    Transient by definition: it clears when the backend recovers, so an
    operator has nothing to do. This is the one row of the taxonomy that a
    Workflow Task failure count alone describes adequately.
    """


class StreamIntegrityError(StreamError):
    """A recorded range no longer reads back as it was written.

    Raised when an offset the annotation names is missing, when a range
    contains the wrong number of records, when ordering is not strictly
    increasing, or when control positions do not match. Those four checks are
    the whole of replay validation, and they are sufficient precisely because
    every provider guarantees a record's bytes cannot change (ADR-003) -- so
    the only damage replay must detect is a record that is no longer there.

    **This must never resolve to an alternate stream result.** Substituting a
    later record for a deleted one would hand Workflow code a different history
    than the one its commands were derived from.
    """


class StreamDecodeError(StreamError):
    """A record was present and intact but could not be decoded.

    Separated from :class:`StreamIntegrityError` because it is a configuration
    error on the *consumer* -- a DataConverter or codec that does not match the
    producer's -- and reporting it as integrity loss sends an operator to
    restore a backend that was never damaged (ADR-015).
    """


#: Metric names. Operators are expected to alert on the integrity metric
#: specifically, since the transient row increments Workflow Task failures too.
METRIC_INTEGRITY: Final = "temporal_external_stream_integrity_failure"
METRIC_DECODE: Final = "temporal_external_stream_decode_failure"
METRIC_STORAGE: Final = "temporal_external_stream_storage_failure"
METRIC_SHUTDOWN_WAKE_FAILED: Final = "temporal_external_stream_shutdown_wake_failed"


@dataclass(frozen=True)
class StreamMetrics:
    """The counters this taxonomy reports through.

    Each failure class gets its own counter rather than one counter with a
    ``reason`` attribute, so an alert on integrity loss cannot be diluted by a
    backend outage sharing the same series.
    """

    integrity: temporalio.common.MetricCounter
    decode: temporalio.common.MetricCounter
    storage: temporalio.common.MetricCounter
    shutdown_wake_failed: temporalio.common.MetricCounter

    @staticmethod
    def create(meter: temporalio.common.MetricMeter) -> StreamMetrics:
        return StreamMetrics(
            integrity=meter.create_counter(
                METRIC_INTEGRITY,
                "External stream replay found a recorded range that no longer reads back "
                "as written. Repair the backend or terminate the Run; this does not clear "
                "on its own.",
            ),
            decode=meter.create_counter(
                METRIC_DECODE,
                "External stream record bytes were intact but could not be decoded. The "
                "stream is fine; the consumer's converter or codec does not match the "
                "producer's.",
            ),
            storage=meter.create_counter(
                METRIC_STORAGE,
                "External stream backend was unreachable, timed out, or errored. "
                "Transient; clears when the backend recovers.",
            ),
            shutdown_wake_failed=meter.create_counter(
                METRIC_SHUTDOWN_WAKE_FAILED,
                "A Worker shutdown wake Signal was still unacknowledged when the graceful "
                "shutdown grace period expired.",
            ),
        )

    def counter_for(
        self, error_type_name: str
    ) -> temporalio.common.MetricCounter | None:
        """The counter for an error class *name*, or ``None`` for anything else.

        By name, because the side that reports these has only a name: a failure
        raised on the Workflow thread is converted inside ``activate()``, and
        what the Worker gets back is a completion whose application failure
        carries the exception's class name. Keeping the mapping here rather than
        at that call site is what stops the two from drifting apart -- the same
        reason :meth:`record` exists for the side that still holds the
        exception.

        ``None`` for every other failure, which is what keeps row four --
        ordinary nondeterminism -- and plain Workflow bugs out of these series.
        """
        return {
            StreamIntegrityError.__name__: self.integrity,
            StreamDecodeError.__name__: self.decode,
            StreamStorageError.__name__: self.storage,
        }.get(error_type_name)

    def record(self, error: BaseException) -> None:
        """Increments the counter matching ``error``'s class, if any."""
        if isinstance(error, StreamIntegrityError):
            self.integrity.add(1)
        elif isinstance(error, StreamDecodeError):
            self.decode.add(1)
        elif isinstance(error, StreamStorageError):
            self.storage.add(1)


def classify_read_failure(
    *, range_validated: bool, cause: BaseException
) -> StreamError:
    """The mechanical classification rule, in one place.

    Unconditional, because structural immutability is required of every
    provider:

        **If the range validated, the bytes are the bytes that were written**,
        so any subsequent failure is a decode failure. Only a missing offset or
        a range that fails validation is integrity loss.

    Args:
        range_validated: Whether the four range checks passed for the range the
            failing record belongs to.
        cause: What actually went wrong, attached as the new error's cause.

    Returns:
        The error to raise. Never raises on its own -- the caller decides
        whether to raise, record a metric, or both.
    """
    # A storage failure is about reaching the backend at all, so it is never
    # reclassified by whether a range validated: there was no range to read.
    if isinstance(cause, StreamStorageError):
        return cause

    if range_validated:
        err: StreamError = StreamDecodeError(
            f"external stream record could not be decoded: {cause}"
        )
    else:
        err = StreamIntegrityError(
            f"external stream recorded range failed validation: {cause}"
        )
    err.__cause__ = cause
    return err
