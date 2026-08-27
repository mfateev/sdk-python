"""Workflow-facing publishers for external output streams.

The publisher is deliberately a different type from the input subscription
handle.  Workflow code only converts values into deterministic logical
payloads; the Worker applies codecs and performs backend I/O after the
activation has returned.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Any, Generic, Protocol, cast

import temporalio.workflow
from temporalio.contrib.external_workflow_streams._api import _run_state
from temporalio.contrib.external_workflow_streams._record import RecordKind
from temporalio.types import AnyType

__all__ = [
    "ExternalOutputStreamOptions",
    "ExternalOutputStreamTopic",
    "external_output_stream",
]

DEFAULT_MAX_PUBLISH_LATENCY = timedelta(milliseconds=100)
DEFAULT_MAX_RECORDS_PER_BATCH = 256
DEFAULT_MAX_LOGICAL_BYTES_PER_BATCH = 1024 * 1024


class _OutputRuntime(Protocol):
    """The deterministic half of the per-Run stream runtime."""

    async def publish_output(
        self,
        *,
        topic: str,
        value: Any,
        value_type: type | None,
        kind: RecordKind,
        max_publish_latency: timedelta,
        max_records: int,
        max_logical_bytes: int,
    ) -> None: ...


@dataclass(frozen=True)
class ExternalOutputStreamOptions:
    """Options inherited by every output topic created from this entry point."""

    max_publish_latency: timedelta = DEFAULT_MAX_PUBLISH_LATENCY
    max_records: int = DEFAULT_MAX_RECORDS_PER_BATCH
    max_logical_bytes: int = DEFAULT_MAX_LOGICAL_BYTES_PER_BATCH

    def __post_init__(self) -> None:
        """Validate direct construction as well as :meth:`with_options`."""
        if self.max_publish_latency <= timedelta(0):
            raise ValueError("max_publish_latency must be positive")
        if self.max_records < 1:
            raise ValueError("max_records must be positive")
        if self.max_logical_bytes < 1:
            raise ValueError("max_logical_bytes must be positive")

    def with_options(
        self,
        *,
        max_publish_latency: timedelta | None = None,
        max_records: int | None = None,
        max_logical_bytes: int | None = None,
    ) -> ExternalOutputStreamOptions:
        """Return a copy with validated latency and logical batch limits."""
        latency = (
            self.max_publish_latency
            if max_publish_latency is None
            else max_publish_latency
        )
        records = self.max_records if max_records is None else max_records
        logical_bytes = (
            self.max_logical_bytes if max_logical_bytes is None else max_logical_bytes
        )
        if latency <= timedelta(0):
            raise ValueError("max_publish_latency must be positive")
        if records < 1:
            raise ValueError("max_records must be positive")
        if logical_bytes < 1:
            raise ValueError("max_logical_bytes must be positive")
        return replace(
            self,
            max_publish_latency=latency,
            max_records=records,
            max_logical_bytes=logical_bytes,
        )

    def topic(
        self, name: str, *, type: type[AnyType] | None = None
    ) -> ExternalOutputStreamTopic[AnyType]:
        """Create a typed Workflow publisher for one output topic."""
        if not name:
            raise ValueError("an output topic needs a non-empty name")
        return ExternalOutputStreamTopic(
            name=name,
            value_type=type,
            options=self,
        )


@dataclass(frozen=True)
class ExternalOutputStreamTopic(Generic[AnyType]):
    """A Workflow-side topic with publishing operations and no subscription API."""

    name: str
    value_type: type[AnyType] | None
    options: ExternalOutputStreamOptions

    async def publish(self, value: AnyType) -> None:
        """Accept one value into the current Workflow Task's output batch."""
        await self._publish(value, RecordKind.DATA)

    async def finish(self) -> None:
        """Append the ordered terminal record for this topic."""
        await self._publish(None, RecordKind.FINISH)

    async def _publish(self, value: Any, kind: RecordKind) -> None:
        if temporalio.workflow.unsafe.is_read_only():
            raise temporalio.workflow.ReadOnlyContextError(
                "While in read-only function, action attempted: publish external output"
            )
        state = _run_state()
        runtime = state.runtime
        if runtime is None or not hasattr(runtime, "publish_output"):
            raise RuntimeError(
                "external output streams are not configured on this Worker; pass "
                "external_stream_backend=... to the Worker"
            )
        await cast(_OutputRuntime, cast(object, runtime)).publish_output(
            topic=self.name,
            value=value,
            value_type=self.value_type,
            kind=kind,
            max_publish_latency=self.options.max_publish_latency,
            max_records=self.options.max_records,
            max_logical_bytes=self.options.max_logical_bytes,
        )


external_output_stream = ExternalOutputStreamOptions()
"""The default Workflow-facing external output stream entry point."""
