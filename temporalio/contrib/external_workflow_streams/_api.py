"""The Workflow-facing API (P9).

.. code-block:: python

    streams = external_stream.with_options(idle_timeout=timedelta(seconds=1))
    tokens = streams.topic("tokens", backend="tokens-redis", type=str)

    async for token in tokens.subscribe():
        process(token)

Workflow code **names** a backend; it never constructs or imports one. Provider
instances hold connections and credentials, live on the Worker outside the
sandbox, and are reached only through an opaque handle.

Everything here is a mirror image of the shipped
:py:mod:`temporalio.contrib.workflow_streams`, not a second implementation of
it, so no name may collide -- and in particular no name here may begin with
``__temporal_workflow_stream``.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Any, Generic, Protocol

import temporalio.workflow
from temporalio.contrib.external_workflow_streams._backend import StreamKey
from temporalio.contrib.external_workflow_streams._codec import StreamPayloadCodec
from temporalio.contrib.external_workflow_streams._record import (
    Cursor,
    StreamRecord,
)
from temporalio.types import AnyType

__all__ = [
    "ExternalStreamOptions",
    "ExternalStreamSubscription",
    "ExternalStreamTopic",
    "external_stream",
]

DEFAULT_IDLE_TIMEOUT = timedelta(seconds=1)
"""How long the complete blocked set waits before parking.

A property of the **set**, not of one subscription: one idle stream must not
park a Workflow Task another stream is still driving.
"""

#: Where per-Run subscription state hangs off the Workflow instance. Reserved,
#: and deliberately not in the `__temporal_workflow_stream*` namespace the
#: shipped contrib feature already owns.
_RUN_STATE_ATTR = "__temporal_external_stream_state"


class ExternalStreamRuntime(Protocol):
    """What Workflow code needs from the Worker, and nothing more.

    An opaque handle across the sandbox boundary: it resolves a backend *name*
    and registers a wait. Workflow code never sees a provider instance.
    """

    def stream_key(self, stream_name: str) -> StreamKey:
        """The full stream identity for a name, from the running Run's chain."""
        ...

    def register(
        self, *, wait_id: int, stream_key: StreamKey, backend_name: str
    ) -> None:
        """Registers a wait with the Worker's subscription manager."""
        ...

    def drain(self, wait_id: int, max_records: int | None = None) -> list[StreamRecord]:
        """Pops buffered records. Performs no I/O."""
        ...

    def codec_for(self, value_type: type | None) -> StreamPayloadCodec[Any]:
        """The Workflow's DataConverter, bound to a topic's declared type."""
        ...

    def new_readiness_future(self) -> asyncio.Future[None]:
        """A future the readiness activation handler will resolve.

        Created by the runtime rather than here because it must belong to the
        Workflow's own deterministic event loop, which this module has no
        business reaching into.
        """
        ...


@dataclass
class _RunState:
    """Per-Run subscription bookkeeping.

    Lives on the Workflow instance rather than in a module global, so it shares
    the instance's lifetime exactly -- a module global would outlive an evicted
    Run and hand its wait ids to the next one.
    """

    runtime: ExternalStreamRuntime | None = None
    next_wait_id: int = 1
    #: `wait_id -> Future`, resolved by the readiness activation handler.
    pending: dict[int, Any] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.pending is None:
            self.pending = {}


def _run_state() -> _RunState:
    instance = temporalio.workflow.instance()
    state = getattr(instance, _RUN_STATE_ATTR, None)
    if state is None:
        state = _RunState()
        setattr(instance, _RUN_STATE_ATTR, state)
    return state


def _install_runtime(instance: Any, runtime: ExternalStreamRuntime) -> None:
    """Gives a Workflow instance its handle to the Worker's manager.

    Called by the Worker when it creates the instance.
    """
    state = getattr(instance, _RUN_STATE_ATTR, None)
    if state is None:
        state = _RunState()
        setattr(instance, _RUN_STATE_ATTR, state)
    state.runtime = runtime


@dataclass(frozen=True)
class ExternalStreamOptions:
    """The entry point, and the options every topic under it inherits."""

    idle_timeout: timedelta = DEFAULT_IDLE_TIMEOUT

    def with_options(
        self, *, idle_timeout: timedelta | None = None
    ) -> ExternalStreamOptions:
        """A copy with the given options replaced.

        Args:
            idle_timeout: How long the complete blocked set waits with no record
                on any active subscription before parking.

        Raises:
            ValueError: The timeout is not positive. Rejected rather than
                coerced -- zero would park instantly and a negative value means
                nothing at all, so neither can be what a caller intended.
        """
        if idle_timeout is not None and idle_timeout <= timedelta(0):
            raise ValueError(
                f"idle_timeout must be positive, got {idle_timeout}. A "
                "non-positive timeout is a configuration error rather than a "
                "request to park immediately."
            )
        return replace(
            self,
            idle_timeout=self.idle_timeout if idle_timeout is None else idle_timeout,
        )

    def topic(
        self, name: str, *, backend: str, type: type[AnyType] | None = None
    ) -> ExternalStreamTopic[Any]:
        """A handle for one stream.

        Args:
            name: The stream name. The only place it appears.
            backend: The **name** of a backend registered on the Worker with
                ``external_stream_backends={...}``. Not a provider instance:
                Workflow code may not hold one.
            type: The value type, used as the decode hint.
        """
        if not name:
            raise ValueError("a topic needs a non-empty name")
        if not backend:
            raise ValueError(
                "a topic needs the name of a backend registered on the Worker; "
                "Workflow code names a backend rather than constructing one"
            )
        return ExternalStreamTopic(
            name=name, backend_name=backend, value_type=type, options=self
        )


@dataclass(frozen=True)
class ExternalStreamTopic(Generic[AnyType]):
    """One stream, from the Workflow's side.

    Has ``subscribe`` and no ``publish``: the consumer and producer handles are
    distinct types, not one object passed across a process boundary.
    """

    name: str
    backend_name: str
    value_type: type[AnyType] | None
    options: ExternalStreamOptions

    def subscribe(self) -> ExternalStreamSubscription[AnyType]:
        """Starts a new subscription and returns its async iterator.

        Each call is an **independent** subscription with its own ``wait_id``,
        cursor, and park intent -- even two calls naming the same stream.
        Delivery is broadcast, so each sees every record from its own cursor.

        ``wait_id`` comes from a per-Run counter in call order, which puts it in
        the same hazard class as timers and activities: inserting, removing, or
        reordering a ``subscribe()`` call renumbers every later wait in the Run
        and must be gated behind ``workflow.patched()``.
        """
        state = _run_state()
        if state.runtime is None:
            raise RuntimeError(
                "external streams are not configured on this Worker; pass "
                "external_stream_backends={...} to the Worker"
            )
        wait_id = state.next_wait_id
        state.next_wait_id += 1

        stream_key = state.runtime.stream_key(self.name)
        state.runtime.register(
            wait_id=wait_id, stream_key=stream_key, backend_name=self.backend_name
        )
        return ExternalStreamSubscription(
            topic=self, wait_id=wait_id, stream_key=stream_key, state=state
        )


class ExternalStreamSubscription(Generic[AnyType]):
    """One subscription's async iterator over decoded values."""

    def __init__(
        self,
        *,
        topic: ExternalStreamTopic[AnyType],
        wait_id: int,
        stream_key: StreamKey,
        state: _RunState,
    ) -> None:
        self._topic = topic
        self._wait_id = wait_id
        self._stream_key = stream_key
        self._state = state
        self._ready: list[StreamRecord] = []
        self._finished = False

    @property
    def wait_id(self) -> int:
        return self._wait_id

    @property
    def stream_key(self) -> StreamKey:
        return self._stream_key

    @property
    def idle_timeout(self) -> timedelta:
        return self._topic.options.idle_timeout

    def __aiter__(self) -> AsyncIterator[AnyType]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[AnyType]:
        while not self._finished:
            if not self._ready:
                # Drain first, then block. Draining is a buffer pop and never
                # touches the backend -- the record is already here or it is
                # not, and if it is not, only Core can say when to look again.
                self._ready = [
                    record
                    for record in self._state.runtime.drain(self._wait_id)  # type: ignore[union-attr]
                    if not record.is_control
                ]
            if self._ready:
                yield await self._decode(self._ready.pop(0))
                continue
            await self._await_readiness()

    async def _await_readiness(self) -> None:
        """Blocks until the readiness activation resolves this wait.

        The future is resolved from the ``ResolveExternalStreamWaits`` branch of
        the activation dispatch, which is the only thing that knows a record
        arrived.
        """
        assert self._state.runtime is not None
        future = self._state.runtime.new_readiness_future()
        self._state.pending[self._wait_id] = future
        try:
            await future
        finally:
            self._state.pending.pop(self._wait_id, None)

    async def _decode(self, record: StreamRecord) -> AnyType:
        assert self._state.runtime is not None
        codec = self._state.runtime.codec_for(self._topic.value_type)
        return await codec.decode(record.payload)


#: The entry point Workflow code uses.
external_stream = ExternalStreamOptions()
