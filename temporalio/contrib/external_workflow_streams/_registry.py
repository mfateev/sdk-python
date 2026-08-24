"""The Worker's named-backend registry (P17).

Backend instances hold connections and credentials, live outside the Workflow
sandbox, and are referenced from Workflow code **by name only**::

    Worker(
        ...,
        external_stream_backends={"tokens-redis": RedisStreamBackend(url=...)},
    )

This is also the **single enforcement point** for the design's central
precondition. A provider that does not declare
``guarantees_immutability = True`` is rejected here -- at Worker construction,
loudly, before any Workflow can name it -- rather than at replay, quietly, after
data has already been consumed and a cursor committed against it.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping

from temporalio.contrib.external_workflow_streams._backend import StreamBackend

__all__ = ["ExternalStreamBackendRegistry"]


class ExternalStreamBackendRegistry(Mapping[str, StreamBackend]):
    """Named provider instances, validated at construction."""

    def __init__(self, backends: Mapping[str, StreamBackend]) -> None:
        """Validate and copy the Worker's named backend mapping."""
        for name, backend in backends.items():
            _validate(name, backend)
        self._backends = dict(backends)

    def __getitem__(self, name: str) -> StreamBackend:
        """Return a backend by Workflow-visible name."""
        try:
            return self._backends[name]
        except KeyError:
            known = ", ".join(sorted(self._backends)) or "<none>"
            raise KeyError(
                f"no external stream backend named {name!r} is registered on this "
                f"Worker; registered backends are: {known}"
            ) from None

    def __iter__(self) -> Iterator[str]:
        """Iterate registered backend names."""
        return iter(self._backends)

    def __len__(self) -> int:
        """Return the number of registered backends."""
        return len(self._backends)

    def __repr__(self) -> str:
        """Return a diagnostic representation containing backend names only."""
        return f"ExternalStreamBackendRegistry({sorted(self._backends)!r})"


def _validate(name: str, backend: object) -> None:
    if not name:
        raise ValueError("an external stream backend needs a non-empty name")
    if not isinstance(backend, StreamBackend):
        raise TypeError(
            f"external stream backend {name!r} is a {type(backend).__name__}, which "
            f"is not a {StreamBackend.__name__}"
        )

    declared = type(backend).guarantees_immutability
    if declared is not True:
        # Deliberately one message for "forgot" and "cannot": both mean replay
        # cannot rely on a record's bytes being the bytes that were written, and
        # the four cheap range checks are only sufficient because it can.
        raise ValueError(
            f"external stream backend {name!r} ({type(backend).__name__}) declares "
            f"guarantees_immutability = {declared!r}. Registration requires True: "
            "every provider must guarantee that a record's bytes cannot change "
            "once written, because replay validates presence, count, order, and "
            "control positions only. A provider that cannot make that guarantee "
            "does not satisfy the backend contract."
        )

    if not type(backend).provider_id:
        raise ValueError(
            f"external stream backend {name!r} ({type(backend).__name__}) declares "
            "no provider_id; every replay annotation header records it, so replay "
            "could not tell which provider wrote a marker"
        )
