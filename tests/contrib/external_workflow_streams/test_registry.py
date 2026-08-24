"""P17 — the Worker's named-backend registry and its one precondition."""

from __future__ import annotations

import uuid

import pytest

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.external_workflow_streams._registry import (
    ExternalStreamBackendRegistry,
)
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox._restrictions import SandboxRestrictions
from tests.contrib.external_workflow_streams.memory_backend import MemoryStreamBackend


# These tests exercise Worker construction, not the sandbox. Keeping this
# unsandboxed avoids re-importing this pytest assertion-rewritten module while
# the constructor validates its registered workflows.
@workflow.defn(sandboxed=False)
class NoOpWorkflow:
    """Workers need at least one registered workflow to be constructible."""

    @workflow.run
    async def run(self) -> None:
        pass


class UndeclaredBackend(MemoryStreamBackend):
    """Never considered whether its records can change."""

    guarantees_immutability = None


class DeniedBackend(MemoryStreamBackend):
    """Considered it and cannot make the guarantee."""

    guarantees_immutability = False


class AnonymousBackend(MemoryStreamBackend):
    """Conforming, but nameless in the annotation header."""

    provider_id = ""


# --- the registry itself ----------------------------------------------------


def test_a_conforming_backend_registers() -> None:
    registry = ExternalStreamBackendRegistry({"tokens": MemoryStreamBackend()})

    assert set(registry) == {"tokens"}
    assert isinstance(registry["tokens"], MemoryStreamBackend)


@pytest.mark.parametrize(
    "backend_type", [UndeclaredBackend, DeniedBackend], ids=["undeclared", "denied"]
)
def test_a_backend_without_the_guarantee_is_rejected_by_name(
    backend_type: type[MemoryStreamBackend],
) -> None:
    """One message for "forgot" and "cannot": both break the same thing.

    Replay validates presence, count, order, and control positions only, and
    that is sufficient *because* the bytes cannot change. A provider that
    cannot promise it has not satisfied the contract, however it got there.
    """
    with pytest.raises(ValueError) as caught:
        ExternalStreamBackendRegistry({"tokens": backend_type()})

    message = str(caught.value)
    assert "guarantees_immutability" in message
    assert "cannot change" in message
    assert "tokens" in message


def test_a_backend_without_a_provider_id_is_rejected() -> None:
    with pytest.raises(ValueError, match="no provider_id"):
        ExternalStreamBackendRegistry({"tokens": AnonymousBackend()})


def test_a_non_backend_is_rejected() -> None:
    with pytest.raises(TypeError, match="not a StreamBackend"):
        ExternalStreamBackendRegistry({"tokens": object()})  # type: ignore[dict-item]


def test_an_unnamed_backend_is_rejected() -> None:
    with pytest.raises(ValueError, match="non-empty name"):
        ExternalStreamBackendRegistry({"": MemoryStreamBackend()})


def test_naming_an_unregistered_backend_lists_what_is_registered() -> None:
    registry = ExternalStreamBackendRegistry(
        {"tokens": MemoryStreamBackend(), "events": MemoryStreamBackend()}
    )

    with pytest.raises(KeyError) as caught:
        registry["typo"]

    assert "events, tokens" in str(caught.value)


def test_one_bad_backend_rejects_the_whole_registry() -> None:
    """Partial registration would leave a Worker in a state nobody asked for."""
    with pytest.raises(ValueError):
        ExternalStreamBackendRegistry(
            {"good": MemoryStreamBackend(), "bad": DeniedBackend()}
        )


# --- through Worker construction --------------------------------------------


async def test_a_conforming_backend_registers_on_a_worker(client: Client) -> None:
    async with Worker(
        client,
        task_queue=f"tq-{uuid.uuid4()}",
        workflows=[NoOpWorkflow],
        external_stream_backends={"tokens": MemoryStreamBackend()},
    ) as worker:
        assert worker._external_stream_backends is not None
        assert set(worker._external_stream_backends) == {"tokens"}


async def test_a_backend_without_the_guarantee_fails_worker_construction(
    client: Client,
) -> None:
    """Loudly, at construction -- not quietly, at replay.

    By replay time a cursor has already been committed against data the
    provider could have changed, and there is nothing left to do about it.
    """
    with pytest.raises(ValueError, match="guarantees_immutability"):
        Worker(
            client,
            task_queue=f"tq-{uuid.uuid4()}",
            workflows=[NoOpWorkflow],
            external_stream_backends={"tokens": DeniedBackend()},
        )


async def test_no_backends_is_the_default(client: Client) -> None:
    async with Worker(
        client, task_queue=f"tq-{uuid.uuid4()}", workflows=[NoOpWorkflow]
    ) as worker:
        assert worker._external_stream_backends is None


# --- the sandbox ------------------------------------------------------------


def test_the_sandbox_refuses_a_direct_provider_import() -> None:
    """Workflow code names a backend; it never constructs one.

    A provider reached from inside the sandbox would be a second, unregistered
    instance -- its own connection, no watcher owning it, and none of the
    registration checks applied to it.
    """
    matcher = SandboxRestrictions.invalid_module_members_default
    contrib = matcher.children["temporalio"].children["contrib"]
    streams = contrib.children["external_workflow_streams"]

    assert streams.children["_redis"].match_self
    assert "RedisStreamBackend" in streams.access

    from temporalio.worker.workflow_sandbox._restrictions import RestrictionContext

    context = RestrictionContext()
    context.is_runtime = True
    assert streams.children["_redis"].match_access(context)
    assert "name it from the Workflow instead" in (
        streams.children["_redis"].leaf_message or ""
    )
