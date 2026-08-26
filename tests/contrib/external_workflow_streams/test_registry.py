"""P17 — validation of the Worker's stream backend."""

from __future__ import annotations

import uuid

import pytest

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.external_workflow_streams._backend import (
    _validate_backend,
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


# --- backend validation -----------------------------------------------------


def test_a_conforming_backend_is_accepted() -> None:
    backend = MemoryStreamBackend()

    assert _validate_backend(backend) is backend


@pytest.mark.parametrize(
    "backend_type", [UndeclaredBackend, DeniedBackend], ids=["undeclared", "denied"]
)
def test_a_backend_without_the_guarantee_is_rejected(
    backend_type: type[MemoryStreamBackend],
) -> None:
    """One message for "forgot" and "cannot": both break the same thing.

    Replay validates presence, count, order, and control positions only, and
    that is sufficient *because* the bytes cannot change. A provider that
    cannot promise it has not satisfied the contract, however it got there.
    """
    with pytest.raises(ValueError) as caught:
        _validate_backend(backend_type())

    message = str(caught.value)
    assert "guarantees_immutability" in message
    assert "cannot change" in message


def test_a_backend_without_a_provider_id_is_rejected() -> None:
    with pytest.raises(ValueError, match="no provider_id"):
        _validate_backend(AnonymousBackend())


def test_a_non_backend_is_rejected() -> None:
    with pytest.raises(TypeError, match="not a StreamBackend"):
        _validate_backend(object())


# --- through Worker construction --------------------------------------------


async def test_a_conforming_backend_registers_on_a_worker(client: Client) -> None:
    async with Worker(
        client,
        task_queue=f"tq-{uuid.uuid4()}",
        workflows=[NoOpWorkflow],
        external_stream_backend=MemoryStreamBackend(),
    ) as worker:
        assert isinstance(worker._external_stream_backend, MemoryStreamBackend)


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
            external_stream_backend=DeniedBackend(),
        )


async def test_no_backend_is_the_default(client: Client) -> None:
    async with Worker(
        client, task_queue=f"tq-{uuid.uuid4()}", workflows=[NoOpWorkflow]
    ) as worker:
        assert worker._external_stream_backend is None


# --- the sandbox ------------------------------------------------------------


def test_the_sandbox_refuses_a_direct_provider_import() -> None:
    """Workflow code never constructs the configured backend.

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
    assert "external_stream_backend=... instead" in (
        streams.children["_redis"].leaf_message or ""
    )
