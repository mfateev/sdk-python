"""P7 — the readiness call and the run-status probe, reachable from Python.

PYTEST_DONT_REWRITE: sandboxed fixture Workflows re-import this module, so pytest's
injected imports would make sandbox validation depend on pytest's import locks.
"""

from __future__ import annotations

import asyncio
import threading
import uuid

import pytest

from temporalio import workflow
from temporalio.bridge.worker import (
    ExternalStreamReadyResult,
    ExternalStreamRunStatus,
)
from temporalio.client import Client
from temporalio.worker import Worker


@workflow.defn
class IdleWorkflow:
    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: False)


def bridge_worker(worker: Worker):  # type: ignore[no-untyped-def]
    """The bridge worker underneath a Python Worker."""
    return worker._workflow_worker._bridge_worker()  # type: ignore[union-attr]


async def _wait_for_first_task_completed(handle, timeout: float = 30) -> None:  # type: ignore[no-untyped-def]
    """Waits until this Worker has run the Workflow's first task and cached the Run.

    Polled rather than slept: how long a Worker takes to poll, execute, and
    complete one Workflow Task is a property of the machine, and a fixed wait is a
    guess about it that a loaded machine invalidates.
    """
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        events = [event async for event in handle.fetch_history_events()]
        if any(e.HasField("workflow_task_completed_event_attributes") for e in events):
            return
        await asyncio.sleep(0.05)
    raise AssertionError(
        "the Workflow's first task never completed, so the Run was never cached "
        "and the probe has no settled state to be repeatable about"
    )


@pytest.fixture
def unknown_run_id() -> str:
    return f"no-such-run-{uuid.uuid4()}"


async def test_readiness_for_an_unknown_run_returns_rather_than_raising(
    client: Client, unknown_run_id: str
) -> None:
    """A watcher must be able to *handle* a missing Run, not catch an exception.

    ``RunNotFound`` is a normal, expected answer -- it is what a watcher sees
    every time its Run is evicted -- so surfacing it as an exception would put
    ordinary lifecycle in the error path.
    """
    async with Worker(
        client, task_queue=f"tq-{uuid.uuid4()}", workflows=[IdleWorkflow]
    ) as worker:
        bridge = bridge_worker(worker)

        result = await bridge.notify_external_stream_ready(unknown_run_id, 1, 0)

        assert result is ExternalStreamReadyResult.RUN_NOT_FOUND
        assert result.needs_wake_signal


async def test_status_for_an_unknown_run_returns_rather_than_raising(
    client: Client, unknown_run_id: str
) -> None:
    async with Worker(
        client, task_queue=f"tq-{uuid.uuid4()}", workflows=[IdleWorkflow]
    ) as worker:
        status = await bridge_worker(worker).external_stream_run_status(unknown_run_id)

        assert status is ExternalStreamRunStatus.RUN_NOT_FOUND


async def test_both_calls_surface_their_full_result_enums(
    client: Client,  # pyright: ignore[reportUnusedParameter]
) -> None:
    """The five and four values reach Python as values, not strings.

    A watcher branches on all five, and a stringly-typed result would let a
    typo silently take the "do nothing" branch for a Run that needed a Signal.
    """
    assert {r.value for r in ExternalStreamReadyResult} == {
        "Accepted",
        "Stale",
        "Parked",
        "NoOpenWorkflowTask",
        "RunNotFound",
    }
    assert {s.value for s in ExternalStreamRunStatus} == {
        "WftOpen",
        "Parked",
        "NoOpenWorkflowTask",
        "RunNotFound",
    }


async def test_the_readiness_call_is_safe_from_several_threads(
    client: Client, unknown_run_id: str
) -> None:
    """One watcher per subscription means concurrent callers by construction.

    Each thread drives its own event loop, so this exercises the call from
    genuinely separate threads rather than from concurrent tasks on one. The
    joins run off the main loop for the reason
    :py:meth:`notify_external_stream_ready_sync` documents: blocking the loop
    that owns the Worker would stop it polling, and the answer comes from a
    lane that Worker drives.
    """
    async with Worker(
        client, task_queue=f"tq-{uuid.uuid4()}", workflows=[IdleWorkflow]
    ) as worker:
        bridge = bridge_worker(worker)
        # One call first, so the Worker is provably polling before anything
        # blocks -- otherwise this test could pass or hang on timing alone.
        assert (
            await bridge.notify_external_stream_ready(unknown_run_id, 1, 0)
        ) is ExternalStreamReadyResult.RUN_NOT_FOUND

        results: list[object] = []
        errors: list[BaseException] = []
        lock = threading.Lock()

        def call_from_own_loop(wait_id: int) -> None:
            async def go() -> None:
                for _ in range(5):
                    got = await bridge.notify_external_stream_ready(
                        unknown_run_id, wait_id, 0
                    )
                    with lock:
                        results.append(got)

            try:
                asyncio.run(go())
            except BaseException as err:  # noqa: BLE001 -- recorded and asserted on
                with lock:
                    errors.append(err)

        def run_all_threads() -> None:
            threads = [
                threading.Thread(target=call_from_own_loop, args=(wait_id,))
                for wait_id in range(1, 9)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=20)
            with lock:
                errors.extend(
                    RuntimeError(f"thread {i} did not finish")
                    for i, thread in enumerate(threads)
                    if thread.is_alive()
                )

        await asyncio.to_thread(run_all_threads)

        assert not errors, f"concurrent readiness calls failed: {errors}"
        assert len(results) == 8 * 5
        assert all(r is ExternalStreamReadyResult.RUN_NOT_FOUND for r in results)


async def test_the_status_probe_is_repeatable(client: Client) -> None:
    """Asking must change nothing, however many times it is asked.

    The Run has to be **settled** before the first question, or the test is about
    something else. `start_workflow` returns when the server accepts the Workflow,
    which is before this Worker has polled it, run its first Workflow Task, and
    cached the Run -- and until that has happened the probe honestly answers
    `RunNotFound`. Answers of `RunNotFound` then `NoOpenWorkflowTask` are Core
    telling the truth twice about two different states, not a probe that changed
    something.

    So the first Workflow Task's completion is awaited first. `IdleWorkflow` blocks
    forever, so after it the Run is cached with no task open, and that state does
    not move again on its own -- which is what makes "the same answer every time" a
    property of the probe rather than of the timing.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client, task_queue=task_queue, workflows=[IdleWorkflow]
    ) as worker:
        handle = await client.start_workflow(
            IdleWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        try:
            description = await handle.describe()
            bridge = bridge_worker(worker)

            await _wait_for_first_task_completed(handle)
            settled = await bridge.external_stream_run_status(description.run_id)
            assert settled is ExternalStreamRunStatus.NO_OPEN_WORKFLOW_TASK, (
                "the Run is cached and blocked with no Workflow Task open, so this "
                f"is the only honest answer; got {settled}"
            )

            answers = [
                await bridge.external_stream_run_status(description.run_id)
                for _ in range(5)
            ]

            # No subscriptions exist, so the honest answer is the same one every
            # time -- and in particular the probe never manufactured a task.
            assert answers == [settled] * 5
        finally:
            await handle.terminate()
