"""A sink Workflow code can write its own observations to.

A Workflow's return value is only visible for the *live* run: the ``Replayer``
runs the same code and reports whether it failed, not what it produced. So a
test that wants to compare what a predicate saw live against what it saw on
replay needs somewhere outside the Workflow to put it.

This module is imported through :py:func:`workflow.unsafe.imports_passed_through`,
so Workflow code inside the sandbox reaches the *same* module object the test
does. Recording is append-only per Run: the live execution appends first and the
replay appends second, which is what makes the two comparable at all.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

#: ``run_id -> [observations of each execution, in the order they ran]``.
#:
#: Deliberately untyped in its element: what is worth comparing between a live
#: run and its replay differs per test -- the states a predicate saw, the
#: records that were delivered -- and narrowing it here would only push a cast
#: into every caller.
OBSERVED: dict[str, list[list[Any]]] = {}


def record(run_id: str, states: Sequence[Any]) -> None:
    """Appends one execution's observed sequence for a Run."""
    OBSERVED.setdefault(run_id, []).append(list(states))


def executions(run_id: str) -> list[list[Any]]:
    return OBSERVED.get(run_id, [])
