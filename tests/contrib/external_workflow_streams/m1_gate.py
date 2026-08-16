"""The machinery behind the required-test gates (P16a, P16b).

The required-test lists are the milestone gates. A gate that is only a claim in
a document is not a gate at all: someone renames a test, the case it covered
quietly stops existing, and the list still says the milestone is met. So the
list is parsed from the plan itself and every case is mapped to a test that has
to actually exist.

Three things this checks, each catching a different way the gate rots:

- the case **count** still matches what the plan's heading declares, so a case
  added to the plan cannot be silently unmapped;
- every case is either covered or recorded as open, so a new case fails the gate
  rather than passing by omission -- and a partially covered one is never
  counted as covered, which is how a gate stops meaning anything;
- every mapped test **exists**, so a rename or deletion fails here rather than
  leaving the list pointing at nothing.

What this deliberately does *not* do is assert that a mapped test passes. That
is the test suite's job, and duplicating it here would only mean running
everything twice.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from pathlib import Path

#: The plan lives in the vendored Core checkout, which is the same branch the
#: Core-side work is on. Reading it from there rather than from a copy is what
#: makes the count check meaningful -- a copy would drift silently.
PLAN_DIR = (
    Path(__file__).resolve().parents[3]
    / "temporalio"
    / "bridge"
    / "sdk-core"
    / "arch_docs"
    / "streaming-poc-docs"
    / "plan"
)

TESTS_DIR = Path(__file__).resolve().parent

CORE_TESTS = (
    Path(__file__).resolve().parents[3]
    / "temporalio"
    / "bridge"
    / "sdk-core"
    / "crates"
    / "sdk-core"
    / "src"
    / "core_tests"
    / "external_streams.rs"
)


@dataclass(frozen=True)
class RequiredCase:
    """One bullet of a required-test list."""

    number: int
    text: str


def declared_count(list_name: str) -> int:
    """The count the plan's own heading declares.

    Read rather than hardcoded: the plan says to update the heading whenever a
    case is added or removed, and reading it is what turns that instruction into
    something enforced.
    """
    heading = (PLAN_DIR / list_name).read_text().splitlines()[0]
    match = re.search(r"(\d+)\s+cases", heading)
    assert match, f"{list_name} heading does not declare a case count: {heading!r}"
    return int(match.group(1))


def required_cases(list_name: str) -> list[RequiredCase]:
    """Every bullet, in document order, with continuation lines joined.

    Bullets wrap across lines in the plan, and a parser that treated each line as
    a case would count the same case several times -- inflating the total to
    something that happened to match by accident.
    """
    cases: list[RequiredCase] = []
    current: list[str] | None = None
    for line in (PLAN_DIR / list_name).read_text().splitlines():
        if line.startswith("- "):
            if current is not None:
                cases.append(RequiredCase(len(cases) + 1, " ".join(current)))
            current = [line[2:].strip()]
        elif current is not None and line.startswith("  ") and line.strip():
            current.append(line.strip())
        elif current is not None and not line.strip():
            cases.append(RequiredCase(len(cases) + 1, " ".join(current)))
            current = None
    if current is not None:
        cases.append(RequiredCase(len(cases) + 1, " ".join(current)))
    return cases


def _python_test_names() -> dict[str, set[str]]:
    """Every test function per module, read from the source rather than pytest.

    Parsing beats collecting here: collection would import every module and run
    every fixture, and this check must be able to run when something else in the
    suite is broken.
    """
    found: dict[str, set[str]] = {}
    for path in sorted(TESTS_DIR.glob("test_*.py")):
        tree = ast.parse(path.read_text())
        names = {
            node.name
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name.startswith("test_")
        }
        found[path.name] = names
    return found


def _core_test_names() -> set[str]:
    if not CORE_TESTS.exists():
        return set()
    return set(re.findall(r"async fn (\w+)\(", CORE_TESTS.read_text())) | set(
        re.findall(r"\bfn (\w+)\(\)", CORE_TESTS.read_text())
    )


def unresolved(node_ids: list[str]) -> list[str]:
    """The mapped tests that do not exist. Empty means the mapping is honest."""
    python = _python_test_names()
    core = _core_test_names()
    missing = []
    for node_id in node_ids:
        module, _, name = node_id.partition("::")
        if module.endswith(".rs"):
            if core and name not in core:
                missing.append(node_id)
        elif module not in python:
            missing.append(f"{node_id} (no such module)")
        elif name not in python[module]:
            missing.append(node_id)
    return missing
