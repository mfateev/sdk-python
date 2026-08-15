"""External Workflow Streams for Temporal Workflows.

.. warning::
    This package is experimental, incomplete, and **exports nothing yet**.
    The public API lands with Milestone 1 (ADR-024); until then everything
    lives under private module names and is reachable only from tests.

High-volume stream payloads live in a pluggable external backend such as Redis
Streams and never enter Temporal History. Deterministic replay is preserved
with compact markers recording consumed offset ranges and the
availability/blocking boundaries the original execution observed.

This is a **mirror image** of the shipped
:py:mod:`temporalio.contrib.workflow_streams`, not a replacement for it, and
the two coexist (ADR-001). No name here may begin with
``__temporal_workflow_stream``.
"""

__all__: list[str] = []
