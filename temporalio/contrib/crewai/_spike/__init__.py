"""Phase 0 Spike: Validate core assumptions about CrewAI + Temporal integration.

This spike validates:
1. LLM calls can be routed through Temporal activities
2. Tool calls are returned correctly (not executed in activity)
3. Tools can be executed as separate activities
4. Memory operations work through activities

Run with: pytest temporalio/contrib/crewai/_spike/
"""
