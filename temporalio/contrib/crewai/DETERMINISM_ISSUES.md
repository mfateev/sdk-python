# CrewAI Determinism Issues for Temporal Workflow Integration

This document catalogs operations in CrewAI that could break Temporal workflow execution.

## Key Principle

**The only rule that matters: All I/O must happen in activities.**

Non-deterministic data (timestamps, UUIDs, etc.) in activity inputs is perfectly fine because:
1. Activity inputs are recorded in workflow history
2. Activities are not re-executed on replay
3. The recorded output is returned on replay

Things like logging timestamps, trace IDs, and diagnostic output do NOT affect workflow determinism because they don't influence the workflow execution path.

---

## Actually Dangerous Issues

### 1. `human_input=True` on Tasks

**Severity**: 🔴 Critical - Blocks workflow thread indefinitely

**Location**: `crewai/agents/agent_builder/base_agent_executor_mixin.py:168`

```python
def _ask_human_input(self, final_answer: str) -> str:
    # ... prints prompt ...
    response = input()  # BLOCKS FOREVER
    return response
```

**Why it's dangerous**: `input()` blocks the workflow thread waiting for stdin, which will never come in a Temporal worker. The workflow will hang indefinitely.

**Mitigation**: Always set `human_input=False` on all tasks.

```python
task = Task(
    description="...",
    human_input=False,  # REQUIRED for Temporal workflows
)
```

---

### 2. `planning=True` on Crew

**Severity**: 🔴 Critical - Untracked LLM call outside activities

**Location**: `crewai/utilities/planning_handler.py:57-78`

```python
def _handle_crew_planning(self) -> PlannerTaskPydanticOutput:
    planning_agent = self._create_planning_agent()  # Creates new Agent with its own LLM
    planner_task = self._create_planner_task(...)
    result = planner_task.execute_sync()  # LLM call NOT through llm_stub!
    return result.pydantic
```

**Why it's dangerous**: Planning creates a separate Agent with its own LLM (`planning_agent_llm`, defaults to `"gpt-4o-mini"`). This LLM call happens outside of any activity, so it's:
- Not recorded in workflow history
- Not retried on failure
- Not visible in Temporal UI

**Mitigation Options**:

1. **Disable planning** (simplest):
   ```python
   crew = Crew(
       agents=[...],
       tasks=[...],
       planning=False,  # REQUIRED for Temporal workflows
   )
   ```

2. **Use llm_stub for planning LLM** (if planning is needed):
   ```python
   crew = Crew(
       agents=[...],
       tasks=[...],
       planning=True,
       planning_llm=llm_stub("gpt-4o"),  # Routes through activities
   )
   ```

---

### 3. `max_rpm` with `time.sleep(60)`

**Severity**: 🟠 High - Blocks workflow thread for 60 seconds

**Location**: `crewai/utilities/rpm_controller.py:73-75`

```python
def _wait_for_next_minute(self) -> None:
    time.sleep(60)  # BLOCKS WORKFLOW THREAD
    self._current_rpm = 0
```

**Why it's dangerous**: When the RPM limit is reached, `time.sleep(60)` blocks the workflow thread. This:
- Prevents workflow from processing other events
- Could cause workflow task timeouts
- Is not a deterministic timer (not recorded in history)

**Mitigation**: Don't set `max_rpm` on agents or crews when running in Temporal workflows.

```python
# DON'T do this in Temporal workflows:
agent = Agent(
    role="...",
    max_rpm=10,  # BAD - will cause time.sleep() blocking
)

# DO this instead:
agent = Agent(
    role="...",
    # max_rpm not set - no blocking sleep
)
```

Note: Rate limiting should be handled at the activity level if needed, not in the workflow.

---

### 4. Hooks that call `input()`

**Severity**: 🔴 Critical - Blocks workflow thread indefinitely

**Locations**:
- `crewai/hooks/tool_hooks.py:104` - `ToolCallHookContext.request_human_input()`
- `crewai/hooks/llm_hooks.py:141` - Similar method

**Why it's dangerous**: If user-registered hooks call `request_human_input()`, they will call `input()` which blocks forever.

**Mitigation**: Don't register hooks that call `input()` or `request_human_input()` when running in Temporal workflows.

---

## NOT Dangerous (Safe to Use)

These are often flagged as determinism issues but are actually safe when using the integration properly:

### `inject_date=True` on Agents ✅ SAFE

The date is injected into the task description, which becomes part of the LLM activity input. Since activity inputs are recorded in history, this is deterministic on replay.

### `datetime.now()` in logging/telemetry ✅ SAFE

Timestamps in logs, traces, and diagnostics don't affect workflow execution paths.

### `uuid.uuid4()` for IDs ✅ SAFE

UUIDs are safe because CrewAI's `akickoff()` execution path does NOT use UUIDs for lookups that affect execution flow:

| Usage | Implementation | Why Safe |
|-------|----------------|----------|
| Task iteration | `tasks[i]` (list index) | Index-based, not ID-based |
| Task mapping | `task.key` = MD5(description) | Deterministic hash |
| Context lookup | `task_outputs[index]` | Index-based |
| Telemetry/events | `task.id` | Logging only, doesn't affect flow |

**ID-based lookups exist but are NOT in `akickoff()` path:**
- `_find_task_index(task_id)` - Only in `crew.replay()` (CrewAI's replay, not Temporal's)
- `training_data.get(agent.id)` - Only in `crew.train()`

Since we use `akickoff()` and Temporal's replay mechanism (not CrewAI's), UUID non-determinism doesn't affect workflow execution.

### `verbose=True` ✅ SAFE

Console output is a side effect but doesn't affect workflow execution.

### `asyncio.gather()` and `asyncio.create_task()` ✅ SAFE

Per Temporal Python SDK, these are deterministic in the workflow event loop.

### `asyncio.sleep()` ✅ SAFE

Automatically mapped to deterministic workflow timers.

### Memory operations via stubs ✅ SAFE

When using `short_term_memory_stub()`, `long_term_memory_stub()`, etc., all memory I/O happens in activities.

### Tool execution via `activity_as_tool()` ✅ SAFE

Tools wrapped with `activity_as_tool()` execute as activities.

### LLM calls via `llm_stub()` ✅ SAFE

LLM calls are routed to activities.

---

## Validation Checklist

Before running a CrewAI crew in a Temporal workflow, verify:

- [ ] `human_input=False` on all tasks
- [ ] `planning=False` on crew (or `planning_llm=llm_stub(...)`)
- [ ] `max_rpm` is NOT set on agents or crew
- [ ] No custom hooks that call `input()` or `request_human_input()`
- [ ] All agents use `llm_stub()` for their LLM
- [ ] All tools are created with `activity_as_tool()`
- [ ] Memory stubs are used if `memory=True`

---

## Summary Table

| Issue | Dangerous? | Why | Mitigation |
|-------|------------|-----|------------|
| `human_input=True` | 🔴 Yes | `input()` blocks forever | Set `human_input=False` |
| `planning=True` | 🔴 Yes | Untracked LLM call | Set `planning=False` or use `planning_llm=llm_stub()` |
| `max_rpm` set | 🟠 Yes | `time.sleep(60)` blocks | Don't set `max_rpm` |
| Hooks with `input()` | 🔴 Yes | `input()` blocks forever | Don't use such hooks |
| `inject_date=True` | ✅ No | Part of activity input | Safe to use |
| `verbose=True` | ✅ No | Just console output | Safe to use |
| `datetime.now()` in logs | ✅ No | Doesn't affect execution | Safe to use |
| `uuid.uuid4()` for IDs | ✅ No | `akickoff()` uses index-based iteration, not ID lookups | Safe to use |
