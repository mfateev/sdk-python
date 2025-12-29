# CrewAI Determinism Issues for Temporal Workflow Integration

This document catalogs all non-deterministic operations in CrewAI code that could break Temporal workflow replay. Each issue is categorized by severity and includes the source location and recommended mitigation.

> **Note**: Asyncio safety information is based on Temporal Python SDK sandbox restrictions
> (see `temporalio/worker/workflow_sandbox/_restrictions.py`). Key findings:
> - ✅ `asyncio.create_task()`, `asyncio.gather()`, `asyncio.sleep()` are **safe**
> - ⚠️ `asyncio.as_completed()`, `asyncio.wait()` are **non-deterministic** (use `workflow.as_completed()`, `workflow.wait()`)

## Summary

The CrewAI framework contains numerous operations that are incompatible with Temporal workflow determinism requirements. These operations must be either:
1. **Moved to activities** (for I/O operations)
2. **Replaced with workflow-safe alternatives** (for time/random operations)
3. **Disabled or intercepted** (for telemetry/logging)

---

## Category 1: Time Operations (Critical)

### 1.1 `datetime.now()` Usage

**Severity**: 🔴 Critical - Breaks replay determinism

**Locations**:
- `lib/crewai/src/crewai/agent/core.py:1457` - Date injection into task description
  ```python
  current_date = datetime.now().strftime(self.date_format)
  task.description += f"\n\nCurrent Date: {current_date}"
  ```
- `lib/crewai/src/crewai/task.py:527,576,600,619,669,693` - Task start/end time tracking
- `lib/crewai/src/crewai/llm.py:1531,1550` - LLM call timing
- `lib/crewai/src/crewai/mcp/client.py:155,222,335,450,468,487` - MCP connection/tool timing
- `lib/crewai/src/crewai/utilities/file_handler.py:93` - File logging timestamps
- `lib/crewai/src/crewai/utilities/logger.py:28` - Logger timestamps

**Mitigation**: Replace with `workflow.now()` or move timing logic to activities.

### 1.2 `time.time()` Usage

**Severity**: 🔴 Critical - Breaks replay determinism

**Locations**:
- `lib/crewai/src/crewai/tools/tool_usage.py:261,448,880` - Tool execution timing
- `lib/crewai/src/crewai/memory/short_term/short_term_memory.py:87,107,155,168,211,230,278,291` - Memory operation timing
- `lib/crewai/src/crewai/memory/long_term/long_term_memory.py:49,68,112,122,160,179,223,233` - LTM timing
- `lib/crewai/src/crewai/memory/entity/entity_memory.py:102,144,202,215,264,306,364,377` - Entity memory timing
- `lib/crewai/src/crewai/memory/external/external_memory.py:78,92,140,153,196,210,258,271` - External memory timing
- `lib/crewai/src/crewai/agent/core.py:332,351,541,562,1141` - Agent memory and MCP timing
- `lib/crewai/src/crewai/agents/agent_builder/base_agent_executor_mixin.py:98` - Training timing

**Mitigation**: Replace with `workflow.now()` for workflow-relevant timing, or move to activities.

---

## Category 2: UUID Generation (Critical)

### 2.1 `uuid.uuid4()` Usage

**Severity**: 🔴 Critical - Breaks replay determinism

**Locations**:
- `lib/crewai/src/crewai/task.py:150` - Task ID generation (`default_factory=uuid.uuid4`)
- `lib/crewai/src/crewai/crew.py:216` - Crew ID generation (`default_factory=uuid.uuid4`)
- `lib/crewai/src/crewai/agents/agent_builder/base_agent.py:119` - Agent ID generation
- `lib/crewai/src/crewai/lite_agent.py:100,153` - Lite agent ID generation
- `lib/crewai/src/crewai/a2a/utils.py:440,543,612` - A2A message ID generation
- `lib/crewai/src/crewai/events/listeners/tracing/*.py` - Various trace ID generation

**Mitigation**:
- Use `workflow.uuid4()` for workflow-safe UUID generation
- Or pre-generate IDs before workflow execution and pass them as inputs

---

## Category 3: I/O Operations (Critical)

### 3.1 Network/HTTP Calls

**Severity**: 🔴 Critical - Must be in activities

**Locations**:
- `lib/crewai/src/crewai/llm.py` - All LLM API calls via httpx
- `lib/crewai/src/crewai/llms/providers/anthropic/completion.py` - Anthropic API
- `lib/crewai/src/crewai/llms/providers/openai/completion.py` - OpenAI API
- `lib/crewai/src/crewai/a2a/utils.py` - A2A HTTP communications
- `lib/crewai/src/crewai/__init__.py:57-60` - Telemetry pixel URL fetch
- `lib/crewai/src/crewai/mcp/client.py` - MCP server connections

**Note**: The design document already accounts for LLM calls via `llm_stub`. However, MCP tool connections and A2A communications also need activity wrapping.

### 3.2 File System Operations

**Severity**: 🔴 Critical - Must be in activities

**Locations**:
- `lib/crewai/src/crewai/task.py:926-942` - Writing task output to file
- `lib/crewai/src/crewai/utilities/file_handler.py:100-119,156-169` - JSON/pickle file operations
- `lib/crewai/src/crewai/utilities/training_handler.py` - Training data persistence
- `lib/crewai/src/crewai/knowledge/source/*.py` - Reading CSV, PDF, JSON, text files
- `lib/crewai/src/crewai/memory/storage/ltm_sqlite_storage.py` - SQLite database operations
- `lib/crewai/src/crewai/events/listeners/tracing/utils.py:94-113` - User data file operations

**Mitigation**: Move all file I/O to activities. The design doc accounts for memory stubs.

### 3.3 Database Operations

**Severity**: 🔴 Critical - Must be in activities

**Locations**:
- `lib/crewai/src/crewai/memory/storage/ltm_sqlite_storage.py` - SQLite operations
- `lib/crewai/src/crewai/rag/` - Vector database operations (ChromaDB, etc.)

---

## Category 4: Concurrency Operations

### 4.1 `asyncio.create_task()` and `asyncio.gather()` ✅ SAFE

**Severity**: 🟢 Safe - These are deterministic in Temporal's event loop

**Note**: Per Temporal Python SDK documentation, `asyncio.create_task()` and `asyncio.gather()` are **safe** to use in workflows. Temporal's workflow event loop ensures deterministic execution order. The results from `asyncio.gather()` are returned in the same order as the input awaitables.

**Locations** (no action required):
- `lib/crewai/src/crewai/crew.py:950` - Creating async tasks for parallel task execution
- `lib/crewai/src/crewai/crews/utils.py:326,350,354` - Parallel crew execution
- `lib/crewai/src/crewai/memory/contextual/contextual_memory.py:90` - Concurrent memory fetches
- `lib/crewai/src/crewai/events/event_bus.py:232,483` - Async event handling

### 4.2 `asyncio.as_completed()` and `asyncio.wait()` ⚠️ NON-DETERMINISTIC

**Severity**: 🔴 Critical - These are non-deterministic and break replay

**Reason**: These functions return results in completion order, which may vary between executions. The Temporal SDK sandbox issues warnings for these.

**Mitigation**:
- Use `workflow.as_completed()` instead of `asyncio.as_completed()`
- Use `workflow.wait()` instead of `asyncio.wait()`

**Note**: A search of the CrewAI codebase did not find usage of `asyncio.as_completed()` or `asyncio.wait()`. CrewAI primarily uses `asyncio.gather()` which is safe.

### 4.3 Threading Operations

**Severity**: 🟠 High - Incompatible with workflow execution

**Locations**:
- `lib/crewai/src/crewai/utilities/rpm_controller.py:20-22,33,78-88` - Timer threads for RPM limiting
- `lib/crewai/src/crewai/agents/cache/cache_handler.py:20` - RWLock for cache

**Mitigation**:
- RPM limiting should be disabled or handled differently in workflows
- Cache should be replaced with workflow-safe mechanism

---

## Category 5: Sleep Operations

### 5.1 `time.sleep()` ⚠️ BLOCKING

**Severity**: 🔴 Critical - Blocks workflow thread, breaks replay

**Locations**:
- `lib/crewai/src/crewai/utilities/rpm_controller.py:74` - `time.sleep(60)` for RPM waiting

**Mitigation**:
- Use `asyncio.sleep()` or `workflow.sleep()` instead
- Or move blocking operations to activities

### 5.2 `asyncio.sleep()` ✅ SAFE

**Severity**: 🟢 Safe - Automatically mapped to deterministic workflow timers

**Note**: Per Temporal Python SDK, `asyncio.sleep()` is **safe** in workflows. It is automatically converted to a deterministic workflow timer that replays correctly.

**Locations** (no action required):
- `lib/crewai/src/crewai/tools/mcp_tool_wrapper.py:115` - MCP retry backoff
- `lib/crewai/src/crewai/agent/core.py:1004,1012,1198` - MCP cleanup/retry delays
- `lib/crewai/src/crewai/mcp/client.py:684,702` - MCP connection retry
- `lib/crewai/src/crewai/mcp/transports/http.py:125` - HTTP transport cleanup
- `lib/crewai/src/crewai/a2a/auth/utils.py:207` - Auth retry backoff

**Note**: While `asyncio.sleep()` itself is safe, the retry logic around it may still involve non-deterministic I/O operations (like MCP connections) that must be in activities.

---

## Category 6: Console/Print Operations (Medium)

### 6.1 Print and Console Output

**Severity**: 🟡 Medium - Side effects, but doesn't break replay

**Locations**:
- Extensive usage throughout codebase via `Printer` class and `console.print()`
- `lib/crewai/src/crewai/utilities/printer.py:62,82` - Core print functionality
- `lib/crewai/src/crewai/events/utils/console_formatter.py` - Console output formatting
- Various locations for verbose output and debugging

**Mitigation**:
- Disable verbose mode in workflow context
- Redirect output to logging that's activity-safe
- Consider making print operations no-op in workflow context

---

## Category 7: Human Input Operations (Critical)

### 7.1 `input()` Function Calls

**Severity**: 🔴 Critical - Blocks indefinitely, incompatible with workflows

**Locations**:
- `lib/crewai/src/crewai/agents/agent_builder/base_agent_executor_mixin.py:168` - Human feedback input
- `lib/crewai/src/crewai/hooks/tool_hooks.py:104` - Tool confirmation input
- `lib/crewai/src/crewai/hooks/llm_hooks.py:141` - LLM call confirmation input
- `lib/crewai/src/crewai/events/listeners/tracing/utils.py:470` - Tracing consent input

**Mitigation**:
- Use Temporal signals for human-in-the-loop
- Use workflow `wait_condition` to pause for human input
- Disable `human_input` flag on tasks in workflow context

### 7.2 Human Input Task Flag

**Severity**: 🔴 Critical - Triggers blocking input() calls

**Location**: `lib/crewai/src/crewai/task.py:154` - `human_input: bool` field

**Mitigation**: Ensure `human_input=False` for all tasks in Temporal workflows, or implement signal-based human input.

---

## Category 8: Callback Execution (Medium)

### 8.1 User-Provided Callbacks

**Severity**: 🟡 Medium - May contain non-deterministic operations

**Locations**:
- `lib/crewai/src/crewai/task.py:579,583,672,676` - Task callbacks
- `lib/crewai/src/crewai/crew.py:218,222,723,871` - Crew task/step callbacks
- `lib/crewai/src/crewai/agents/crew_agent_executor.py:506-507` - Step callbacks

**Mitigation**:
- Document that callbacks must be deterministic
- Or disable callbacks in workflow context
- Consider moving callback execution to activities

---

## Category 9: Event Bus Operations (Medium)

### 9.1 Event Emission

**Severity**: 🟡 Medium - Side effects for telemetry/tracing

**Locations**:
- Extensive `crewai_event_bus.emit()` calls throughout:
  - Task execution events
  - Agent execution events
  - Memory operations
  - Tool usage events
  - LLM call events

**Mitigation**:
- Disable telemetry/tracing in workflow context
- Or make event handlers no-op

---

## Category 10: Caching (Medium)

### 10.1 In-Memory Cache

**Severity**: 🟡 Medium - Cache state may differ on replay

**Locations**:
- `lib/crewai/src/crewai/agents/cache/cache_handler.py` - Tool result cache
- `lib/crewai/src/crewai/agent/core.py:1141` - MCP schema cache

**Mitigation**:
- Disable caching in workflow context
- Or ensure cache is populated deterministically from activity results

---

## Category 11: Environment Variables (Low)

### 11.1 `os.getenv()` / `os.environ` Access

**Severity**: 🟢 Low - Usually configuration, but could differ between replays

**Locations**: Extensive usage throughout for configuration

**Mitigation**:
- Pass configuration as workflow inputs
- Ensure environment is consistent across workers

---

## Category 12: Planning Operations (Critical)

### 12.1 CrewPlanner LLM Calls

**Severity**: 🔴 Critical - Makes LLM call during planning

**Location**: `lib/crewai/src/crewai/utilities/planning_handler.py:57-77`
```python
def _handle_crew_planning(self) -> PlannerTaskPydanticOutput:
    planning_agent = self._create_planning_agent()
    tasks_summary = self._create_tasks_summary()
    planner_task = self._create_planner_task(...)
    result = planner_task.execute_sync()  # LLM call!
```

**Mitigation**:
- If `planning=True`, the planning LLM call must be wrapped in an activity
- Or disable planning in workflow context

---

## Recommendations for Implementation

### Must Have (for basic functionality)
1. ✅ LLM calls via activities (covered by `llm_stub`)
2. ✅ Memory storage via activities (covered by memory stubs)
3. ✅ Tool execution via activities (covered by `activity_as_tool`)
4. ⚠️ Disable `human_input` on tasks
5. ⚠️ Disable `inject_date` on agents or use `workflow.now()`
6. ⚠️ Disable `planning` or wrap in activity
7. ⚠️ Handle task/crew IDs deterministically

### Should Have (for robust operation)
1. Disable telemetry and event emission in workflow context
2. Disable verbose output and console printing
3. Replace cache with workflow-safe alternative
4. Disable RPM controller or handle differently
5. Make callbacks workflow-safe or disable them

### Nice to Have (for full feature parity)
1. Implement human-in-the-loop via Temporal signals
2. Support streaming via Temporal updates

### Already Safe (no action needed)
1. ✅ Parallel task execution via `asyncio.gather()` / `asyncio.create_task()` - deterministic in Temporal
2. ✅ `asyncio.sleep()` - automatically mapped to workflow timers

---

## Validation Checklist

Before running a CrewAI crew in a Temporal workflow, verify:

- [ ] All agents use `llm_stub()` instead of direct LLM
- [ ] All tools are created with `activity_as_tool()`
- [ ] Memory stubs are used if memory is enabled
- [ ] `human_input=False` on all tasks
- [ ] `inject_date=False` on all agents (or handled via workflow.now())
- [ ] `planning=False` on crew (or handled via activity)
- [ ] `verbose=False` to minimize console output
- [ ] No custom callbacks that perform I/O
- [ ] Task `output_file` is not set (or file write is in activity)
