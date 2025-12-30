# CrewAI + Temporal Integration: Implementation Plan

This document outlines the phased implementation approach for the CrewAI Temporal integration.

## Overview

The integration enables durable execution of CrewAI crews by routing all I/O operations through Temporal activities. The implementation follows patterns established by the OpenAI Agents integration.

---

## Phase 0: Interface Verification and Spike

**Goal**: Verify CrewAI's public interfaces and validate core assumptions before implementation.

### Deliverables
- Document verified interfaces:
  - `BaseLLM.acall()` signature, parameters, and return type
  - `Storage` interface methods (`save`, `asave`, `search`, `asearch`, `reset`, `areset`)
  - `LTMSQLiteStorage` interface methods
  - `BaseKnowledgeStorage` interface methods
  - `CrewStructuredTool` construction requirements
- Spike: Minimal proof-of-concept with single LLM call through activity
- Document any private API dependencies that cannot be avoided
- Verify CrewAI version compatibility (minimum supported version)
- Verify Python version requirements

### Success Criteria
- All interface assumptions documented and verified against CrewAI source
- Spike demonstrates LLM call routing through activity works
- Clear understanding of tool call flow (LLM returns tool calls → agent executes → results back to LLM)

### Dependencies
- CrewAI package installed
- Temporal Python SDK

---

## Phase 1: Core Infrastructure and LLM Activity

**Goal**: Establish the foundational architecture and implement the most critical component - LLM call routing.

### Deliverables
- Project structure and module scaffolding
- Data models for activity I/O (`_models.py`)
  - `LLMCallInput` with consistent field types
  - `LLMCallOutput` including `tool_calls` field
- Pydantic payload converter configuration for proper serialization
- Activity configuration pattern using class-based activities (following OpenAI agents pattern):
  ```python
  class CrewAIActivities:
      def __init__(self, config: CrewAIActivityConfig):
          self._config = config

      @activity.defn(name="crewai_llm_call")
      async def llm_call(self, input: LLMCallInput) -> LLMCallOutput:
          ...
  ```
- `llm_stub()` function and `_LLMStub` class
  - Properly handle and return tool calls from `LLMCallOutput`
  - Return type must match what CrewAI expects from `BaseLLM.acall()`
- LLM call activity (`crewai_llm_call`)
- Basic worker configuration (`CrewAIActivityConfig`, `crewai_activities`)
- Utility module (`_utils.py`):
  - `_serialize_tools()` - serialize tool definitions for activity input
  - `_to_dict()` - convert objects to dicts for serialization
- Unit tests for LLM stub and activity

### Success Criteria
- A minimal crew with a single agent can execute an LLM call through an activity
- Tool calls from LLM are properly returned and can be processed by CrewAI
- Activity inputs/outputs serialize correctly via Pydantic converter
- Retries work via Temporal's retry policy

### Dependencies
- Phase 0 complete

---

## Phase 2: Tool Integration

**Goal**: Enable CrewAI agents to use Temporal activities as tools.

### Deliverables
- `activity_as_tool()` converter function
- Argument schema generation from function signatures (`_build_args_schema()`)
- `_is_temporal_tool()` utility function for validation:
  ```python
  def _is_temporal_tool(tool: Any) -> bool:
      """Check if a tool was created via activity_as_tool()."""
      # Check for marker attribute set by activity_as_tool()
      return getattr(tool, '_is_temporal_activity_tool', False)
  ```
- Tool execution via `workflow.execute_activity()`
- Proper handling of activity args (single input object, not positional args)
- Tests for tool invocation flow

### Success Criteria
- Activities decorated with `@activity.defn` can be wrapped as CrewAI tools
- Agent tool calls execute as activities with proper timeout/retry configuration
- Tool results are correctly passed back to the LLM context
- `_is_temporal_tool()` correctly identifies activity-backed tools

### Dependencies
- Phase 1 complete

---

## Phase 3: Memory System Integration

**Goal**: Make CrewAI's memory system durable via activity-backed storage.

### Deliverables
- `_RAGStorageStub` for short-term and entity memory
- `_LTMStorageStub` for long-term memory (SQLite-based)
- Memory stub factory functions (`short_term_memory_stub`, `entity_memory_stub`, `long_term_memory_stub`)
- Memory activities:
  - `crewai_memory_save`
  - `crewai_memory_search`
  - `crewai_memory_reset`
  - `crewai_ltm_save`
  - `crewai_ltm_load`
- Tests for memory persistence and retrieval

### Success Criteria
- Crews with `memory=True` work correctly in workflows
- Memory operations survive worker restarts
- Memory data persists across workflow executions
- All three memory types (short-term, long-term, entity) work correctly

### Dependencies
- Phase 1 complete

---

## Phase 4: Knowledge Base Integration

**Goal**: Enable durable knowledge base queries for RAG use cases.

### Deliverables
- `_KnowledgeStorageStub` implementing `BaseKnowledgeStorage`
- `knowledge_storage_stub()` factory function
- Knowledge activities:
  - `crewai_knowledge_search`
  - `crewai_knowledge_save`
  - `crewai_knowledge_reset`
- Tests for knowledge retrieval flows

### Success Criteria
- Agents with knowledge sources can query them via activities
- Knowledge operations are visible in Temporal UI
- Search results are correctly serialized/deserialized

### Dependencies
- Phase 1 complete (can run in parallel with Phase 3)

---

## Phase 5: Crew Runner and Validation

**Goal**: Provide a validated, ergonomic API for running crews in workflows.

### Deliverables
- `TemporalCrewRunner` class with `akickoff()` and `akickoff_for_each()`
- Comprehensive crew configuration validation:
  - All agents use `llm_stub()` (Phase 1)
  - All tools use `activity_as_tool()` (Phase 2)
  - Memory stubs used if `memory=True` - validate all three types (Phase 3)
  - Knowledge storage stubs used if knowledge sources present (Phase 4)
  - `human_input=False` on all tasks
  - `planning=False` or `planning_llm=llm_stub()`
  - `max_rpm` not set on agents or crew
- Clear error messages guiding users to correct configuration
- Integration tests with multi-agent crews

### Success Criteria
- Runner catches all configuration issues at construction time
- Users receive actionable error messages for each violation
- Multi-agent, multi-task crews execute correctly
- Validation covers all stub types (LLM, tools, memory, knowledge)

### Dependencies
- Phases 1-4 complete

---

## Phase 6: Plugin Architecture (Optional Enhancement)

**Goal**: Provide simplified worker setup via plugin pattern (following OpenAI agents integration).

### Deliverables
- `CrewAIPlugin` class extending `SimplePlugin`:
  - Configures Pydantic payload converter
  - Registers all CrewAI activities
  - Sets up workflow runner configuration
- Simplified worker setup:
  ```python
  plugin = CrewAIPlugin(
      config=CrewAIActivityConfig(...),
  )
  client = await Client.connect("localhost:7233", plugins=[plugin])
  worker = Worker(client, task_queue="crewai", workflows=[MyWorkflow])
  ```

### Success Criteria
- Single plugin configures everything needed
- Backward compatible with manual setup

### Dependencies
- Phases 1-5 complete

---

## Phase 7: Documentation and Examples

**Goal**: Provide comprehensive documentation and real-world examples.

**Note**: Documentation should be written incrementally as each phase completes. This phase is for final polish and comprehensive examples.

### Deliverables
- API reference documentation
- Migration guide for existing CrewAI users
- Example workflows:
  - Basic single-agent crew
  - Multi-agent research crew
  - Crew with memory
  - Crew with knowledge base
  - Crew with custom tools
- Troubleshooting guide for common issues
- Version compatibility matrix

### Success Criteria
- Users can get started with minimal friction
- Common patterns are well-documented
- Error scenarios have documented solutions

### Dependencies
- Phases 1-5 complete (Phase 6 optional)

---

## Phase 8: Advanced Features (Future)

**Goal**: Address advanced use cases and enhance the integration.

### Potential Features
- **Human-in-the-loop**: Signal-based human input for tasks
- **Streaming support**: Investigate Temporal updates for streaming responses
- **CrewAI Flows**: Support for the Flows orchestration layer
- **Observability enhancements**: Custom search attributes for crew/agent/task tracking
- **Caching**: Activity result caching for repeated queries
- **Planning support**: First-class `planning_llm=llm_stub()` pattern

### Success Criteria
- Advanced features work reliably
- Performance optimizations yield measurable improvements

### Dependencies
- Phase 7 complete
- User feedback from initial release

---

## Implementation Notes

### Module Structure

```
temporalio/contrib/crewai/
├── __init__.py           # Public API exports
├── _llm.py               # Phase 1: llm_stub implementation
├── _tools.py             # Phase 2: activity_as_tool converter
├── _runner.py            # Phase 5: TemporalCrewRunner
├── _memory.py            # Phase 3: Memory stub implementations
├── _knowledge.py         # Phase 4: Knowledge storage stub
├── _worker.py            # Phase 1: Activity configuration and factory
├── _activities.py        # Phase 1: Activity class with all activity methods
├── _models.py            # Phase 1: Data models for activity I/O
├── _converter.py         # Phase 1: Pydantic payload converter
├── _plugin.py            # Phase 6: Optional plugin architecture
└── _utils.py             # Phase 1+: Shared utilities
                          #   - _serialize_tools()
                          #   - _is_temporal_tool()
                          #   - _to_dict()
                          #   - _build_args_schema()
```

### Activity Configuration Pattern

Following the OpenAI agents integration, use class-based activities for configuration injection:

```python
# _activities.py
class CrewAIActivities:
    """All CrewAI activities with injected configuration."""

    def __init__(self, config: CrewAIActivityConfig):
        self._config = config

    @activity.defn(name="crewai_llm_call")
    async def llm_call(self, input: LLMCallInput) -> LLMCallOutput:
        llm = self._config.llm_factory(input.model)
        # ... implementation

    @activity.defn(name="crewai_memory_save")
    async def memory_save(self, input: MemorySaveInput) -> None:
        storage = self._config.get_storage(input.storage_type)
        # ... implementation

# _worker.py
def crewai_activities(config: CrewAIActivityConfig) -> list[Callable]:
    """Get all CrewAI activities configured with the given config."""
    instance = CrewAIActivities(config)
    return [
        instance.llm_call,
        instance.memory_save,
        # ... etc
    ]
```

### Tool Call Flow

The LLM stub must properly handle tool calls:

```python
# _llm.py
class _LLMStub(BaseLLM):
    async def acall(self, messages, tools, ...):
        result = await workflow.execute_activity(
            "crewai_llm_call",
            LLMCallInput(model=self.model, messages=messages, tools=tools, ...),
            ...
        )

        # Return in format CrewAI expects
        # This may need to return the full result object, not just content
        if result.tool_calls:
            return self._create_response_with_tool_calls(result)
        return result.content
```

### Testing Strategy

#### Unit Tests
- Each stub/activity in isolation
- Serialization round-trips for all data models
- Validation logic for all configuration checks

#### Integration Tests
- Full crew execution with mocked LLM responses
- Multi-agent, multi-task scenarios
- Memory persistence across activity calls
- Tool invocation and result handling

#### Replay/Recovery Tests (Critical for Temporal)
- Worker restart mid-execution
- Activity retry scenarios
- Determinism verification (same input → same history)

#### E2E Tests (Optional, requires API keys)
- Real LLM calls
- Real memory storage
- Real knowledge base queries

### Key Constraints

1. **Async-only execution**: Must use `crew.akickoff()`, not `kickoff()` or `kickoff_async()`
2. **No blocking calls**: `input()`, `time.sleep()` in workflow code will cause issues
3. **Public APIs only**: Rely on CrewAI's public extension points
4. **Serializable I/O**: All activity inputs/outputs must be JSON-serializable via Pydantic
5. **Class-based activities**: Use class-based pattern for configuration injection (not `activity.info().activity_config`)

---

## Risk Mitigation

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| CrewAI internal API changes | Medium | Medium | Use only public interfaces, version-pin in tests, document minimum supported version |
| Async method incompatibility | High | Low | Thoroughly test `akickoff()` path, document sync limitations |
| Memory storage complexity | Medium | Medium | Start with simple implementations, iterate based on feedback |
| Performance overhead | Low | Low | Activity batching is not critical for LLM-bound workloads |
| Tool call handling mismatch | High | Medium | Phase 0 spike validates tool call flow before full implementation |
| Pydantic version conflicts | Medium | Medium | Test with multiple Pydantic versions, document requirements |
| CrewAI version compatibility | Medium | High | Define minimum supported version, test against multiple versions |
| `CrewOutput` serialization | Medium | Medium | Verify all fields are serializable, add custom converter if needed |

---

## Success Metrics

1. **Functionality**: All CrewAI features work correctly in Temporal workflows
2. **Reliability**: Crews survive worker restarts and recover from failures
3. **Observability**: All I/O operations visible in Temporal UI
4. **Usability**: Minimal code changes required for existing CrewAI users
5. **Performance**: No significant overhead beyond activity scheduling
6. **Validation**: Clear, actionable errors for misconfigured crews

---

## Version Compatibility

| Dependency | Minimum Version | Notes |
|------------|-----------------|-------|
| CrewAI | TBD (Phase 0) | Verify `akickoff()` and storage interfaces |
| Python | 3.9+ | Match Temporal SDK requirements |
| Pydantic | 2.0+ | Required for payload converter |
| Temporal SDK | 1.x | Current stable version |

---

## Changelog

- **v1.1**: Added Phase 0 (Interface Verification), restructured phases to resolve dependencies, added activity configuration pattern, expanded testing strategy, added tool call handling details, added plugin phase, expanded risk table.
- **v1.0**: Initial implementation plan.
