# Phase 0: Interface Verification and Spike

This document records the verified CrewAI interfaces based on source code analysis of the CrewAI repository (version at commit as of Dec 2024).

## Verified Interfaces

### 1. `BaseLLM.acall()` - LLM Async Interface

**Location**: `crewai/llms/base_llm.py:161-197`

```python
async def acall(
    self,
    messages: str | list[LLMMessage],
    tools: list[dict[str, BaseTool]] | None = None,
    callbacks: list[Any] | None = None,
    available_functions: dict[str, Any] | None = None,
    from_task: Task | None = None,
    from_agent: Agent | None = None,
    response_model: type[BaseModel] | None = None,
) -> str | Any:
```

**Key Findings**:
- Default implementation raises `NotImplementedError` - subclasses must override
- Return type is `str | Any`:
  - `str` for simple text responses
  - Tool call results when `available_functions` provided and tool executed
  - **Raw `tool_calls` list** when tools requested but `available_functions` is None/empty

**Critical Discovery** (from `crewai/llm.py:1203`):
```python
# If there are tool_calls but no available_functions, return the tool_calls directly
if tool_calls and not available_functions and not text_response:
    return tool_calls
```

**Implication for Our Design**:
Our stub should NOT pass `available_functions` to the activity. The activity makes the LLM call and returns either:
1. Text content (string)
2. Tool calls (list) that CrewAI's agent executor will handle

The tool execution happens in the workflow via `activity_as_tool`, not inside the LLM activity.

---

### 2. `Storage` Interface (Memory Base)

**Location**: `crewai/memory/storage/interface.py`

```python
class Storage:
    """Abstract base class defining the storage interface"""

    def save(self, value: Any, metadata: dict[str, Any]) -> None:
        pass

    def search(
        self, query: str, limit: int, score_threshold: float
    ) -> dict[str, Any] | list[Any]:
        return {}

    def reset(self) -> None:
        pass
```

**Key Findings**:
- **No async methods defined in base interface!**
- Async methods are added by concrete implementations (RAGStorage)
- Return type for search is `dict[str, Any] | list[Any]`

---

### 3. `RAGStorage` - Short-term & Entity Memory

**Location**: `crewai/memory/storage/rag_storage.py`

```python
class RAGStorage(BaseRAGStorage):
    def __init__(
        self,
        type: str,  # "short_term", "entity", etc.
        allow_reset: bool = True,
        embedder_config: ProviderSpec | BaseEmbeddingsProvider[Any] | None = None,
        crew: Crew | None = None,
        path: str | None = None,
    ) -> None

    def save(self, value: Any, metadata: dict[str, Any]) -> None
    async def asave(self, value: Any, metadata: dict[str, Any]) -> None

    def search(
        self,
        query: str,
        limit: int = 5,
        filter: dict[str, Any] | None = None,  # Note: 'filter', not 'metadata_filter'
        score_threshold: float = 0.6,
    ) -> list[Any]

    async def asearch(
        self,
        query: str,
        limit: int = 5,
        filter: dict[str, Any] | None = None,
        score_threshold: float = 0.6,
    ) -> list[Any]

    def reset(self) -> None
    # NOTE: No areset() method!
```

**Key Findings**:
- Parameter is `filter`, NOT `metadata_filter` (differs from knowledge storage)
- **No `areset()` method** - only sync `reset()`
- Requires `type` string: "short_term" or "entity"
- Optional `crew` and `embedder_config` for initialization

**Implication**: Our stub needs to call sync `reset()` in executor or add our own async wrapper.

---

### 4. `LTMSQLiteStorage` - Long-term Memory

**Location**: `crewai/memory/storage/ltm_sqlite_storage.py`

```python
class LTMSQLiteStorage:
    def __init__(self, db_path: str | None = None) -> None

    def save(
        self,
        task_description: str,
        metadata: dict[str, Any],
        datetime: str,
        score: int | float,
    ) -> None

    def load(
        self,
        task_description: str,
        latest_n: int
    ) -> list[dict[str, Any]] | None

    def reset(self) -> None

    async def asave(
        self,
        task_description: str,
        metadata: dict[str, Any],
        datetime: str,
        score: int | float,
    ) -> None

    async def aload(
        self,
        task_description: str,
        latest_n: int
    ) -> list[dict[str, Any]] | None

    async def areset(self) -> None
```

**Key Findings**:
- **Different interface** than RAGStorage!
- Uses `load()` not `search()`
- Parameters: `task_description`, `metadata`, `datetime`, `score`
- Return: `list[dict[str, Any]] | None` with keys: `metadata`, `datetime`, `score`
- Has `areset()` unlike RAGStorage

---

### 5. `BaseKnowledgeStorage` - Knowledge Base Interface

**Location**: `crewai/knowledge/storage/base_knowledge_storage.py`

```python
class BaseKnowledgeStorage(ABC):
    @abstractmethod
    def search(
        self,
        query: list[str],  # NOTE: list[str], not str!
        limit: int = 5,
        metadata_filter: dict[str, Any] | None = None,
        score_threshold: float = 0.6,
    ) -> list[SearchResult]

    @abstractmethod
    async def asearch(
        self,
        query: list[str],
        limit: int = 5,
        metadata_filter: dict[str, Any] | None = None,
        score_threshold: float = 0.6,
    ) -> list[SearchResult]

    @abstractmethod
    def save(self, documents: list[str]) -> None

    @abstractmethod
    async def asave(self, documents: list[str]) -> None

    @abstractmethod
    def reset(self) -> None

    @abstractmethod
    async def areset(self) -> None
```

**Key Findings**:
- `query` is `list[str]`, NOT a single string
- Uses `metadata_filter` (not `filter` like RAGStorage)
- Returns `list[SearchResult]`
- `save()` takes `documents: list[str]`
- Has both sync and async versions of all methods

---

### 6. `CrewStructuredTool` - Tool Interface

**Location**: `crewai/tools/structured_tool.py`

```python
class CrewStructuredTool:
    def __init__(
        self,
        name: str,
        description: str,
        args_schema: type[BaseModel],
        func: Callable[..., Any],
        result_as_answer: bool = False,
        max_usage_count: int | None = None,
        current_usage_count: int = 0,
    ) -> None

    def invoke(
        self,
        input: str | dict,
        config: dict | None = None,
        **kwargs: Any
    ) -> Any

    async def ainvoke(
        self,
        input: str | dict,
        config: dict | None = None,
        **kwargs: Any,
    ) -> Any
```

**Key Findings**:
- Tools use Pydantic `BaseModel` for `args_schema`
- Input can be `str` (JSON) or `dict`
- Has both sync `invoke()` and async `ainvoke()`
- `ainvoke()` handles both async and sync functions (runs sync in executor)

---

### 7. `Crew` Memory Properties

**Location**: `crewai/crew.py:182-193`

```python
short_term_memory: InstanceOf[ShortTermMemory] | None = Field(default=None)
long_term_memory: InstanceOf[LongTermMemory] | None = Field(default=None)
entity_memory: InstanceOf[EntityMemory] | None = Field(default=None)
```

**Key Findings**:
- All memory properties are optional (can be None)
- When `memory=True`, Crew initializes defaults in `_initialize_default_memories()`
- Memory classes wrap the storage implementations

---

### 8. `Crew.akickoff()` - Async Execution

**Location**: `crewai/crew.py:819-883`

```python
async def akickoff(
    self, inputs: dict[str, Any] | None = None
) -> CrewOutput | CrewStreamingOutput:
    """Native async kickoff method using async task execution throughout."""
```

**Key Findings**:
- Returns `CrewOutput` or `CrewStreamingOutput` (if streaming)
- Supports both sequential and hierarchical processes
- Has `akickoff_for_each()` for batch execution

---

## Discrepancies from Original Design

| Aspect | Original Design | Actual Interface | Impact |
|--------|-----------------|------------------|--------|
| LLM tool calls | Tool calls executed internally | Tool calls returned to caller if no `available_functions` | Need to return tool_calls from activity, not execute them |
| RAGStorage.asearch filter | `metadata_filter` | `filter` | Update parameter name |
| RAGStorage.areset | Assumed exists | **Does not exist** | Need sync wrapper or skip |
| LTM interface | `search(query, limit, ...)` | `load(task_description, latest_n)` | Completely different API |
| Knowledge query | `query: str` | `query: list[str]` | Update type |

---

## Updated Data Models

Based on verified interfaces, here are the corrected data models:

### LLM Activity I/O

```python
@dataclass
class LLMCallInput:
    """Input for LLM call activity."""
    model: str
    messages: list[dict[str, Any]]  # LLMMessage format
    tools: list[dict] | None = None
    # Note: Do NOT pass available_functions - we want tool_calls returned
    llm_kwargs: dict[str, Any] = field(default_factory=dict)
    from_task_id: str | None = None
    from_agent_role: str | None = None


@dataclass
class LLMCallOutput:
    """Output from LLM call activity."""
    content: str | None = None
    tool_calls: list[dict] | None = None  # Returned when LLM requests tool execution
    usage: dict[str, int] | None = None
```

### Memory Activity I/O

```python
@dataclass
class MemorySaveInput:
    """Input for RAGStorage save activity."""
    storage_type: str  # "short_term" or "entity"
    value: Any
    metadata: dict[str, Any]


@dataclass
class MemorySearchInput:
    """Input for RAGStorage search activity."""
    storage_type: str
    query: str
    limit: int = 5
    filter: dict[str, Any] | None = None  # Note: 'filter', not 'metadata_filter'
    score_threshold: float = 0.6


@dataclass
class MemorySearchOutput:
    """Output from memory search activity."""
    results: list[dict[str, Any]]


@dataclass
class LTMSaveInput:
    """Input for LTMSQLiteStorage save activity."""
    task_description: str
    metadata: dict[str, Any]
    datetime: str  # ISO format string
    score: float
    db_path: str | None = None


@dataclass
class LTMLoadInput:
    """Input for LTMSQLiteStorage load activity."""
    task_description: str
    latest_n: int
    db_path: str | None = None


@dataclass
class LTMLoadOutput:
    """Output from LTM load activity."""
    results: list[dict[str, Any]]  # Each has: metadata, datetime, score
```

### Knowledge Activity I/O

```python
@dataclass
class KnowledgeSearchInput:
    """Input for knowledge search activity."""
    query: list[str]  # Note: list of strings, not single string
    limit: int = 5
    metadata_filter: dict[str, Any] | None = None
    score_threshold: float = 0.6
    collection_name: str | None = None


@dataclass
class KnowledgeSearchOutput:
    """Output from knowledge search activity."""
    results: list[dict[str, Any]]  # SearchResult as dict


@dataclass
class KnowledgeSaveInput:
    """Input for knowledge save activity."""
    documents: list[str]
    collection_name: str | None = None
```

---

## Stub Interface Updates

### LLM Stub

The key change is how we handle tool calls:

```python
class _LLMStub(BaseLLM):
    async def acall(
        self,
        messages: str | list[dict],
        tools: list[dict] | None = None,
        callbacks: Any | None = None,
        available_functions: dict | None = None,  # We ignore this
        from_task: Any | None = None,
        from_agent: Any | None = None,
        response_model: type[BaseModel] | None = None,
    ) -> str | list | Any:
        """Execute LLM call via Temporal activity.

        The activity does NOT receive available_functions, so if the LLM
        returns tool_calls, they are returned directly to the caller (CrewAI's
        agent executor) for execution via activity_as_tool wrappers.
        """
        input_data = LLMCallInput(
            model=self.model,
            messages=self._normalize_messages(messages),
            tools=_serialize_tools(tools) if tools else None,
            llm_kwargs=self._get_llm_kwargs(),
        )

        result: LLMCallOutput = await workflow.execute_activity(
            "crewai_llm_call",
            input_data,
            start_to_close_timeout=self.activity_config.start_to_close_timeout,
            retry_policy=self.activity_config.retry_policy,
        )

        # If LLM returned tool calls, return them for CrewAI to handle
        if result.tool_calls:
            return result.tool_calls

        return result.content or ""
```

### RAGStorage Stub

```python
class _RAGStorageStub:
    """Stub that routes RAGStorage operations through activities."""

    async def asave(self, value: Any, metadata: dict[str, Any]) -> None:
        await workflow.execute_activity(
            "crewai_memory_save",
            MemorySaveInput(
                storage_type=self._type,
                value=value,
                metadata=metadata,
            ),
            ...
        )

    async def asearch(
        self,
        query: str,
        limit: int = 5,
        filter: dict[str, Any] | None = None,  # Correct parameter name
        score_threshold: float = 0.6,
    ) -> list[Any]:
        result = await workflow.execute_activity(
            "crewai_memory_search",
            MemorySearchInput(
                storage_type=self._type,
                query=query,
                limit=limit,
                filter=filter,
                score_threshold=score_threshold,
            ),
            ...
        )
        return result.results

    def reset(self) -> None:
        # RAGStorage has no areset(), must call sync version
        # This is problematic in workflow - may need to wrap in activity
        raise NotImplementedError(
            "reset() must be called via activity. Use reset_memory_activity()."
        )
```

### LTMStorage Stub

```python
class _LTMStorageStub:
    """Stub that routes LTMSQLiteStorage operations through activities."""

    async def asave(
        self,
        task_description: str,
        metadata: dict[str, Any],
        datetime: str,
        score: int | float,
    ) -> None:
        await workflow.execute_activity(
            "crewai_ltm_save",
            LTMSaveInput(
                task_description=task_description,
                metadata=metadata,
                datetime=datetime,
                score=float(score),
                db_path=self._db_path,
            ),
            ...
        )

    async def aload(
        self,
        task_description: str,
        latest_n: int,
    ) -> list[dict[str, Any]] | None:
        result = await workflow.execute_activity(
            "crewai_ltm_load",
            LTMLoadInput(
                task_description=task_description,
                latest_n=latest_n,
                db_path=self._db_path,
            ),
            ...
        )
        return result.results if result.results else None

    async def areset(self) -> None:
        await workflow.execute_activity(
            "crewai_ltm_reset",
            LTMResetInput(db_path=self._db_path),
            ...
        )
```

---

## Spike Implementation Plan

The spike should validate:

1. **LLM call routing**: Single agent with `llm_stub()` can make LLM calls via activity
2. **Tool call flow**: LLM returns tool_calls → CrewAI handles → tools execute via activity
3. **Memory save/search**: Basic RAGStorage operations work through activities
4. **Crew.akickoff()**: Full async execution path works

### Spike Code Structure

```
temporalio/contrib/crewai/
├── _spike/
│   ├── __init__.py
│   ├── test_llm_activity.py      # Test LLM call routing
│   ├── test_tool_flow.py         # Test tool call handling
│   ├── test_memory.py            # Test memory operations
│   └── test_full_crew.py         # Test complete crew execution
```

### Spike Test Cases

1. **test_llm_simple_response**: LLM returns text → activity returns content
2. **test_llm_tool_calls**: LLM requests tool → activity returns tool_calls → workflow executes tool activity
3. **test_memory_save_search**: Save value → search returns it
4. **test_crew_single_agent**: Full crew with one agent executes task

---

## Next Steps

1. **Update DESIGN.md** with corrected interfaces
2. **Implement spike** to validate assumptions
3. **Run spike tests** against real CrewAI
4. **Document any additional findings**
5. **Proceed to Phase 1** with validated interfaces

---

## Appendix: Type Definitions

### LLMMessage (from CrewAI)

```python
# crewai/utilities/types.py
class LLMMessage(TypedDict, total=False):
    role: str  # "system", "user", "assistant"
    content: str
```

### SearchResult (from CrewAI)

```python
# crewai/rag/types.py
class SearchResult(TypedDict):
    content: str
    metadata: dict[str, Any]
    score: float
```
