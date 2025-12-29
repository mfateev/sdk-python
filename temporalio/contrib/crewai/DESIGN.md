# CrewAI + Temporal Integration Design Document

## Overview

This document describes the design for integrating CrewAI with Temporal, enabling durable execution of AI agent crews. The integration follows the same patterns established by the LangGraph and OpenAI Agents integrations: execute orchestration logic in workflows, perform all IO operations in activities.

## Goals

1. **Durability**: Crew execution survives worker restarts, network failures, and long-running operations
2. **Observability**: Each LLM call, tool execution, and memory operation is visible in Temporal UI
3. **Reliability**: Automatic retries for transient failures with configurable policies
4. **Minimal API surface**: Users should need minimal changes to existing CrewAI code
5. **Public APIs only**: Use CrewAI's public extension points, avoid internal APIs

## User Experience

### Basic Usage

```python
from datetime import timedelta
from crewai import Agent, Task, Crew
from temporalio import workflow
from temporalio.contrib.crewai import (
    llm_stub,
    activity_as_tool,
    TemporalCrewRunner,
)

# Define tools as Temporal activities
@activity.defn
async def search_web(query: str) -> str:
    """Search the web for information."""
    # ... actual implementation
    return results

@activity.defn
async def read_file(path: str) -> str:
    """Read contents of a file."""
    # ... actual implementation
    return content


@workflow.defn
class ResearchWorkflow:
    @workflow.run
    async def run(self, topic: str) -> str:
        # Create agents with activity-backed LLM
        researcher = Agent(
            role="Senior Researcher",
            goal=f"Research {topic} thoroughly",
            backstory="You are an expert researcher...",
            llm=llm_stub("gpt-4o"),  # LLM calls become activities
            inject_date=False,  # REQUIRED: datetime.now() is non-deterministic
            tools=[
                activity_as_tool(
                    search_web,
                    start_to_close_timeout=timedelta(seconds=60),
                ),
                activity_as_tool(
                    read_file,
                    start_to_close_timeout=timedelta(seconds=30),
                ),
            ],
        )

        writer = Agent(
            role="Technical Writer",
            goal="Write clear documentation",
            backstory="You are an expert technical writer...",
            llm=llm_stub("gpt-4o"),
            inject_date=False,
        )

        # Define tasks
        research_task = Task(
            description=f"Research {topic} and gather key information",
            expected_output="Comprehensive research notes",
            agent=researcher,
            human_input=False,  # REQUIRED: input() blocks indefinitely in workflows
        )

        write_task = Task(
            description="Write documentation based on research",
            expected_output="Well-structured documentation",
            agent=writer,
            human_input=False,
        )

        # Create and run crew
        crew = Crew(
            agents=[researcher, writer],
            tasks=[research_task, write_task],
            verbose=False,  # Recommended: minimize console I/O
            planning=False,  # REQUIRED unless planning wrapped in activity
        )

        # Execute crew with Temporal durability
        runner = TemporalCrewRunner(crew)
        result = await runner.kickoff()

        return result.raw
```

### With Memory (Activity-Backed Storage)

```python
from temporalio.contrib.crewai import (
    llm_stub,
    short_term_memory_stub,
    long_term_memory_stub,
    entity_memory_stub,
)

@workflow.defn
class CrewWithMemoryWorkflow:
    @workflow.run
    async def run(self, inputs: dict) -> str:
        agent = Agent(
            role="Research Assistant",
            llm=llm_stub("gpt-4o"),
            # ... other config
        )

        crew = Crew(
            agents=[agent],
            tasks=[...],
            memory=True,
            # Memory operations become activities
            short_term_memory=short_term_memory_stub(
                activity_config=ActivityConfig(
                    start_to_close_timeout=timedelta(seconds=30),
                ),
            ),
            long_term_memory=long_term_memory_stub(
                activity_config=ActivityConfig(
                    start_to_close_timeout=timedelta(seconds=30),
                ),
            ),
            entity_memory=entity_memory_stub(
                activity_config=ActivityConfig(
                    start_to_close_timeout=timedelta(seconds=30),
                ),
            ),
        )

        runner = TemporalCrewRunner(crew)
        return (await runner.kickoff()).raw
```

### With Knowledge Base

```python
from temporalio.contrib.crewai import llm_stub, knowledge_storage_stub

@workflow.defn
class CrewWithKnowledgeWorkflow:
    @workflow.run
    async def run(self, query: str) -> str:
        # Knowledge queries become activities
        knowledge = Knowledge(
            collection_name="company_docs",
            sources=[...],
            storage=knowledge_storage_stub(
                activity_config=ActivityConfig(
                    start_to_close_timeout=timedelta(seconds=60),
                ),
            ),
        )

        agent = Agent(
            role="Knowledge Expert",
            llm=llm_stub("gpt-4o"),
            knowledge_sources=[knowledge],
        )

        # ... rest of crew setup
```

### Configuration Options

```python
from temporalio.contrib.crewai import llm_stub, LLMActivityConfig

# Configure LLM activity behavior
llm = llm_stub(
    model="gpt-4o",
    activity_config=LLMActivityConfig(
        start_to_close_timeout=timedelta(minutes=5),
        retry_policy=RetryPolicy(
            initial_interval=timedelta(seconds=1),
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=3,
        ),
        heartbeat_timeout=timedelta(seconds=30),
    ),
    # Pass through to underlying LLM
    temperature=0.7,
    max_tokens=4096,
)
```

### Worker Setup

```python
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.contrib.crewai import (
    crewai_activities,
    CrewAIActivityConfig,
)

async def main():
    client = await Client.connect("localhost:7233")

    # Create activity config with actual LLM/storage implementations
    activity_config = CrewAIActivityConfig(
        # Default LLM for activities (can be overridden per-agent)
        default_llm_factory=lambda model: LLM(model=model),
        # Storage factories for memory activities
        short_term_storage_factory=lambda: RAGStorage(type="short_term"),
        long_term_storage_factory=lambda: LTMSQLiteStorage(),
        entity_storage_factory=lambda: RAGStorage(type="entity"),
        knowledge_storage_factory=lambda config: KnowledgeStorage(**config),
    )

    worker = Worker(
        client,
        task_queue="crewai-tasks",
        workflows=[ResearchWorkflow],
        activities=crewai_activities(activity_config),
    )

    await worker.run()
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Temporal Workflow                              │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │                         CrewAI Orchestration                       │  │
│  │  ┌─────────┐    ┌─────────┐    ┌─────────────────────────────┐   │  │
│  │  │  Crew   │───▶│  Task   │───▶│     Agent Executor Loop      │   │  │
│  │  └─────────┘    └─────────┘    │  (deterministic orchestration)│   │  │
│  │                                 └──────────────┬────────────────┘   │  │
│  └─────────────────────────────────────────────────┼───────────────────┘  │
│                                                    │                      │
│         ┌──────────────────────────────────────────┼──────────────────┐   │
│         │                    IO Operations         │                  │   │
│         │         ┌────────────────────────────────┼───────────┐      │   │
│         ▼         ▼                                ▼           ▼      │   │
│  ┌────────────┐ ┌────────────┐ ┌─────────────────────┐ ┌───────────┐  │   │
│  │ llm_stub   │ │ Tool Stub  │ │   memory_stub        │ │ knowledge │  │   │
│  │            │ │(activity_  │ │                      │ │   _stub   │  │   │
│  │            │ │ as_tool)   │ │                      │ │           │  │   │
│  └─────┬──────┘ └─────┬──────┘ └──────────┬───────────┘ └─────┬─────┘  │   │
│        │              │                   │                   │        │   │
└────────┼──────────────┼───────────────────┼───────────────────┼────────┘   │
         │              │                   │                   │            │
         ▼              ▼                   ▼                   ▼            │
   ┌──────────────────────────────────────────────────────────────────┐     │
   │                      Temporal Activities                          │     │
   │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────┐ │     │
   │  │  LLM Call    │ │  Tool        │ │  Memory      │ │Knowledge │ │     │
   │  │  Activity    │ │  Activity    │ │  Activity    │ │ Activity │ │     │
   │  └──────┬───────┘ └──────┬───────┘ └──────┬───────┘ └────┬─────┘ │     │
   └─────────┼────────────────┼────────────────┼──────────────┼───────┘     │
             │                │                │              │              │
             ▼                ▼                ▼              ▼              │
   ┌──────────────────────────────────────────────────────────────────┐     │
   │                      External Services                            │     │
   │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────┐ │     │
   │  │  OpenAI /    │ │  Web APIs    │ │  ChromaDB /  │ │ Vector   │ │     │
   │  │  Anthropic   │ │  Databases   │ │  SQLite      │ │ Store    │ │     │
   │  └──────────────┘ └──────────────┘ └──────────────┘ └──────────┘ │     │
   └──────────────────────────────────────────────────────────────────┘     │
```

---

## Component Design

### 1. LLM Stub (llm_stub)

Creates a CrewAI-compatible LLM that routes calls to activities.

```python
# File: temporalio/contrib/crewai/_llm.py

from crewai.llms.base_llm import BaseLLM
from temporalio import workflow
from dataclasses import dataclass
from typing import Any


@dataclass
class LLMActivityConfig:
    """Configuration for LLM activity execution."""
    start_to_close_timeout: timedelta = timedelta(minutes=2)
    retry_policy: RetryPolicy | None = None
    heartbeat_timeout: timedelta | None = None
    task_queue: str | None = None


def llm_stub(
    model: str,
    activity_config: LLMActivityConfig | None = None,
    **llm_kwargs: Any,
) -> BaseLLM:
    """Create an LLM stub that executes calls as Temporal activities.

    This function returns a CrewAI-compatible LLM that routes all
    actual LLM calls to Temporal activities, providing durability
    and observability for AI operations.

    Args:
        model: The model name (e.g., "gpt-4o", "claude-3-opus")
        activity_config: Configuration for activity execution
        **llm_kwargs: Additional arguments passed to the underlying LLM

    Returns:
        A BaseLLM implementation that executes via activities

    Example:
        agent = Agent(
            role="Researcher",
            llm=llm_stub("gpt-4o"),
        )
    """
    return _LLMStub(model, activity_config, **llm_kwargs)


class _LLMStub(BaseLLM):
    """Internal LLM stub implementation."""

    def __init__(
        self,
        model: str,
        activity_config: LLMActivityConfig | None = None,
        **llm_kwargs: Any,
    ):
        self.model = model
        self.activity_config = activity_config or LLMActivityConfig()
        self.llm_kwargs = llm_kwargs
        self.model_name = model

    def call(
        self,
        messages: list[dict[str, str]],
        tools: list[dict] | None = None,
        callbacks: Any | None = None,
        available_functions: dict | None = None,
        **kwargs: Any,
    ) -> str:
        """Synchronous LLM call - not supported in workflows."""
        raise NotImplementedError(
            "llm_stub only supports async execution. "
            "Use Crew.kickoff_async() or run within an async context."
        )

    async def acall(
        self,
        messages: list[dict[str, str]],
        tools: list[dict] | None = None,
        callbacks: Any | None = None,
        available_functions: dict | None = None,
        **kwargs: Any,
    ) -> str:
        """Async LLM call - executes as Temporal activity."""
        input_data = LLMCallInput(
            model=self.model,
            messages=messages,
            tools=_serialize_tools(tools) if tools else None,
            llm_kwargs={**self.llm_kwargs, **kwargs},
        )

        result = await workflow.execute_activity(
            "crewai_llm_call",
            input_data,
            start_to_close_timeout=self.activity_config.start_to_close_timeout,
            retry_policy=self.activity_config.retry_policy,
            heartbeat_timeout=self.activity_config.heartbeat_timeout,
            task_queue=self.activity_config.task_queue,
        )

        return result.content

    @property
    def supports_function_calling(self) -> bool:
        return True

    @property
    def supports_stop_words(self) -> bool:
        return True
```

### 2. Activity-as-Tool Converter

Converts Temporal activities into CrewAI tools.

```python
# File: temporalio/contrib/crewai/_tools.py

from crewai.tools.structured_tool import CrewStructuredTool
from temporalio import activity, workflow
from typing import Any, Callable
import inspect
import json


def activity_as_tool(
    fn: Callable,
    *,
    start_to_close_timeout: timedelta | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    retry_policy: RetryPolicy | None = None,
    heartbeat_timeout: timedelta | None = None,
    task_queue: str | None = None,
) -> CrewStructuredTool:
    """Convert a Temporal activity function to a CrewAI tool.

    This function takes a Temporal activity and wraps it as a CrewAI
    CrewStructuredTool. When the agent invokes the tool, it executes
    as a Temporal activity with full durability guarantees.

    Args:
        fn: A function decorated with @activity.defn
        start_to_close_timeout: Maximum time for activity execution
        schedule_to_close_timeout: Maximum time from scheduling to completion
        retry_policy: Retry configuration for failed activities
        heartbeat_timeout: Heartbeat timeout for long-running activities
        task_queue: Override task queue for this activity

    Returns:
        A CrewStructuredTool that executes the activity

    Example:
        @activity.defn
        async def search_web(query: str) -> str:
            # ... implementation
            return results

        tool = activity_as_tool(
            search_web,
            start_to_close_timeout=timedelta(seconds=60),
        )

        agent = Agent(tools=[tool], ...)
    """
    # Validate it's an activity
    defn = activity._Definition.from_callable(fn)
    if not defn:
        raise ValueError(
            f"Function {fn.__name__} must be decorated with @activity.defn"
        )

    activity_name = defn.name

    # Extract function signature for schema
    sig = inspect.signature(fn)
    doc = fn.__doc__ or f"Execute {activity_name} activity"

    # Build args schema from type hints
    args_schema = _build_args_schema(fn)

    async def _run_activity(**kwargs: Any) -> str:
        """Execute the activity and return result as string."""
        # Convert kwargs to positional args based on signature
        args = [kwargs[p] for p in sig.parameters if p in kwargs]

        result = await workflow.execute_activity(
            activity_name,
            args=args if len(args) != 1 else args[0],
            start_to_close_timeout=start_to_close_timeout,
            schedule_to_close_timeout=schedule_to_close_timeout,
            retry_policy=retry_policy,
            heartbeat_timeout=heartbeat_timeout,
            task_queue=task_queue,
        )

        # CrewAI expects string results
        return str(result) if result is not None else ""

    return CrewStructuredTool(
        name=activity_name,
        description=doc,
        func=lambda **kw: None,  # Sync version not supported
        coroutine=_run_activity,
        args_schema=args_schema,
    )


def _build_args_schema(fn: Callable) -> type:
    """Build a Pydantic model from function signature."""
    from pydantic import create_model

    hints = get_type_hints(fn)
    sig = inspect.signature(fn)

    fields = {}
    for name, param in sig.parameters.items():
        if name == "self":
            continue
        type_hint = hints.get(name, Any)
        if param.default is inspect.Parameter.empty:
            fields[name] = (type_hint, ...)
        else:
            fields[name] = (type_hint, param.default)

    return create_model(f"{fn.__name__}Args", **fields)
```

### 3. Memory Storage Stubs

Activity-backed storage for CrewAI memory system.

```python
# File: temporalio/contrib/crewai/_memory.py

from crewai.memory.storage.interface import Storage
from crewai.memory.short_term.short_term_memory import ShortTermMemory
from crewai.memory.long_term.long_term_memory import LongTermMemory
from crewai.memory.entity.entity_memory import EntityMemory
from temporalio import workflow
from dataclasses import dataclass
from typing import Any


@dataclass
class MemoryActivityConfig:
    """Configuration for memory activity execution."""
    start_to_close_timeout: timedelta = timedelta(seconds=30)
    retry_policy: RetryPolicy | None = None
    task_queue: str | None = None


def short_term_memory_stub(
    activity_config: MemoryActivityConfig | None = None,
    embedder_config: dict | None = None,
    **kwargs: Any,
) -> ShortTermMemory:
    """Create a short-term memory stub that executes via activities.

    Args:
        activity_config: Configuration for activity execution
        embedder_config: Configuration for the embedder
        **kwargs: Additional arguments passed to ShortTermMemory

    Returns:
        A ShortTermMemory with activity-backed storage

    Example:
        crew = Crew(
            agents=[...],
            memory=True,
            short_term_memory=short_term_memory_stub(),
        )
    """
    storage = _RAGStorageStub(
        storage_type="short_term",
        activity_config=activity_config,
        embedder_config=embedder_config,
    )
    return ShortTermMemory(storage=storage, **kwargs)


def entity_memory_stub(
    activity_config: MemoryActivityConfig | None = None,
    embedder_config: dict | None = None,
    **kwargs: Any,
) -> EntityMemory:
    """Create an entity memory stub that executes via activities.

    Args:
        activity_config: Configuration for activity execution
        embedder_config: Configuration for the embedder
        **kwargs: Additional arguments passed to EntityMemory

    Returns:
        An EntityMemory with activity-backed storage
    """
    storage = _RAGStorageStub(
        storage_type="entity",
        activity_config=activity_config,
        embedder_config=embedder_config,
    )
    return EntityMemory(storage=storage, **kwargs)


class _RAGStorageStub(Storage):
    """Internal RAG storage stub that routes operations to activities."""

    def __init__(
        self,
        storage_type: str,  # "short_term", "entity", etc.
        activity_config: MemoryActivityConfig | None = None,
        embedder_config: dict | None = None,
    ):
        self.storage_type = storage_type
        self.activity_config = activity_config or MemoryActivityConfig()
        self.embedder_config = embedder_config or {}

    def save(self, value: Any, metadata: dict[str, Any]) -> None:
        """Sync save - not supported in workflow context."""
        raise NotImplementedError(
            "Memory stubs only support async operations in workflows"
        )

    async def asave(self, value: Any, metadata: dict[str, Any]) -> None:
        """Save to memory via activity."""
        await workflow.execute_activity(
            "crewai_memory_save",
            MemorySaveInput(
                storage_type=self.storage_type,
                value=value,
                metadata=metadata,
                embedder_config=self.embedder_config,
            ),
            start_to_close_timeout=self.activity_config.start_to_close_timeout,
            retry_policy=self.activity_config.retry_policy,
            task_queue=self.activity_config.task_queue,
        )

    def search(
        self,
        query: str,
        limit: int = 5,
        score_threshold: float = 0.6,
    ) -> list[Any]:
        """Sync search - not supported in workflow context."""
        raise NotImplementedError(
            "Memory stubs only support async operations in workflows"
        )

    async def asearch(
        self,
        query: str,
        limit: int = 5,
        score_threshold: float = 0.6,
    ) -> list[Any]:
        """Search memory via activity."""
        result = await workflow.execute_activity(
            "crewai_memory_search",
            MemorySearchInput(
                storage_type=self.storage_type,
                query=query,
                limit=limit,
                score_threshold=score_threshold,
                embedder_config=self.embedder_config,
            ),
            start_to_close_timeout=self.activity_config.start_to_close_timeout,
            retry_policy=self.activity_config.retry_policy,
            task_queue=self.activity_config.task_queue,
        )
        return result.results

    def reset(self) -> None:
        """Sync reset - not supported in workflow context."""
        raise NotImplementedError(
            "Memory stubs only support async operations in workflows"
        )

    async def areset(self) -> None:
        """Reset memory via activity."""
        await workflow.execute_activity(
            "crewai_memory_reset",
            MemoryResetInput(
                storage_type=self.storage_type,
                embedder_config=self.embedder_config,
            ),
            start_to_close_timeout=self.activity_config.start_to_close_timeout,
            retry_policy=self.activity_config.retry_policy,
            task_queue=self.activity_config.task_queue,
        )
```

### 4. Long-Term Memory Storage

```python
# File: temporalio/contrib/crewai/_memory.py (continued)

def long_term_memory_stub(
    activity_config: MemoryActivityConfig | None = None,
    db_path: str | None = None,
    **kwargs: Any,
) -> LongTermMemory:
    """Create a long-term memory stub that executes via activities.

    Args:
        activity_config: Configuration for activity execution
        db_path: Path to the SQLite database
        **kwargs: Additional arguments passed to LongTermMemory

    Returns:
        A LongTermMemory with activity-backed storage

    Example:
        crew = Crew(
            agents=[...],
            memory=True,
            long_term_memory=long_term_memory_stub(),
        )
    """
    storage = _LTMStorageStub(
        activity_config=activity_config,
        db_path=db_path,
    )
    return LongTermMemory(storage=storage, **kwargs)


class _LTMStorageStub:
    """Internal SQLite-based long-term memory storage stub.

    Long-term memory uses SQLite, which is file IO. This routes
    all database operations through activities.
    """

    def __init__(
        self,
        activity_config: MemoryActivityConfig | None = None,
        db_path: str | None = None,
    ):
        self.activity_config = activity_config or MemoryActivityConfig()
        self.db_path = db_path

    async def asave(
        self,
        task_description: str,
        score: float,
        metadata: dict,
        datetime: str,
    ) -> None:
        """Save to long-term memory via activity."""
        await workflow.execute_activity(
            "crewai_ltm_save",
            LTMSaveInput(
                task_description=task_description,
                score=score,
                metadata=metadata,
                datetime=datetime,
                db_path=self.db_path,
            ),
            start_to_close_timeout=self.activity_config.start_to_close_timeout,
            retry_policy=self.activity_config.retry_policy,
            task_queue=self.activity_config.task_queue,
        )

    async def aload(
        self,
        task_description: str,
        latest_n: int = 3,
    ) -> list[dict]:
        """Load from long-term memory via activity."""
        result = await workflow.execute_activity(
            "crewai_ltm_load",
            LTMLoadInput(
                task_description=task_description,
                latest_n=latest_n,
                db_path=self.db_path,
            ),
            start_to_close_timeout=self.activity_config.start_to_close_timeout,
            retry_policy=self.activity_config.retry_policy,
            task_queue=self.activity_config.task_queue,
        )
        return result.results
```

### 5. Knowledge Storage

```python
# File: temporalio/contrib/crewai/_knowledge.py

from crewai.knowledge.storage.base_knowledge_storage import BaseKnowledgeStorage
from crewai.rag.types import SearchResult
from temporalio import workflow
from dataclasses import dataclass
from typing import Any


@dataclass
class KnowledgeActivityConfig:
    """Configuration for knowledge activity execution."""
    start_to_close_timeout: timedelta = timedelta(seconds=60)
    retry_policy: RetryPolicy | None = None
    task_queue: str | None = None


def knowledge_storage_stub(
    collection_name: str | None = None,
    activity_config: KnowledgeActivityConfig | None = None,
    embedder_config: dict | None = None,
) -> BaseKnowledgeStorage:
    """Create a knowledge storage stub that executes via activities.

    Args:
        collection_name: Name of the knowledge collection
        activity_config: Configuration for activity execution
        embedder_config: Configuration for the embedder

    Returns:
        A BaseKnowledgeStorage that executes via activities

    Example:
        knowledge = Knowledge(
            collection_name="docs",
            storage=knowledge_storage_stub(),
        )
    """
    return _KnowledgeStorageStub(
        collection_name=collection_name,
        activity_config=activity_config,
        embedder_config=embedder_config,
    )


class _KnowledgeStorageStub(BaseKnowledgeStorage):
    """Internal knowledge storage stub that routes operations to activities."""

    def __init__(
        self,
        collection_name: str | None = None,
        activity_config: KnowledgeActivityConfig | None = None,
        embedder_config: dict | None = None,
    ):
        self.collection_name = collection_name
        self.activity_config = activity_config or KnowledgeActivityConfig()
        self.embedder_config = embedder_config or {}

    def search(
        self,
        query: list[str],
        limit: int = 5,
        metadata_filter: dict[str, Any] | None = None,
        score_threshold: float = 0.6,
    ) -> list[SearchResult]:
        """Sync search - not supported in workflow context."""
        raise NotImplementedError(
            "Knowledge storage stubs only support async operations in workflows"
        )

    async def asearch(
        self,
        query: list[str],
        limit: int = 5,
        metadata_filter: dict[str, Any] | None = None,
        score_threshold: float = 0.6,
    ) -> list[SearchResult]:
        """Search knowledge base via activity."""
        result = await workflow.execute_activity(
            "crewai_knowledge_search",
            KnowledgeSearchInput(
                collection_name=self.collection_name,
                query=query,
                limit=limit,
                metadata_filter=metadata_filter,
                score_threshold=score_threshold,
                embedder_config=self.embedder_config,
            ),
            start_to_close_timeout=self.activity_config.start_to_close_timeout,
            retry_policy=self.activity_config.retry_policy,
            task_queue=self.activity_config.task_queue,
        )
        return result.results

    def save(self, documents: list[str]) -> None:
        """Sync save - not supported in workflow context."""
        raise NotImplementedError(
            "Knowledge storage stubs only support async operations in workflows"
        )

    async def asave(self, documents: list[str]) -> None:
        """Save documents to knowledge base via activity."""
        await workflow.execute_activity(
            "crewai_knowledge_save",
            KnowledgeSaveInput(
                collection_name=self.collection_name,
                documents=documents,
                embedder_config=self.embedder_config,
            ),
            start_to_close_timeout=self.activity_config.start_to_close_timeout,
            retry_policy=self.activity_config.retry_policy,
            task_queue=self.activity_config.task_queue,
        )

    def reset(self) -> None:
        """Sync reset - not supported in workflow context."""
        raise NotImplementedError(
            "Knowledge storage stubs only support async operations in workflows"
        )

    async def areset(self) -> None:
        """Reset knowledge base via activity."""
        await workflow.execute_activity(
            "crewai_knowledge_reset",
            KnowledgeResetInput(
                collection_name=self.collection_name,
                embedder_config=self.embedder_config,
            ),
            start_to_close_timeout=self.activity_config.start_to_close_timeout,
            retry_policy=self.activity_config.retry_policy,
            task_queue=self.activity_config.task_queue,
        )
```

### 6. Crew Runner

Orchestrates crew execution within a workflow context.

```python
# File: temporalio/contrib/crewai/_runner.py

from crewai import Crew
from crewai.crews.crew_output import CrewOutput
from typing import Any


class TemporalCrewRunner:
    """Runner that executes a CrewAI Crew within a Temporal workflow.

    This class wraps a Crew and ensures it executes properly in a
    workflow context, using async methods and activity-backed IO.
    """

    def __init__(self, crew: Crew):
        self.crew = crew
        self._validate_crew()

    def _validate_crew(self) -> None:
        """Validate that the crew is configured for Temporal execution."""
        # Validate crew-level settings
        if getattr(self.crew, 'planning', False):
            raise ValueError(
                "Crew with planning=True is not supported in workflows. "
                "CrewPlanner makes synchronous LLM calls. Set planning=False."
            )

        for agent in self.crew.agents:
            # Check that agents use llm_stub
            if not isinstance(agent.llm, _LLMStub):
                raise ValueError(
                    f"Agent '{agent.role}' must use llm_stub() for workflow execution. "
                    f"Got: {type(agent.llm).__name__}"
                )

            # Check inject_date is disabled (datetime.now() is non-deterministic)
            if getattr(agent, 'inject_date', True):
                raise ValueError(
                    f"Agent '{agent.role}' must set inject_date=False for workflow execution. "
                    "datetime.now() is non-deterministic."
                )

            # Check that tools are activity-backed (if any)
            for tool in (agent.tools or []):
                if not _is_temporal_tool(tool):
                    raise ValueError(
                        f"Tool '{tool.name}' in agent '{agent.role}' must be created "
                        "with activity_as_tool() for workflow execution."
                    )

        # Validate tasks
        for task in self.crew.tasks:
            # Check human_input is disabled (input() blocks indefinitely)
            if getattr(task, 'human_input', False):
                raise ValueError(
                    f"Task '{task.description[:50]}...' must set human_input=False "
                    "for workflow execution. Use Temporal signals for human input."
                )

            # Warn about output_file (file I/O should be in activities)
            if getattr(task, 'output_file', None):
                import warnings
                warnings.warn(
                    f"Task '{task.description[:50]}...' has output_file set. "
                    "File I/O in workflows is non-deterministic. Consider using "
                    "an activity for file operations.",
                    UserWarning,
                )

        # Validate memory configuration if enabled
        if self.crew.memory:
            if self.crew.short_term_memory:
                storage = getattr(self.crew.short_term_memory, 'storage', None)
                if not isinstance(storage, _RAGStorageStub):
                    raise ValueError(
                        "Crew with memory=True must use short_term_memory_stub()"
                    )

    async def kickoff(self, inputs: dict[str, Any] | None = None) -> CrewOutput:
        """Execute the crew asynchronously.

        Args:
            inputs: Optional inputs to pass to the crew

        Returns:
            CrewOutput containing the execution results
        """
        return await self.crew.kickoff_async(inputs=inputs)

    async def kickoff_for_each(
        self,
        inputs: list[dict[str, Any]],
    ) -> list[CrewOutput]:
        """Execute the crew for each input set.

        Args:
            inputs: List of input dictionaries

        Returns:
            List of CrewOutput results
        """
        return await self.crew.kickoff_for_each_async(inputs=inputs)
```

---

## Activity Definitions

### 7. LLM Activity

```python
# File: temporalio/contrib/crewai/_activities.py

from temporalio import activity
from dataclasses import dataclass
from typing import Any


@dataclass
class LLMCallInput:
    """Input for LLM call activity."""
    model: str
    messages: list[dict[str, str]]
    tools: list[dict] | None = None
    llm_kwargs: dict[str, Any] | None = None


@dataclass
class LLMCallOutput:
    """Output from LLM call activity."""
    content: str
    tool_calls: list[dict] | None = None
    usage: dict[str, int] | None = None


@activity.defn(name="crewai_llm_call")
async def llm_call_activity(input: LLMCallInput) -> LLMCallOutput:
    """Execute an LLM call.

    This activity creates a real LLM instance and makes the API call.
    The LLM factory is injected via activity context.
    """
    # Get LLM factory from activity context
    config = activity.info().activity_config
    llm_factory = config.llm_factory

    # Create LLM instance
    llm = llm_factory(input.model)

    # Make the call
    kwargs = input.llm_kwargs or {}
    result = await llm.acall(
        messages=input.messages,
        tools=input.tools,
        **kwargs,
    )

    # Send heartbeat for long calls
    activity.heartbeat({"model": input.model, "status": "completed"})

    return LLMCallOutput(
        content=result if isinstance(result, str) else result.content,
        tool_calls=getattr(result, "tool_calls", None),
        usage=getattr(result, "usage", None),
    )
```

### 8. Memory Activities

```python
# File: temporalio/contrib/crewai/_activities.py (continued)

@dataclass
class MemorySaveInput:
    """Input for memory save activity."""
    storage_type: str  # "short_term", "entity", etc.
    value: Any
    metadata: dict[str, Any]
    embedder_config: dict | None = None


@dataclass
class MemorySearchInput:
    """Input for memory search activity."""
    storage_type: str
    query: str
    limit: int = 5
    score_threshold: float = 0.6
    embedder_config: dict | None = None


@dataclass
class MemorySearchOutput:
    """Output from memory search activity."""
    results: list[dict[str, Any]]


@activity.defn(name="crewai_memory_save")
async def memory_save_activity(input: MemorySaveInput) -> None:
    """Save to memory storage."""
    config = activity.info().activity_config
    storage = config.get_storage(input.storage_type, input.embedder_config)

    await storage.asave(value=input.value, metadata=input.metadata)

    activity.heartbeat({"storage_type": input.storage_type, "action": "save"})


@activity.defn(name="crewai_memory_search")
async def memory_search_activity(input: MemorySearchInput) -> MemorySearchOutput:
    """Search memory storage."""
    config = activity.info().activity_config
    storage = config.get_storage(input.storage_type, input.embedder_config)

    results = await storage.asearch(
        query=input.query,
        limit=input.limit,
        score_threshold=input.score_threshold,
    )

    activity.heartbeat({
        "storage_type": input.storage_type,
        "action": "search",
        "results_count": len(results),
    })

    return MemorySearchOutput(results=list(results))


@activity.defn(name="crewai_memory_reset")
async def memory_reset_activity(input: MemoryResetInput) -> None:
    """Reset memory storage."""
    config = activity.info().activity_config
    storage = config.get_storage(input.storage_type, input.embedder_config)

    await storage.areset()

    activity.heartbeat({"storage_type": input.storage_type, "action": "reset"})
```

### 9. Knowledge Activities

```python
# File: temporalio/contrib/crewai/_activities.py (continued)

@dataclass
class KnowledgeSearchInput:
    """Input for knowledge search activity."""
    collection_name: str | None
    query: list[str]
    limit: int = 5
    metadata_filter: dict[str, Any] | None = None
    score_threshold: float = 0.6
    embedder_config: dict | None = None


@dataclass
class KnowledgeSearchOutput:
    """Output from knowledge search activity."""
    results: list[dict[str, Any]]  # SearchResult as dict


@activity.defn(name="crewai_knowledge_search")
async def knowledge_search_activity(
    input: KnowledgeSearchInput,
) -> KnowledgeSearchOutput:
    """Search knowledge base."""
    config = activity.info().activity_config
    storage = config.knowledge_storage_factory(
        collection_name=input.collection_name,
        embedder_config=input.embedder_config,
    )

    results = await storage.asearch(
        query=input.query,
        limit=input.limit,
        metadata_filter=input.metadata_filter,
        score_threshold=input.score_threshold,
    )

    activity.heartbeat({
        "collection": input.collection_name,
        "action": "search",
        "results_count": len(results),
    })

    # Convert SearchResult objects to dicts for serialization
    return KnowledgeSearchOutput(
        results=[_search_result_to_dict(r) for r in results]
    )


@activity.defn(name="crewai_knowledge_save")
async def knowledge_save_activity(input: KnowledgeSaveInput) -> None:
    """Save documents to knowledge base."""
    config = activity.info().activity_config
    storage = config.knowledge_storage_factory(
        collection_name=input.collection_name,
        embedder_config=input.embedder_config,
    )

    await storage.asave(documents=input.documents)

    activity.heartbeat({
        "collection": input.collection_name,
        "action": "save",
        "doc_count": len(input.documents),
    })
```

---

## Data Models

```python
# File: temporalio/contrib/crewai/_models.py

from dataclasses import dataclass, field
from typing import Any


@dataclass
class LLMCallInput:
    """Input for LLM call activity."""
    model: str
    messages: list[dict[str, str]]
    tools: list[dict] | None = None
    llm_kwargs: dict[str, Any] = field(default_factory=dict)


@dataclass
class LLMCallOutput:
    """Output from LLM call activity."""
    content: str
    tool_calls: list[dict] | None = None
    usage: dict[str, int] | None = None


@dataclass
class MemorySaveInput:
    """Input for memory save activity."""
    storage_type: str
    value: Any
    metadata: dict[str, Any]
    embedder_config: dict = field(default_factory=dict)


@dataclass
class MemorySearchInput:
    """Input for memory search activity."""
    storage_type: str
    query: str
    limit: int = 5
    score_threshold: float = 0.6
    embedder_config: dict = field(default_factory=dict)


@dataclass
class MemorySearchOutput:
    """Output from memory search activity."""
    results: list[dict[str, Any]]


@dataclass
class MemoryResetInput:
    """Input for memory reset activity."""
    storage_type: str
    embedder_config: dict = field(default_factory=dict)


@dataclass
class LTMSaveInput:
    """Input for long-term memory save activity."""
    task_description: str
    score: float
    metadata: dict[str, Any]
    datetime: str
    db_path: str | None = None


@dataclass
class LTMLoadInput:
    """Input for long-term memory load activity."""
    task_description: str
    latest_n: int = 3
    db_path: str | None = None


@dataclass
class LTMLoadOutput:
    """Output from long-term memory load activity."""
    results: list[dict[str, Any]]


@dataclass
class KnowledgeSearchInput:
    """Input for knowledge search activity."""
    query: list[str]
    collection_name: str | None = None
    limit: int = 5
    metadata_filter: dict[str, Any] | None = None
    score_threshold: float = 0.6
    embedder_config: dict = field(default_factory=dict)


@dataclass
class KnowledgeSearchOutput:
    """Output from knowledge search activity."""
    results: list[dict[str, Any]]


@dataclass
class KnowledgeSaveInput:
    """Input for knowledge save activity."""
    documents: list[str]
    collection_name: str | None = None
    embedder_config: dict = field(default_factory=dict)


@dataclass
class KnowledgeResetInput:
    """Input for knowledge reset activity."""
    collection_name: str | None = None
    embedder_config: dict = field(default_factory=dict)
```

---

## Worker Configuration

```python
# File: temporalio/contrib/crewai/_worker.py

from dataclasses import dataclass, field
from typing import Any, Callable
from crewai.llm import LLM
from crewai.memory.storage.rag_storage import RAGStorage
from crewai.memory.storage.ltm_sqlite_storage import LTMSQLiteStorage
from crewai.knowledge.storage.knowledge_storage import KnowledgeStorage


@dataclass
class CrewAIActivityConfig:
    """Configuration for CrewAI activities.

    This configuration is passed to the worker and provides factories
    for creating the actual LLM and storage instances used in activities.
    """

    # LLM factory: model name -> LLM instance
    llm_factory: Callable[[str], Any] = field(
        default_factory=lambda: lambda model: LLM(model=model)
    )

    # Memory storage factories
    short_term_storage_factory: Callable[..., RAGStorage] = field(
        default_factory=lambda: lambda **kw: RAGStorage(type="short_term", **kw)
    )

    entity_storage_factory: Callable[..., RAGStorage] = field(
        default_factory=lambda: lambda **kw: RAGStorage(type="entity", **kw)
    )

    ltm_storage_factory: Callable[..., LTMSQLiteStorage] = field(
        default_factory=lambda: lambda **kw: LTMSQLiteStorage(**kw)
    )

    # Knowledge storage factory
    knowledge_storage_factory: Callable[..., KnowledgeStorage] = field(
        default_factory=lambda: lambda **kw: KnowledgeStorage(**kw)
    )

    def get_storage(
        self,
        storage_type: str,
        embedder_config: dict | None = None,
    ) -> Any:
        """Get storage instance by type."""
        factories = {
            "short_term": self.short_term_storage_factory,
            "entity": self.entity_storage_factory,
            "long_term": self.ltm_storage_factory,
        }
        factory = factories.get(storage_type)
        if not factory:
            raise ValueError(f"Unknown storage type: {storage_type}")

        kwargs = {}
        if embedder_config:
            kwargs["embedder_config"] = embedder_config

        return factory(**kwargs)


def crewai_activities(config: CrewAIActivityConfig | None = None) -> list[Callable]:
    """Get all CrewAI activities configured with the given config.

    Args:
        config: Activity configuration with LLM and storage factories

    Returns:
        List of activity functions to register with the worker

    Example:
        worker = Worker(
            client,
            task_queue="crewai-tasks",
            workflows=[MyCrewWorkflow],
            activities=crewai_activities(CrewAIActivityConfig(
                llm_factory=lambda model: LLM(model=model),
            )),
        )
    """
    config = config or CrewAIActivityConfig()

    # Create activity instances bound to config
    # This uses a class-based approach to inject config

    class ConfiguredActivities:
        def __init__(self, cfg: CrewAIActivityConfig):
            self.config = cfg

        @activity.defn(name="crewai_llm_call")
        async def llm_call(self, input: LLMCallInput) -> LLMCallOutput:
            llm = self.config.llm_factory(input.model)
            result = await llm.acall(
                messages=input.messages,
                tools=input.tools,
                **(input.llm_kwargs or {}),
            )
            activity.heartbeat({"model": input.model, "status": "completed"})
            return LLMCallOutput(
                content=result if isinstance(result, str) else str(result),
            )

        @activity.defn(name="crewai_memory_save")
        async def memory_save(self, input: MemorySaveInput) -> None:
            storage = self.config.get_storage(
                input.storage_type,
                input.embedder_config,
            )
            await storage.asave(value=input.value, metadata=input.metadata)

        @activity.defn(name="crewai_memory_search")
        async def memory_search(self, input: MemorySearchInput) -> MemorySearchOutput:
            storage = self.config.get_storage(
                input.storage_type,
                input.embedder_config,
            )
            results = await storage.asearch(
                query=input.query,
                limit=input.limit,
                score_threshold=input.score_threshold,
            )
            return MemorySearchOutput(results=list(results))

        @activity.defn(name="crewai_memory_reset")
        async def memory_reset(self, input: MemoryResetInput) -> None:
            storage = self.config.get_storage(
                input.storage_type,
                input.embedder_config,
            )
            await storage.areset()

        @activity.defn(name="crewai_ltm_save")
        async def ltm_save(self, input: LTMSaveInput) -> None:
            storage = self.config.ltm_storage_factory(db_path=input.db_path)
            await storage.asave(
                task_description=input.task_description,
                score=input.score,
                metadata=input.metadata,
                datetime=input.datetime,
            )

        @activity.defn(name="crewai_ltm_load")
        async def ltm_load(self, input: LTMLoadInput) -> LTMLoadOutput:
            storage = self.config.ltm_storage_factory(db_path=input.db_path)
            results = await storage.aload(
                task_description=input.task_description,
                latest_n=input.latest_n,
            )
            return LTMLoadOutput(results=results or [])

        @activity.defn(name="crewai_knowledge_search")
        async def knowledge_search(
            self, input: KnowledgeSearchInput
        ) -> KnowledgeSearchOutput:
            storage = self.config.knowledge_storage_factory(
                collection_name=input.collection_name,
                embedder=input.embedder_config,
            )
            results = await storage.asearch(
                query=input.query,
                limit=input.limit,
                metadata_filter=input.metadata_filter,
                score_threshold=input.score_threshold,
            )
            return KnowledgeSearchOutput(
                results=[_to_dict(r) for r in results]
            )

        @activity.defn(name="crewai_knowledge_save")
        async def knowledge_save(self, input: KnowledgeSaveInput) -> None:
            storage = self.config.knowledge_storage_factory(
                collection_name=input.collection_name,
                embedder=input.embedder_config,
            )
            await storage.asave(documents=input.documents)

        @activity.defn(name="crewai_knowledge_reset")
        async def knowledge_reset(self, input: KnowledgeResetInput) -> None:
            storage = self.config.knowledge_storage_factory(
                collection_name=input.collection_name,
                embedder=input.embedder_config,
            )
            await storage.areset()

    instance = ConfiguredActivities(config)
    return [
        instance.llm_call,
        instance.memory_save,
        instance.memory_search,
        instance.memory_reset,
        instance.ltm_save,
        instance.ltm_load,
        instance.knowledge_search,
        instance.knowledge_save,
        instance.knowledge_reset,
    ]


def _to_dict(obj: Any) -> dict:
    """Convert object to dict for serialization."""
    if hasattr(obj, "__dict__"):
        return obj.__dict__
    if hasattr(obj, "dict"):
        return obj.dict()
    return dict(obj)
```

---

## Public API (Module Exports)

```python
# File: temporalio/contrib/crewai/__init__.py

"""CrewAI integration for Temporal workflows.

This module provides components for running CrewAI crews with Temporal
durability. All IO operations (LLM calls, tool execution, memory access,
knowledge queries) are executed as Temporal activities.

Example:
    from temporalio.contrib.crewai import (
        llm_stub,
        activity_as_tool,
        TemporalCrewRunner,
        crewai_activities,
    )

    @workflow.defn
    class MyCrewWorkflow:
        @workflow.run
        async def run(self) -> str:
            agent = Agent(
                role="Researcher",
                llm=llm_stub("gpt-4o"),
                tools=[activity_as_tool(my_activity)],
            )
            crew = Crew(agents=[agent], tasks=[...])
            runner = TemporalCrewRunner(crew)
            return (await runner.kickoff()).raw
"""

from temporalio.contrib.crewai._llm import (
    llm_stub,
    LLMActivityConfig,
)
from temporalio.contrib.crewai._tools import (
    activity_as_tool,
)
from temporalio.contrib.crewai._memory import (
    short_term_memory_stub,
    long_term_memory_stub,
    entity_memory_stub,
    MemoryActivityConfig,
)
from temporalio.contrib.crewai._knowledge import (
    knowledge_storage_stub,
    KnowledgeActivityConfig,
)
from temporalio.contrib.crewai._runner import (
    TemporalCrewRunner,
)
from temporalio.contrib.crewai._worker import (
    CrewAIActivityConfig,
    crewai_activities,
)
from temporalio.contrib.crewai._models import (
    LLMCallInput,
    LLMCallOutput,
    MemorySaveInput,
    MemorySearchInput,
    MemorySearchOutput,
    MemoryResetInput,
    LTMSaveInput,
    LTMLoadInput,
    LTMLoadOutput,
    KnowledgeSearchInput,
    KnowledgeSearchOutput,
    KnowledgeSaveInput,
    KnowledgeResetInput,
)

__all__ = [
    # LLM
    "llm_stub",
    "LLMActivityConfig",
    # Tools
    "activity_as_tool",
    # Memory
    "short_term_memory_stub",
    "long_term_memory_stub",
    "entity_memory_stub",
    "MemoryActivityConfig",
    # Knowledge
    "knowledge_storage_stub",
    "KnowledgeActivityConfig",
    # Runner
    "TemporalCrewRunner",
    # Worker
    "CrewAIActivityConfig",
    "crewai_activities",
    # Models
    "LLMCallInput",
    "LLMCallOutput",
    "MemorySaveInput",
    "MemorySearchInput",
    "MemorySearchOutput",
    "MemoryResetInput",
    "LTMSaveInput",
    "LTMLoadInput",
    "LTMLoadOutput",
    "KnowledgeSearchInput",
    "KnowledgeSearchOutput",
    "KnowledgeSaveInput",
    "KnowledgeResetInput",
]
```

---

## File Structure

```
temporalio/contrib/crewai/
├── __init__.py           # Public API exports
├── _llm.py               # llm_stub implementation
├── _tools.py             # activity_as_tool converter
├── _memory.py            # Memory stub implementations
├── _knowledge.py         # Knowledge storage stub
├── _runner.py            # TemporalCrewRunner
├── _worker.py            # Activity configuration and factory
├── _models.py            # Data models for activity IO
├── _utils.py             # Internal utilities
└── DESIGN.md             # This document
```

---

## Considerations and Limitations

### Streaming

CrewAI supports streaming LLM responses. In Temporal workflows, streaming is not directly supported because:
1. Activity results must be serializable
2. Workflow code must be deterministic

**Recommendation**: Disable streaming for Temporal execution. The `llm_stub` will not support streaming mode initially.

### Human-in-the-Loop

CrewAI supports human input via `human_input=True` on tasks. For Temporal:
- Use Temporal signals to receive human input
- Use workflow `wait_condition` to pause for input
- The `TemporalCrewRunner` could expose methods for this

### Tool Results as Context

When a tool is executed, its result becomes part of the conversation context. Since tools are activities, the result is already serialized and returned to the workflow, which passes it to subsequent LLM calls.

### Memory Persistence Across Runs

CrewAI's memory persists across crew runs (stored in ChromaDB/SQLite). With Temporal:
- Memory storage location is determined by the activity worker
- Workers should have access to the same storage backend
- Consider using external databases for production

### Error Handling

CrewAI has internal retry logic for LLM calls. With Temporal:
- Temporal's retry policy handles transient failures
- Application errors from CrewAI are propagated
- Consider configuring retry policies appropriately

### Determinism

Workflow code must be deterministic. The integration ensures:
- All IO happens in activities (non-deterministic OK there)
- Orchestration logic runs in workflow (must be deterministic)
- Random/time operations in workflows should use `workflow.random()` and `workflow.now()`

See [DETERMINISM_ISSUES.md](./DETERMINISM_ISSUES.md) for a comprehensive analysis of CrewAI operations that could break workflow determinism.

#### Asyncio Operations

Per Temporal Python SDK sandbox restrictions:

| Operation | Status | Notes |
|-----------|--------|-------|
| `asyncio.create_task()` | ✅ Safe | Deterministic in Temporal's event loop |
| `asyncio.gather()` | ✅ Safe | Results returned in input order |
| `asyncio.sleep()` | ✅ Safe | Mapped to deterministic workflow timers |
| `asyncio.as_completed()` | ⚠️ Non-deterministic | Use `workflow.as_completed()` |
| `asyncio.wait()` | ⚠️ Non-deterministic | Use `workflow.wait()` |

**Good news**: CrewAI primarily uses `asyncio.gather()` and `asyncio.create_task()` for concurrency, which are safe.

#### Required Crew Configuration

For workflow-safe execution, crews must be configured with:

```python
crew = Crew(
    agents=[...],
    tasks=[
        Task(
            ...,
            human_input=False,  # REQUIRED: input() blocks indefinitely
        )
    ],
    verbose=False,         # Recommended: minimize console I/O side effects
    planning=False,        # REQUIRED unless planning is wrapped in activity
)

# For agents:
agent = Agent(
    ...,
    llm=llm_stub("gpt-4o"),  # REQUIRED: LLM calls via activities
    inject_date=False,        # REQUIRED: datetime.now() is non-deterministic
)
```

#### CrewAI Operations Requiring Attention

| Category | Issue | Mitigation |
|----------|-------|------------|
| **Time** | `datetime.now()` in date injection | Set `inject_date=False` on agents |
| **Time** | `time.time()` in memory/metrics | Handled by memory stubs (activities) |
| **UUID** | `uuid.uuid4()` for task/crew IDs | Pre-generated before workflow or handled by framework |
| **I/O** | LLM API calls | Use `llm_stub()` (activities) |
| **I/O** | Tool execution | Use `activity_as_tool()` |
| **I/O** | Memory storage | Use memory stubs (activities) |
| **I/O** | File operations | Task `output_file` should be avoided or use activity |
| **Human Input** | `input()` calls | Set `human_input=False` or implement via signals |
| **Planning** | CrewPlanner LLM calls | Set `planning=False` or wrap in activity |
| **Threading** | RPM controller timers | Disable or handle via activity-level rate limiting |
| **Callbacks** | User-provided callbacks | Ensure callbacks are deterministic (no I/O) |

#### Validation Checklist

Before running a CrewAI crew in a Temporal workflow:

- [ ] All agents use `llm_stub()` instead of direct LLM
- [ ] All tools are created with `activity_as_tool()`
- [ ] Memory stubs are used if `memory=True`
- [ ] `human_input=False` on all tasks
- [ ] `inject_date=False` on all agents
- [ ] `planning=False` on crew (or planning wrapped in activity)
- [ ] `verbose=False` recommended
- [ ] No custom callbacks that perform I/O
- [ ] Task `output_file` is not set (or file write handled via activity)

---

## Future Enhancements

1. **Streaming Support**: Investigate using Temporal's update feature for streaming
2. **Human-in-the-Loop**: First-class support for human input via signals
3. **Crew-level Parallelism**: Support for parallel task execution
4. **Flow Integration**: Support for CrewAI Flows
5. **Caching**: Activity result caching for repeated queries
6. **Observability**: Custom search attributes for crew/agent/task tracking

---

## References

- [CrewAI Documentation](https://docs.crewai.com/)
- [CrewAI Source Code](https://github.com/crewAIInc/crewAI)
- [Temporal Python SDK](https://docs.temporal.io/develop/python)
- [LangGraph Integration](../langgraph/DESIGN.md)
- [OpenAI Agents Integration](../openai_agents/)
