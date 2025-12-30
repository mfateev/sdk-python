# Temporal CrewAI Integration

⚠️ **Experimental** - This module is experimental and may change in future versions.

## Introduction

This integration combines [CrewAI](https://github.com/crewAIInc/crewAI) with [Temporal's durable execution](https://docs.temporal.io/evaluate/understanding-temporal#durable-execution).
It allows you to build durable AI agent crews that never lose their progress, automatically retry failed operations, and provide full observability into agent execution.

Temporal and CrewAI are complementary technologies.
Temporal provides a crash-proof system foundation, taking care of the distributed systems challenges inherent to production agentic systems.
CrewAI offers a powerful framework for defining multi-agent crews with roles, goals, tools, and collaboration patterns.

This document is organized as follows:

- **[Quick Start](#quick-start)** - Your first durable CrewAI crew
- **[Architecture](#architecture)** - How the integration works
- **[LLM Configuration](#llm-configuration)** - Configuring LLM calls as activities
- **[Tool Integration](#tool-integration)** - Using activities as agent tools
- **[Memory System](#memory-system)** - Durable memory for crews
- **[Knowledge Base](#knowledge-base)** - RAG with durable storage
- **[Validation](#validation)** - Ensuring correct configuration
- **[Plugin Setup](#plugin-setup)** - Simplified worker configuration

## Architecture

The diagram below shows how CrewAI integrates with Temporal.
Each LLM call and tool execution runs as a Temporal activity, providing automatic retries and failure recovery.
The workflow orchestrates the crew execution, maintaining state across all operations.

```text
            +---------------------+
            |   Temporal Server   |      (Stores workflow state,
            +---------------------+       schedules activities,
                     ^                    persists progress)
                     |
        Save state,  |   Schedule Tasks,
        progress,    |   load state on resume
        timeouts     |
                     |
+------------------------------------------------------+
|                      Worker                          |
|   +----------------------------------------------+   |
|   |              Workflow Code                   |   |
|   |    (CrewAI Crew with llm_stub agents)        |   |
|   +----------------------------------------------+   |
|          |          |                |               |
|          v          v                v               |
|   +-----------+ +-----------+ +-------------+        |
|   | Activity  | | Activity  | |  Activity   |        |
|   | (LLM Call)| | (Tool)    | | (Memory)    |        |
|   +-----------+ +-----------+ +-------------+        |
|         |           |                |               |
+------------------------------------------------------+
          |           |                |
          v           v                v
      [LLM APIs, External Services, Vector DBs, etc.]
```

## Installation

```bash
pip install temporalio crewai
```

## Quick Start

```python
from datetime import timedelta
from crewai import Agent, Crew, Task
from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.contrib.crewai import (
    CrewAIPlugin,
    CrewAIActivityConfig,
    llm_stub,
    activity_as_tool,
    TemporalCrewRunner,
)


# 1. Define any custom tools as activities
@activity.defn
async def search_web(query: str) -> str:
    """Search the web for information."""
    # Your implementation here
    return f"Results for: {query}"


# 2. Define your workflow
@workflow.defn
class ResearchCrewWorkflow:
    @workflow.run
    async def run(self, topic: str) -> str:
        # Create agent with llm_stub (routes LLM calls through activities)
        researcher = Agent(
            role="Researcher",
            goal=f"Research {topic} thoroughly",
            backstory="You are an expert researcher.",
            llm=llm_stub("gpt-4"),  # Use stub instead of direct LLM
            tools=[activity_as_tool(search_web)],  # Wrap activity as tool
        )

        # Create task
        task = Task(
            description=f"Research the topic: {topic}",
            expected_output="A comprehensive summary",
            agent=researcher,
        )

        # Create crew and run with validation
        crew = Crew(agents=[researcher], tasks=[task])
        runner = TemporalCrewRunner(crew)
        result = await runner.kickoff()
        return result.raw


# 3. Run with Temporal
async def main():
    # Create plugin with LLM factory
    plugin = CrewAIPlugin(
        config=CrewAIActivityConfig(
            llm_factory=lambda model: __import__("crewai").LLM(model=model),
        )
    )

    # Connect to Temporal
    client = await Client.connect("localhost:7233", plugins=[plugin])

    # Start worker
    async with Worker(
        client,
        task_queue="crewai-queue",
        workflows=[ResearchCrewWorkflow],
        activities=[search_web],  # Register custom activities
    ):
        # Execute workflow
        result = await client.execute_workflow(
            ResearchCrewWorkflow.run,
            "artificial intelligence trends",
            id="research-workflow-1",
            task_queue="crewai-queue",
        )
        print(result)
```

## LLM Configuration

All LLM calls must go through `llm_stub()` to ensure they execute as Temporal activities.

### Basic Usage

```python
from temporalio.contrib.crewai import llm_stub

agent = Agent(
    role="Writer",
    goal="Write compelling content",
    llm=llm_stub("gpt-4"),  # Model name passed to LLM factory
)
```

### Activity Options

Configure timeouts and retries for LLM calls:

```python
from datetime import timedelta
from temporalio.common import RetryPolicy
from temporalio.contrib.crewai import llm_stub, LLMActivityConfig

# Create config with custom settings
llm_config = LLMActivityConfig(
    start_to_close_timeout=timedelta(minutes=5),
    retry_policy=RetryPolicy(
        initial_interval=timedelta(seconds=1),
        maximum_interval=timedelta(seconds=60),
        backoff_coefficient=2.0,
        maximum_attempts=3,
    ),
    heartbeat_timeout=timedelta(seconds=30),
    task_queue="llm-workers",  # Route to specialized workers
)

agent = Agent(
    role="Writer",
    llm=llm_stub("gpt-4", activity_config=llm_config),
)
```

### LLM Factory Configuration

The `llm_factory` in `CrewAIActivityConfig` creates LLM instances on the worker side:

```python
from crewai import LLM

config = CrewAIActivityConfig(
    llm_factory=lambda model: LLM(
        model=model,
        temperature=0.7,
        # Any other LLM configuration
    ),
)
```

## Tool Integration

Wrap Temporal activities as CrewAI tools using `activity_as_tool()`:

### Defining Tool Activities

```python
from temporalio import activity

@activity.defn
async def search_database(query: str, limit: int = 10) -> str:
    """Search the internal database for relevant information."""
    # Your implementation
    return f"Found {limit} results for: {query}"


@activity.defn
async def send_email(to: str, subject: str, body: str) -> str:
    """Send an email to a recipient."""
    # Your implementation
    return f"Email sent to {to}"
```

### Using Tools in Agents

```python
from datetime import timedelta
from temporalio.common import RetryPolicy
from temporalio.contrib.crewai import activity_as_tool

agent = Agent(
    role="Assistant",
    goal="Help users with their requests",
    llm=llm_stub("gpt-4"),
    tools=[
        # Basic usage
        activity_as_tool(search_database),

        # With custom timeout
        activity_as_tool(
            send_email,
            start_to_close_timeout=timedelta(seconds=30),
        ),

        # With retry policy
        activity_as_tool(
            external_api_call,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(
                maximum_attempts=5,
                initial_interval=timedelta(seconds=1),
            ),
        ),

        # Route to specialized workers
        activity_as_tool(
            gpu_processing,
            task_queue="gpu-workers",
            start_to_close_timeout=timedelta(hours=1),
        ),
    ],
)
```

### Tool Configuration Options

| Option | Description |
|--------|-------------|
| `start_to_close_timeout` | Max time for a single execution (default: 60s) |
| `schedule_to_close_timeout` | Total time including retries |
| `retry_policy` | Temporal `RetryPolicy` for failures |
| `heartbeat_timeout` | Interval for long-running tools |
| `task_queue` | Route to specialized workers |
| `cancellation_type` | How cancellation is handled |
| `name` | Override tool name (default: activity name) |
| `description` | Override tool description (default: docstring) |

## Memory System

CrewAI's memory system can be made durable using Temporal activity-backed storage:

### Short-Term Memory

```python
from crewai.memory.short_term import ShortTermMemory
from temporalio.contrib.crewai import short_term_memory_stub

crew = Crew(
    agents=[...],
    tasks=[...],
    memory=True,
    short_term_memory=ShortTermMemory(
        storage=short_term_memory_stub(),
    ),
)
```

### Long-Term Memory

```python
from crewai.memory.long_term import LongTermMemory
from temporalio.contrib.crewai import long_term_memory_stub

crew = Crew(
    agents=[...],
    tasks=[...],
    memory=True,
    long_term_memory=LongTermMemory(
        storage=long_term_memory_stub(),
    ),
)
```

### Entity Memory

```python
from crewai.memory.entity import EntityMemory
from temporalio.contrib.crewai import entity_memory_stub

crew = Crew(
    agents=[...],
    tasks=[...],
    memory=True,
    entity_memory=EntityMemory(
        storage=entity_memory_stub(),
    ),
)
```

### Memory Activity Configuration

Configure memory storage factories on the worker side:

```python
from crewai.memory.storage.rag_storage import RAGStorage
from crewai.memory.storage.ltm_sqlite_storage import LTMSQLiteStorage

config = CrewAIActivityConfig(
    llm_factory=lambda model: LLM(model=model),
    rag_storage_factory=lambda storage_type: RAGStorage(type=storage_type),
    ltm_storage_factory=lambda db_path: LTMSQLiteStorage(db_path=db_path),
)
```

## Knowledge Base

Enable durable knowledge base queries for RAG use cases:

### Using Knowledge Storage

```python
from temporalio.contrib.crewai import knowledge_storage_stub

# Create knowledge storage stub
storage = knowledge_storage_stub(collection_name="my_docs")

# Use with CrewAI's knowledge sources
# (Configuration depends on your knowledge source setup)
```

### Knowledge Activity Configuration

```python
from crewai.knowledge.storage import KnowledgeStorage

config = CrewAIActivityConfig(
    llm_factory=lambda model: LLM(model=model),
    knowledge_storage_factory=lambda name: KnowledgeStorage(collection_name=name),
)
```

## Validation

`TemporalCrewRunner` validates that your crew is properly configured for Temporal execution:

### What Gets Validated

| Component | Requirement |
|-----------|-------------|
| Agent LLM | Must use `llm_stub()` |
| Agent tools | Must use `activity_as_tool()` |
| Agent max_rpm | Must not be set (incompatible with Temporal) |
| Task human_input | Must be `False` |
| Crew max_rpm | Must not be set |
| Planning LLM | Must use `llm_stub()` if planning enabled |
| Memory storage | Must use memory stubs if memory enabled |
| Knowledge storage | Must use knowledge stubs if configured |

### Validation Example

```python
from temporalio.contrib.crewai import TemporalCrewRunner, CrewValidationError

try:
    runner = TemporalCrewRunner(crew)
    result = await runner.kickoff()
except CrewValidationError as e:
    print(f"Configuration error: {e}")
    # Error messages guide you to fix issues:
    # - Agent 'Researcher' must use llm_stub() instead of a direct LLM
    # - Agent 'Researcher' tool 'search' must use activity_as_tool()
```

### Skipping Validation

For testing, you can skip validation (not recommended for production):

```python
runner = TemporalCrewRunner(crew, skip_validation=True)
```

## Plugin Setup

Use `CrewAIPlugin` for simplified worker configuration:

### Basic Plugin Usage

```python
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.contrib.crewai import CrewAIPlugin, CrewAIActivityConfig

# Create plugin
plugin = CrewAIPlugin(
    config=CrewAIActivityConfig(
        llm_factory=lambda model: LLM(model=model),
    )
)

# Connect with plugin (configures data converter)
client = await Client.connect("localhost:7233", plugins=[plugin])

# Worker automatically gets CrewAI activities registered
async with Worker(
    client,
    task_queue="crewai-queue",
    workflows=[MyCrewWorkflow],
    # activities are automatically added by plugin
):
    await worker_running()
```

### What the Plugin Configures

1. **Data Converter**: Sets up Pydantic payload converter for type-safe serialization
2. **Activities**: Registers all CrewAI activities (LLM, memory, knowledge)
3. **Sandbox**: Configures passthrough for the crewai module in sandboxed workflows

### Separate Activity Worker

If you run activities on a separate worker:

```python
# Workflow worker (no activities)
plugin = CrewAIPlugin(
    config=config,
    register_activities=False,  # Don't register activities
)
workflow_worker = Worker(
    client,
    task_queue="crewai-queue",
    workflows=[MyCrewWorkflow],
)

# Activity worker (no workflows)
activity_worker = Worker(
    client,
    task_queue="crewai-queue",
    activities=crewai_activities(config) + [my_custom_activity],
)
```

## Manual Setup (Without Plugin)

If you prefer manual configuration:

```python
from temporalio.contrib.crewai import (
    crewai_activities,
    crewai_data_converter,
    CrewAIActivityConfig,
)

config = CrewAIActivityConfig(
    llm_factory=lambda model: LLM(model=model),
)

# Connect with data converter
client = await Client.connect(
    "localhost:7233",
    data_converter=crewai_data_converter,
)

# Create worker with activities
worker = Worker(
    client,
    task_queue="crewai-queue",
    workflows=[MyCrewWorkflow],
    activities=crewai_activities(config) + [my_custom_activity],
)
```

## Configuration Reference

### CrewAIActivityConfig

| Field | Type | Description |
|-------|------|-------------|
| `llm_factory` | `Callable[[str], Any]` | Factory to create LLM from model name (required) |
| `llm_activity_config` | `LLMActivityConfig` | Activity config for LLM calls |
| `rag_storage_factory` | `Callable[[str], Any]` | Factory for RAG storage |
| `ltm_storage_factory` | `Callable[[str\|None], Any]` | Factory for LTM storage |
| `knowledge_storage_factory` | `Callable[[str\|None], Any]` | Factory for knowledge storage |

### LLMActivityConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `start_to_close_timeout` | `timedelta` | 60s | Max time for LLM call |
| `schedule_to_close_timeout` | `timedelta\|None` | None | Total time including retries |
| `retry_policy` | `RetryPolicy\|None` | None | Retry policy for failures |
| `heartbeat_timeout` | `timedelta\|None` | None | Heartbeat interval |
| `task_queue` | `str\|None` | None | Route to specific workers |
| `cancellation_type` | `ActivityCancellationType` | TRY_CANCEL | Cancellation handling |

## Important Notes

### Async Execution Required

CrewAI crews must run with `akickoff()` (async), not `kickoff()` (sync). The `TemporalCrewRunner` handles this automatically:

```python
# Correct - uses async
runner = TemporalCrewRunner(crew)
result = await runner.kickoff()

# Also correct - async with inputs
result = await runner.kickoff(inputs={"topic": "AI"})

# Process multiple inputs
results = await runner.kickoff_for_each(inputs=[
    {"topic": "AI"},
    {"topic": "ML"},
])
```

### Human Input Not Supported

Human input (`human_input=True` on tasks) is not supported in Temporal workflows. For human-in-the-loop patterns, use Temporal signals:

```python
@workflow.defn
class ApprovalWorkflow:
    def __init__(self):
        self._approved = None

    @workflow.signal
    def approve(self, approved: bool):
        self._approved = approved

    @workflow.run
    async def run(self, data: str) -> str:
        # Run crew to generate proposal
        result = await runner.kickoff()

        # Wait for human approval via signal
        await workflow.wait_condition(lambda: self._approved is not None)

        if self._approved:
            return f"Approved: {result.raw}"
        return "Rejected"
```

### Rate Limiting

Rate limiting (`max_rpm` on agents or crews) is not compatible with Temporal workflows. Temporal handles retries and backoff through its retry policies instead.

### Activity Registration

When using the plugin, LLM, memory, and knowledge activities are automatically registered. You only need to register your custom tool activities:

```python
worker = Worker(
    client,
    task_queue="crewai-queue",
    workflows=[MyWorkflow],
    activities=[my_tool_activity],  # Only custom activities
)
```
