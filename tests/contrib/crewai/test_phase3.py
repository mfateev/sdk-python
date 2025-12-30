"""Phase 3 Tests: Memory System Integration.

These tests verify the memory storage stub functionality:
- RAGStorage stub (short-term and entity memory)
- LTMSQLiteStorage stub (long-term memory)
- Memory activities execution
- Storage factory configuration
"""

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from temporalio import activity, workflow
from temporalio.contrib.crewai import (
    CrewAIActivityConfig,
    entity_memory_stub,
    long_term_memory_stub,
    short_term_memory_stub,
)
from temporalio.contrib.crewai._memory import (
    _is_temporal_memory_stub,
    _LTMStorageStub,
    _RAGStorageStub,
)
from temporalio.contrib.crewai._models import (
    LTMLoadInput,
    LTMResetInput,
    LTMSaveInput,
    MemoryResetInput,
    MemorySaveInput,
    MemorySearchInput,
)
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

# =============================================================================
# Unit Tests: Memory Stub Creation
# =============================================================================


def test_short_term_memory_stub_creation():
    """Test that short_term_memory_stub creates a valid stub."""
    stub = short_term_memory_stub()

    assert isinstance(stub, _RAGStorageStub)
    assert stub._storage_type == "short_term"
    assert stub._start_to_close_timeout == timedelta(seconds=30)
    assert stub._task_queue is None


def test_entity_memory_stub_creation():
    """Test that entity_memory_stub creates a valid stub."""
    stub = entity_memory_stub()

    assert isinstance(stub, _RAGStorageStub)
    assert stub._storage_type == "entity"


def test_long_term_memory_stub_creation():
    """Test that long_term_memory_stub creates a valid stub."""
    stub = long_term_memory_stub()

    assert isinstance(stub, _LTMStorageStub)
    assert stub._db_path is None


def test_long_term_memory_stub_with_db_path():
    """Test that long_term_memory_stub accepts a db_path."""
    stub = long_term_memory_stub(db_path="/tmp/test.db")

    assert stub._db_path == "/tmp/test.db"


def test_memory_stub_with_custom_timeout():
    """Test that memory stubs accept custom timeout."""
    stub = short_term_memory_stub(start_to_close_timeout=timedelta(seconds=60))

    assert stub._start_to_close_timeout == timedelta(seconds=60)


def test_memory_stub_with_task_queue():
    """Test that memory stubs accept task queue."""
    stub = entity_memory_stub(task_queue="memory-queue")

    assert stub._task_queue == "memory-queue"


# =============================================================================
# Unit Tests: _is_temporal_memory_stub
# =============================================================================


def test_is_temporal_memory_stub_returns_true_for_rag_stub():
    """Test _is_temporal_memory_stub returns True for RAG stubs."""
    stub = short_term_memory_stub()
    assert _is_temporal_memory_stub(stub) is True


def test_is_temporal_memory_stub_returns_true_for_ltm_stub():
    """Test _is_temporal_memory_stub returns True for LTM stubs."""
    stub = long_term_memory_stub()
    assert _is_temporal_memory_stub(stub) is True


def test_is_temporal_memory_stub_returns_false_for_non_stub():
    """Test _is_temporal_memory_stub returns False for non-stub objects."""
    assert _is_temporal_memory_stub("not a stub") is False
    assert _is_temporal_memory_stub(None) is False
    assert _is_temporal_memory_stub({}) is False


# =============================================================================
# Unit Tests: Synchronous methods raise NotImplementedError
# =============================================================================


def test_rag_storage_stub_sync_save_raises():
    """Test that RAGStorage sync save raises NotImplementedError."""
    stub = short_term_memory_stub()

    with pytest.raises(NotImplementedError) as exc_info:
        stub.save("value", {"key": "meta"})

    assert "Synchronous save()" in str(exc_info.value)


def test_rag_storage_stub_sync_search_raises():
    """Test that RAGStorage sync search raises NotImplementedError."""
    stub = short_term_memory_stub()

    with pytest.raises(NotImplementedError) as exc_info:
        stub.search("query")

    assert "Synchronous search()" in str(exc_info.value)


def test_rag_storage_stub_sync_reset_raises():
    """Test that RAGStorage sync reset raises NotImplementedError."""
    stub = short_term_memory_stub()

    with pytest.raises(NotImplementedError) as exc_info:
        stub.reset()

    assert "Synchronous reset()" in str(exc_info.value)


def test_ltm_storage_stub_sync_save_raises():
    """Test that LTMStorage sync save raises NotImplementedError."""
    stub = long_term_memory_stub()

    with pytest.raises(NotImplementedError) as exc_info:
        stub.save("task", {"key": "meta"}, "2024-01-01", 1.0)

    assert "Synchronous save()" in str(exc_info.value)


def test_ltm_storage_stub_sync_load_raises():
    """Test that LTMStorage sync load raises NotImplementedError."""
    stub = long_term_memory_stub()

    with pytest.raises(NotImplementedError) as exc_info:
        stub.load("task", 5)

    assert "Synchronous load()" in str(exc_info.value)


def test_ltm_storage_stub_sync_reset_raises():
    """Test that LTMStorage sync reset raises NotImplementedError."""
    stub = long_term_memory_stub()

    with pytest.raises(NotImplementedError) as exc_info:
        stub.reset()

    assert "Synchronous reset()" in str(exc_info.value)


# =============================================================================
# Unit Tests: CrewAIActivityConfig storage factories
# =============================================================================


def test_config_get_rag_storage_raises_without_factory():
    """Test that get_rag_storage raises without factory configured."""
    config = CrewAIActivityConfig(llm_factory=lambda m: MagicMock())

    with pytest.raises(ValueError) as exc_info:
        config.get_rag_storage("short_term")

    assert "rag_storage_factory must be configured" in str(exc_info.value)


def test_config_get_ltm_storage_raises_without_factory():
    """Test that get_ltm_storage raises without factory configured."""
    config = CrewAIActivityConfig(llm_factory=lambda m: MagicMock())

    with pytest.raises(ValueError) as exc_info:
        config.get_ltm_storage(None)

    assert "ltm_storage_factory must be configured" in str(exc_info.value)


def test_config_get_rag_storage_calls_factory():
    """Test that get_rag_storage calls the configured factory."""
    mock_storage = MagicMock()
    factory = MagicMock(return_value=mock_storage)

    config = CrewAIActivityConfig(
        llm_factory=lambda m: MagicMock(),
        rag_storage_factory=factory,
    )

    result = config.get_rag_storage("short_term")

    factory.assert_called_once_with("short_term")
    assert result is mock_storage


def test_config_get_ltm_storage_calls_factory():
    """Test that get_ltm_storage calls the configured factory."""
    mock_storage = MagicMock()
    factory = MagicMock(return_value=mock_storage)

    config = CrewAIActivityConfig(
        llm_factory=lambda m: MagicMock(),
        ltm_storage_factory=factory,
    )

    result = config.get_ltm_storage("/tmp/test.db")

    factory.assert_called_once_with("/tmp/test.db")
    assert result is mock_storage


# =============================================================================
# Integration Tests: Memory Activities via Direct Activity Calls
# =============================================================================

# Note: Similar to Phase 2, we test the underlying mechanism by calling
# activities directly with dict args, avoiding sandbox import issues.


@activity.defn(name="mock_memory_save")
async def mock_memory_save_activity(args: dict) -> None:
    """Mock activity for memory save."""
    pass


@activity.defn(name="mock_memory_search")
async def mock_memory_search_activity(args: dict) -> dict:
    """Mock activity for memory search."""
    query = args.get("query", "")
    return {
        "results": [
            {"content": f"Result for {query}", "score": 0.9},
            {"content": "Another result", "score": 0.8},
        ]
    }


@activity.defn(name="mock_ltm_save")
async def mock_ltm_save_activity(args: dict) -> None:
    """Mock activity for LTM save."""
    pass


@activity.defn(name="mock_ltm_load")
async def mock_ltm_load_activity(args: dict) -> dict:
    """Mock activity for LTM load."""
    task_desc = args.get("task_description", "")
    return {
        "results": [
            {"metadata": {"agent": "test"}, "datetime": task_desc, "score": 0.95}
        ]
    }


# Workflows that call activities directly (similar to Phase 2 pattern)


@workflow.defn
class DirectRAGMemoryWorkflow:
    """Workflow that directly calls memory activities with dict args."""

    @workflow.run
    async def run(self, query: str) -> list:
        # Save
        await workflow.execute_activity(
            "mock_memory_save",
            {"storage_type": "short_term", "value": "test", "metadata": {}},
            start_to_close_timeout=timedelta(seconds=30),
        )

        # Search
        result = await workflow.execute_activity(
            "mock_memory_search",
            {"storage_type": "short_term", "query": query, "limit": 5},
            start_to_close_timeout=timedelta(seconds=30),
        )

        return result["results"] if result else []


@workflow.defn
class DirectLTMMemoryWorkflow:
    """Workflow that directly calls LTM activities with dict args."""

    @workflow.run
    async def run(self, task_desc: str) -> list:
        # Save
        await workflow.execute_activity(
            "mock_ltm_save",
            {
                "task_description": task_desc,
                "metadata": {"agent": "test"},
                "datetime": "2024-01-01",
                "score": 0.95,
            },
            start_to_close_timeout=timedelta(seconds=30),
        )

        # Load
        result = await workflow.execute_activity(
            "mock_ltm_load",
            {"task_description": task_desc, "latest_n": 5},
            start_to_close_timeout=timedelta(seconds=30),
        )

        return result["results"] if result else []


@pytest.mark.asyncio
async def test_rag_memory_workflow_integration():
    """Test RAG memory operations via workflow."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[DirectRAGMemoryWorkflow],
            activities=[mock_memory_save_activity, mock_memory_search_activity],
        ):
            results = await env.client.execute_workflow(
                DirectRAGMemoryWorkflow.run,
                "test query",
                id="test-rag-memory",
                task_queue="test-queue",
            )

            assert len(results) == 2
            assert "test query" in results[0]["content"]


@pytest.mark.asyncio
async def test_ltm_memory_workflow_integration():
    """Test LTM memory operations via workflow."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[DirectLTMMemoryWorkflow],
            activities=[mock_ltm_save_activity, mock_ltm_load_activity],
        ):
            results = await env.client.execute_workflow(
                DirectLTMMemoryWorkflow.run,
                "test task",
                id="test-ltm-memory",
                task_queue="test-queue",
            )

            assert len(results) == 1
            assert results[0]["score"] == 0.95


# =============================================================================
# Integration Tests: Activity implementations with mock storage
# =============================================================================


@pytest.mark.asyncio
async def test_memory_save_activity():
    """Test the memory save activity implementation."""
    from temporalio.contrib.crewai._activities import CrewAIActivities

    mock_storage = AsyncMock()
    config = CrewAIActivityConfig(
        llm_factory=lambda m: MagicMock(),
        rag_storage_factory=lambda t: mock_storage,
    )

    activities = CrewAIActivities(config)

    input_data = MemorySaveInput(
        storage_type="short_term",
        value="test value",
        metadata={"key": "value"},
    )

    # Call the activity method directly (not via Temporal)
    await activities.memory_save(input_data)

    mock_storage.asave.assert_called_once_with("test value", {"key": "value"})


@pytest.mark.asyncio
async def test_memory_search_activity():
    """Test the memory search activity implementation."""
    from temporalio.contrib.crewai._activities import CrewAIActivities

    mock_storage = AsyncMock()
    mock_storage.asearch.return_value = [{"content": "result", "score": 0.9}]

    config = CrewAIActivityConfig(
        llm_factory=lambda m: MagicMock(),
        rag_storage_factory=lambda t: mock_storage,
    )

    activities = CrewAIActivities(config)

    input_data = MemorySearchInput(
        storage_type="short_term",
        query="test query",
        limit=5,
        score_threshold=0.6,
    )

    result = await activities.memory_search(input_data)

    mock_storage.asearch.assert_called_once_with(
        query="test query",
        limit=5,
        filter=None,
        score_threshold=0.6,
    )
    assert len(result.results) == 1


@pytest.mark.asyncio
async def test_memory_reset_activity():
    """Test the memory reset activity implementation."""
    from temporalio.contrib.crewai._activities import CrewAIActivities

    mock_storage = MagicMock()
    config = CrewAIActivityConfig(
        llm_factory=lambda m: MagicMock(),
        rag_storage_factory=lambda t: mock_storage,
    )

    activities = CrewAIActivities(config)

    input_data = MemoryResetInput(storage_type="entity")

    await activities.memory_reset(input_data)

    mock_storage.reset.assert_called_once()


@pytest.mark.asyncio
async def test_ltm_save_activity():
    """Test the LTM save activity implementation."""
    from temporalio.contrib.crewai._activities import CrewAIActivities

    mock_storage = AsyncMock()
    config = CrewAIActivityConfig(
        llm_factory=lambda m: MagicMock(),
        ltm_storage_factory=lambda p: mock_storage,
    )

    activities = CrewAIActivities(config)

    input_data = LTMSaveInput(
        task_description="test task",
        metadata={"agent": "test"},
        datetime="2024-01-01T00:00:00",
        score=0.95,
        db_path=None,
    )

    await activities.ltm_save(input_data)

    mock_storage.asave.assert_called_once_with(
        task_description="test task",
        metadata={"agent": "test"},
        datetime="2024-01-01T00:00:00",
        score=0.95,
    )


@pytest.mark.asyncio
async def test_ltm_load_activity():
    """Test the LTM load activity implementation."""
    from temporalio.contrib.crewai._activities import CrewAIActivities

    mock_storage = AsyncMock()
    mock_storage.aload.return_value = [
        {"metadata": {"agent": "test"}, "datetime": "2024-01-01", "score": 0.9}
    ]

    config = CrewAIActivityConfig(
        llm_factory=lambda m: MagicMock(),
        ltm_storage_factory=lambda p: mock_storage,
    )

    activities = CrewAIActivities(config)

    input_data = LTMLoadInput(
        task_description="test task",
        latest_n=5,
        db_path=None,
    )

    result = await activities.ltm_load(input_data)

    mock_storage.aload.assert_called_once_with(
        task_description="test task",
        latest_n=5,
    )
    assert len(result.results) == 1


@pytest.mark.asyncio
async def test_ltm_reset_activity():
    """Test the LTM reset activity implementation."""
    from temporalio.contrib.crewai._activities import CrewAIActivities

    mock_storage = AsyncMock()
    config = CrewAIActivityConfig(
        llm_factory=lambda m: MagicMock(),
        ltm_storage_factory=lambda p: mock_storage,
    )

    activities = CrewAIActivities(config)

    input_data = LTMResetInput(db_path=None)

    await activities.ltm_reset(input_data)

    mock_storage.areset.assert_called_once()
