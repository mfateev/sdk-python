"""Phase 4 Tests: Knowledge Base Integration.

These tests verify the knowledge storage stub functionality:
- KnowledgeStorage stub creation
- Knowledge activities execution
- Storage factory configuration
"""

from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from temporalio import activity, workflow
from temporalio.contrib.crewai import (
    CrewAIActivityConfig,
    knowledge_storage_stub,
)
from temporalio.contrib.crewai._knowledge import (
    _is_temporal_knowledge_stub,
    _KnowledgeStorageStub,
)
from temporalio.contrib.crewai._models import (
    KnowledgeResetInput,
    KnowledgeSaveInput,
    KnowledgeSearchInput,
)
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

# =============================================================================
# Unit Tests: Knowledge Stub Creation
# =============================================================================


def test_knowledge_storage_stub_creation():
    """Test that knowledge_storage_stub creates a valid stub."""
    stub = knowledge_storage_stub()

    assert isinstance(stub, _KnowledgeStorageStub)
    assert stub._collection_name is None
    assert stub._start_to_close_timeout == timedelta(seconds=30)
    assert stub._task_queue is None


def test_knowledge_storage_stub_with_collection_name():
    """Test that knowledge_storage_stub accepts collection_name."""
    stub = knowledge_storage_stub(collection_name="my_docs")

    assert stub._collection_name == "my_docs"


def test_knowledge_storage_stub_with_custom_timeout():
    """Test that knowledge stub accepts custom timeout."""
    stub = knowledge_storage_stub(start_to_close_timeout=timedelta(seconds=60))

    assert stub._start_to_close_timeout == timedelta(seconds=60)


def test_knowledge_storage_stub_with_task_queue():
    """Test that knowledge stub accepts task queue."""
    stub = knowledge_storage_stub(task_queue="knowledge-queue")

    assert stub._task_queue == "knowledge-queue"


# =============================================================================
# Unit Tests: _is_temporal_knowledge_stub
# =============================================================================


def test_is_temporal_knowledge_stub_returns_true_for_stub():
    """Test _is_temporal_knowledge_stub returns True for knowledge stubs."""
    stub = knowledge_storage_stub()
    assert _is_temporal_knowledge_stub(stub) is True


def test_is_temporal_knowledge_stub_returns_false_for_non_stub():
    """Test _is_temporal_knowledge_stub returns False for non-stub objects."""
    assert _is_temporal_knowledge_stub("not a stub") is False
    assert _is_temporal_knowledge_stub(None) is False
    assert _is_temporal_knowledge_stub({}) is False


# =============================================================================
# Unit Tests: Synchronous methods raise NotImplementedError
# =============================================================================


def test_knowledge_stub_sync_search_raises():
    """Test that KnowledgeStorage sync search raises NotImplementedError."""
    stub = knowledge_storage_stub()

    with pytest.raises(NotImplementedError) as exc_info:
        stub.search(["query"])

    assert "Synchronous search()" in str(exc_info.value)


def test_knowledge_stub_sync_save_raises():
    """Test that KnowledgeStorage sync save raises NotImplementedError."""
    stub = knowledge_storage_stub()

    with pytest.raises(NotImplementedError) as exc_info:
        stub.save(["doc1", "doc2"])

    assert "Synchronous save()" in str(exc_info.value)


def test_knowledge_stub_sync_reset_raises():
    """Test that KnowledgeStorage sync reset raises NotImplementedError."""
    stub = knowledge_storage_stub()

    with pytest.raises(NotImplementedError) as exc_info:
        stub.reset()

    assert "Synchronous reset()" in str(exc_info.value)


# =============================================================================
# Unit Tests: CrewAIActivityConfig knowledge factory
# =============================================================================


def test_config_get_knowledge_storage_raises_without_factory():
    """Test that get_knowledge_storage raises without factory configured."""
    config = CrewAIActivityConfig(llm_factory=lambda m: MagicMock())

    with pytest.raises(ValueError) as exc_info:
        config.get_knowledge_storage(None)

    assert "knowledge_storage_factory must be configured" in str(exc_info.value)


def test_config_get_knowledge_storage_calls_factory():
    """Test that get_knowledge_storage calls the configured factory."""
    mock_storage = MagicMock()
    factory = MagicMock(return_value=mock_storage)

    config = CrewAIActivityConfig(
        llm_factory=lambda m: MagicMock(),
        knowledge_storage_factory=factory,
    )

    result = config.get_knowledge_storage("my_collection")

    factory.assert_called_once_with("my_collection")
    assert result is mock_storage


# =============================================================================
# Integration Tests: Knowledge Activities via Direct Activity Calls
# =============================================================================


@activity.defn(name="mock_knowledge_search")
async def mock_knowledge_search_activity(args: dict) -> dict:
    """Mock activity for knowledge search."""
    query = args.get("query", [])
    query_str = " ".join(query) if query else ""
    return {
        "results": [
            {"content": f"Result for {query_str}", "score": 0.9},
            {"content": "Another knowledge result", "score": 0.85},
        ]
    }


@activity.defn(name="mock_knowledge_save")
async def mock_knowledge_save_activity(args: dict) -> None:
    """Mock activity for knowledge save."""
    pass


@activity.defn(name="mock_knowledge_reset")
async def mock_knowledge_reset_activity(args: dict) -> None:
    """Mock activity for knowledge reset."""
    pass


@workflow.defn
class DirectKnowledgeWorkflow:
    """Workflow that directly calls knowledge activities with dict args."""

    @workflow.run
    async def run(self, query: list[str]) -> list[dict[str, Any]]:
        # Save
        await workflow.execute_activity(
            "mock_knowledge_save",
            {"documents": ["doc1", "doc2"], "collection_name": "test"},
            start_to_close_timeout=timedelta(seconds=30),
        )

        # Search
        result = await workflow.execute_activity(
            "mock_knowledge_search",
            {"query": query, "limit": 5, "collection_name": "test"},
            start_to_close_timeout=timedelta(seconds=30),
        )

        # Reset
        await workflow.execute_activity(
            "mock_knowledge_reset",
            {"collection_name": "test"},
            start_to_close_timeout=timedelta(seconds=30),
        )

        return result["results"] if result else []


@pytest.mark.asyncio
async def test_knowledge_workflow_integration():
    """Test knowledge operations via workflow."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[DirectKnowledgeWorkflow],
            activities=[
                mock_knowledge_search_activity,
                mock_knowledge_save_activity,
                mock_knowledge_reset_activity,
            ],
        ):
            results = await env.client.execute_workflow(
                DirectKnowledgeWorkflow.run,
                ["test", "query"],
                id="test-knowledge",
                task_queue="test-queue",
            )

            assert len(results) == 2
            assert "test query" in results[0]["content"]


# =============================================================================
# Integration Tests: Activity implementations with mock storage
# =============================================================================


@pytest.mark.asyncio
async def test_knowledge_search_activity():
    """Test the knowledge search activity implementation."""
    from temporalio.contrib.crewai._activities import CrewAIActivities

    mock_storage = AsyncMock()
    mock_storage.asearch.return_value = [{"content": "result", "score": 0.9}]

    config = CrewAIActivityConfig(
        llm_factory=lambda m: MagicMock(),
        knowledge_storage_factory=lambda n: mock_storage,
    )

    activities = CrewAIActivities(config)

    input_data = KnowledgeSearchInput(
        query=["test", "query"],
        limit=5,
        score_threshold=0.6,
        collection_name="test_collection",
    )

    result = await activities.knowledge_search(input_data)

    mock_storage.asearch.assert_called_once_with(
        query=["test", "query"],
        limit=5,
        metadata_filter=None,
        score_threshold=0.6,
    )
    assert len(result.results) == 1


@pytest.mark.asyncio
async def test_knowledge_save_activity():
    """Test the knowledge save activity implementation."""
    from temporalio.contrib.crewai._activities import CrewAIActivities

    mock_storage = AsyncMock()

    config = CrewAIActivityConfig(
        llm_factory=lambda m: MagicMock(),
        knowledge_storage_factory=lambda n: mock_storage,
    )

    activities = CrewAIActivities(config)

    input_data = KnowledgeSaveInput(
        documents=["doc1", "doc2"],
        collection_name="test_collection",
    )

    await activities.knowledge_save(input_data)

    mock_storage.asave.assert_called_once_with(["doc1", "doc2"])


@pytest.mark.asyncio
async def test_knowledge_reset_activity():
    """Test the knowledge reset activity implementation."""
    from temporalio.contrib.crewai._activities import CrewAIActivities

    mock_storage = AsyncMock()

    config = CrewAIActivityConfig(
        llm_factory=lambda m: MagicMock(),
        knowledge_storage_factory=lambda n: mock_storage,
    )

    activities = CrewAIActivities(config)

    input_data = KnowledgeResetInput(collection_name="test_collection")

    await activities.knowledge_reset(input_data)

    mock_storage.areset.assert_called_once()
