"""Data models for CrewAI activity I/O.

These dataclasses define the input/output types for all CrewAI activities.
They are designed to be JSON-serializable via the Pydantic payload converter.
"""

from dataclasses import dataclass, field
from typing import Any


# =============================================================================
# LLM Activity Models
# =============================================================================


@dataclass
class LLMCallInput:
    """Input for LLM call activity.

    Note: We do NOT include available_functions - we want tool_calls returned
    directly, not executed inside the activity. Tool execution happens in the
    workflow via activity_as_tool (Phase 2).
    """

    model: str
    """The model name (e.g., "gpt-4", "gpt-4o-mini")."""

    messages: list[dict[str, Any]]
    """Input messages in LLMMessage format: {"role": str, "content": str}."""

    tools: list[dict] | None = None
    """Tool definitions for the LLM. Serialized as plain dicts."""

    llm_kwargs: dict[str, Any] = field(default_factory=dict)
    """Additional kwargs to pass to the LLM (e.g., temperature, max_tokens)."""


@dataclass
class LLMCallOutput:
    """Output from LLM call activity.

    Either content OR tool_calls will be populated, not both.
    - content: Text response from LLM
    - tool_calls: List of tool call requests from LLM
    """

    content: str | None = None
    """Text content from the LLM response."""

    tool_calls: list[dict] | None = None
    """Tool calls requested by the LLM. Format varies by provider."""

    usage: dict[str, int] | None = None
    """Token usage statistics if available."""


# =============================================================================
# Tool Activity Models (Phase 2)
# =============================================================================


@dataclass
class ToolCallInput:
    """Input for tool execution activity."""

    tool_name: str
    """Name of the tool to execute."""

    tool_args: dict[str, Any]
    """Arguments to pass to the tool."""


@dataclass
class ToolCallOutput:
    """Output from tool execution activity."""

    result: str
    """String result from the tool execution."""

    error: str | None = None
    """Error message if the tool execution failed."""


# =============================================================================
# Memory Activity Models (Phase 3)
# =============================================================================


@dataclass
class MemorySaveInput:
    """Input for RAGStorage save activity."""

    storage_type: str
    """Type of memory storage: "short_term" or "entity"."""

    value: Any
    """Value to store."""

    metadata: dict[str, Any]
    """Metadata associated with the value."""


@dataclass
class MemorySearchInput:
    """Input for RAGStorage search activity."""

    storage_type: str
    """Type of memory storage: "short_term" or "entity"."""

    query: str
    """Search query string."""

    limit: int = 5
    """Maximum number of results to return."""

    filter: dict[str, Any] | None = None
    """Filter criteria (note: 'filter', not 'metadata_filter')."""

    score_threshold: float = 0.6
    """Minimum relevance score threshold."""


@dataclass
class MemorySearchOutput:
    """Output from memory search activity."""

    results: list[dict[str, Any]]
    """List of search results."""


@dataclass
class MemoryResetInput:
    """Input for memory reset activity."""

    storage_type: str
    """Type of memory storage to reset."""


# =============================================================================
# Long-term Memory Activity Models (Phase 3)
# =============================================================================


@dataclass
class LTMSaveInput:
    """Input for LTMSQLiteStorage save activity."""

    task_description: str
    """Description of the task."""

    metadata: dict[str, Any]
    """Metadata associated with the task."""

    datetime: str
    """ISO format datetime string."""

    score: float
    """Score for the task execution."""

    db_path: str | None = None
    """Path to the SQLite database file."""


@dataclass
class LTMLoadInput:
    """Input for LTMSQLiteStorage load activity."""

    task_description: str
    """Description of the task to search for."""

    latest_n: int
    """Number of most recent results to return."""

    db_path: str | None = None
    """Path to the SQLite database file."""


@dataclass
class LTMLoadOutput:
    """Output from LTM load activity."""

    results: list[dict[str, Any]]
    """List of task results. Each has: metadata, datetime, score."""


@dataclass
class LTMResetInput:
    """Input for LTM reset activity."""

    db_path: str | None = None
    """Path to the SQLite database file."""


# =============================================================================
# Knowledge Activity Models (Phase 4)
# =============================================================================


@dataclass
class KnowledgeSearchInput:
    """Input for knowledge search activity."""

    query: list[str]
    """Search queries (note: list of strings, not single string)."""

    limit: int = 5
    """Maximum number of results per query."""

    metadata_filter: dict[str, Any] | None = None
    """Filter criteria."""

    score_threshold: float = 0.6
    """Minimum relevance score threshold."""

    collection_name: str | None = None
    """Name of the knowledge collection."""


@dataclass
class KnowledgeSearchOutput:
    """Output from knowledge search activity."""

    results: list[dict[str, Any]]
    """List of search results as dicts."""


@dataclass
class KnowledgeSaveInput:
    """Input for knowledge save activity."""

    documents: list[str]
    """Documents to save."""

    collection_name: str | None = None
    """Name of the knowledge collection."""


@dataclass
class KnowledgeResetInput:
    """Input for knowledge reset activity."""

    collection_name: str | None = None
    """Name of the knowledge collection to reset."""
