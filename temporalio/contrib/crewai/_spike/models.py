"""Data models for Phase 0 spike."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class LLMCallInput:
    """Input for LLM call activity."""

    model: str
    messages: list[dict[str, Any]]
    tools: list[dict] | None = None
    llm_kwargs: dict[str, Any] = field(default_factory=dict)


@dataclass
class LLMCallOutput:
    """Output from LLM call activity."""

    content: str | None = None
    tool_calls: list[dict] | None = None
    usage: dict[str, int] | None = None


@dataclass
class ToolCallInput:
    """Input for tool execution activity."""

    tool_name: str
    tool_args: dict[str, Any]


@dataclass
class ToolCallOutput:
    """Output from tool execution activity."""

    result: str
    error: str | None = None


@dataclass
class MemorySaveInput:
    """Input for memory save activity."""

    storage_type: str
    value: Any
    metadata: dict[str, Any]


@dataclass
class MemorySearchInput:
    """Input for memory search activity."""

    storage_type: str
    query: str
    limit: int = 5
    filter: dict[str, Any] | None = None
    score_threshold: float = 0.6


@dataclass
class MemorySearchOutput:
    """Output from memory search activity."""

    results: list[dict[str, Any]]
