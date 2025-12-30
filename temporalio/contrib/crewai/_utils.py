"""Shared utilities for CrewAI Temporal integration."""

from typing import Any


def _serialize_tools(tools: list[Any] | None) -> list[dict] | None:
    """Serialize tool definitions for activity input.

    Converts CrewAI tool objects to plain dicts for JSON serialization.
    Handles various tool types including Pydantic models, dataclasses,
    and plain objects.

    Args:
        tools: List of tool objects to serialize

    Returns:
        List of serialized tool dicts, or None if input is None
    """
    if tools is None:
        return None

    serialized = []
    for tool in tools:
        if isinstance(tool, dict):
            serialized.append(tool)
        elif hasattr(tool, "model_dump"):
            # Pydantic model
            serialized.append(tool.model_dump())
        elif hasattr(tool, "__dict__"):
            # Regular object - filter out private attributes
            serialized.append(
                {k: v for k, v in tool.__dict__.items() if not k.startswith("_")}
            )
        else:
            # Fallback - just use string representation
            serialized.append({"name": str(tool)})

    return serialized


def _to_dict(obj: Any) -> dict[str, Any]:
    """Convert an object to a dict for serialization.

    Args:
        obj: Object to convert

    Returns:
        Dict representation of the object

    Raises:
        TypeError: If the object cannot be converted to a dict
    """
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if hasattr(obj, "__dict__"):
        return {k: v for k, v in obj.__dict__.items() if not k.startswith("_")}
    raise TypeError(f"Cannot convert {type(obj).__name__} to dict")


def _is_temporal_tool(tool: Any) -> bool:
    """Check if a tool was created via activity_as_tool().

    This is used by validation logic to ensure all tools in a crew
    are backed by Temporal activities.

    Args:
        tool: Tool object to check

    Returns:
        True if the tool was created via activity_as_tool()
    """
    return getattr(tool, "_is_temporal_activity_tool", False)


def _safe_heartbeat(details: dict[str, Any]) -> None:
    """Heartbeat if in activity context, otherwise no-op.

    This helper handles the case where activity methods are called
    directly in unit tests without a Temporal activity context.

    Args:
        details: Heartbeat details dict
    """
    from temporalio import activity

    try:
        activity.heartbeat(details)
    except RuntimeError:
        # Not in activity context (e.g., unit tests)
        pass
