"""Shared utilities for CrewAI Temporal integration."""

import inspect
from collections.abc import Callable
from typing import Any, get_type_hints

from pydantic import BaseModel, Field, create_model


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


def _build_args_schema(func: Callable) -> type[BaseModel]:
    """Build a Pydantic args schema from a function signature.

    This is used to generate the args_schema for CrewStructuredTool
    from an activity function's signature.

    Args:
        func: The function to build schema from

    Returns:
        A Pydantic model class for the function's arguments

    Example:
        @activity.defn
        async def search(query: str, limit: int = 10) -> str:
            ...

        schema = _build_args_schema(search)
        # schema has fields: query (required str), limit (optional int, default 10)
    """
    sig = inspect.signature(func)

    # Try to get type hints, fall back to empty dict if it fails
    try:
        type_hints = get_type_hints(func)
    except Exception:
        type_hints = {}

    fields: dict[str, Any] = {}
    for param_name, param in sig.parameters.items():
        # Skip self/cls for methods
        if param_name in ("self", "cls"):
            continue
        # Skip *args, **kwargs
        if param.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            continue

        # Get type annotation
        annotation = type_hints.get(param_name, Any)

        # Get default value
        if param.default is param.empty:
            default = ...  # Required field
        else:
            default = param.default

        fields[param_name] = (annotation, Field(default=default))

    # Generate schema name from function name
    func_name = getattr(func, "__name__", "Activity")
    # Convert snake_case to TitleCase and add Schema suffix
    schema_name = "".join(word.title() for word in func_name.split("_")) + "Schema"

    return create_model(schema_name, **fields)
