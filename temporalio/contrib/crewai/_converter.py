"""Payload converter for CrewAI activities.

This module provides a data converter that supports serialization of
CrewAI activity inputs and outputs, including dataclasses and complex types.
"""

from temporalio.contrib.pydantic import (
    PydanticPayloadConverter,
    ToJsonOptions,
    pydantic_data_converter,
)
from temporalio.converter import DataConverter, DefaultPayloadConverter


class CrewAIPayloadConverter(PydanticPayloadConverter):
    """Payload converter for CrewAI activity I/O.

    Uses Pydantic for serialization with exclude_unset=True to minimize
    payload size by excluding fields that weren't explicitly set.
    """

    def __init__(self) -> None:
        """Initialize with exclude_unset=True."""
        super().__init__(ToJsonOptions(exclude_unset=True))


# Pre-configured data converter for CrewAI
crewai_data_converter = DataConverter(payload_converter_class=CrewAIPayloadConverter)
"""Data converter configured for CrewAI.

Use this when creating a Temporal client for CrewAI workflows:

    client = await Client.connect(
        "localhost:7233",
        data_converter=crewai_data_converter,
    )
"""


def make_crewai_data_converter(
    existing: DataConverter | None = None,
) -> DataConverter:
    """Create or update a DataConverter for CrewAI.

    This function is useful when you need to combine the CrewAI converter
    with other custom converters.

    Args:
        existing: Optional existing DataConverter to update

    Returns:
        DataConverter configured with CrewAIPayloadConverter

    Raises:
        ValueError: If existing converter is incompatible

    Example:
        # Create fresh converter
        converter = make_crewai_data_converter()

        # Update existing converter (if compatible)
        converter = make_crewai_data_converter(existing_converter)
    """
    if existing is None:
        return crewai_data_converter

    # If using default converter, we can replace it
    if existing.payload_converter_class is DefaultPayloadConverter:
        return DataConverter(
            payload_converter_class=CrewAIPayloadConverter,
            failure_converter_class=existing.failure_converter_class,
        )

    # If already using Pydantic converter, it's compatible
    if issubclass(existing.payload_converter_class, PydanticPayloadConverter):
        return existing

    # Otherwise, incompatible
    raise ValueError(
        "CrewAI requires PydanticPayloadConverter or a subclass. "
        f"Got: {existing.payload_converter_class.__name__}"
    )
