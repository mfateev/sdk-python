"""Anthropic Claude Agent SDK integration with Temporal.

.. warning::
    This API is experimental and may change in future versions.
    Use with caution in production environments.

This module provides integration between the Anthropic Claude Agent SDK and
Temporal workflows, enabling durable execution of AI agents.
"""

from temporalio.contrib.anthropic._activities import invoke_llm_activity
from temporalio.contrib.anthropic._plugin import (
    AnthropicAgentsPlugin,
    AnthropicPayloadConverter,
    TransportActivityParameters,
)
from temporalio.contrib.anthropic._transport import TemporalTransport

__all__ = [
    "AnthropicAgentsPlugin",
    "AnthropicPayloadConverter",
    "invoke_llm_activity",
    "TemporalTransport",
    "TransportActivityParameters",
]
