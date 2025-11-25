"""Specialized agents module."""

from .schema_agent import SchemaAgent
from .sql_agent import SQLAgent
from .pipeline_agent import PipelineAgent
from .metadata_agent import MetadataAgent
from .chat_agent import ChatAgent

__all__ = [
    "SchemaAgent",
    "SQLAgent",
    "PipelineAgent",
    "MetadataAgent",
    "ChatAgent",
]


def initialize_agents():
    """Initialize and register all agents."""
    from core.agent_registry import AgentRegistry
    
    agents = [
        SchemaAgent(),
        SQLAgent(),
        PipelineAgent(),
        MetadataAgent(),
        ChatAgent(),
    ]
    
    for agent in agents:
        AgentRegistry.register(agent)
    
    return agents
