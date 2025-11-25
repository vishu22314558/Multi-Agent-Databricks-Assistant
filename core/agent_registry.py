"""
Agent registry for discovering and managing agents.
"""

from typing import Dict, List, Optional, Type
import logging

from core.base_agent import BaseAgent
from core.message import AgentType

logger = logging.getLogger(__name__)


class AgentRegistry:
    """
    Registry for managing agent instances.
    
    Provides agent discovery, registration, and lookup functionality.
    """
    
    _instance: Optional["AgentRegistry"] = None
    _agents: Dict[AgentType, BaseAgent] = {}
    
    def __new__(cls) -> "AgentRegistry":
        """Singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._agents = {}
        return cls._instance
    
    @classmethod
    def register(cls, agent: BaseAgent) -> None:
        """
        Register an agent in the registry.
        
        Args:
            agent: Agent instance to register
        """
        registry = cls()
        registry._agents[agent.agent_type] = agent
        logger.info(f"Registered agent: {agent.name} ({agent.agent_type.value})")
    
    @classmethod
    def get(cls, agent_type: AgentType) -> Optional[BaseAgent]:
        """
        Get an agent by type.
        
        Args:
            agent_type: Type of agent to retrieve
            
        Returns:
            Agent instance or None if not found
        """
        registry = cls()
        return registry._agents.get(agent_type)
    
    @classmethod
    def get_all(cls) -> Dict[AgentType, BaseAgent]:
        """Get all registered agents."""
        registry = cls()
        return registry._agents.copy()
    
    @classmethod
    def list_agents(cls) -> List[Dict[str, str]]:
        """
        List all registered agents with their info.
        
        Returns:
            List of agent info dictionaries
        """
        registry = cls()
        return [
            {
                "type": agent.agent_type.value,
                "name": agent.name,
                "description": agent.description,
                "capabilities": agent.get_capabilities(),
            }
            for agent in registry._agents.values()
        ]
    
    @classmethod
    def find_best_agent(cls, query: str, exclude: Optional[List[AgentType]] = None) -> Optional[BaseAgent]:
        """
        Find the best agent for a query based on confidence scores.
        
        Args:
            query: User query
            exclude: Agent types to exclude from consideration
            
        Returns:
            Best matching agent or None
        """
        registry = cls()
        exclude = exclude or []
        
        best_agent = None
        best_score = 0.0
        
        for agent_type, agent in registry._agents.items():
            if agent_type in exclude:
                continue
            if agent_type == AgentType.ORCHESTRATOR:
                continue  # Don't route to orchestrator
            
            score = agent.can_handle(query)
            if score > best_score:
                best_score = score
                best_agent = agent
        
        logger.debug(f"Best agent for query: {best_agent.name if best_agent else 'None'} (score: {best_score})")
        return best_agent
    
    @classmethod
    def clear(cls) -> None:
        """Clear all registered agents."""
        registry = cls()
        registry._agents.clear()
        logger.info("Cleared agent registry")


def register_agent(agent_class: Type[BaseAgent]) -> Type[BaseAgent]:
    """
    Decorator to automatically register an agent class.
    
    Usage:
        @register_agent
        class MyAgent(BaseAgent):
            ...
    """
    # Note: This decorator marks the class for registration
    # Actual registration happens when the agent is instantiated
    agent_class._auto_register = True
    return agent_class
