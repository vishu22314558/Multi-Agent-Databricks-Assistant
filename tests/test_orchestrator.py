"""
Tests for the Orchestrator agent.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

from core.message import (
    AgentType, TaskRequest, TaskResponse, TaskStatus, 
    ConversationContext, RoutingDecision
)
from core.orchestrator import OrchestratorAgent
from core.agent_registry import AgentRegistry


@pytest.fixture
def context():
    """Create a test conversation context."""
    return ConversationContext()


@pytest.fixture
def mock_llm():
    """Mock the LLM."""
    with patch('core.base_agent.ChatOllama') as mock:
        mock_instance = MagicMock()
        mock_instance.invoke.return_value = MagicMock(content="schema")
        mock.return_value = mock_instance
        yield mock_instance


class TestOrchestratorAgent:
    """Tests for OrchestratorAgent."""
    
    def test_initialization(self, mock_llm):
        """Test orchestrator initialization."""
        orchestrator = OrchestratorAgent()
        
        assert orchestrator.agent_type == AgentType.ORCHESTRATOR
        assert orchestrator.name == "Orchestrator"
    
    def test_get_capabilities(self, mock_llm):
        """Test capabilities list."""
        orchestrator = OrchestratorAgent()
        capabilities = orchestrator.get_capabilities()
        
        assert "route" in capabilities
        assert "coordinate" in capabilities
    
    def test_route_to_schema_agent(self, mock_llm, context):
        """Test routing to schema agent."""
        orchestrator = OrchestratorAgent()
        
        routing = orchestrator._analyze_and_route(
            "create a table for customer data",
            context
        )
        
        assert routing.primary_agent == AgentType.SCHEMA
    
    def test_route_to_sql_agent(self, mock_llm, context):
        """Test routing to SQL agent."""
        orchestrator = OrchestratorAgent()
        
        routing = orchestrator._analyze_and_route(
            "optimize this slow query",
            context
        )
        
        assert routing.primary_agent == AgentType.SQL
    
    def test_route_to_pipeline_agent(self, mock_llm, context):
        """Test routing to pipeline agent."""
        orchestrator = OrchestratorAgent()
        
        routing = orchestrator._analyze_and_route(
            "create a delta live tables pipeline",
            context
        )
        
        assert routing.primary_agent == AgentType.PIPELINE
    
    def test_route_to_metadata_agent(self, mock_llm, context):
        """Test routing to metadata agent."""
        orchestrator = OrchestratorAgent()
        
        routing = orchestrator._analyze_and_route(
            "add PII tags to the customer table",
            context
        )
        
        assert routing.primary_agent == AgentType.METADATA
    
    def test_route_to_chat_agent(self, mock_llm, context):
        """Test routing to chat agent for general questions."""
        orchestrator = OrchestratorAgent()
        
        routing = orchestrator._analyze_and_route(
            "what is the best practice for partitioning",
            context
        )
        
        assert routing.primary_agent == AgentType.CHAT
    
    def test_multi_agent_detection(self, mock_llm, context):
        """Test detection of multi-agent queries."""
        orchestrator = OrchestratorAgent()
        
        routing = orchestrator._analyze_and_route(
            "create a table and then add PII tags",
            context
        )
        
        assert routing.requires_collaboration == True
    
    def test_routing_decision_structure(self, mock_llm, context):
        """Test that routing decisions have correct structure."""
        orchestrator = OrchestratorAgent()
        
        routing = orchestrator._analyze_and_route(
            "create table from csv",
            context
        )
        
        assert isinstance(routing, RoutingDecision)
        assert routing.primary_agent is not None
        assert isinstance(routing.secondary_agents, list)
        assert isinstance(routing.reasoning, str)
        assert isinstance(routing.workflow_steps, list)


class TestAgentRegistry:
    """Tests for AgentRegistry."""
    
    def test_singleton(self):
        """Test that AgentRegistry is a singleton."""
        registry1 = AgentRegistry()
        registry2 = AgentRegistry()
        
        assert registry1 is registry2
    
    def test_register_and_get(self, mock_llm):
        """Test agent registration and retrieval."""
        AgentRegistry.clear()
        
        orchestrator = OrchestratorAgent()
        AgentRegistry.register(orchestrator)
        
        retrieved = AgentRegistry.get(AgentType.ORCHESTRATOR)
        
        assert retrieved is orchestrator
    
    def test_list_agents(self, mock_llm):
        """Test listing agents."""
        AgentRegistry.clear()
        
        orchestrator = OrchestratorAgent()
        AgentRegistry.register(orchestrator)
        
        agents = AgentRegistry.list_agents()
        
        assert len(agents) == 1
        assert agents[0]["type"] == "orchestrator"
    
    def test_clear_registry(self, mock_llm):
        """Test clearing the registry."""
        AgentRegistry.clear()
        
        orchestrator = OrchestratorAgent()
        AgentRegistry.register(orchestrator)
        
        AgentRegistry.clear()
        
        assert AgentRegistry.get(AgentType.ORCHESTRATOR) is None
