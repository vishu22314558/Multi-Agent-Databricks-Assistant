"""
Tests for specialized agents.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

from core.message import (
    AgentType, TaskRequest, TaskResponse, TaskStatus, ConversationContext
)
from agents.schema_agent import SchemaAgent
from agents.sql_agent import SQLAgent
from agents.pipeline_agent import PipelineAgent
from agents.metadata_agent import MetadataAgent
from agents.chat_agent import ChatAgent


@pytest.fixture
def context():
    """Create a test conversation context."""
    ctx = ConversationContext()
    ctx.current_catalog = "test_catalog"
    ctx.current_schema = "test_schema"
    return ctx


@pytest.fixture
def mock_llm():
    """Mock the LLM to avoid actual API calls."""
    with patch('core.base_agent.ChatOllama') as mock:
        mock_instance = MagicMock()
        mock_instance.invoke.return_value = MagicMock(content="Test response")
        mock.return_value = mock_instance
        yield mock_instance


class TestSchemaAgent:
    """Tests for SchemaAgent."""
    
    def test_initialization(self, mock_llm):
        """Test agent initialization."""
        agent = SchemaAgent()
        assert agent.agent_type == AgentType.SCHEMA
        assert agent.name == "Schema Agent"
    
    def test_get_capabilities(self, mock_llm):
        """Test capabilities list."""
        agent = SchemaAgent()
        capabilities = agent.get_capabilities()
        
        assert "create table" in capabilities
        assert "ddl" in capabilities
        assert "alter table" in capabilities
    
    def test_can_handle_create_table(self, mock_llm):
        """Test query matching for create table."""
        agent = SchemaAgent()
        
        score = agent.can_handle("create table for customer data")
        assert score > 0.5


class TestSQLAgent:
    """Tests for SQLAgent."""
    
    def test_initialization(self, mock_llm):
        """Test agent initialization."""
        agent = SQLAgent()
        assert agent.agent_type == AgentType.SQL
        assert agent.name == "SQL Agent"
    
    def test_analyze_query_complexity(self, mock_llm):
        """Test query complexity analysis."""
        agent = SQLAgent()
        
        simple_query = "SELECT * FROM table"
        analysis = agent.analyze_query(simple_query)
        assert analysis["estimated_complexity"] == "simple"


class TestPipelineAgent:
    """Tests for PipelineAgent."""
    
    def test_initialization(self, mock_llm):
        """Test agent initialization."""
        agent = PipelineAgent()
        assert agent.agent_type == AgentType.PIPELINE
        assert agent.name == "Pipeline Agent"


class TestMetadataAgent:
    """Tests for MetadataAgent."""
    
    def test_suggest_tags_for_pii_column(self, mock_llm):
        """Test PII tag suggestions."""
        agent = MetadataAgent()
        
        tags = agent.suggest_tags_for_column("ssn", "STRING")
        assert tags.get("pii_level") == "high"
        
        tags = agent.suggest_tags_for_column("email", "STRING")
        assert tags.get("pii_level") == "medium"


class TestChatAgent:
    """Tests for ChatAgent."""
    
    def test_get_quick_tips(self, mock_llm):
        """Test quick tips retrieval."""
        agent = ChatAgent()
        
        tips = agent.get_quick_tips("delta_lake")
        assert len(tips) > 0
