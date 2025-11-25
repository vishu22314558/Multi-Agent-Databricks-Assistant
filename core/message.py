"""
Message types and routing for multi-agent communication.
"""

from enum import Enum
from typing import Any, Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field
import uuid


class AgentType(str, Enum):
    """Types of agents in the system."""
    ORCHESTRATOR = "orchestrator"
    SCHEMA = "schema"
    SQL = "sql"
    PIPELINE = "pipeline"
    METADATA = "metadata"
    CHAT = "chat"


class MessageRole(str, Enum):
    """Message roles in conversation."""
    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"
    AGENT = "agent"


class TaskStatus(str, Enum):
    """Status of agent tasks."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    DELEGATED = "delegated"


class Message(BaseModel):
    """Base message class for agent communication."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    role: MessageRole
    content: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        use_enum_values = True


class AgentMessage(Message):
    """Message from/to an agent."""
    agent_type: AgentType
    source_agent: Optional[AgentType] = None
    target_agent: Optional[AgentType] = None
    task_id: Optional[str] = None
    
    class Config:
        use_enum_values = True


class TaskRequest(BaseModel):
    """Request for an agent to perform a task."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    query: str
    context: Dict[str, Any] = Field(default_factory=dict)
    source_agent: Optional[AgentType] = None
    target_agent: AgentType
    priority: int = Field(default=1, ge=1, le=10)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        use_enum_values = True


class TaskResponse(BaseModel):
    """Response from an agent task."""
    task_id: str
    status: TaskStatus
    result: Optional[str] = None
    data: Dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None
    agent_type: AgentType
    execution_time_ms: Optional[int] = None
    completed_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        use_enum_values = True


class ConversationContext(BaseModel):
    """Context maintained across conversation turns."""
    conversation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    messages: List[Message] = Field(default_factory=list)
    current_table: Optional[str] = None
    current_catalog: Optional[str] = None
    current_schema: Optional[str] = None
    uploaded_files: List[str] = Field(default_factory=list)
    agent_memory: Dict[str, Any] = Field(default_factory=dict)
    
    def add_message(self, message: Message) -> None:
        """Add a message to the conversation."""
        self.messages.append(message)
    
    def get_recent_messages(self, n: int = 10) -> List[Message]:
        """Get the n most recent messages."""
        return self.messages[-n:] if self.messages else []
    
    def get_context_summary(self) -> str:
        """Get a summary of the current context."""
        parts = []
        if self.current_catalog:
            parts.append(f"Catalog: {self.current_catalog}")
        if self.current_schema:
            parts.append(f"Schema: {self.current_schema}")
        if self.current_table:
            parts.append(f"Table: {self.current_table}")
        if self.uploaded_files:
            parts.append(f"Files: {', '.join(self.uploaded_files)}")
        return " | ".join(parts) if parts else "No context"


class RoutingDecision(BaseModel):
    """Decision made by the orchestrator for routing."""
    primary_agent: AgentType
    secondary_agents: List[AgentType] = Field(default_factory=list)
    reasoning: str
    requires_collaboration: bool = False
    workflow_steps: List[str] = Field(default_factory=list)
    
    class Config:
        use_enum_values = True


# Intent classification for routing
AGENT_INTENTS = {
    AgentType.SCHEMA: [
        "create table", "ddl", "alter table", "add column", "drop column",
        "rename column", "change type", "schema", "csv to table", "infer types",
        "create database", "create schema"
    ],
    AgentType.SQL: [
        "query", "select", "optimize", "slow query", "performance", "index",
        "join", "aggregate", "filter", "sql", "execute query"
    ],
    AgentType.PIPELINE: [
        "dlt", "delta live tables", "pipeline", "bronze", "silver", "gold",
        "streaming", "etl", "ingestion", "medallion"
    ],
    AgentType.METADATA: [
        "tag", "comment", "property", "metadata", "governance", "pii",
        "classification", "document table", "describe"
    ],
    AgentType.CHAT: [
        "explain", "what is", "how to", "best practice", "help", "recommend",
        "difference between", "compare"
    ]
}
