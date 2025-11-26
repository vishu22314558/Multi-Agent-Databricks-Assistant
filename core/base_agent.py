"""
Base agent class that all specialized agents inherit from.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Callable
import logging
import time

try:
    # Try new langchain-ollama package first
    from langchain_ollama import ChatOllama
except ImportError:
    # Fall back to community package
    from langchain_community.chat_models import ChatOllama

try:
    # Try new location first
    from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
except ImportError:
    try:
        # Try alternate location
        from langchain.schema.messages import HumanMessage, SystemMessage, AIMessage
    except ImportError:
        # Last resort - define simple message classes
        class HumanMessage:
            def __init__(self, content: str):
                self.content = content
                self.type = "human"
        
        class SystemMessage:
            def __init__(self, content: str):
                self.content = content
                self.type = "system"
        
        class AIMessage:
            def __init__(self, content: str):
                self.content = content
                self.type = "ai"

from core.message import (
    AgentType, AgentMessage, TaskRequest, TaskResponse, 
    TaskStatus, ConversationContext, MessageRole
)
from config.settings import get_settings

logger = logging.getLogger(__name__)


class BaseAgent(ABC):
    """
    Abstract base class for all agents in the multi-agent system.
    
    Each specialized agent must implement:
    - process(): Main processing logic
    - get_capabilities(): List of agent capabilities
    """
    
    def __init__(
        self,
        agent_type: AgentType,
        name: str,
        description: str,
        model: Optional[str] = None,
        temperature: float = 0.1,
        tools: Optional[List[Callable]] = None,
    ):
        self.agent_type = agent_type
        self.name = name
        self.description = description
        self.tools = tools or []
        self.settings = get_settings()
        
        # Initialize LLM
        self.llm = ChatOllama(
            model=model or self.settings.ollama.model,
            temperature=temperature,
            base_url=self.settings.ollama.base_url,
            num_ctx=self.settings.ollama.num_ctx,
        )
        
        # Load system prompt
        self.system_prompt = self._load_system_prompt()
        
        logger.info(f"Initialized {self.name} agent")
    
    def _load_system_prompt(self) -> str:
        """Load system prompt from file."""
        prompt_file = self.settings.prompts_dir / f"{self.agent_type.value}_agent.txt"
        if prompt_file.exists():
            return prompt_file.read_text()
        return self._get_default_prompt()
    
    def _get_default_prompt(self) -> str:
        """Get default system prompt if file not found."""
        return f"""You are {self.name}, a specialized AI agent for Databricks tasks.

Your role: {self.description}

Guidelines:
- Be concise and precise in your responses
- Provide working code examples when applicable
- Follow Databricks best practices
- If unsure, ask clarifying questions
- Always validate inputs before processing
"""
    
    @abstractmethod
    def process(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """
        Process a task request and return a response.
        
        Args:
            request: The task request to process
            context: Conversation context
            
        Returns:
            TaskResponse with results or error
        """
        pass
    
    @abstractmethod
    def get_capabilities(self) -> List[str]:
        """Return list of capabilities this agent handles."""
        pass
    
    def can_handle(self, query: str) -> float:
        """
        Determine confidence score for handling a query.
        
        Args:
            query: User query
            
        Returns:
            Confidence score between 0 and 1
        """
        query_lower = query.lower()
        capabilities = self.get_capabilities()
        
        score = 0.0
        matches = 0
        
        for capability in capabilities:
            if capability.lower() in query_lower:
                matches += 1
        
        if matches > 0:
            score = min(0.9, 0.3 + (matches * 0.2))
        
        return score
    
    def _build_messages(
        self,
        query: str,
        context: ConversationContext,
        additional_context: Optional[str] = None,
    ) -> List:
        """Build message list for LLM."""
        messages = [SystemMessage(content=self.system_prompt)]
        
        # Add conversation history
        for msg in context.get_recent_messages(self.settings.agents.memory_window):
            # Handle both enum and string values for role
            role = msg.role.value if hasattr(msg.role, 'value') else msg.role
            if role == "user":
                messages.append(HumanMessage(content=msg.content))
            elif role == "assistant":
                messages.append(AIMessage(content=msg.content))
        
        # Build current query with context
        query_with_context = query
        if additional_context:
            query_with_context = f"{additional_context}\n\nQuery: {query}"
        
        context_summary = context.get_context_summary()
        if context_summary != "No context":
            query_with_context = f"Context: {context_summary}\n\n{query_with_context}"
        
        messages.append(HumanMessage(content=query_with_context))
        
        return messages
    
    def invoke(
        self,
        query: str,
        context: ConversationContext,
        additional_context: Optional[str] = None,
    ) -> str:
        """
        Invoke the LLM with the query.
        
        Args:
            query: User query
            context: Conversation context
            additional_context: Additional context to include
            
        Returns:
            LLM response string
        """
        messages = self._build_messages(query, context, additional_context)
        
        try:
            response = self.llm.invoke(messages)
            return response.content
        except Exception as e:
            logger.error(f"Error invoking LLM: {e}")
            raise
    
    def execute_task(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """
        Execute a task with timing and error handling.
        
        Args:
            request: Task request
            context: Conversation context
            
        Returns:
            TaskResponse with results
        """
        start_time = time.time()
        
        try:
            response = self.process(request, context)
            response.execution_time_ms = int((time.time() - start_time) * 1000)
            return response
        except Exception as e:
            logger.error(f"Error executing task {request.id}: {e}")
            return TaskResponse(
                task_id=request.id,
                status=TaskStatus.FAILED,
                error=str(e),
                agent_type=self.agent_type,
                execution_time_ms=int((time.time() - start_time) * 1000),
            )
    
    def delegate_to(
        self,
        target_agent: AgentType,
        query: str,
        context: Dict[str, Any],
    ) -> TaskRequest:
        """
        Create a task request for delegation to another agent.
        
        Args:
            target_agent: Agent to delegate to
            query: Query for the target agent
            context: Additional context
            
        Returns:
            TaskRequest for the target agent
        """
        return TaskRequest(
            query=query,
            context=context,
            source_agent=self.agent_type,
            target_agent=target_agent,
        )
    
    def format_response(
        self,
        content: str,
        data: Optional[Dict[str, Any]] = None,
        code: Optional[str] = None,
        code_language: str = "sql",
    ) -> str:
        """
        Format response with optional code blocks.
        
        Args:
            content: Main response content
            data: Optional data dictionary
            code: Optional code to include
            code_language: Language for code block
            
        Returns:
            Formatted response string
        """
        parts = [content]
        
        if code:
            parts.append(f"\n```{code_language}\n{code}\n```")
        
        return "\n".join(parts)
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name}, type={self.agent_type.value})"
