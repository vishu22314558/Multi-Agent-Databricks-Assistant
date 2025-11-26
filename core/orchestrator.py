"""
Orchestrator Agent - Routes queries and coordinates multi-agent workflows.
"""

import logging
import re
from typing import Dict, List, Optional, Tuple, Union

from core.base_agent import BaseAgent
from core.agent_registry import AgentRegistry
from core.message import (
    AgentType, TaskRequest, TaskResponse, TaskStatus,
    ConversationContext, RoutingDecision, AGENT_INTENTS, Message, MessageRole
)
from config.settings import get_settings

logger = logging.getLogger(__name__)


def get_enum_value(val: Union[str, AgentType]) -> str:
    """Get string value from enum or string."""
    if hasattr(val, 'value'):
        return val.value
    return str(val)


class OrchestratorAgent(BaseAgent):
    """
    Main orchestrator that routes queries to specialized agents
    and coordinates multi-agent workflows.
    """
    
    def __init__(self):
        super().__init__(
            agent_type=AgentType.ORCHESTRATOR,
            name="Orchestrator",
            description="Routes queries to specialized agents and coordinates workflows",
            model=get_settings().agents.orchestrator_model,
            temperature=0.0,  # Deterministic routing
        )
        self.max_iterations = get_settings().agents.max_iterations
    
    def get_capabilities(self) -> List[str]:
        """Orchestrator handles routing and coordination."""
        return ["route", "coordinate", "multi-step", "workflow"]
    
    def process(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """
        Process a query by routing to appropriate agents.
        
        Args:
            request: Task request with user query
            context: Conversation context
            
        Returns:
            Aggregated response from agent(s)
        """
        query = request.query
        
        # Analyze the query and decide routing
        routing = self._analyze_and_route(query, context)
        
        logger.info(f"Routing decision: {get_enum_value(routing.primary_agent)}, "
                   f"collaboration: {routing.requires_collaboration}")
        
        # Execute based on routing decision
        if routing.requires_collaboration:
            response = self._execute_collaborative_workflow(
                query, routing, context
            )
        else:
            response = self._execute_single_agent(
                query, routing.primary_agent, context
            )
        
        # Add response to context
        context.add_message(Message(
            role=MessageRole.ASSISTANT,
            content=response.result or "",
            metadata={"agent": get_enum_value(routing.primary_agent)}
        ))
        
        return response
    
    def _analyze_and_route(
        self,
        query: str,
        context: ConversationContext,
    ) -> RoutingDecision:
        """
        Analyze query and determine routing.
        
        Uses a combination of:
        1. Keyword matching for quick classification
        2. LLM analysis for complex/ambiguous queries
        """
        query_lower = query.lower()
        
        # Quick keyword-based routing
        agent_scores: Dict[AgentType, float] = {}
        
        for agent_type, keywords in AGENT_INTENTS.items():
            score = sum(1 for kw in keywords if kw in query_lower)
            if score > 0:
                agent_scores[agent_type] = score
        
        # Check for multi-agent indicators
        multi_agent_keywords = ["and then", "also", "additionally", "after that", "finally"]
        requires_collaboration = any(kw in query_lower for kw in multi_agent_keywords)
        
        # Determine primary agent
        if agent_scores:
            primary_agent = max(agent_scores, key=agent_scores.get)
        else:
            # Use LLM for ambiguous queries
            primary_agent = self._llm_route(query, context)
        
        # Determine secondary agents if collaboration needed
        secondary_agents = []
        if requires_collaboration and len(agent_scores) > 1:
            sorted_agents = sorted(agent_scores.items(), key=lambda x: x[1], reverse=True)
            secondary_agents = [a[0] for a in sorted_agents[1:3]]  # Top 2 secondary
        
        # Build workflow steps for complex queries
        workflow_steps = self._build_workflow_steps(query, primary_agent, secondary_agents)
        
        return RoutingDecision(
            primary_agent=primary_agent,
            secondary_agents=secondary_agents,
            requires_collaboration=requires_collaboration or len(secondary_agents) > 0,
            reasoning=f"Query matched {get_enum_value(primary_agent)} based on intent analysis",
            workflow_steps=workflow_steps,
        )
    
    def _llm_route(
        self,
        query: str,
        context: ConversationContext,
    ) -> AgentType:
        """Use LLM to route ambiguous queries."""
        routing_prompt = f"""Analyze this query and determine which agent should handle it.

Available agents:
- schema: DDL generation, CREATE/ALTER TABLE, schema modifications
- sql: Query optimization, SQL debugging, performance tuning  
- pipeline: Delta Live Tables, ETL pipelines, Bronze/Silver/Gold
- metadata: Tags, comments, table properties, governance
- chat: General questions, explanations, best practices

Query: {query}

Respond with ONLY the agent name (schema/sql/pipeline/metadata/chat):"""

        try:
            response = self.invoke(routing_prompt, context)
            agent_name = response.strip().lower()
            
            # Map to AgentType
            mapping = {
                "schema": AgentType.SCHEMA,
                "sql": AgentType.SQL,
                "pipeline": AgentType.PIPELINE,
                "metadata": AgentType.METADATA,
                "chat": AgentType.CHAT,
            }
            
            return mapping.get(agent_name, AgentType.CHAT)
        except Exception as e:
            logger.error(f"LLM routing failed: {e}")
            return AgentType.CHAT  # Default to chat
    
    def _build_workflow_steps(
        self,
        query: str,
        primary: AgentType,
        secondary: List[AgentType],
    ) -> List[str]:
        """Build workflow steps for multi-agent tasks."""
        steps = [f"Route to {get_enum_value(primary)} agent"]
        
        for agent in secondary:
            steps.append(f"Delegate to {get_enum_value(agent)} agent")
        
        steps.append("Aggregate results and respond")
        
        return steps
    
    def _execute_single_agent(
        self,
        query: str,
        agent_type: AgentType,
        context: ConversationContext,
    ) -> TaskResponse:
        """Execute query with a single agent."""
        agent = AgentRegistry.get(agent_type)
        
        if not agent:
            logger.warning(f"Agent {get_enum_value(agent_type)} not found, falling back to chat")
            agent = AgentRegistry.get(AgentType.CHAT)
        
        if not agent:
            return TaskResponse(
                task_id="error",
                status=TaskStatus.FAILED,
                error="No suitable agent found",
                agent_type=agent_type,
            )
        
        request = TaskRequest(
            query=query,
            target_agent=agent_type,
            context={"source": "orchestrator"},
        )
        
        return agent.execute_task(request, context)
    
    def _execute_collaborative_workflow(
        self,
        query: str,
        routing: RoutingDecision,
        context: ConversationContext,
    ) -> TaskResponse:
        """Execute a multi-agent collaborative workflow."""
        results = []
        all_data = {}
        
        # Execute primary agent
        primary_response = self._execute_single_agent(
            query, routing.primary_agent, context
        )
        results.append(f"**{get_enum_value(routing.primary_agent).title()} Agent:**\n{primary_response.result}")
        all_data[get_enum_value(routing.primary_agent)] = primary_response.data
        
        # Execute secondary agents with context from primary
        for agent_type in routing.secondary_agents:
            # Build context-aware query for secondary agent
            secondary_query = self._build_secondary_query(
                query, agent_type, primary_response
            )
            
            secondary_response = self._execute_single_agent(
                secondary_query, agent_type, context
            )
            
            results.append(f"\n**{get_enum_value(agent_type).title()} Agent:**\n{secondary_response.result}")
            all_data[get_enum_value(agent_type)] = secondary_response.data
        
        # Aggregate results
        combined_result = "\n".join(results)
        
        return TaskResponse(
            task_id=f"workflow-{get_enum_value(routing.primary_agent)}",
            status=TaskStatus.COMPLETED,
            result=combined_result,
            data=all_data,
            agent_type=AgentType.ORCHESTRATOR,
        )
    
    def _build_secondary_query(
        self,
        original_query: str,
        agent_type: AgentType,
        primary_response: TaskResponse,
    ) -> str:
        """Build query for secondary agent with context."""
        # Extract relevant context from primary response
        context_info = ""
        if primary_response.data:
            if "table_name" in primary_response.data:
                context_info = f"Table: {primary_response.data['table_name']}"
            if "ddl" in primary_response.data:
                context_info += f"\nDDL: {primary_response.data['ddl'][:500]}..."
        
        return f"""Based on the previous step:
{context_info}

Original request: {original_query}

Your specific task related to {get_enum_value(agent_type)}:"""
    
    def route_query(
        self,
        query: str,
        context: ConversationContext,
    ) -> TaskResponse:
        """
        Main entry point for processing user queries.
        
        Args:
            query: User's query
            context: Conversation context
            
        Returns:
            TaskResponse with results
        """
        # Add query to context
        context.add_message(Message(
            role=MessageRole.USER,
            content=query,
        ))
        
        # Create task request
        request = TaskRequest(
            query=query,
            target_agent=AgentType.ORCHESTRATOR,
            context={},
        )
        
        return self.execute_task(request, context)
