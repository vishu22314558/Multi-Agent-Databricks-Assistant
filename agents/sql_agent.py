"""
SQL Agent - Handles query optimization, debugging, and SQL operations.
"""

import logging
import re
from typing import Any, Dict, List, Optional

from core.base_agent import BaseAgent
from core.message import AgentType, TaskRequest, TaskResponse, TaskStatus, ConversationContext
from tools.sql_tools import SQLTools
from tools.databricks_tools import DatabricksTools

logger = logging.getLogger(__name__)


class SQLAgent(BaseAgent):
    """
    Specialized agent for SQL operations and optimization.
    
    Capabilities:
    - Query optimization recommendations
    - SQL debugging and error analysis
    - Performance tuning suggestions
    - Query rewriting
    """
    
    def __init__(self):
        super().__init__(
            agent_type=AgentType.SQL,
            name="SQL Agent",
            description="Handles query optimization, debugging, and SQL operations",
            temperature=0.1,
        )
        self.sql_tools = SQLTools()
        self.databricks_tools = DatabricksTools()
    
    def get_capabilities(self) -> List[str]:
        return [
            "query", "select", "optimize", "slow query", "performance", 
            "index", "join", "aggregate", "filter", "sql", "execute query",
            "explain plan", "debug query", "rewrite query"
        ]
    
    def process(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Process SQL-related requests."""
        query = request.query.lower()
        
        # Determine the specific operation
        if "optimize" in query or "slow" in query or "performance" in query:
            return self._handle_optimization(request, context)
        elif "debug" in query or "error" in query or "fix" in query:
            return self._handle_debugging(request, context)
        elif "explain" in query:
            return self._handle_explain(request, context)
        elif "execute" in query or "run" in query:
            return self._handle_execution(request, context)
        else:
            return self._handle_general_sql(request, context)
    
    def _handle_optimization(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Provide query optimization recommendations."""
        sql_query = request.context.get("sql_query", "")
        
        prompt = f"""Analyze this SQL query and provide optimization recommendations for Databricks.

Query: {request.query}

{f'SQL Code:{chr(10)}```sql{chr(10)}{sql_query}{chr(10)}```' if sql_query else ''}

Provide:
1. Performance analysis identifying potential bottlenecks
2. Specific optimization recommendations:
   - Indexing/Z-ORDER suggestions
   - Join optimization
   - Filter pushdown opportunities
   - Partitioning recommendations
3. Rewritten optimized query if applicable
4. OPTIMIZE and VACUUM recommendations
5. Caching strategies if appropriate

Focus on Delta Lake and Databricks-specific optimizations."""

        result = self.invoke(prompt, context)
        
        return TaskResponse(
            task_id=request.id,
            status=TaskStatus.COMPLETED,
            result=result,
            data={"original_query": sql_query},
            agent_type=self.agent_type,
        )
    
    def _handle_debugging(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Debug SQL queries and errors."""
        sql_query = request.context.get("sql_query", "")
        error_message = request.context.get("error_message", "")
        
        prompt = f"""Debug this SQL query and identify the issue.

Query/Request: {request.query}

{f'SQL Code:{chr(10)}```sql{chr(10)}{sql_query}{chr(10)}```' if sql_query else ''}

{f'Error Message:{chr(10)}{error_message}' if error_message else ''}

Provide:
1. Identification of the issue(s)
2. Root cause analysis
3. Corrected SQL code
4. Explanation of the fix
5. Prevention tips for similar issues"""

        result = self.invoke(prompt, context)
        
        return TaskResponse(
            task_id=request.id,
            status=TaskStatus.COMPLETED,
            result=result,
            data={"original_query": sql_query, "error": error_message},
            agent_type=self.agent_type,
        )
    
    def _handle_explain(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Explain query execution plans."""
        sql_query = request.context.get("sql_query", "")
        
        prompt = f"""Explain how this SQL query works and analyze its execution.

Query/Request: {request.query}

{f'SQL Code:{chr(10)}```sql{chr(10)}{sql_query}{chr(10)}```' if sql_query else ''}

Provide:
1. Step-by-step explanation of query execution
2. How to generate and interpret EXPLAIN ANALYZE output
3. Key operations (scans, joins, aggregations)
4. Data flow through the query
5. Potential performance considerations

If no specific query is provided, explain the EXPLAIN command in Databricks."""

        result = self.invoke(prompt, context)
        
        return TaskResponse(
            task_id=request.id,
            status=TaskStatus.COMPLETED,
            result=result,
            agent_type=self.agent_type,
        )
    
    def _handle_execution(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Handle SQL execution requests."""
        sql_query = request.context.get("sql_query", "")
        
        # Validate the query first
        if sql_query:
            validation = self.sql_tools.validate_sql(sql_query)
            if not validation["is_valid"]:
                return TaskResponse(
                    task_id=request.id,
                    status=TaskStatus.FAILED,
                    error=f"SQL validation failed: {validation['error']}",
                    agent_type=self.agent_type,
                )
        
        # Check if execution is enabled
        from config.settings import get_settings
        if not get_settings().features.enable_sql_execution:
            return TaskResponse(
                task_id=request.id,
                status=TaskStatus.COMPLETED,
                result="SQL execution is disabled. Here's the query for manual execution:\n\n"
                       f"```sql\n{sql_query}\n```",
                data={"sql": sql_query, "execution_enabled": False},
                agent_type=self.agent_type,
            )
        
        # Execute the query
        try:
            if self.databricks_tools.is_configured:
                result_data = self.databricks_tools.execute_sql(sql_query)
                return TaskResponse(
                    task_id=request.id,
                    status=TaskStatus.COMPLETED,
                    result=f"Query executed successfully.\n\n**Results:**\n{result_data}",
                    data={"sql": sql_query, "results": result_data},
                    agent_type=self.agent_type,
                )
            else:
                return TaskResponse(
                    task_id=request.id,
                    status=TaskStatus.COMPLETED,
                    result="Databricks not configured. Here's the query for manual execution:\n\n"
                           f"```sql\n{sql_query}\n```",
                    data={"sql": sql_query},
                    agent_type=self.agent_type,
                )
        except Exception as e:
            return TaskResponse(
                task_id=request.id,
                status=TaskStatus.FAILED,
                error=f"Query execution failed: {str(e)}",
                agent_type=self.agent_type,
            )
    
    def _handle_general_sql(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Handle general SQL questions."""
        prompt = f"""Answer this SQL-related question for Databricks.

Question: {request.query}

Provide:
1. Clear answer with examples
2. Databricks-specific syntax if applicable
3. Best practices
4. Related tips"""

        result = self.invoke(prompt, context)
        
        return TaskResponse(
            task_id=request.id,
            status=TaskStatus.COMPLETED,
            result=result,
            agent_type=self.agent_type,
        )
    
    def analyze_query(self, sql: str) -> Dict[str, Any]:
        """
        Analyze a SQL query and return insights.
        
        Args:
            sql: SQL query to analyze
            
        Returns:
            Dictionary with analysis results
        """
        analysis = {
            "has_joins": bool(re.search(r'\bJOIN\b', sql, re.IGNORECASE)),
            "has_aggregations": bool(re.search(r'\b(COUNT|SUM|AVG|MIN|MAX|GROUP BY)\b', sql, re.IGNORECASE)),
            "has_subqueries": bool(re.search(r'\(\s*SELECT', sql, re.IGNORECASE)),
            "has_window_functions": bool(re.search(r'\bOVER\s*\(', sql, re.IGNORECASE)),
            "has_cte": bool(re.search(r'\bWITH\b.*\bAS\s*\(', sql, re.IGNORECASE)),
            "estimated_complexity": "simple",
        }
        
        # Estimate complexity
        complexity_score = sum([
            analysis["has_joins"] * 2,
            analysis["has_aggregations"],
            analysis["has_subqueries"] * 2,
            analysis["has_window_functions"] * 2,
            analysis["has_cte"],
        ])
        
        if complexity_score >= 5:
            analysis["estimated_complexity"] = "complex"
        elif complexity_score >= 2:
            analysis["estimated_complexity"] = "moderate"
        
        return analysis
