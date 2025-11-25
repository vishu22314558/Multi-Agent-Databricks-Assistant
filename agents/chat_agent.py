"""
Chat Agent - Handles general Q&A, explanations, and best practices.
"""

import logging
from typing import Any, Dict, List, Optional

from core.base_agent import BaseAgent
from core.message import AgentType, TaskRequest, TaskResponse, TaskStatus, ConversationContext

logger = logging.getLogger(__name__)


class ChatAgent(BaseAgent):
    """
    General-purpose agent for Q&A and explanations.
    
    Capabilities:
    - Databricks concept explanations
    - Best practices recommendations
    - Comparisons and recommendations
    - General help and guidance
    """
    
    def __init__(self):
        super().__init__(
            agent_type=AgentType.CHAT,
            name="Chat Agent",
            description="Handles general questions, explanations, and best practices",
            temperature=0.3,  # Slightly more creative for explanations
        )
    
    def get_capabilities(self) -> List[str]:
        return [
            "explain", "what is", "how to", "best practice", "help", 
            "recommend", "difference between", "compare", "why",
            "when to use", "tutorial", "guide"
        ]
    
    def process(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Process general chat requests."""
        query = request.query.lower()
        
        # Determine the type of question
        if "best practice" in query or "recommend" in query:
            return self._handle_best_practices(request, context)
        elif "difference" in query or "compare" in query or " vs " in query:
            return self._handle_comparison(request, context)
        elif "what is" in query or "explain" in query or "how does" in query:
            return self._handle_explanation(request, context)
        elif "how to" in query or "how do i" in query:
            return self._handle_howto(request, context)
        else:
            return self._handle_general_chat(request, context)
    
    def _handle_best_practices(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Provide best practice recommendations."""
        prompt = f"""Provide best practice recommendations for this Databricks topic.

Question: {request.query}

Structure your response as:

1. **Key Best Practices:**
   - List the most important recommendations
   - Explain why each matters

2. **Common Mistakes to Avoid:**
   - List common anti-patterns
   - Explain the consequences

3. **Implementation Tips:**
   - Practical advice for implementation
   - Code examples if applicable

4. **Additional Resources:**
   - Relevant Databricks documentation topics
   - Related features to explore

Keep the response focused and actionable."""

        result = self.invoke(prompt, context)
        
        return TaskResponse(
            task_id=request.id,
            status=TaskStatus.COMPLETED,
            result=result,
            agent_type=self.agent_type,
        )
    
    def _handle_comparison(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Handle comparison questions."""
        prompt = f"""Compare and contrast the following Databricks concepts/features.

Question: {request.query}

Structure your response as:

1. **Overview:**
   - Brief description of each option

2. **Key Differences:**
   | Feature | Option A | Option B |
   | --- | --- | --- |
   (Use a comparison table)

3. **When to Use Each:**
   - Scenarios where each excels
   - Decision criteria

4. **Performance Considerations:**
   - Speed, cost, scalability differences

5. **Recommendation:**
   - General guidance on when to choose each

Be objective and highlight trade-offs."""

        result = self.invoke(prompt, context)
        
        return TaskResponse(
            task_id=request.id,
            status=TaskStatus.COMPLETED,
            result=result,
            agent_type=self.agent_type,
        )
    
    def _handle_explanation(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Explain Databricks concepts."""
        prompt = f"""Explain this Databricks concept clearly and thoroughly.

Question: {request.query}

Structure your response as:

1. **Definition:**
   - Clear, concise explanation
   - Why it exists and its purpose

2. **How It Works:**
   - Technical details at appropriate level
   - Key components or mechanisms

3. **Example:**
   - Practical example with code if applicable
   - Real-world use case

4. **Key Points to Remember:**
   - Most important takeaways
   - Common misconceptions

5. **Related Concepts:**
   - Connected features or topics
   - Natural next steps to learn

Make it accessible but technically accurate."""

        result = self.invoke(prompt, context)
        
        return TaskResponse(
            task_id=request.id,
            status=TaskStatus.COMPLETED,
            result=result,
            agent_type=self.agent_type,
        )
    
    def _handle_howto(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Provide step-by-step guidance."""
        prompt = f"""Provide step-by-step guidance for this Databricks task.

Question: {request.query}

Structure your response as:

1. **Prerequisites:**
   - What you need before starting
   - Required permissions or setup

2. **Step-by-Step Instructions:**
   - Numbered steps with clear actions
   - Code snippets where applicable
   - Expected outcomes at each step

3. **Verification:**
   - How to verify success
   - What the result should look like

4. **Troubleshooting:**
   - Common issues and solutions
   - Where to look if something goes wrong

5. **Tips:**
   - Efficiency improvements
   - Alternative approaches

Be practical and actionable."""

        result = self.invoke(prompt, context)
        
        return TaskResponse(
            task_id=request.id,
            status=TaskStatus.COMPLETED,
            result=result,
            agent_type=self.agent_type,
        )
    
    def _handle_general_chat(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Handle general conversation."""
        # Add Databricks context to general questions
        databricks_context = """You are an expert in Databricks, Delta Lake, Apache Spark, 
and data engineering. Provide helpful, accurate answers focused on the Databricks ecosystem.
If the question isn't directly related to Databricks, try to connect it to relevant 
data engineering concepts where appropriate."""

        result = self.invoke(
            request.query, 
            context,
            additional_context=databricks_context
        )
        
        return TaskResponse(
            task_id=request.id,
            status=TaskStatus.COMPLETED,
            result=result,
            agent_type=self.agent_type,
        )
    
    def get_quick_tips(self, topic: str) -> List[str]:
        """
        Get quick tips for common topics.
        
        Args:
            topic: Topic to get tips for
            
        Returns:
            List of quick tip strings
        """
        tips_database = {
            "delta_lake": [
                "Use OPTIMIZE regularly for better query performance",
                "Enable auto-optimization in table properties",
                "Use Z-ORDER on frequently filtered columns",
                "Run VACUUM periodically to clean up old files",
                "Use time travel for debugging and auditing",
            ],
            "unity_catalog": [
                "Use three-level namespace: catalog.schema.table",
                "Grant minimum necessary permissions",
                "Use tags for data classification",
                "Document tables with COMMENT statements",
                "Leverage data lineage for impact analysis",
            ],
            "performance": [
                "Filter early and select only needed columns",
                "Use broadcast joins for small dimension tables",
                "Partition by frequently filtered date columns",
                "Cache intermediate results for iterative processing",
                "Use Delta caching for repeated queries",
            ],
            "dlt": [
                "Use expectations for data quality enforcement",
                "Implement Bronze/Silver/Gold medallion architecture",
                "Use streaming tables for real-time data",
                "Enable schema evolution for changing sources",
                "Monitor pipelines with event logs",
            ],
        }
        
        topic_lower = topic.lower().replace(" ", "_")
        return tips_database.get(topic_lower, [
            "Check Databricks documentation for detailed guidance",
            "Use the SQL Editor for testing queries",
            "Monitor cluster performance with Ganglia metrics",
        ])
