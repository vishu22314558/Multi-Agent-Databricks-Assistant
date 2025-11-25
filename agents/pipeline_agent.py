"""
Pipeline Agent - Handles Delta Live Tables and ETL pipeline generation.
"""

import logging
from typing import Any, Dict, List, Optional

from core.base_agent import BaseAgent
from core.message import AgentType, TaskRequest, TaskResponse, TaskStatus, ConversationContext

logger = logging.getLogger(__name__)


class PipelineAgent(BaseAgent):
    """
    Specialized agent for DLT pipelines and ETL operations.
    
    Capabilities:
    - Generate Bronze/Silver/Gold layer code
    - Create Delta Live Tables pipelines
    - Streaming pipeline configuration
    - Data quality expectations
    """
    
    def __init__(self):
        super().__init__(
            agent_type=AgentType.PIPELINE,
            name="Pipeline Agent",
            description="Handles Delta Live Tables, ETL pipelines, and medallion architecture",
            temperature=0.1,
        )
    
    def get_capabilities(self) -> List[str]:
        return [
            "dlt", "delta live tables", "pipeline", "bronze", "silver", "gold",
            "streaming", "etl", "ingestion", "medallion", "expectations",
            "data quality", "auto loader"
        ]
    
    def process(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Process pipeline-related requests."""
        query = request.query.lower()
        
        # Determine the specific layer/operation
        if "bronze" in query:
            return self._generate_bronze_layer(request, context)
        elif "silver" in query:
            return self._generate_silver_layer(request, context)
        elif "gold" in query:
            return self._generate_gold_layer(request, context)
        elif "full pipeline" in query or "complete pipeline" in query or "medallion" in query:
            return self._generate_full_pipeline(request, context)
        elif "streaming" in query or "stream" in query:
            return self._generate_streaming_pipeline(request, context)
        else:
            return self._handle_general_pipeline(request, context)
    
    def _generate_bronze_layer(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Generate Bronze layer DLT code."""
        source_info = request.context.get("source", "")
        table_name = request.context.get("table_name", "bronze_table")
        
        prompt = f"""Generate a Delta Live Tables Bronze layer definition.

Request: {request.query}
Source: {source_info or 'Not specified'}
Table Name: {table_name}

Requirements:
1. Use @dlt.table decorator
2. Include raw data ingestion (Auto Loader if applicable)
3. Add ingestion metadata columns (_ingest_timestamp, _source_file)
4. Include basic data quality expectations
5. Handle schema evolution
6. Add appropriate comments

Provide complete Python code with explanations."""

        result = self.invoke(prompt, context)
        
        # Extract code for download
        code = self._extract_code(result, "python")
        
        return TaskResponse(
            task_id=request.id,
            status=TaskStatus.COMPLETED,
            result=result,
            data={
                "layer": "bronze",
                "code": code,
                "table_name": table_name,
            },
            agent_type=self.agent_type,
        )
    
    def _generate_silver_layer(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Generate Silver layer DLT code."""
        source_table = request.context.get("source_table", "bronze_table")
        table_name = request.context.get("table_name", "silver_table")
        
        prompt = f"""Generate a Delta Live Tables Silver layer definition.

Request: {request.query}
Source Table: {source_table}
Target Table: {table_name}

Requirements:
1. Use @dlt.table decorator with appropriate settings
2. Include data cleaning and standardization:
   - Type casting
   - Null handling
   - Deduplication
3. Add data quality expectations with @dlt.expect or @dlt.expect_or_drop
4. Reference the Bronze layer using dlt.read()
5. Include business logic transformations
6. Add proper documentation

Provide complete Python code with explanations."""

        result = self.invoke(prompt, context)
        code = self._extract_code(result, "python")
        
        return TaskResponse(
            task_id=request.id,
            status=TaskStatus.COMPLETED,
            result=result,
            data={
                "layer": "silver",
                "code": code,
                "source_table": source_table,
                "table_name": table_name,
            },
            agent_type=self.agent_type,
        )
    
    def _generate_gold_layer(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Generate Gold layer DLT code."""
        source_tables = request.context.get("source_tables", ["silver_table"])
        table_name = request.context.get("table_name", "gold_table")
        
        prompt = f"""Generate a Delta Live Tables Gold layer definition.

Request: {request.query}
Source Tables: {source_tables}
Target Table: {table_name}

Requirements:
1. Use @dlt.table decorator optimized for analytics
2. Include business-level aggregations and calculations
3. Join multiple Silver tables if applicable
4. Optimize for query performance:
   - Appropriate partitioning
   - Z-ordering hints
5. Add comprehensive data quality checks
6. Include business metrics and KPIs
7. Add detailed documentation

Provide complete Python code with explanations."""

        result = self.invoke(prompt, context)
        code = self._extract_code(result, "python")
        
        return TaskResponse(
            task_id=request.id,
            status=TaskStatus.COMPLETED,
            result=result,
            data={
                "layer": "gold",
                "code": code,
                "source_tables": source_tables,
                "table_name": table_name,
            },
            agent_type=self.agent_type,
        )
    
    def _generate_full_pipeline(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Generate complete medallion architecture pipeline."""
        source_info = request.context.get("source", "")
        base_name = request.context.get("base_name", "data")
        
        prompt = f"""Generate a complete Delta Live Tables pipeline with Bronze, Silver, and Gold layers.

Request: {request.query}
Data Source: {source_info or 'Not specified'}
Base Name: {base_name}

Create a complete medallion architecture with:

## Bronze Layer
- Raw data ingestion with Auto Loader
- Metadata columns
- Schema evolution handling

## Silver Layer  
- Data cleansing and standardization
- Type casting and null handling
- Deduplication logic
- Quality expectations

## Gold Layer
- Business aggregations
- Analytics-ready tables
- Performance optimization

Include:
1. All necessary imports
2. DLT table definitions for each layer
3. Data quality expectations at each level
4. Comments explaining each transformation
5. Best practices for production deployment

Provide the complete Python file that can be used directly in Databricks."""

        result = self.invoke(prompt, context)
        code = self._extract_code(result, "python")
        
        return TaskResponse(
            task_id=request.id,
            status=TaskStatus.COMPLETED,
            result=result,
            data={
                "layer": "full_pipeline",
                "code": code,
                "layers": ["bronze", "silver", "gold"],
            },
            agent_type=self.agent_type,
        )
    
    def _generate_streaming_pipeline(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Generate streaming DLT pipeline."""
        source_info = request.context.get("source", "kafka")
        
        prompt = f"""Generate a streaming Delta Live Tables pipeline.

Request: {request.query}
Streaming Source: {source_info}

Requirements:
1. Use streaming table definitions (@dlt.table with streaming=True or @dlt.view)
2. Configure appropriate checkpointing
3. Handle late-arriving data with watermarks
4. Include error handling for bad records
5. Set up proper trigger intervals
6. Add monitoring and alerting hooks

For Kafka sources, include:
- Connection configuration
- Schema registry integration if applicable
- Consumer group settings

Provide complete Python code optimized for streaming workloads."""

        result = self.invoke(prompt, context)
        code = self._extract_code(result, "python")
        
        return TaskResponse(
            task_id=request.id,
            status=TaskStatus.COMPLETED,
            result=result,
            data={
                "pipeline_type": "streaming",
                "code": code,
                "source": source_info,
            },
            agent_type=self.agent_type,
        )
    
    def _handle_general_pipeline(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Handle general pipeline questions."""
        prompt = f"""Answer this Delta Live Tables / pipeline question.

Question: {request.query}

Provide:
1. Clear explanation
2. Code examples if applicable
3. Best practices for Databricks DLT
4. Common pitfalls to avoid"""

        result = self.invoke(prompt, context)
        
        return TaskResponse(
            task_id=request.id,
            status=TaskStatus.COMPLETED,
            result=result,
            agent_type=self.agent_type,
        )
    
    def _extract_code(self, text: str, language: str = "python") -> Optional[str]:
        """Extract code block from response."""
        import re
        pattern = rf'```{language}\n(.*?)\n```'
        match = re.search(pattern, text, re.DOTALL)
        return match.group(1) if match else None
    
    def generate_expectations(
        self,
        columns: List[Dict[str, str]],
        strictness: str = "warn",
    ) -> str:
        """
        Generate DLT expectations for columns.
        
        Args:
            columns: List of column definitions with name and type
            strictness: "warn", "drop", or "fail"
            
        Returns:
            Python code string with expectations
        """
        expectations = []
        decorator = {
            "warn": "@dlt.expect",
            "drop": "@dlt.expect_or_drop", 
            "fail": "@dlt.expect_or_fail",
        }.get(strictness, "@dlt.expect")
        
        for col in columns:
            name = col.get("name", "column")
            dtype = col.get("type", "string").lower()
            
            # Generate appropriate expectation based on type
            if "int" in dtype or "long" in dtype:
                expectations.append(
                    f'{decorator}("{name}_valid", "{name} IS NOT NULL AND {name} >= 0")'
                )
            elif "string" in dtype:
                expectations.append(
                    f'{decorator}("{name}_not_empty", "{name} IS NOT NULL AND LENGTH({name}) > 0")'
                )
            elif "timestamp" in dtype or "date" in dtype:
                expectations.append(
                    f'{decorator}("{name}_valid_date", "{name} IS NOT NULL")'
                )
        
        return "\n".join(expectations)
