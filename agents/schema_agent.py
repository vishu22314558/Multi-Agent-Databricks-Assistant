"""
Schema Agent - Handles DDL generation, ALTER statements, and schema operations.
"""

import logging
import re
from typing import Any, Dict, List, Optional

import pandas as pd

from core.base_agent import BaseAgent
from core.message import AgentType, TaskRequest, TaskResponse, TaskStatus, ConversationContext
from tools.schema_tools import SchemaInferenceTools
from tools.databricks_tools import DatabricksTools

logger = logging.getLogger(__name__)


class SchemaAgent(BaseAgent):
    """
    Specialized agent for schema and DDL operations.
    
    Capabilities:
    - Generate CREATE TABLE statements from CSV
    - Generate ALTER TABLE statements
    - Infer column types from data
    - Create databases and schemas
    """
    
    def __init__(self):
        super().__init__(
            agent_type=AgentType.SCHEMA,
            name="Schema Agent",
            description="Handles DDL generation, schema modifications, and type inference",
            temperature=0.0,  # Deterministic for DDL
        )
        self.schema_tools = SchemaInferenceTools()
        self.databricks_tools = DatabricksTools()
    
    def get_capabilities(self) -> List[str]:
        return [
            "create table", "ddl", "alter table", "add column", "drop column",
            "rename column", "change type", "schema", "csv to table", 
            "infer types", "create database", "create schema", "modify table"
        ]
    
    def process(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Process schema-related requests."""
        query = request.query.lower()
        
        # Determine the specific operation
        if "csv" in query or "upload" in query or "file" in query:
            return self._handle_csv_to_table(request, context)
        elif "alter" in query or "modify" in query:
            return self._handle_alter_table(request, context)
        elif "create table" in query or "ddl" in query:
            return self._handle_create_table(request, context)
        elif "create database" in query or "create schema" in query:
            return self._handle_create_database(request, context)
        else:
            return self._handle_general_schema(request, context)
    
    def _handle_csv_to_table(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Generate DDL from CSV file."""
        # Check for uploaded file in context
        csv_data = request.context.get("csv_data")
        table_name = request.context.get("table_name", "new_table")
        
        if csv_data is None:
            # Generate example DDL with instructions
            result = self._generate_csv_instructions(request.query, context)
            return TaskResponse(
                task_id=request.id,
                status=TaskStatus.COMPLETED,
                result=result,
                agent_type=self.agent_type,
            )
        
        try:
            # Convert to DataFrame if string
            if isinstance(csv_data, str):
                import io
                df = pd.read_csv(io.StringIO(csv_data))
            else:
                df = csv_data
            
            # Infer schema
            schema = self.schema_tools.infer_schema(df)
            
            # Generate DDL
            catalog = context.current_catalog or "main"
            schema_name = context.current_schema or "default"
            full_table_name = f"{catalog}.{schema_name}.{table_name}"
            
            ddl = self._generate_ddl(full_table_name, schema, df)
            
            # Update context
            context.current_table = full_table_name
            
            return TaskResponse(
                task_id=request.id,
                status=TaskStatus.COMPLETED,
                result=f"Generated DDL for table `{full_table_name}`:\n\n```sql\n{ddl}\n```\n\n"
                       f"**Schema Summary:**\n"
                       f"- Columns: {len(schema)}\n"
                       f"- Rows in sample: {len(df)}",
                data={
                    "ddl": ddl,
                    "table_name": full_table_name,
                    "schema": schema,
                    "row_count": len(df),
                },
                agent_type=self.agent_type,
            )
        except Exception as e:
            logger.error(f"Error generating DDL from CSV: {e}")
            return TaskResponse(
                task_id=request.id,
                status=TaskStatus.FAILED,
                error=f"Failed to generate DDL: {str(e)}",
                agent_type=self.agent_type,
            )
    
    def _generate_csv_instructions(
        self,
        query: str,
        context: ConversationContext,
    ) -> str:
        """Generate instructions when no CSV is provided."""
        prompt = f"""User wants to create a table from CSV. No file was uploaded.

Query: {query}

Generate:
1. Instructions on how to upload a CSV file
2. An example CREATE TABLE statement with common columns
3. Tips for data type selection in Databricks

Keep it concise and practical."""

        return self.invoke(prompt, context)
    
    def _generate_ddl(
        self,
        table_name: str,
        schema: Dict[str, str],
        df: pd.DataFrame,
    ) -> str:
        """Generate CREATE TABLE DDL."""
        columns = []
        for col_name, col_type in schema.items():
            # Clean column name
            clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', col_name)
            columns.append(f"    {clean_name} {col_type}")
        
        columns_str = ",\n".join(columns)
        
        ddl = f"""CREATE TABLE IF NOT EXISTS {table_name} (
{columns_str}
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)"""
        
        return ddl
    
    def _handle_alter_table(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Handle ALTER TABLE requests."""
        query = request.query
        table_name = context.current_table or request.context.get("table_name")
        
        prompt = f"""Generate ALTER TABLE statement(s) for the following request.

Table: {table_name or 'Not specified'}
Request: {query}

Provide:
1. The appropriate ALTER TABLE SQL statement(s)
2. Brief explanation of what each statement does
3. Any warnings about data loss or compatibility

Use Databricks SQL syntax (Unity Catalog compatible)."""

        result = self.invoke(prompt, context)
        
        # Try to extract the SQL
        sql_match = re.search(r'```sql\n(.*?)\n```', result, re.DOTALL)
        sql = sql_match.group(1) if sql_match else None
        
        return TaskResponse(
            task_id=request.id,
            status=TaskStatus.COMPLETED,
            result=result,
            data={"sql": sql, "table_name": table_name},
            agent_type=self.agent_type,
        )
    
    def _handle_create_table(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Handle CREATE TABLE requests."""
        query = request.query
        
        catalog = context.current_catalog or "main"
        schema_name = context.current_schema or "default"
        
        prompt = f"""Generate a CREATE TABLE statement based on the following request.

Current catalog: {catalog}
Current schema: {schema_name}
Request: {query}

Requirements:
1. Use DELTA format
2. Include appropriate TBLPROPERTIES for optimization
3. Use Unity Catalog three-level namespace (catalog.schema.table)
4. Add comments for each column if the purpose is clear
5. Use appropriate data types

Provide the complete DDL statement."""

        result = self.invoke(prompt, context)
        
        return TaskResponse(
            task_id=request.id,
            status=TaskStatus.COMPLETED,
            result=result,
            agent_type=self.agent_type,
        )
    
    def _handle_create_database(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Handle CREATE DATABASE/SCHEMA requests."""
        query = request.query
        
        prompt = f"""Generate CREATE DATABASE or CREATE SCHEMA statement(s) based on the request.

Request: {query}

Include:
1. CREATE DATABASE/SCHEMA statement with IF NOT EXISTS
2. Appropriate location if applicable
3. Any recommended properties
4. Brief explanation

Use Databricks Unity Catalog syntax."""

        result = self.invoke(prompt, context)
        
        return TaskResponse(
            task_id=request.id,
            status=TaskStatus.COMPLETED,
            result=result,
            agent_type=self.agent_type,
        )
    
    def _handle_general_schema(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Handle general schema questions."""
        result = self.invoke(request.query, context)
        
        return TaskResponse(
            task_id=request.id,
            status=TaskStatus.COMPLETED,
            result=result,
            agent_type=self.agent_type,
        )
