"""
Metadata Agent - Handles tags, comments, and governance operations.
"""

import logging
from typing import Any, Dict, List, Optional

from core.base_agent import BaseAgent
from core.message import AgentType, TaskRequest, TaskResponse, TaskStatus, ConversationContext
from tools.databricks_tools import DatabricksTools

logger = logging.getLogger(__name__)


class MetadataAgent(BaseAgent):
    """
    Specialized agent for metadata and governance operations.
    
    Capabilities:
    - Add/manage table and column comments
    - Apply and manage tags
    - Set table properties
    - Data classification and governance
    """
    
    def __init__(self):
        super().__init__(
            agent_type=AgentType.METADATA,
            name="Metadata Agent",
            description="Handles tags, comments, table properties, and data governance",
            temperature=0.1,
        )
        self.databricks_tools = DatabricksTools()
    
    def get_capabilities(self) -> List[str]:
        return [
            "tag", "comment", "property", "metadata", "governance", "pii",
            "classification", "document table", "describe", "catalog",
            "lineage", "audit", "sensitive data"
        ]
    
    def process(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Process metadata-related requests."""
        query = request.query.lower()
        
        # Determine the specific operation
        if "comment" in query:
            return self._handle_comments(request, context)
        elif "tag" in query:
            return self._handle_tags(request, context)
        elif "propert" in query:
            return self._handle_properties(request, context)
        elif "pii" in query or "sensitive" in query or "classif" in query:
            return self._handle_classification(request, context)
        elif "describe" in query or "document" in query:
            return self._handle_documentation(request, context)
        else:
            return self._handle_general_metadata(request, context)
    
    def _handle_comments(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Handle comment operations."""
        table_name = context.current_table or request.context.get("table_name")
        columns = request.context.get("columns", [])
        
        prompt = f"""Generate SQL statements to add comments to a Databricks table.

Request: {request.query}
Table: {table_name or 'Not specified'}
Columns: {columns or 'Not specified'}

Generate:
1. COMMENT ON TABLE statement if table comment needed
2. ALTER TABLE ... ALTER COLUMN ... COMMENT statements for columns
3. Best practices for writing good documentation

Use Unity Catalog compatible syntax:
- COMMENT ON TABLE catalog.schema.table IS 'description'
- ALTER TABLE catalog.schema.table ALTER COLUMN col_name COMMENT 'description'

Provide complete SQL statements."""

        result = self.invoke(prompt, context)
        
        return TaskResponse(
            task_id=request.id,
            status=TaskStatus.COMPLETED,
            result=result,
            data={"table_name": table_name, "operation": "comments"},
            agent_type=self.agent_type,
        )
    
    def _handle_tags(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Handle tag operations."""
        table_name = context.current_table or request.context.get("table_name")
        
        # Common governance tags
        common_tags = [
            "pii", "confidential", "public", "internal", 
            "gdpr", "hipaa", "financial", "sensitive"
        ]
        
        prompt = f"""Generate SQL statements to manage tags on a Databricks table or column.

Request: {request.query}
Table: {table_name or 'Not specified'}

Common tag categories:
- Data Classification: {', '.join(common_tags)}
- Data Quality: quality_score, validated, raw
- Business Domain: finance, hr, sales, marketing
- Retention: retain_7_years, archive, delete_after_90_days

Generate:
1. ALTER TABLE ... SET TAGS statements
2. ALTER TABLE ... ALTER COLUMN ... SET TAGS for column-level tags
3. Explanation of the tagging strategy

Unity Catalog Tag Syntax:
- ALTER TABLE catalog.schema.table SET TAGS ('key' = 'value', ...)
- ALTER TABLE catalog.schema.table ALTER COLUMN col SET TAGS ('key' = 'value')
- ALTER TABLE catalog.schema.table UNSET TAGS ('key1', 'key2')

Provide complete SQL statements with explanations."""

        result = self.invoke(prompt, context)
        
        return TaskResponse(
            task_id=request.id,
            status=TaskStatus.COMPLETED,
            result=result,
            data={"table_name": table_name, "operation": "tags"},
            agent_type=self.agent_type,
        )
    
    def _handle_properties(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Handle table property operations."""
        table_name = context.current_table or request.context.get("table_name")
        
        prompt = f"""Generate SQL statements to manage table properties in Databricks.

Request: {request.query}
Table: {table_name or 'Not specified'}

Common Delta Lake Properties:
- delta.autoOptimize.optimizeWrite: Auto-optimize writes
- delta.autoOptimize.autoCompact: Auto-compact small files
- delta.logRetentionDuration: Transaction log retention
- delta.deletedFileRetentionDuration: Deleted file retention
- delta.dataSkippingNumIndexedCols: Data skipping columns
- delta.checkpoint.writeStatsAsJson: Write statistics as JSON
- delta.checkpoint.writeStatsAsStruct: Write statistics as struct

Generate:
1. ALTER TABLE ... SET TBLPROPERTIES statements
2. Recommendations based on table usage patterns
3. Best practices for property configuration

Provide complete SQL statements with explanations."""

        result = self.invoke(prompt, context)
        
        return TaskResponse(
            task_id=request.id,
            status=TaskStatus.COMPLETED,
            result=result,
            data={"table_name": table_name, "operation": "properties"},
            agent_type=self.agent_type,
        )
    
    def _handle_classification(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Handle data classification and PII tagging."""
        table_name = context.current_table or request.context.get("table_name")
        columns = request.context.get("columns", [])
        
        prompt = f"""Generate a data classification strategy and SQL statements for governance.

Request: {request.query}
Table: {table_name or 'Not specified'}
Columns: {columns or 'Auto-detect'}

Create a comprehensive classification approach:

1. **PII Detection Patterns:**
   - Names (first_name, last_name, full_name)
   - Contact (email, phone, address)
   - Identifiers (ssn, national_id, passport)
   - Financial (credit_card, bank_account)
   - Health (diagnosis, prescription)

2. **Classification Tags:**
   - pii_level: none, low, medium, high
   - data_sensitivity: public, internal, confidential, restricted
   - compliance: gdpr, hipaa, pci, sox

3. **SQL Statements:**
   - Column-level PII tags
   - Table-level classification tags
   - Row-level security setup (if applicable)

4. **Governance Recommendations:**
   - Access control suggestions
   - Masking/encryption recommendations
   - Audit logging setup

Provide complete SQL statements and governance plan."""

        result = self.invoke(prompt, context)
        
        return TaskResponse(
            task_id=request.id,
            status=TaskStatus.COMPLETED,
            result=result,
            data={
                "table_name": table_name,
                "operation": "classification",
                "columns_analyzed": columns,
            },
            agent_type=self.agent_type,
        )
    
    def _handle_documentation(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Generate comprehensive table documentation."""
        table_name = context.current_table or request.context.get("table_name")
        schema_info = request.context.get("schema", {})
        
        prompt = f"""Generate comprehensive documentation for a Databricks table.

Request: {request.query}
Table: {table_name or 'Not specified'}
Schema Info: {schema_info or 'Not provided'}

Create documentation including:

1. **Table Overview:**
   - Purpose and business context
   - Data source and refresh frequency
   - Owner and stakeholders

2. **Schema Documentation:**
   - Column descriptions
   - Data types and constraints
   - Relationships to other tables

3. **Data Quality:**
   - Key quality rules
   - Known issues or limitations
   - Quality metrics

4. **Access and Security:**
   - Required permissions
   - Sensitive data flags
   - Usage guidelines

5. **SQL Statements:**
   - COMMENT statements for table and columns
   - TAG statements for classification
   - Property settings

Generate both:
- Human-readable documentation (markdown)
- Executable SQL statements

Provide complete documentation."""

        result = self.invoke(prompt, context)
        
        return TaskResponse(
            task_id=request.id,
            status=TaskStatus.COMPLETED,
            result=result,
            data={"table_name": table_name, "operation": "documentation"},
            agent_type=self.agent_type,
        )
    
    def _handle_general_metadata(
        self,
        request: TaskRequest,
        context: ConversationContext,
    ) -> TaskResponse:
        """Handle general metadata questions."""
        prompt = f"""Answer this metadata/governance question for Databricks Unity Catalog.

Question: {request.query}

Provide:
1. Clear explanation
2. Relevant SQL examples
3. Best practices for data governance
4. Unity Catalog specific features"""

        result = self.invoke(prompt, context)
        
        return TaskResponse(
            task_id=request.id,
            status=TaskStatus.COMPLETED,
            result=result,
            agent_type=self.agent_type,
        )
    
    def suggest_tags_for_column(
        self,
        column_name: str,
        data_type: str,
        sample_values: Optional[List[str]] = None,
    ) -> Dict[str, str]:
        """
        Suggest appropriate tags for a column based on its name and type.
        
        Args:
            column_name: Name of the column
            data_type: Data type of the column
            sample_values: Optional sample values
            
        Returns:
            Dictionary of suggested tags
        """
        tags = {}
        name_lower = column_name.lower()
        
        # PII detection patterns
        pii_patterns = {
            "high": ["ssn", "social_security", "national_id", "passport", "credit_card", "bank_account"],
            "medium": ["email", "phone", "address", "dob", "date_of_birth", "salary", "income"],
            "low": ["name", "first_name", "last_name", "full_name", "username"],
        }
        
        for level, patterns in pii_patterns.items():
            if any(p in name_lower for p in patterns):
                tags["pii_level"] = level
                tags["requires_masking"] = "true" if level in ["high", "medium"] else "false"
                break
        
        # Data type based suggestions
        if "timestamp" in data_type.lower() or "date" in data_type.lower():
            if any(x in name_lower for x in ["created", "updated", "modified"]):
                tags["audit_column"] = "true"
        
        # Identifier patterns
        if name_lower.endswith("_id") or name_lower == "id":
            tags["column_type"] = "identifier"
        
        return tags
