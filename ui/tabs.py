"""
Tab implementations for the Streamlit UI.
"""

import streamlit as st
from typing import Any, Dict, Optional
import pandas as pd
import io

from core.message import ConversationContext, AgentType
from core.orchestrator import OrchestratorAgent
from core.agent_registry import AgentRegistry
from tools.file_tools import FileTools
from tools.schema_tools import SchemaInferenceTools
from ui.components import (
    render_chat_message, render_code_block, render_data_preview,
    render_schema_display, render_validation_results, render_download_button
)


def render_chat_tab(orchestrator: OrchestratorAgent, context: ConversationContext):
    """
    Render the main chat interface tab.
    
    Args:
        orchestrator: Orchestrator agent instance
        context: Conversation context
    """
    st.header("💬 Chat with Databricks Agent")
    
    # Display chat history
    for msg in context.messages:
        avatar = "👤" if msg.role.value == "user" else "🤖"
        with st.chat_message(msg.role.value, avatar=avatar):
            st.markdown(msg.content)
    
    # Chat input
    if prompt := st.chat_input("Ask me about Databricks..."):
        # Display user message
        with st.chat_message("user", avatar="👤"):
            st.markdown(prompt)
        
        # Get response from orchestrator
        with st.chat_message("assistant", avatar="🤖"):
            with st.spinner("Thinking..."):
                response = orchestrator.route_query(prompt, context)
                st.markdown(response.result or "I couldn't process that request.")
                
                # Show which agent handled it
                if response.agent_type:
                    st.caption(f"*Handled by {response.agent_type.value} agent*")


def render_create_table_tab(context: ConversationContext):
    """
    Render the table creation tab.
    
    Args:
        context: Conversation context
    """
    st.header("📊 Create Table from CSV")
    
    file_tools = FileTools()
    schema_tools = SchemaInferenceTools()
    
    # File upload
    uploaded_file = st.file_uploader(
        "Upload CSV file",
        type=["csv", "tsv"],
        help="Upload a CSV or TSV file to generate DDL"
    )
    
    if uploaded_file:
        # Read the file
        df, error = file_tools.read_csv(uploaded_file, uploaded_file.name)
        
        if error:
            st.error(f"Error reading file: {error}")
            return
        
        # Clean column names
        df = file_tools.clean_column_names(df)
        
        # Show preview
        render_data_preview(df)
        
        # Table configuration
        col1, col2 = st.columns(2)
        
        with col1:
            table_name = st.text_input(
                "Table Name",
                value="new_table",
                help="Name for the new table"
            )
        
        with col2:
            catalog = st.text_input(
                "Catalog",
                value=context.current_catalog or "main"
            )
            schema_name = st.text_input(
                "Schema",
                value=context.current_schema or "default"
            )
        
        # Infer schema
        if st.button("Generate DDL", type="primary"):
            with st.spinner("Inferring schema..."):
                schema = schema_tools.infer_schema(df)
                
                # Display schema
                render_schema_display(schema)
                
                # Generate DDL
                full_name = f"{catalog}.{schema_name}.{table_name}"
                
                columns_ddl = []
                for col_name, col_type in schema.items():
                    columns_ddl.append(f"    {col_name} {col_type}")
                
                ddl = f"""CREATE TABLE IF NOT EXISTS {full_name} (
{','.join(columns_ddl)}
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)"""
                
                st.subheader("Generated DDL")
                render_code_block(ddl, "sql")
                
                # Download button
                render_download_button(
                    ddl,
                    f"{table_name}_ddl.sql",
                    "Download DDL"
                )
                
                # Store in context
                context.current_table = full_name
                st.session_state["generated_ddl"] = ddl


def render_alter_table_tab(context: ConversationContext):
    """
    Render the alter table tab.
    
    Args:
        context: Conversation context
    """
    st.header("🔄 Alter Table")
    
    # Table selection
    table_name = st.text_input(
        "Table Name",
        value=context.current_table or "",
        help="Full table name (catalog.schema.table)"
    )
    
    # Operation selection
    operation = st.selectbox(
        "Operation",
        ["Add Column", "Drop Column", "Rename Column", "Change Type", "Custom SQL"]
    )
    
    if operation == "Add Column":
        col_name = st.text_input("Column Name")
        col_type = st.selectbox(
            "Data Type",
            ["STRING", "BIGINT", "INT", "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP", "DECIMAL(10,2)"]
        )
        col_comment = st.text_input("Comment (optional)")
        
        if st.button("Generate ALTER Statement"):
            sql = f"ALTER TABLE {table_name}\nADD COLUMN {col_name} {col_type}"
            if col_comment:
                sql += f" COMMENT '{col_comment}'"
            render_code_block(sql, "sql")
    
    elif operation == "Drop Column":
        col_name = st.text_input("Column Name to Drop")
        
        if st.button("Generate ALTER Statement"):
            st.warning("⚠️ This operation will permanently delete the column and its data!")
            sql = f"ALTER TABLE {table_name}\nDROP COLUMN {col_name}"
            render_code_block(sql, "sql")
    
    elif operation == "Rename Column":
        old_name = st.text_input("Current Column Name")
        new_name = st.text_input("New Column Name")
        
        if st.button("Generate ALTER Statement"):
            sql = f"ALTER TABLE {table_name}\nRENAME COLUMN {old_name} TO {new_name}"
            render_code_block(sql, "sql")
    
    elif operation == "Change Type":
        col_name = st.text_input("Column Name")
        new_type = st.selectbox(
            "New Data Type",
            ["STRING", "BIGINT", "INT", "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP"]
        )
        
        if st.button("Generate ALTER Statement"):
            st.warning("⚠️ Changing data types may cause data loss if types are incompatible!")
            sql = f"ALTER TABLE {table_name}\nALTER COLUMN {col_name} TYPE {new_type}"
            render_code_block(sql, "sql")
    
    elif operation == "Custom SQL":
        custom_sql = st.text_area(
            "Custom ALTER Statement",
            height=150,
            placeholder="ALTER TABLE catalog.schema.table ..."
        )
        
        if custom_sql:
            render_code_block(custom_sql, "sql")


def render_metadata_tab(context: ConversationContext):
    """
    Render the metadata management tab.
    
    Args:
        context: Conversation context
    """
    st.header("📝 Metadata Management")
    
    # Table selection
    table_name = st.text_input(
        "Table Name",
        value=context.current_table or "",
        help="Full table name (catalog.schema.table)"
    )
    
    tabs = st.tabs(["Comments", "Tags", "Properties"])
    
    with tabs[0]:  # Comments
        st.subheader("Table & Column Comments")
        
        table_comment = st.text_area(
            "Table Comment",
            placeholder="Description of the table purpose..."
        )
        
        if table_comment and st.button("Generate Comment SQL", key="gen_comment"):
            sql = f"COMMENT ON TABLE {table_name} IS '{table_comment}'"
            render_code_block(sql, "sql")
    
    with tabs[1]:  # Tags
        st.subheader("Table Tags")
        
        # Common tag presets
        tag_presets = {
            "PII": {"pii": "true"},
            "Confidential": {"data_classification": "confidential"},
            "GDPR Subject": {"gdpr": "subject", "retention_days": "365"},
            "Public": {"data_classification": "public"},
        }
        
        selected_presets = st.multiselect(
            "Quick Tag Presets",
            list(tag_presets.keys())
        )
        
        # Custom tags
        st.write("Custom Tags (key=value, one per line):")
        custom_tags = st.text_area(
            "Custom Tags",
            placeholder="owner=data_team\ndomain=finance",
            label_visibility="collapsed"
        )
        
        if st.button("Generate Tag SQL", key="gen_tags"):
            all_tags = {}
            
            for preset in selected_presets:
                all_tags.update(tag_presets[preset])
            
            if custom_tags:
                for line in custom_tags.strip().split("\n"):
                    if "=" in line:
                        key, value = line.split("=", 1)
                        all_tags[key.strip()] = value.strip()
            
            if all_tags:
                tag_str = ", ".join(f"'{k}' = '{v}'" for k, v in all_tags.items())
                sql = f"ALTER TABLE {table_name}\nSET TAGS ({tag_str})"
                render_code_block(sql, "sql")
    
    with tabs[2]:  # Properties
        st.subheader("Table Properties")
        
        auto_optimize = st.checkbox("Enable Auto Optimize", value=True)
        auto_compact = st.checkbox("Enable Auto Compact", value=True)
        log_retention = st.number_input("Log Retention (days)", value=30, min_value=1)
        
        if st.button("Generate Properties SQL", key="gen_props"):
            props = []
            if auto_optimize:
                props.append("'delta.autoOptimize.optimizeWrite' = 'true'")
            if auto_compact:
                props.append("'delta.autoOptimize.autoCompact' = 'true'")
            props.append(f"'delta.logRetentionDuration' = '{log_retention} days'")
            
            sql = f"ALTER TABLE {table_name}\nSET TBLPROPERTIES (\n    {','.join(props)}\n)"
            render_code_block(sql, "sql")


def render_pipeline_tab(context: ConversationContext):
    """
    Render the DLT pipeline generation tab.
    
    Args:
        context: Conversation context
    """
    st.header("⚙️ Delta Live Tables Pipeline")
    
    # Pipeline configuration
    pipeline_name = st.text_input("Pipeline Name", value="my_pipeline")
    
    layer = st.selectbox(
        "Layer Type",
        ["Bronze (Raw)", "Silver (Cleansed)", "Gold (Business)", "Full Pipeline"]
    )
    
    source_type = st.selectbox(
        "Source Type",
        ["CSV/JSON Files", "Existing Table", "Streaming (Kafka)", "Custom"]
    )
    
    # Source configuration
    if source_type == "CSV/JSON Files":
        source_path = st.text_input("Source Path", placeholder="/mnt/data/raw/")
        file_format = st.selectbox("File Format", ["json", "csv", "parquet"])
    elif source_type == "Existing Table":
        source_table = st.text_input("Source Table", placeholder="catalog.schema.table")
    
    # Generate pipeline
    if st.button("Generate Pipeline Code", type="primary"):
        code = generate_pipeline_code(
            layer, source_type, pipeline_name,
            source_path if source_type == "CSV/JSON Files" else None,
            source_table if source_type == "Existing Table" else None,
            file_format if source_type == "CSV/JSON Files" else None,
        )
        
        st.subheader("Generated Pipeline Code")
        render_code_block(code, "python")
        
        render_download_button(
            code,
            f"{pipeline_name}_pipeline.py",
            "Download Pipeline Code"
        )


def generate_pipeline_code(
    layer: str,
    source_type: str,
    pipeline_name: str,
    source_path: Optional[str] = None,
    source_table: Optional[str] = None,
    file_format: Optional[str] = None,
) -> str:
    """Generate DLT pipeline code."""
    
    code_parts = [
        "import dlt",
        "from pyspark.sql.functions import *",
        "",
    ]
    
    if "Bronze" in layer or "Full" in layer:
        code_parts.extend([
            f"@dlt.table(",
            f'    name="{pipeline_name}_bronze",',
            f'    comment="Raw data ingestion layer"',
            ")",
            f"def {pipeline_name}_bronze():",
        ])
        
        if source_type == "CSV/JSON Files":
            code_parts.extend([
                "    return (",
                f'        spark.readStream.format("cloudFiles")',
                f'        .option("cloudFiles.format", "{file_format or "json"}")',
                f'        .load("{source_path or "/path/to/data/"}")',
                '        .withColumn("_ingest_timestamp", current_timestamp())',
                '        .withColumn("_source_file", input_file_name())',
                "    )",
            ])
        else:
            code_parts.extend([
                f'    return spark.read.table("{source_table or "source_table"}")',
            ])
        code_parts.append("")
    
    if "Silver" in layer or "Full" in layer:
        code_parts.extend([
            f"@dlt.table(",
            f'    name="{pipeline_name}_silver",',
            f'    comment="Cleansed and validated data"',
            ")",
            '@dlt.expect_or_drop("valid_record", "id IS NOT NULL")',
            f"def {pipeline_name}_silver():",
            "    return (",
            f'        dlt.read("{pipeline_name}_bronze")',
            "        .dropDuplicates()",
            '        .filter(col("id").isNotNull())',
            "    )",
            "",
        ])
    
    if "Gold" in layer or "Full" in layer:
        code_parts.extend([
            f"@dlt.table(",
            f'    name="{pipeline_name}_gold",',
            f'    comment="Business-level aggregations"',
            ")",
            f"def {pipeline_name}_gold():",
            "    return (",
            f'        dlt.read("{pipeline_name}_silver")',
            "        .groupBy()",
            "        .agg(",
            '            count("*").alias("total_count")',
            "        )",
            "    )",
        ])
    
    return "\n".join(code_parts)
