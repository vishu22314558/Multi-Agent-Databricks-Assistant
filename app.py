"""
Databricks Multi-Agent System - Main Application

A sophisticated multi-agent architecture for Databricks data engineering tasks,
powered by local LLMs (Ollama) with specialized agents for different domains.
"""

import streamlit as st
import logging
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Page configuration - MUST be first Streamlit command
st.set_page_config(
    page_title="Databricks Multi-Agent System",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Import modules
try:
    from config.settings import get_settings
    from core.message import ConversationContext
    from core.orchestrator import OrchestratorAgent
    from core.agent_registry import AgentRegistry
    from agents import initialize_agents
    from tools.databricks_tools import DatabricksTools
    from utils.llm_client import get_ollama_client
except ImportError as e:
    st.error(f"Import error: {e}")
    st.stop()


def init_session_state():
    """Initialize Streamlit session state."""
    if "context" not in st.session_state:
        st.session_state.context = ConversationContext()
    
    if "orchestrator" not in st.session_state:
        initialize_agents()
        st.session_state.orchestrator = OrchestratorAgent()
        AgentRegistry.register(st.session_state.orchestrator)
    
    if "settings" not in st.session_state:
        st.session_state.settings = get_settings()
    
    if "messages" not in st.session_state:
        st.session_state.messages = []


def render_sidebar():
    """Render the sidebar."""
    settings = st.session_state.settings
    
    with st.sidebar:
        st.title("🤖 Databricks Agent")
        st.caption("Multi-Agent System")
        
        st.subheader("📡 Connections")
        
        # Ollama status
        try:
            ollama_client = get_ollama_client()
            ollama_status = ollama_client.check_connection()
            
            if ollama_status.get("connected"):
                st.success(f"✅ Ollama: {settings.ollama.model}")
            else:
                st.error("❌ Ollama: Not connected")
                st.caption("Run `ollama serve` to start")
        except Exception as e:
            st.error(f"❌ Ollama error: {e}")
        
        # Databricks status
        try:
            databricks = DatabricksTools()
            if databricks.is_configured:
                st.success("✅ Databricks: Configured")
            else:
                st.info("ℹ️ Databricks: Not configured")
        except Exception as e:
            st.warning(f"Databricks: {e}")
        
        st.divider()
        
        # Model selection
        st.subheader("⚙️ Configuration")
        
        model = st.selectbox(
            "Ollama Model",
            ["llama3.1", "llama3.2", "codellama", "mistral"],
            index=0,
        )
        
        catalog = st.text_input("Default Catalog", value="main")
        schema = st.text_input("Default Schema", value="default")
        
        st.session_state.context.current_catalog = catalog
        st.session_state.context.current_schema = schema
        
        st.divider()
        
        # Available Agents
        st.subheader("🤖 Available Agents")
        
        agents_info = [
            ("🗂️ Schema Agent", "DDL generation, ALTER statements, type inference"),
            ("🔍 SQL Agent", "Query optimization, debugging, performance"),
            ("🔄 Pipeline Agent", "DLT, Bronze/Silver/Gold layers"),
            ("🏷️ Metadata Agent", "Tags, comments, governance"),
            ("💬 Chat Agent", "General Q&A, best practices"),
        ]
        
        for name, desc in agents_info:
            with st.expander(name):
                st.caption(desc)
        
        st.divider()
        
        # Clear button
        if st.button("🗑️ Clear Conversation", use_container_width=True):
            st.session_state.messages = []
            st.session_state.context = ConversationContext()
            st.rerun()


# =============================================================================
# TAB 1: CHAT (Routes to all agents via Orchestrator)
# =============================================================================
def render_chat_tab():
    """Render the chat interface - routes to appropriate agent."""
    st.header("💬 Chat with Databricks Agent")
    st.caption("Ask anything! The orchestrator will route to the best agent.")
    
    # Show agent routing info
    with st.expander("ℹ️ How routing works"):
        st.markdown("""
        Your question is automatically routed to the best agent:
        - **Schema questions** → Schema Agent (DDL, tables, columns)
        - **SQL questions** → SQL Agent (optimization, queries, performance)
        - **Pipeline questions** → Pipeline Agent (DLT, ETL, medallion)
        - **Metadata questions** → Metadata Agent (tags, comments, governance)
        - **General questions** → Chat Agent (explanations, best practices)
        """)
    
    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    
    # Chat input
    if prompt := st.chat_input("Ask me about Databricks..."):
        # Add user message
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Get response from orchestrator
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                try:
                    response = st.session_state.orchestrator.route_query(
                        prompt, 
                        st.session_state.context
                    )
                    result = response.result or "I couldn't process that request."
                    st.markdown(result)
                    
                    # Show which agent handled it
                    if hasattr(response, 'agent_type') and response.agent_type:
                        agent_name = response.agent_type if isinstance(response.agent_type, str) else response.agent_type.value
                        st.caption(f"_Handled by {agent_name.title()} Agent_")
                    
                    st.session_state.messages.append({"role": "assistant", "content": result})
                except Exception as e:
                    error_msg = f"Error: {str(e)}"
                    st.error(error_msg)
                    logger.error(f"Chat error: {e}", exc_info=True)


# =============================================================================
# TAB 2: SCHEMA AGENT - Create/Alter Tables
# =============================================================================
def render_schema_tab():
    """Render the Schema Agent tab - DDL and table operations."""
    st.header("🗂️ Schema Agent")
    st.caption("Create tables, generate DDL, modify schemas")
    
    sub_tabs = st.tabs(["📊 Create from CSV", "✏️ Create Manual", "🔄 Alter Table"])
    
    # --- Create from CSV ---
    with sub_tabs[0]:
        st.subheader("Create Table from CSV")
        
        uploaded_file = st.file_uploader("Upload CSV file", type=["csv", "tsv"])
        
        if uploaded_file:
            try:
                df = pd.read_csv(uploaded_file)
                st.write(f"**Preview** ({len(df)} rows, {len(df.columns)} columns)")
                st.dataframe(df.head(10), use_container_width=True)
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    table_name = st.text_input("Table Name", value="new_table", key="csv_table")
                with col2:
                    catalog = st.text_input("Catalog", value="main", key="csv_catalog")
                with col3:
                    schema = st.text_input("Schema", value="default", key="csv_schema")
                
                if st.button("🔨 Generate DDL", type="primary", key="gen_csv_ddl"):
                    # Infer types
                    type_mapping = {
                        'int64': 'BIGINT', 'int32': 'INT', 'float64': 'DOUBLE',
                        'float32': 'FLOAT', 'object': 'STRING', 'bool': 'BOOLEAN',
                        'datetime64[ns]': 'TIMESTAMP', 'datetime64': 'TIMESTAMP',
                    }
                    
                    columns = []
                    for col in df.columns:
                        dtype = str(df[col].dtype)
                        spark_type = type_mapping.get(dtype, 'STRING')
                        clean_col = col.replace(' ', '_').replace('-', '_').lower()
                        columns.append(f"    {clean_col} {spark_type}")
                    
                    ddl = f"""CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table_name} (
{','.join(columns)}
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)"""
                    
                    st.code(ddl, language="sql")
                    st.download_button("⬇️ Download DDL", ddl, f"{table_name}.sql", "text/plain")
                    
            except Exception as e:
                st.error(f"Error: {e}")
        else:
            st.info("Upload a CSV file to auto-generate DDL")
    
    # --- Create Manual ---
    with sub_tabs[1]:
        st.subheader("Create Table Manually")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            table_name = st.text_input("Table Name", value="my_table", key="manual_table")
        with col2:
            catalog = st.text_input("Catalog", value="main", key="manual_catalog")
        with col3:
            schema = st.text_input("Schema", value="default", key="manual_schema")
        
        st.write("**Define Columns:**")
        
        # Dynamic column input
        if "columns" not in st.session_state:
            st.session_state.columns = [{"name": "id", "type": "BIGINT"}]
        
        for i, col in enumerate(st.session_state.columns):
            c1, c2, c3 = st.columns([3, 2, 1])
            with c1:
                st.session_state.columns[i]["name"] = st.text_input(
                    "Column Name", value=col["name"], key=f"col_name_{i}", label_visibility="collapsed"
                )
            with c2:
                st.session_state.columns[i]["type"] = st.selectbox(
                    "Type",
                    ["BIGINT", "INT", "STRING", "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP", "DECIMAL(10,2)"],
                    index=["BIGINT", "INT", "STRING", "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP", "DECIMAL(10,2)"].index(col["type"]) if col["type"] in ["BIGINT", "INT", "STRING", "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP", "DECIMAL(10,2)"] else 0,
                    key=f"col_type_{i}",
                    label_visibility="collapsed"
                )
            with c3:
                if st.button("❌", key=f"del_col_{i}"):
                    st.session_state.columns.pop(i)
                    st.rerun()
        
        if st.button("➕ Add Column"):
            st.session_state.columns.append({"name": f"column_{len(st.session_state.columns)}", "type": "STRING"})
            st.rerun()
        
        if st.button("🔨 Generate DDL", type="primary", key="gen_manual_ddl"):
            cols_ddl = [f"    {c['name']} {c['type']}" for c in st.session_state.columns]
            ddl = f"""CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table_name} (
{','.join(cols_ddl)}
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)"""
            st.code(ddl, language="sql")
            st.download_button("⬇️ Download DDL", ddl, f"{table_name}.sql", "text/plain")
    
    # --- Alter Table ---
    with sub_tabs[2]:
        st.subheader("Alter Table")
        
        table_name = st.text_input("Full Table Name", value="catalog.schema.table", key="alter_table")
        
        operation = st.selectbox(
            "Operation",
            ["Add Column", "Drop Column", "Rename Column", "Add Comment"]
        )
        
        if operation == "Add Column":
            col1, col2 = st.columns(2)
            with col1:
                col_name = st.text_input("Column Name", key="add_col_name")
            with col2:
                col_type = st.selectbox("Type", ["STRING", "BIGINT", "INT", "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP"])
            
            if st.button("Generate ALTER", key="gen_add"):
                st.code(f"ALTER TABLE {table_name}\nADD COLUMN {col_name} {col_type};", language="sql")
        
        elif operation == "Drop Column":
            col_name = st.text_input("Column to Drop", key="drop_col_name")
            if st.button("Generate ALTER", key="gen_drop"):
                st.warning("⚠️ This will permanently delete the column!")
                st.code(f"ALTER TABLE {table_name}\nDROP COLUMN {col_name};", language="sql")
        
        elif operation == "Rename Column":
            col1, col2 = st.columns(2)
            with col1:
                old_name = st.text_input("Current Name", key="old_name")
            with col2:
                new_name = st.text_input("New Name", key="new_name")
            if st.button("Generate ALTER", key="gen_rename"):
                st.code(f"ALTER TABLE {table_name}\nRENAME COLUMN {old_name} TO {new_name};", language="sql")
        
        elif operation == "Add Comment":
            comment = st.text_area("Table Comment", key="table_comment")
            if st.button("Generate ALTER", key="gen_comment"):
                st.code(f"COMMENT ON TABLE {table_name} IS '{comment}';", language="sql")


# =============================================================================
# TAB 3: SQL AGENT - Query Optimization
# =============================================================================
def render_sql_tab():
    """Render the SQL Agent tab - optimization and debugging."""
    st.header("🔍 SQL Agent")
    st.caption("Optimize queries, debug SQL, analyze performance")
    
    sub_tabs = st.tabs(["⚡ Optimize Query", "🐛 Debug SQL", "📊 Analyze"])
    
    # --- Optimize ---
    with sub_tabs[0]:
        st.subheader("Query Optimization")
        
        query = st.text_area(
            "Paste your SQL query:",
            height=200,
            placeholder="SELECT * FROM catalog.schema.table WHERE ...",
            key="optimize_query"
        )
        
        if st.button("🚀 Optimize", type="primary") and query:
            with st.spinner("Analyzing query..."):
                try:
                    response = st.session_state.orchestrator.route_query(
                        f"Optimize this SQL query for Databricks Delta Lake:\n\n```sql\n{query}\n```",
                        st.session_state.context
                    )
                    st.markdown(response.result)
                except Exception as e:
                    st.error(f"Error: {e}")
    
    # --- Debug ---
    with sub_tabs[1]:
        st.subheader("Debug SQL")
        
        query = st.text_area(
            "Paste your SQL query:",
            height=150,
            key="debug_query"
        )
        error = st.text_area(
            "Error message (optional):",
            height=100,
            key="error_msg"
        )
        
        if st.button("🔍 Debug", type="primary") and query:
            with st.spinner("Debugging..."):
                try:
                    prompt = f"Debug this SQL query:\n\n```sql\n{query}\n```"
                    if error:
                        prompt += f"\n\nError message:\n{error}"
                    
                    response = st.session_state.orchestrator.route_query(prompt, st.session_state.context)
                    st.markdown(response.result)
                except Exception as e:
                    st.error(f"Error: {e}")
    
    # --- Analyze ---
    with sub_tabs[2]:
        st.subheader("Quick SQL Reference")
        
        st.markdown("""
        **Common Optimization Commands:**
        ```sql
        -- Optimize table (compact small files)
        OPTIMIZE catalog.schema.table;
        
        -- Optimize with Z-ORDER
        OPTIMIZE catalog.schema.table ZORDER BY (column1, column2);
        
        -- Vacuum old files
        VACUUM catalog.schema.table RETAIN 168 HOURS;
        
        -- Analyze table statistics
        ANALYZE TABLE catalog.schema.table COMPUTE STATISTICS;
        
        -- Explain query plan
        EXPLAIN FORMATTED SELECT * FROM table;
        ```
        """)


# =============================================================================
# TAB 4: PIPELINE AGENT - DLT Pipelines
# =============================================================================
def render_pipeline_tab():
    """Render the Pipeline Agent tab - DLT generation."""
    st.header("🔄 Pipeline Agent")
    st.caption("Generate Delta Live Tables pipelines")
    
    col1, col2 = st.columns(2)
    
    with col1:
        pipeline_name = st.text_input("Pipeline Name", value="my_pipeline")
        source_path = st.text_input("Source Path", value="/mnt/data/raw/")
    
    with col2:
        layer = st.selectbox(
            "Layer",
            ["Bronze (Raw)", "Silver (Cleansed)", "Gold (Aggregated)", "Full Medallion Pipeline"]
        )
        file_format = st.selectbox("Source Format", ["json", "csv", "parquet"])
    
    if st.button("🔨 Generate Pipeline", type="primary"):
        
        if "Full" in layer:
            code = f'''import dlt
from pyspark.sql.functions import *

# =============================================================================
# BRONZE LAYER - Raw Data Ingestion
# =============================================================================
@dlt.table(
    name="{pipeline_name}_bronze",
    comment="Raw data ingestion with audit columns"
)
def {pipeline_name}_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "{file_format}")
        .option("cloudFiles.schemaLocation", "/mnt/schema/{pipeline_name}")
        .load("{source_path}")
        .withColumn("_ingest_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

# =============================================================================
# SILVER LAYER - Cleansed & Validated
# =============================================================================
@dlt.table(
    name="{pipeline_name}_silver",
    comment="Cleansed and validated data"
)
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")
@dlt.expect("valid_timestamp", "_ingest_timestamp IS NOT NULL")
def {pipeline_name}_silver():
    return (
        dlt.read("{pipeline_name}_bronze")
        .dropDuplicates(["id"])
        .filter(col("id").isNotNull())
        .withColumn("_processed_timestamp", current_timestamp())
    )

# =============================================================================
# GOLD LAYER - Business Aggregations
# =============================================================================
@dlt.table(
    name="{pipeline_name}_gold",
    comment="Business-level aggregations and metrics"
)
def {pipeline_name}_gold():
    return (
        dlt.read("{pipeline_name}_silver")
        .groupBy(date_trunc("day", col("_ingest_timestamp")).alias("date"))
        .agg(
            count("*").alias("record_count"),
            countDistinct("id").alias("unique_ids")
        )
        .orderBy("date")
    )
'''
        elif "Bronze" in layer:
            code = f'''import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="{pipeline_name}_bronze",
    comment="Raw data ingestion layer"
)
def {pipeline_name}_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "{file_format}")
        .option("cloudFiles.schemaLocation", "/mnt/schema/{pipeline_name}")
        .load("{source_path}")
        .withColumn("_ingest_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )
'''
        elif "Silver" in layer:
            code = f'''import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="{pipeline_name}_silver",
    comment="Cleansed and validated data"
)
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")
@dlt.expect("valid_data", "value IS NOT NULL")
def {pipeline_name}_silver():
    return (
        dlt.read("{pipeline_name}_bronze")
        .dropDuplicates(["id"])
        .filter(col("id").isNotNull())
        .withColumn("_processed_timestamp", current_timestamp())
    )
'''
        else:  # Gold
            code = f'''import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="{pipeline_name}_gold",
    comment="Business aggregations"
)
def {pipeline_name}_gold():
    return (
        dlt.read("{pipeline_name}_silver")
        .groupBy("category")
        .agg(
            count("*").alias("total_count"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount")
        )
    )
'''
        
        st.code(code, language="python")
        st.download_button("⬇️ Download Pipeline", code, f"{pipeline_name}_pipeline.py", "text/plain")


# =============================================================================
# TAB 5: METADATA AGENT - Tags, Comments, Governance
# =============================================================================
def render_metadata_tab():
    """Render the Metadata Agent tab - governance and documentation."""
    st.header("🏷️ Metadata Agent")
    st.caption("Manage tags, comments, and data governance")
    
    table_name = st.text_input("Table Name", value="catalog.schema.table", key="meta_table")
    
    sub_tabs = st.tabs(["💬 Comments", "🏷️ Tags", "⚙️ Properties", "🔒 PII Detection"])
    
    # --- Comments ---
    with sub_tabs[0]:
        st.subheader("Table & Column Comments")
        
        table_comment = st.text_area("Table Comment", placeholder="Description of the table...")
        
        if st.button("Generate Comment SQL", key="gen_tbl_comment"):
            sql = f"COMMENT ON TABLE {table_name} IS '{table_comment}';"
            st.code(sql, language="sql")
        
        st.divider()
        
        st.write("**Column Comments:**")
        col_name = st.text_input("Column Name", key="comment_col")
        col_comment = st.text_input("Comment", key="comment_text")
        
        if st.button("Generate Column Comment", key="gen_col_comment"):
            sql = f"ALTER TABLE {table_name} ALTER COLUMN {col_name} COMMENT '{col_comment}';"
            st.code(sql, language="sql")
    
    # --- Tags ---
    with sub_tabs[1]:
        st.subheader("Table Tags")
        
        # Preset tags
        st.write("**Quick Tags:**")
        col1, col2, col3, col4 = st.columns(4)
        
        tags = {}
        with col1:
            if st.checkbox("PII"):
                tags["pii"] = "true"
        with col2:
            if st.checkbox("Confidential"):
                tags["classification"] = "confidential"
        with col3:
            if st.checkbox("GDPR"):
                tags["gdpr"] = "subject"
        with col4:
            if st.checkbox("Production"):
                tags["environment"] = "production"
        
        # Custom tags
        st.write("**Custom Tags:**")
        custom_tags = st.text_area(
            "One per line (key=value):",
            placeholder="owner=data_team\ndomain=sales",
            key="custom_tags"
        )
        
        if custom_tags:
            for line in custom_tags.strip().split("\n"):
                if "=" in line:
                    k, v = line.split("=", 1)
                    tags[k.strip()] = v.strip()
        
        if st.button("Generate Tag SQL", key="gen_tags") and tags:
            tag_str = ", ".join([f"'{k}' = '{v}'" for k, v in tags.items()])
            sql = f"ALTER TABLE {table_name} SET TAGS ({tag_str});"
            st.code(sql, language="sql")
    
    # --- Properties ---
    with sub_tabs[2]:
        st.subheader("Table Properties")
        
        col1, col2 = st.columns(2)
        with col1:
            auto_optimize = st.checkbox("Auto Optimize Write", value=True)
            auto_compact = st.checkbox("Auto Compact", value=True)
        with col2:
            log_retention = st.number_input("Log Retention (days)", value=30, min_value=1)
            file_retention = st.number_input("Deleted File Retention (days)", value=7, min_value=1)
        
        if st.button("Generate Properties SQL", key="gen_props"):
            props = []
            if auto_optimize:
                props.append("'delta.autoOptimize.optimizeWrite' = 'true'")
            if auto_compact:
                props.append("'delta.autoOptimize.autoCompact' = 'true'")
            props.append(f"'delta.logRetentionDuration' = '{log_retention} days'")
            props.append(f"'delta.deletedFileRetentionDuration' = '{file_retention} days'")
            
            sql = f"ALTER TABLE {table_name} SET TBLPROPERTIES (\n    " + ",\n    ".join(props) + "\n);"
            st.code(sql, language="sql")
    
    # --- PII Detection ---
    with sub_tabs[3]:
        st.subheader("PII Detection Helper")
        
        st.markdown("""
        **Common PII Column Patterns:**
        
        | Risk Level | Column Names | Recommended Tags |
        |------------|--------------|------------------|
        | 🔴 High | ssn, social_security, credit_card, bank_account | `pii=true`, `pii_level=high` |
        | 🟠 Medium | email, phone, address, date_of_birth, salary | `pii=true`, `pii_level=medium` |
        | 🟡 Low | first_name, last_name, username, ip_address | `pii=true`, `pii_level=low` |
        """)
        
        col_to_tag = st.text_input("Column to tag as PII", key="pii_col")
        pii_level = st.selectbox("PII Level", ["high", "medium", "low"], key="pii_level")
        
        if st.button("Generate PII Tags", key="gen_pii"):
            sql = f"""ALTER TABLE {table_name} 
ALTER COLUMN {col_to_tag} SET TAGS (
    'pii' = 'true',
    'pii_level' = '{pii_level}',
    'requires_masking' = '{"true" if pii_level in ["high", "medium"] else "false"}'
);"""
            st.code(sql, language="sql")


# =============================================================================
# MAIN APPLICATION
# =============================================================================
def main():
    """Main application entry point."""
    # Initialize
    init_session_state()
    
    # Sidebar
    render_sidebar()
    
    # Main content
    st.title("🏗️ Databricks Multi-Agent System")
    st.caption("Intelligent assistance for data engineering tasks")
    
    # Metrics row
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Messages", len(st.session_state.messages))
    with col2:
        st.metric("Agents", "5 Active")
    with col3:
        st.metric("Catalog", st.session_state.context.current_catalog or "main")
    with col4:
        st.metric("Schema", st.session_state.context.current_schema or "default")
    
    st.divider()
    
    # Main tabs - ALL 5 AGENTS
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "💬 Chat",
        "🗂️ Schema Agent", 
        "🔍 SQL Agent",
        "🔄 Pipeline Agent",
        "🏷️ Metadata Agent"
    ])
    
    with tab1:
        render_chat_tab()
    
    with tab2:
        render_schema_tab()
    
    with tab3:
        render_sql_tab()
    
    with tab4:
        render_pipeline_tab()
    
    with tab5:
        render_metadata_tab()


if __name__ == "__main__":
    main()
