"""
Databricks Multi-Agent System - Main Application

A sophisticated multi-agent architecture for Databricks data engineering tasks,
powered by local LLMs (Ollama) with specialized agents for different domains.
"""

import streamlit as st
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="Databricks Multi-Agent System",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Import components
from config.settings import get_settings, reload_settings
from core.message import ConversationContext, AgentType
from core.orchestrator import OrchestratorAgent
from core.agent_registry import AgentRegistry
from agents import initialize_agents
from tools.databricks_tools import DatabricksTools
from utils.llm_client import get_ollama_client
from ui.components import render_connection_status, render_metric_cards
from ui.tabs import (
    render_chat_tab,
    render_create_table_tab,
    render_alter_table_tab,
    render_metadata_tab,
    render_pipeline_tab,
)


def init_session_state():
    """Initialize Streamlit session state."""
    if "context" not in st.session_state:
        st.session_state.context = ConversationContext()
    
    if "orchestrator" not in st.session_state:
        # Initialize agents
        initialize_agents()
        st.session_state.orchestrator = OrchestratorAgent()
        AgentRegistry.register(st.session_state.orchestrator)
    
    if "settings" not in st.session_state:
        st.session_state.settings = get_settings()


def render_sidebar():
    """Render the sidebar with configuration options."""
    settings = st.session_state.settings
    
    with st.sidebar:
        st.title("🤖 Databricks Agent")
        st.caption("Multi-Agent System")
        
        # Connection status
        st.subheader("📡 Connections")
        
        # Ollama status
        ollama_client = get_ollama_client()
        ollama_status = ollama_client.check_connection()
        
        if ollama_status["connected"]:
            st.success(f"✅ Ollama: {settings.ollama.model}")
        else:
            st.error("❌ Ollama: Not connected")
            st.caption("Run `ollama serve` to start")
        
        # Databricks status
        databricks = DatabricksTools()
        if databricks.is_configured:
            db_status = databricks.test_connection()
            if db_status["success"]:
                st.success("✅ Databricks: Connected")
            else:
                st.warning("⚠️ Databricks: Auth configured but connection failed")
        else:
            st.info("ℹ️ Databricks: Not configured")
            st.caption("Add credentials to .env file")
        
        st.divider()
        
        # Model selection
        st.subheader("⚙️ Configuration")
        
        model = st.selectbox(
            "Ollama Model",
            ["llama3.1", "llama3.2", "codellama", "mistral"],
            index=0,
            help="Select the LLM model to use"
        )
        
        if model != settings.ollama.model:
            settings.ollama.model = model
            st.rerun()
        
        # Context settings
        catalog = st.text_input(
            "Default Catalog",
            value=settings.databricks.catalog,
        )
        schema = st.text_input(
            "Default Schema",
            value=settings.databricks.schema_name,
        )
        
        # Update context
        st.session_state.context.current_catalog = catalog
        st.session_state.context.current_schema = schema
        
        st.divider()
        
        # Agent info
        st.subheader("🤖 Available Agents")
        
        agents = AgentRegistry.list_agents()
        for agent in agents:
            if agent["type"] != "orchestrator":
                with st.expander(f"**{agent['name']}**"):
                    st.caption(agent["description"])
        
        st.divider()
        
        # Clear conversation
        if st.button("🗑️ Clear Conversation", use_container_width=True):
            st.session_state.context = ConversationContext()
            st.session_state.context.current_catalog = catalog
            st.session_state.context.current_schema = schema
            st.rerun()
        
        # About
        st.divider()
        st.caption("Built with ❤️ using Streamlit & Ollama")
        st.caption("[Documentation](https://github.com)")


def render_main_content():
    """Render the main content area."""
    # Header
    st.title("🏗️ Databricks Multi-Agent System")
    st.caption("Intelligent assistance for data engineering tasks")
    
    # Quick stats
    context = st.session_state.context
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Messages", len(context.messages))
    with col2:
        st.metric("Current Table", context.current_table or "None")
    with col3:
        st.metric("Catalog", context.current_catalog or "main")
    with col4:
        st.metric("Schema", context.current_schema or "default")
    
    st.divider()
    
    # Main tabs
    tabs = st.tabs([
        "💬 Chat",
        "📊 Create Table",
        "🔄 Alter Table",
        "📝 Metadata",
        "⚙️ Pipelines"
    ])
    
    with tabs[0]:
        render_chat_tab(
            st.session_state.orchestrator,
            st.session_state.context
        )
    
    with tabs[1]:
        render_create_table_tab(st.session_state.context)
    
    with tabs[2]:
        render_alter_table_tab(st.session_state.context)
    
    with tabs[3]:
        render_metadata_tab(st.session_state.context)
    
    with tabs[4]:
        render_pipeline_tab(st.session_state.context)


def main():
    """Main application entry point."""
    # Initialize session state
    init_session_state()
    
    # Render sidebar
    render_sidebar()
    
    # Render main content
    render_main_content()


if __name__ == "__main__":
    main()
