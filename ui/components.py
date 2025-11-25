"""
Reusable Streamlit UI components.
"""

import streamlit as st
from typing import Any, Dict, List, Optional
import pandas as pd


def render_connection_status(is_connected: bool, details: Optional[str] = None):
    """Render connection status indicator."""
    if is_connected:
        st.success(f"✅ Connected{f': {details}' if details else ''}")
    else:
        st.error(f"❌ Not connected{f': {details}' if details else ''}")


def render_agent_selector(agents: List[Dict[str, str]], current: Optional[str] = None) -> Optional[str]:
    """Render agent selection dropdown."""
    options = ["Auto (Orchestrator)"] + [a["name"] for a in agents]
    
    selected = st.selectbox(
        "Route to Agent",
        options,
        index=0,
        help="Select 'Auto' to let the orchestrator decide, or choose a specific agent"
    )
    
    if selected == "Auto (Orchestrator)":
        return None
    
    for agent in agents:
        if agent["name"] == selected:
            return agent["type"]
    
    return None


def render_code_block(code: str, language: str = "sql", show_copy: bool = True):
    """Render a code block with optional copy button."""
    st.code(code, language=language)


def render_data_preview(df: pd.DataFrame, max_rows: int = 10):
    """Render a preview of DataFrame data."""
    st.write(f"**Preview** ({len(df)} rows, {len(df.columns)} columns)")
    st.dataframe(df.head(max_rows), use_container_width=True)
    
    if len(df) > max_rows:
        st.caption(f"Showing first {max_rows} of {len(df)} rows")


def render_schema_display(schema: Dict[str, str]):
    """Render schema information."""
    st.write("**Schema:**")
    schema_df = pd.DataFrame([
        {"Column": k, "Type": v} for k, v in schema.items()
    ])
    st.dataframe(schema_df, use_container_width=True, hide_index=True)


def render_file_uploader(
    label: str = "Upload CSV file",
    accepted_types: List[str] = None,
    key: Optional[str] = None,
) -> Optional[Any]:
    """Render file uploader with validation."""
    if accepted_types is None:
        accepted_types = ["csv"]
    return st.file_uploader(
        label,
        type=accepted_types,
        key=key,
        help=f"Supported formats: {', '.join(accepted_types)}"
    )


def render_validation_results(validation: Dict[str, Any]):
    """Render validation results."""
    if validation.get("is_valid"):
        st.success("✅ Validation passed")
    else:
        st.error("❌ Validation failed")
    
    if validation.get("errors"):
        st.write("**Errors:**")
        for error in validation["errors"]:
            st.error(f"• {error}")
    
    if validation.get("warnings"):
        st.write("**Warnings:**")
        for warning in validation["warnings"]:
            st.warning(f"• {warning}")


def render_chat_message(role: str, content: str, avatar: Optional[str] = None):
    """Render a chat message."""
    if avatar is None:
        avatar = "👤" if role == "user" else "🤖"
    
    with st.chat_message(role, avatar=avatar):
        st.markdown(content)


def render_expandable_section(title: str, content: str, expanded: bool = False):
    """Render an expandable section."""
    with st.expander(title, expanded=expanded):
        st.markdown(content)


def render_metric_cards(metrics: Dict[str, Any]):
    """Render metric cards in columns."""
    cols = st.columns(len(metrics))
    
    for col, (name, value) in zip(cols, metrics.items()):
        with col:
            st.metric(label=name, value=value)


def render_download_button(
    data: str,
    filename: str,
    label: str = "Download",
    mime_type: str = "text/plain",
):
    """Render a download button."""
    st.download_button(
        label=f"⬇️ {label}",
        data=data,
        file_name=filename,
        mime=mime_type,
    )
