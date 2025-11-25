"""
Custom CSS styles for the Streamlit UI.
"""


def get_custom_css() -> str:
    """
    Get custom CSS for styling the application.
    
    Returns:
        CSS string
    """
    return """
    <style>
    /* Main container */
    .main {
        padding: 1rem;
    }
    
    /* Header styling */
    h1 {
        color: #FF6B00;
        font-weight: 700;
    }
    
    h2, h3 {
        color: #1E3A5F;
    }
    
    /* Card-like containers */
    .stExpander {
        background-color: #f8f9fa;
        border-radius: 8px;
        border: 1px solid #e9ecef;
    }
    
    /* Button styling */
    .stButton > button {
        border-radius: 6px;
        font-weight: 500;
        transition: all 0.2s;
    }
    
    .stButton > button:hover {
        transform: translateY(-1px);
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    /* Primary button */
    .stButton > button[kind="primary"] {
        background-color: #FF6B00;
        border-color: #FF6B00;
    }
    
    .stButton > button[kind="primary"]:hover {
        background-color: #E65C00;
        border-color: #E65C00;
    }
    
    /* Input fields */
    .stTextInput > div > div > input {
        border-radius: 6px;
    }
    
    .stTextArea > div > div > textarea {
        border-radius: 6px;
    }
    
    /* Code blocks */
    .stCodeBlock {
        border-radius: 8px;
        border: 1px solid #e9ecef;
    }
    
    /* Dataframe styling */
    .stDataFrame {
        border-radius: 8px;
        overflow: hidden;
    }
    
    /* Chat messages */
    .stChatMessage {
        padding: 1rem;
        margin-bottom: 0.5rem;
        border-radius: 12px;
    }
    
    /* Success/Error/Warning messages */
    .stSuccess {
        background-color: #d4edda;
        border-color: #c3e6cb;
        border-radius: 6px;
    }
    
    .stError {
        background-color: #f8d7da;
        border-color: #f5c6cb;
        border-radius: 6px;
    }
    
    .stWarning {
        background-color: #fff3cd;
        border-color: #ffeeba;
        border-radius: 6px;
    }
    
    /* Sidebar */
    .css-1d391kg {
        background-color: #f8f9fa;
    }
    
    /* Metrics */
    .stMetric {
        background-color: #ffffff;
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid #e9ecef;
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 6px 6px 0 0;
        padding: 8px 16px;
    }
    
    /* Agent status indicators */
    .agent-status {
        display: inline-flex;
        align-items: center;
        padding: 4px 8px;
        border-radius: 4px;
        font-size: 0.85rem;
        margin-right: 8px;
    }
    
    .agent-status.active {
        background-color: #d4edda;
        color: #155724;
    }
    
    .agent-status.inactive {
        background-color: #f8d7da;
        color: #721c24;
    }
    
    /* Loading spinner */
    .stSpinner > div {
        border-color: #FF6B00;
    }
    
    /* File uploader */
    .stFileUploader {
        border: 2px dashed #dee2e6;
        border-radius: 8px;
        padding: 1rem;
    }
    
    .stFileUploader:hover {
        border-color: #FF6B00;
    }
    
    /* Tooltips */
    .stTooltipIcon {
        color: #6c757d;
    }
    
    /* Progress bar */
    .stProgress > div > div > div {
        background-color: #FF6B00;
    }
    
    /* Select boxes */
    .stSelectbox > div > div {
        border-radius: 6px;
    }
    
    /* Multi-select */
    .stMultiSelect > div > div {
        border-radius: 6px;
    }
    
    /* Checkbox */
    .stCheckbox > label > span {
        font-weight: 400;
    }
    
    /* Radio buttons */
    .stRadio > div {
        gap: 0.5rem;
    }
    
    /* Slider */
    .stSlider > div > div > div {
        background-color: #FF6B00;
    }
    
    /* JSON display */
    .stJson {
        border-radius: 8px;
        border: 1px solid #e9ecef;
    }
    
    /* Download button */
    .stDownloadButton > button {
        background-color: #28a745;
        border-color: #28a745;
    }
    
    .stDownloadButton > button:hover {
        background-color: #218838;
        border-color: #1e7e34;
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Custom scrollbar */
    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }
    
    ::-webkit-scrollbar-track {
        background: #f1f1f1;
        border-radius: 4px;
    }
    
    ::-webkit-scrollbar-thumb {
        background: #c1c1c1;
        border-radius: 4px;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: #a1a1a1;
    }
    </style>
    """


def apply_custom_css() -> None:
    """Apply custom CSS to the Streamlit app."""
    import streamlit as st
    st.markdown(get_custom_css(), unsafe_allow_html=True)
