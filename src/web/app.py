import streamlit as st
from pathlib import Path
import sys
import os

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.core.config import get_config

st.set_page_config(
    page_title="DataStage Impact Analyzer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("📊 DataStage Impact Analyzer")

# Sidebar Configuration
st.sidebar.header("Configuration")

# Export Directory Selection
if "export_dir" not in st.session_state:
    # Default to 'Datastage export jobs' in current directory or config
    default_dir = Path("Datastage export jobs")
    if default_dir.exists():
        st.session_state["export_dir"] = str(default_dir.absolute())
    else:
        st.session_state["export_dir"] = ""

export_dir = st.sidebar.text_input(
    "DSX Export Directory", 
    value=st.session_state["export_dir"],
    help="Path to the directory containing .dsx files"
)

if export_dir:
    st.session_state["export_dir"] = export_dir
    if not Path(export_dir).exists():
        st.sidebar.error("Directory does not exist!")
    else:
        st.sidebar.success("Directory found!")

st.sidebar.markdown("---")
st.sidebar.info(
    """
    **DataStage Impact Analyzer**
    
    Analyze dependencies between Jobs and Tables in IBM DataStage DSX files.
    """
)
