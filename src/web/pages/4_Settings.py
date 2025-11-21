import streamlit as st
from src.web.utils import get_dependency_analyzer

st.header("Settings")

if "export_dir" not in st.session_state or not st.session_state["export_dir"]:
    st.warning("Please configure the Export Directory in the sidebar first.")
    st.stop()

export_dir = st.session_state["export_dir"]
analyzer = get_dependency_analyzer(export_dir)

st.subheader("Cache Management")

col1, col2 = st.columns(2)

with col1:
    st.info(f"Current Export Directory: {export_dir}")

with col2:
    if st.button("Rebuild Cache"):
        with st.spinner("Rebuilding cache... This may take a while."):
            stats = analyzer.build_cache_index(force_rebuild=True)
            analyzer.build_dependency_graph() # Rebuild graph as well
            st.success("Cache rebuilt successfully!")
            st.json(stats)

st.divider()

st.subheader("About")
st.markdown("""
**DataStage Impact Analyzer** v1.0

This tool helps you analyze dependencies in IBM DataStage projects.
- **Dashboard**: View project statistics.
- **Impact Analysis**: Visualize column/table dependencies.
- **DSX Detail**: Inspect Job internals.
""")
