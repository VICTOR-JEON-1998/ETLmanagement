import streamlit as st
import pandas as pd
from src.web.utils import get_dependency_analyzer

st.header("Dashboard")

if "export_dir" not in st.session_state or not st.session_state["export_dir"]:
    st.warning("Please configure the Export Directory in the sidebar first.")
    st.stop()

export_dir = st.session_state["export_dir"]

with st.spinner("Analyzing dependencies..."):
    analyzer = get_dependency_analyzer(export_dir)
    # Ensure graph is built
    if not hasattr(analyzer, 'graph') or analyzer.graph is None:
        analyzer.build_dependency_graph()
    
    stats = analyzer.graph.get_statistics()

# Display Metrics
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Total Jobs", stats.get("total_jobs", 0))

with col2:
    st.metric("Total Tables", stats.get("total_tables", 0))

with col3:
    st.metric("Total Edges", stats.get("total_edges", 0))

st.divider()

# Top Connected Tables (if available in stats or calculated here)
st.subheader("Top Connected Tables")
# We might need to calculate this if get_statistics doesn't provide it detailed enough
# For now, let's assume we can get it from the graph object directly if needed
# But accessing analyzer.graph.job_to_sources directly is better

# Calculate table usage counts
table_usage = {}
for job, sources in analyzer.graph.job_to_sources.items():
    for source in sources:
        table_usage[source] = table_usage.get(source, 0) + 1

for job, targets in analyzer.graph.job_to_targets.items():
    for target in targets:
        table_usage[target] = table_usage.get(target, 0) + 1

if table_usage:
    df_usage = pd.DataFrame(list(table_usage.items()), columns=["Table", "Connections"])
    df_usage = df_usage.sort_values("Connections", ascending=False).head(10)
    st.bar_chart(df_usage.set_index("Table"))
else:
    st.info("No table connections found.")
