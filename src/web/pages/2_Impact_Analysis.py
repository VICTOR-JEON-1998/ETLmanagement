import streamlit as st
from src.web.utils import get_dependency_analyzer, get_graph_config, convert_to_agraph
from src.datastage.impact_tracer import ImpactTracer
from streamlit_agraph import agraph, Node, Edge, Config
import pandas as pd

st.set_page_config(page_title="Impact Analysis", page_icon="🕸️", layout="wide")

st.title("🕸️ Impact Analysis")

if "export_dir" not in st.session_state or not st.session_state["export_dir"]:
    st.warning("Please configure the Export Directory in the sidebar first.")
    st.stop()

export_dir = st.session_state["export_dir"]
analyzer = get_dependency_analyzer(export_dir)

# Ensure graph is built
if not hasattr(analyzer, 'graph') or analyzer.graph is None:
    with st.spinner("Building Dependency Graph..."):
        analyzer.build_dependency_graph()

# Input Section
analysis_type = st.radio("Analysis Type", ["Table/Job Dependency", "Column Impact Analysis", "Cascading Impact Analysis"], horizontal=True)

if analysis_type == "Table/Job Dependency":
    col1, col2 = st.columns([2, 1])
    with col1:
        # Autocomplete for tables/jobs
        all_nodes = list(analyzer.graph.job_to_sources.keys()) + \
                    list(set(t for tables in analyzer.graph.job_to_sources.values() for t in tables)) + \
                    list(set(t for tables in analyzer.graph.job_to_targets.values() for t in tables))
        all_nodes = sorted(list(set(all_nodes)))
        
        selected_node = st.selectbox("Select Table or Job", all_nodes, index=None, placeholder="Type to search...")

    with col2:
        max_level = st.slider("Impact Level", min_value=1, max_value=5, value=2)
        direction = st.selectbox("Direction", ["all", "upstream", "downstream"], index=2) # Default downstream

    if selected_node:
        st.subheader(f"Impact Graph for: {selected_node}")
        
        with st.spinner("Generating Graph..."):
            nodes, edges = convert_to_agraph(analyzer.graph, selected_node, max_level, direction)
            
            config = get_graph_config()
            
            # Render Graph
            return_value = agraph(nodes=nodes, edges=edges, config=config)
            
            # Display stats
            st.info(f"Nodes: {len(nodes)}, Edges: {len(edges)}")
            
            # List affected items
            if nodes:
                st.markdown("### Affected Items")
                affected_jobs = [n.label for n in nodes if n.color == "#3357FF"] # Blue for Jobs
                affected_tables = [n.label for n in nodes if n.color == "#33FF57"] # Green for Tables
                
                c1, c2 = st.columns(2)
                with c1:
                    st.write(f"**Jobs ({len(affected_jobs)})**")
                    st.write(affected_jobs)
                with c2:
                    st.write(f"**Tables ({len(affected_tables)})**")
                    st.write(affected_tables)

elif analysis_type == "Column Impact Analysis":
    st.info("Enter a column name to find which Tables contain it and which Jobs use those Tables.")
    column_name = st.text_input("Column Name", placeholder="e.g., CUST_ID")
    
    if column_name:
        with st.spinner(f"Searching for column '{column_name}'..."):
            # Use the backend to find tables and jobs
            results = analyzer.find_tables_using_column(column_name)
            
            if not results:
                st.warning(f"No tables found containing column '{column_name}'")
            else:
                st.success(f"Found {len(results)} tables containing '{column_name}'")
                
                # Display results
                for item in results:
                    full_name = item.get("full_name", "Unknown")
                    jobs = item.get("related_jobs", [])
                    
                    with st.expander(f"Table: {full_name} (Used by {len(jobs)} Jobs)"):
                        st.write(f"**Schema:** {item.get('schema', '')}")
                        st.write(f"**Table:** {item.get('table_name', '')}")
                        
                        if jobs:
                            st.markdown("#### Affected Jobs:")
                            job_df = []
                            for job in jobs:
                                job_df.append({"Job Name": job.get("job_name"), "File": job.get("file_path")})
                            st.table(job_df)
                        else:
                            st.info("No Jobs found using this table.")

elif analysis_type == "Cascading Impact Analysis":
    st.info("Trace the impact of a column change across the entire ETL chain (Table -> Job -> Table -> Job...).")
    column_name = st.text_input("Column Name", placeholder="e.g., SHOP_CD")
    max_depth = st.slider("Max Depth", 1, 5, 3)
    
    if st.button("Trace Impact"):
        if not column_name:
            st.warning("Please enter a column name.")
        else:
            with st.spinner(f"Tracing impact for '{column_name}'..."):
                tracer = ImpactTracer(analyzer)
                result = tracer.trace_impact(column_name, max_depth)
                
                impact_chain = result.get("impact_chain", [])
                initial_tables = result.get("initial_tables", [])
                
                if not impact_chain and not initial_tables:
                    st.warning("No impact found.")
                else:
                    st.success(f"Found {len(impact_chain)} cascading impact steps.")
                    
                    st.markdown("### Initial Source Tables")
                    st.write(initial_tables)
                    
                    if impact_chain:
                        st.markdown("### Impact Chain")
                        
                        # Visualize as a graph
                        nodes = []
                        edges = []
                        node_ids = set()
                        
                        # Add initial tables
                        for t in initial_tables:
                            if t not in node_ids:
                                nodes.append(Node(id=t, label=t, size=20, color="#33FF57")) # Green
                                node_ids.add(t)
                        
                        for step in impact_chain:
                            src = step['source_table']
                            job = step['job']
                            tgt = step['target_table']
                            level = step['level']
                            
                            if src not in node_ids:
                                nodes.append(Node(id=src, label=src, size=20, color="#33FF57"))
                                node_ids.add(src)
                            
                            if job not in node_ids:
                                nodes.append(Node(id=job, label=job, size=25, color="#3357FF")) # Blue
                                node_ids.add(job)
                                
                            if tgt not in node_ids:
                                nodes.append(Node(id=tgt, label=tgt, size=20, color="#FF5733")) # Red (Target)
                                node_ids.add(tgt)
                            
                            edges.append(Edge(source=src, target=job, label=f"L{level}"))
                            edges.append(Edge(source=job, target=tgt, label=f"L{level}"))
                        
                        config = Config(width=1000, height=600, directed=True, physics=True, hierarchical=False)
                        agraph(nodes=nodes, edges=edges, config=config)
                        
                        # Table View
                        st.markdown("### Detailed Steps")
                        df = pd.DataFrame(impact_chain)
                        st.dataframe(df)
