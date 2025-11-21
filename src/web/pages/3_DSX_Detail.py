import streamlit as st
from pathlib import Path
import re
from src.web.utils import get_dependency_analyzer
from src.datastage.dsx_parser import DSXParser

st.header("DSX Detail Viewer")

if "export_dir" not in st.session_state or not st.session_state["export_dir"]:
    st.warning("Please configure the Export Directory in the sidebar first.")
    st.stop()

export_dir = st.session_state["export_dir"]
analyzer = get_dependency_analyzer(export_dir)

# Ensure index is built
if not analyzer.job_index:
    analyzer.build_cache_index()

# 1. Select Job
# Get all cached jobs
cached_jobs = analyzer.job_index.get_all_cached_jobs()
job_names = sorted([j.get("job_name") for j in cached_jobs if j.get("job_name")])

selected_job_name = st.selectbox("Select Job", job_names, index=None, placeholder="Type to search...")

if selected_job_name:
    # Find metadata for selected job
    job_metadata = next((j for j in cached_jobs if j.get("job_name") == selected_job_name), None)
    
    if job_metadata:
        file_path = job_metadata.get("file_path")
        st.info(f"File: {file_path}")
        
        # 2. Read and Parse Job Content
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
            
            # Extract specific job content if multiple jobs in file
            dsjob_pattern = rf'BEGIN DSJOB\s+Identifier "{re.escape(selected_job_name)}"(.*?)END DSJOB'
            match = re.search(dsjob_pattern, content, re.DOTALL)
            
            if match:
                job_content = match.group(0) # Include BEGIN/END
                inner_content = match.group(1)
            else:
                # Fallback: might be the only job or name mismatch in Identifier
                # Try to find by Name inside DSJOB
                # For now, just show full content if it's a single job file
                job_content = content
                inner_content = content

            # Tabs for different views
            tab1, tab2, tab3 = st.tabs(["Overview", "Stages & Properties", "Raw DSX"])
            
            with tab1:
                st.subheader("Job Overview")
                st.json({
                    "Job Name": selected_job_name,
                    "File Path": file_path,
                    "Source Tables": job_metadata.get("source_tables", []),
                    "Target Tables": job_metadata.get("target_tables", [])
                })
                
            with tab2:
                st.subheader("Stages")
                # Parse stages using DSXParser logic (simplified here or use parser)
                parser = DSXParser()
                # We can reuse _extract_stages if we access it, or just parse manually
                # Let's use regex to find stages
                stage_pattern = r'BEGIN DSRECORD\s+Identifier "([^"]+)"\s+DateModified ".*?"\s+Name "([^"]+)"\s+Type "([^"]+)"'
                # Note: The pattern might vary. Let's look for "Type" which indicates Stage type (e.g., "CustomInput", "ODBCConnector")
                # Actually, DSXParser._extract_stages is better but it's internal.
                # Let's just list what we can find.
                
                stages = []
                for m in re.finditer(r'BEGIN DSRECORD\s+Identifier "([^"]+)"', job_content):
                    sid = m.group(1)
                    # Extract body
                    s_start = m.start()
                    s_end = job_content.find("END DSRECORD", s_start)
                    if s_end != -1:
                        s_body = job_content[s_start:s_end]
                        name_match = re.search(r'Name "([^"]+)"', s_body)
                        type_match = re.search(r'OLEType "([^"]+)"', s_body)
                        
                        if name_match:
                            s_name = name_match.group(1)
                            s_type = type_match.group(1) if type_match else "Unknown"
                            
                            # Filter for Stages (usually CContainerView, CCustomInput, etc. or just check if it has OLEType)
                            if s_type and "Stage" in s_type or "Connector" in s_type or "Input" in s_type or "Output" in s_type:
                                stages.append({"Name": s_name, "Type": s_type, "ID": sid, "Body": s_body})

                for stage in stages:
                    with st.expander(f"{stage['Name']} ({stage['Type']})"):
                        st.text(stage['Body'][:500] + "..." if len(stage['Body']) > 500 else stage['Body'])
                        
                        # Try to extract SQL
                        sql_match = re.search(r'SQL "(.*?)"', stage['Body'], re.DOTALL)
                        if sql_match:
                            st.markdown("**SQL Query:**")
                            st.code(sql_match.group(1), language="sql")

            with tab3:
                st.subheader("Raw DSX Content")
                st.text_area("Content", job_content, height=600)

        except Exception as e:
            st.error(f"Error reading file: {e}")
    else:
        st.error("Job metadata not found.")
