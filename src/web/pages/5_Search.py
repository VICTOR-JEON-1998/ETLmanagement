import streamlit as st
import pandas as pd
from src.web.utils import get_dependency_analyzer

st.header("Search & Data Dictionary")

if "export_dir" not in st.session_state or not st.session_state["export_dir"]:
    st.warning("Please configure the Export Directory in the sidebar first.")
    st.stop()

export_dir = st.session_state["export_dir"]
analyzer = get_dependency_analyzer(export_dir)

# Ensure index is built
if not analyzer.job_index:
    analyzer.build_cache_index()

tab1, tab2 = st.tabs(["Search Tables", "Search Columns"])

with tab1:
    st.subheader("Table Search")
    
    # Get all tables from cache stats or iterate
    # This might be heavy if there are many tables, so let's use the cache stats if possible
    # or just build a list from metadata
    
    with st.spinner("Loading table list..."):
        all_tables = set()
        for job in analyzer.job_index.get_all_cached_jobs():
            for table in job.get("tables", []):
                if table.get("full_name"):
                    all_tables.add(table.get("full_name"))
        
        all_tables = sorted(list(all_tables))
    
    search_term = st.text_input("Search Table Name", placeholder="e.g., TB_CUST_MST")
    
    if search_term:
        filtered_tables = [t for t in all_tables if search_term.upper() in t.upper()]
        
        if filtered_tables:
            st.success(f"Found {len(filtered_tables)} tables.")
            
            selected_table = st.selectbox("Select Table to View Details", filtered_tables)
            
            if selected_table:
                # Find usage
                table_name_only = selected_table.split(".")[-1] if "." in selected_table else selected_table
                schema_only = selected_table.split(".")[0] if "." in selected_table else None
                
                jobs_using = analyzer.find_jobs_using_table(table_name_only, schema_only)
                
                st.markdown("### Table Details")
                st.write(f"**Full Name:** {selected_table}")
                st.write(f"**Schema:** {schema_only if schema_only else 'N/A'}")
                st.write(f"**Table Name:** {table_name_only}")
                
                st.markdown("### Used In Jobs")
                if jobs_using:
                    df_jobs = pd.DataFrame([
                        {"Job Name": j.get("job_name"), "File": j.get("file_path")} 
                        for j in jobs_using
                    ])
                    st.dataframe(df_jobs, use_container_width=True)
                else:
                    st.info("No jobs found using this table.")
                
                # Show Columns if available (need to find a job that uses it and extract columns)
                # Or use analyzer.find_jobs_using_table result which contains 'all_tables' but maybe not columns
                # We can try to find columns from the first job that uses it
                
                st.markdown("### Columns (Sample)")
                found_columns = False
                for job in jobs_using:
                    # Check if we have column info in this job
                    # The find_jobs_using_table returns 'all_tables' but not 'columns' directly in the list item
                    # But we can fetch the full job metadata
                    job_meta = analyzer.job_index.get_cached_job(job.get("job_name"), job.get("file_path"))
                    if job_meta:
                        cols = job_meta.get("columns", {}).get(selected_table, [])
                        if cols:
                            df_cols = pd.DataFrame(cols)
                            # Select relevant columns for display
                            display_cols = ["name", "type", "nullable"]
                            display_cols = [c for c in display_cols if c in df_cols.columns]
                            st.dataframe(df_cols[display_cols], use_container_width=True)
                            found_columns = True
                            break
                
                if not found_columns:
                    st.info("No column information available for this table.")

        else:
            st.warning("No tables found matching your search.")

with tab2:
    st.subheader("Column Search")
    
    col_search_term = st.text_input("Search Column Name", placeholder="e.g., CUST_ID")
    
    if col_search_term:
        with st.spinner(f"Searching for column '{col_search_term}'..."):
            results = analyzer.find_tables_using_column(col_search_term)
            
            if results:
                st.success(f"Found {len(results)} tables containing '{col_search_term}'.")
                
                # Group by Job
                jobs_dict = {} # job_name -> {file_path, tables: []}
                
                for item in results:
                    full_name = item.get("full_name", "Unknown")
                    related_jobs = item.get("related_jobs", [])
                    
                    for job in related_jobs:
                        j_name = job.get("job_name")
                        j_path = job.get("file_path")
                        j_key = f"{j_name}::{j_path}"
                        
                        if j_key not in jobs_dict:
                            jobs_dict[j_key] = {
                                "job_name": j_name,
                                "file_path": j_path,
                                "tables": []
                            }
                        
                        if full_name not in jobs_dict[j_key]["tables"]:
                            jobs_dict[j_key]["tables"].append(full_name)
                
                st.success(f"Found {len(jobs_dict)} Jobs affected by '{col_search_term}'.")
                
                # Display grouped results
                for j_key, j_info in jobs_dict.items():
                    with st.expander(f"Job: {j_info['job_name']}"):
                        st.caption(f"File: {j_info['file_path']}")
                        
                        st.markdown("**Affected Tables:**")
                        for t in j_info['tables']:
                            st.text(f"- {t}")
                        
                        # Link to DSX Detail (Optional, if we can pass state)
                        # st.button("View Details", key=j_key)
            else:
                st.warning("No columns found.")
