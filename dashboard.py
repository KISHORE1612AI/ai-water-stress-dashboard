"""
Big Data Analytics Dashboard
=============================
Interactive Streamlit dashboard for plant health Big Data analytics.
Integrates Hadoop, Spark, MapReduce, NoSQL, and Stream processing visualizations.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import os
import sys
from datetime import datetime

# Import Big Data components
from mapreduce_engine import MapReduceEngine, plant_health_mapper, aggregation_reducer
from nosql_db import NoSQLDatabase
from bigdata_pipeline import run_hadoop_batch_job, run_spark_analytics, HDFSSimulator, SparkRDDSimulator
from stream_processor import StreamProcessor, SensorDataSimulator, run_stream_analytics_demo

# Page configuration
st.set_page_config(
    page_title="Big Data Analytics Dashboard - Plant Health",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin-bottom: 30px;
    }
    .metric-card {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .tech-badge {
        display: inline-block;
        padding: 5px 15px;
        margin: 5px;
        background: #667eea;
        color: white;
        border-radius: 20px;
        font-size: 14px;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown("""
<div class="main-header">
    <h1>📊 Big Data Analytics Dashboard</h1>
    <p>AI-Driven Water Stress Management in Plants</p>
    <div>
        <span class="tech-badge">Hadoop</span>
        <span class="tech-badge">Spark</span>
        <span class="tech-badge">MapReduce</span>
        <span class="tech-badge">NoSQL</span>
        <span class="tech-badge">Streaming</span>
    </div>
</div>
""", unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.image("https://img.icons8.com/color/96/000000/big-data.png", width=80)
    st.title("Big Data Tools")
    
    selected_tab = st.radio(
        "Select Analytics Module",
        ["📈 Overview", "🗄️ Hadoop HDFS", "⚡ Spark RDD", "🔄 MapReduce", 
         "💾 NoSQL Database", "📡 Stream Analytics", "📤 Data Exports"]
    )
    
    st.divider()
    
    # Run all pipelines button
    if st.button("🚀 Run All Pipelines", type="primary"):
        with st.spinner("Running all Big Data pipelines..."):
            try:
                # Run Hadoop
                hadoop_stats = run_hadoop_batch_job("plant_health_data.csv")
                
                # Run Spark
                spark_results = run_spark_analytics("plant_health_data.csv")
                
                # Run Stream Analytics
                stream_results = run_stream_analytics_demo("plant_health_data.csv")
                
                st.success("✅ All pipelines executed successfully!")
                st.session_state['pipelines_run'] = True
                
            except Exception as e:
                st.error(f"Error running pipelines: {str(e)}")

# Main content area
if selected_tab == "📈 Overview":
    st.header("Big Data Analytics Overview")
    
    # Load data
    try:
        df = pd.read_csv("plant_health_data.csv")
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Records", f"{len(df):,}")
        with col2:
            st.metric("Plant IDs", df['Plant_ID'].nunique())
        with col3:
            st.metric("Sensor Fields", len(df.columns) - 3)
        with col4:
            st.metric("Health Categories", df['Plant_Health_Status'].nunique())
        
        st.divider()
        
        # Visualization
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Health Status Distribution")
            status_counts = df['Plant_Health_Status'].value_counts()
            fig = px.pie(values=status_counts.values, names=status_counts.index,
                        color_discrete_sequence=px.colors.sequential.RdBu)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Sensor Readings Summary")
            numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns[:5]
            summary_data = []
            for col in numeric_cols:
                summary_data.append({
                    'Sensor': col,
                    'Mean': df[col].mean(),
                    'Std': df[col].std()
                })
            summary_df = pd.DataFrame(summary_data)
            fig = px.bar(summary_df, x='Sensor', y='Mean', error_y='Std',
                        color='Mean', color_continuous_scale='Viridis')
            st.plotly_chart(fig, use_container_width=True)
        
        # Data preview
        st.subheader("Data Preview")
        st.dataframe(df.head(10), use_container_width=True)
        
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")

elif selected_tab == "🗄️ Hadoop HDFS":
    st.header("Hadoop HDFS Simulation")
    
    st.markdown("""
    **Hadoop Distributed File System (HDFS)** stores large files across multiple machines.
    This simulation demonstrates:
    - **Block-based storage**: Data partitioned into blocks
    - **Replication**: Each block stored multiple times
    - **Distributed processing**: Parallel operations on partitions
    """)
    
    if st.button("Run Hadoop Batch Job"):
        with st.spinner("Running Hadoop job..."):
            try:
                stats = run_hadoop_batch_job("plant_health_data.csv")
                
                st.success("✅ Hadoop job completed!")
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Records Processed", f"{stats['records_processed']:,}")
                with col2:
                    st.metric("Partitions Created", stats['partitions'])
                with col3:
                    st.metric("Status", "SUCCESS")
                
                # Show HDFS structure
                st.subheader("HDFS Directory Structure")
                hdfs = HDFSSimulator()
                info = hdfs.get_file_info("plant_health_partitioned")
                
                partition_data = []
                for block in info['blocks']:
                    partition_data.append({
                        'Partition': block['partition'],
                        'Records': block['records'],
                        'Size (bytes)': block['size_bytes'],
                        'Path': block['path']
                    })
                
                st.dataframe(pd.DataFrame(partition_data), use_container_width=True)
                
            except Exception as e:
                st.error(f"Error: {str(e)}")

elif selected_tab == "⚡ Spark RDD":
    st.header("Apache Spark RDD Operations")
    
    st.markdown("""
    **Resilient Distributed Datasets (RDDs)** are Spark's fundamental data structure.
    Demonstrates:
    - **Transformations**: map, filter, reduceByKey (lazy evaluation)
    - **Actions**: collect, count, take (triggers computation)
    - **Distributed processing**: Parallel operations across partitions
    """)
    
    if st.button("Run Spark Analytics"):
        with st.spinner("Running Spark RDD operations..."):
            try:
                results = run_spark_analytics("plant_health_data.csv")
                
                st.success("✅ Spark analytics completed!")
                
                # Display results
                st.subheader("Health Status Distribution (RDD.reduceByKey)")
                status_df = pd.DataFrame(list(results['status_distribution'].items()),
                                        columns=['Health Status', 'Count'])
                fig = px.bar(status_df, x='Health Status', y='Count',
                           color='Count', color_continuous_scale='Turbo')
                st.plotly_chart(fig, use_container_width=True)
                
                # Aggregations
                st.subheader("Sensor Averages by Health Status")
                agg_data = []
                for sensor, values in results['aggregations'].items():
                    for status, avg_val in values.items():
                        agg_data.append({
                            'Sensor': sensor,
                            'Health Status': status,
                            'Average Value': avg_val
                        })
                
                agg_df = pd.DataFrame(agg_data)
                fig = px.bar(agg_df, x='Sensor', y='Average Value', color='Health Status',
                           barmode='group', color_discrete_sequence=px.colors.qualitative.Set2)
                st.plotly_chart(fig, use_container_width=True)
                
            except Exception as e:
                st.error(f"Error: {str(e)}")

elif selected_tab == "🔄 MapReduce":
    st.header("MapReduce Engine")
    
    st.markdown("""
    **MapReduce** processes large datasets in three phases:
    1. **Map**: Transform input into key-value pairs
    2. **Shuffle**: Group values by key
    3. **Reduce**: Aggregate grouped values
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("MapReduce Configuration")
        num_workers = st.slider("Number of Workers", 1, 8, 4)
        sample_size = st.slider("Sample Size", 100, 1000, 500)
    
    with col2:
        st.subheader("MapReduce Flow")
        st.code("""
1. MAP: (record) → [(key, value), ...]
2. SHUFFLE: Group by key
3. REDUCE: (key, [values]) → result
        """, language="python")
    
    if st.button("Run MapReduce Job"):
        with st.spinner("Running MapReduce..."):
            try:
                df = pd.read_csv("plant_health_data.csv")
                sample_data = df.head(sample_size).to_dict('records')
                
                engine = MapReduceEngine(num_workers=num_workers)
                results = engine.run(sample_data, plant_health_mapper, aggregation_reducer)
                
                st.success("✅ MapReduce job completed!")
                
                # Display results
                results_data = []
                for key, value in results.items():
                    results_data.append({
                        'Key': key,
                        'Count': value['count'],
                        'Mean': value['mean'],
                        'Min': value['min'],
                        'Max': value['max']
                    })
                
                results_df = pd.DataFrame(results_data)
                st.dataframe(results_df, use_container_width=True)
                
                # Visualization
                fig = px.bar(results_df, x='Key', y='Mean', error_y='Count',
                           color='Mean', color_continuous_scale='Plasma')
                st.plotly_chart(fig, use_container_width=True)
                
            except Exception as e:
                st.error(f"Error: {str(e)}")

elif selected_tab == "💾 NoSQL Database":
    st.header("NoSQL Database Operations")
    
    st.markdown("""
    **NoSQL Database** (Document Store using TinyDB):
    - Flexible schema (JSON documents)
    - Fast querying and aggregation
    - Horizontal scalability simulation
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Load Data to NoSQL"):
            with st.spinner("Loading data..."):
                try:
                    df = pd.read_csv("plant_health_data.csv")
                    db = NoSQLDatabase()
                    
                    # Sample data to avoid overwhelming
                    sample = df.head(1000)
                    db.load_from_dataframe(sample, 'sensors')
                    
                    st.success(f"✅ Loaded {len(sample)} documents")
                    st.session_state['nosql_loaded'] = True
                    
                    # Show stats
                    stats = db.get_statistics()
                    st.json(stats)
                    
                    db.close()
                    
                except Exception as e:
                    st.error(f"Error: {str(e)}")
    
    with col2:
        if st.button("Query NoSQL Database"):
            with st.spinner("Querying..."):
                try:
                    db = NoSQLDatabase()
                    
                    # Aggregation
                    agg_results = db.aggregate_by_field('sensors', 'Plant_Health_Status', 'Soil_Moisture')
                    
                    st.success("✅ Query completed")
                    
                    # Display results
                    agg_df = pd.DataFrame(agg_results).T.reset_index()
                    agg_df.columns = ['Status', 'Count', 'Sum', 'Mean', 'Min', 'Max']
                    st.dataframe(agg_df, use_container_width=True)
                    
                    db.close()
                    
                except Exception as e:
                    st.error(f"Error: {str(e)}")

elif selected_tab == "📡 Stream Analytics":
    st.header("Real-Time Stream Processing")
    
    st.markdown("""
    **Stream Analytics** processes data in real-time:
    - **Sliding Windows**: Time-based and count-based windows
    - **Real-time Filtering**: Process only relevant events
    - **Live Aggregation**: Statistics on streaming data
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        num_events = st.slider("Number of Events", 100, 2000, 500)
        window_size = st.slider("Window Size", 50, 500, 100)
    
    with col2:
        st.subheader("Stream Configuration")
        st.write(f"- Events to generate: {num_events}")
        st.write(f"- Window size: {window_size}")
        st.write(f"- Filter: Soil Moisture < 25")
    
    if st.button("Start Stream Processing"):
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        try:
            # Load base data
            df = pd.read_csv("plant_health_data.csv")
            
            # Initialize components
            simulator = SensorDataSimulator(df)
            processor = StreamProcessor()
            processor.create_window('main_window', window_size=window_size, window_type='count')
            processor.add_filter(lambda e: e.get('Soil_Moisture', 100) < 25)
            
            # Generate and process in batches
            batch_size = 100
            all_processed = []
            
            for i in range(0, num_events, batch_size):
                batch = simulator.generate_stream(batch_size)
                processed = processor.process_batch(batch)
                all_processed.extend(processed)
                
                progress = min((i + batch_size) / num_events, 1.0)
                progress_bar.progress(progress)
                status_text.text(f"Processed {i + batch_size}/{num_events} events...")
            
            status_text.text("Stream processing complete!")
            
            st.success(f"✅ Processed {len(all_processed)} events out of {num_events}")
            
            # Visualization
            if all_processed:
                events_df = pd.DataFrame(all_processed)
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("Soil Moisture Over Time")
                    fig = px.line(events_df.tail(200), y='Soil_Moisture',
                                color='Plant_Health_Status',
                                color_discrete_map={
                                    'Healthy': 'green',
                                    'Moderate Stress': 'orange',
                                    'High Stress': 'red'
                                })
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    st.subheader("Temperature Distribution")
                    fig = px.histogram(events_df, x='Ambient_Temperature',
                                     nbins=30, color='Plant_Health_Status')
                    st.plotly_chart(fig, use_container_width=True)
                
        except Exception as e:
            st.error(f"Error: {str(e)}")

elif selected_tab == "📤 Data Exports":
    st.header("PowerBI / Data Exports")
    
    st.markdown("""
    Export processed data for external tools like PowerBI, Tableau, or Excel.
    All files are saved in the `powerbi_exports/` directory.
    """)
    
    export_dir = "powerbi_exports"
    
    if os.path.exists(export_dir):
        # List available exports
        files = [f for f in os.listdir(export_dir) if f.endswith('.csv') or f.endswith('.parquet')]
        
        if files:
            st.subheader("Available Exports")
            
            for file in files:
                file_path = os.path.join(export_dir, file)
                file_size = os.path.getsize(file_path) / 1024  # KB
                
                col1, col2, col3 = st.columns([3, 1, 1])
                
                with col1:
                    st.write(f"📄 {file}")
                with col2:
                    st.write(f"{file_size:.2f} KB")
                with col3:
                    with open(file_path, 'rb') as f:
                        st.download_button(
                            label="Download",
                            data=f,
                            file_name=file,
                            mime="text/csv" if file.endswith('.csv') else "application/octet-stream"
                        )
            
            # Preview
            st.subheader("Data Preview")
            selected_file = st.selectbox("Select file to preview", files)
            
            if selected_file:
                file_path = os.path.join(export_dir, selected_file)
                if selected_file.endswith('.csv'):
                    preview_df = pd.read_csv(file_path)
                    st.dataframe(preview_df.head(20), use_container_width=True)
                    
                    # Quick stats
                    st.subheader("Quick Statistics")
                    st.write(preview_df.describe())
        else:
            st.info("No export files found. Run the pipelines first!")
    else:
        st.warning("Export directory not found. Run pipelines to generate exports.")

# Footer
st.divider()
st.markdown("""
<div style='text-align: center; color: #666; padding: 20px;'>
    <p><strong>Big Data Analytics Dashboard</strong> | VIT 2025</p>
    <p>Built by: Kishore kumar ss </p>
    <p>Technologies: Hadoop • Spark • MapReduce • NoSQL • Stream Processing</p>
</div>
""", unsafe_allow_html=True)
