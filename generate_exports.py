"""
Generate All Big Data Exports
==============================
Runs all Big Data pipelines and generates PowerBI export files.
"""

import pandas as pd
import numpy as np
import os
import sys

# Import Big Data components
from bigdata_pipeline import run_hadoop_batch_job, run_spark_analytics
from stream_processor import run_stream_analytics_demo
from nosql_db import NoSQLDatabase

def generate_correlations(input_file: str, output_dir: str = "powerbi_exports"):
    """Generate correlation matrix export."""
    print("\n[Exports] Generating correlations...")
    
    df = pd.read_csv(input_file)
    
    # Select numeric columns
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    numeric_data = df[numeric_cols]
    
    # Calculate correlations
    corr_matrix = numeric_data.corr()
    
    # Export
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "correlations.csv")
    corr_matrix.to_csv(output_file)
    
    print(f"[Exports] Correlations saved to: {output_file}")
    return output_file

def main():
    """Run all pipelines and generate exports."""
    print("="*70)
    print("GENERATING ALL BIG DATA EXPORTS")
    print("="*70)
    
    input_file = "plant_health_data.csv"
    output_dir = "powerbi_exports"
    
    if not os.path.exists(input_file):
        print(f"ERROR: {input_file} not found!")
        return
    
    try:
        # 1. Run Hadoop batch job
        print("\n1. Running Hadoop Batch Job...")
        hadoop_stats = run_hadoop_batch_job(input_file, output_dir)
        print(f"✅ Hadoop: Processed {hadoop_stats['records_processed']} records")
        
        # 2. Run Spark analytics
        print("\n2. Running Spark Analytics...")
        spark_results = run_spark_analytics(input_file, output_dir)
        print(f"✅ Spark: Generated {spark_results['summary_file']}")
        
        # 3. Run Stream analytics
        print("\n3. Running Stream Analytics...")
        stream_results = run_stream_analytics_demo(input_file, output_dir)
        print(f"✅ Stream: Processed {stream_results['processed_events']} events")
        
        # 4. Generate correlations
        print("\n4. Generating Correlations...")
        corr_file = generate_correlations(input_file, output_dir)
        print(f"✅ Correlations: Generated {corr_file}")
        
        # 5. Load data into NoSQL
        print("\n5. Loading data into NoSQL...")
        db = NoSQLDatabase()
        df = pd.read_csv(input_file)
        sample = df.head(1000)  # Sample to avoid overwhelming
        db.load_from_dataframe(sample, 'sensors')
        stats = db.get_statistics()
        print(f"✅ NoSQL: Loaded {stats['total_documents']} documents")
        db.close()
        
        # Summary
        print("\n" + "="*70)
        print("EXPORT GENERATION COMPLETE")
        print("="*70)
        print(f"\nExports available in: {output_dir}/")
        
        # List all exports
        if os.path.exists(output_dir):
            files = os.listdir(output_dir)
            print("\nGenerated files:")
            for file in sorted(files):
                file_path = os.path.join(output_dir, file)
                if os.path.isfile(file_path):
                    size = os.path.getsize(file_path) / 1024
                    print(f"  - {file} ({size:.2f} KB)")
        
        print("\n✅ All pipelines executed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
