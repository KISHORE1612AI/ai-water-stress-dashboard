"""
Big Data Pipeline - Hadoop & Spark Simulation
==============================================
Simulates Hadoop HDFS and Spark RDD operations for plant health data processing.
Demonstrates batch processing, data partitioning, and distributed transformations.
"""

import pandas as pd
import numpy as np
import os
import json
import shutil
from typing import List, Dict, Any, Callable
from datetime import datetime
import pickle
from collections import Counter


class HDFSSimulator:
    """
    Simulates Hadoop Distributed File System (HDFS) operations:
    - Block-based storage
    - Data partitioning
    - Replication
    - File operations
    """
    
    def __init__(self, base_path: str = "hdfs_storage", block_size: int = 1024*1024):
        """
        Initialize HDFS simulator.
        
        Args:
            base_path: Root directory for HDFS storage
            block_size: Block size in bytes (default: 1MB)
        """
        self.base_path = base_path
        self.block_size = block_size
        self.replication_factor = 2
        
        os.makedirs(base_path, exist_ok=True)
        print(f"[HDFS] Initialized at: {base_path}")
        print(f"[HDFS] Block size: {block_size} bytes")
    
    def write_file(self, data: pd.DataFrame, filename: str, partitions: int = 4) -> Dict:
        """
        Write DataFrame to HDFS with partitioning.
        
        Args:
            data: DataFrame to write
            filename: Target filename
            partitions: Number of partitions
            
        Returns:
            Metadata about written file
        """
        print(f"\n[HDFS] Writing {filename} with {partitions} partitions...")
        
        # Create directory for this file
        file_dir = os.path.join(self.base_path, filename)
        os.makedirs(file_dir, exist_ok=True)
        
        # Partition data
        partition_size = len(data) // partitions
        metadata = {
            'filename': filename,
            'total_records': len(data),
            'partitions': partitions,
            'partition_size': partition_size,
            'blocks': []
        }
        
        for i in range(partitions):
            start_idx = i * partition_size
            end_idx = start_idx + partition_size if i < partitions - 1 else len(data)
            
            partition_data = data.iloc[start_idx:end_idx]
            
            # Write partition
            partition_file = os.path.join(file_dir, f"part-{i:05d}.parquet")
            partition_data.to_parquet(partition_file, index=False)
            
            file_size = os.path.getsize(partition_file)
            metadata['blocks'].append({
                'partition': i,
                'path': partition_file,
                'records': len(partition_data),
                'size_bytes': file_size
            })
            
            print(f"  Partition {i}: {len(partition_data)} records, {file_size} bytes")
        
        # Save metadata
        metadata_file = os.path.join(file_dir, '_metadata.json')
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"[HDFS] Write complete: {len(data)} records in {partitions} partitions")
        return metadata
    
    def read_file(self, filename: str) -> pd.DataFrame:
        """
        Read file from HDFS (all partitions).
        
        Args:
            filename: File to read
            
        Returns:
            Combined DataFrame from all partitions
        """
        file_dir = os.path.join(self.base_path, filename)
        
        # Read metadata
        metadata_file = os.path.join(file_dir, '_metadata.json')
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
        
        # Read all partitions
        partitions = []
        for block in metadata['blocks']:
            partition_df = pd.read_parquet(block['path'])
            partitions.append(partition_df)
        
        combined = pd.concat(partitions, ignore_index=True)
        print(f"[HDFS] Read {len(combined)} records from {filename}")
        
        return combined
    
    def get_file_info(self, filename: str) -> Dict:
        """Get file metadata."""
        metadata_file = os.path.join(self.base_path, filename, '_metadata.json')
        with open(metadata_file, 'r') as f:
            return json.load(f)


class SparkRDDSimulator:
    """
    Simulates Apache Spark RDD (Resilient Distributed Dataset) operations:
    - Transformations: map, filter, flatMap, reduceByKey
    - Actions: collect, count, reduce, take
    - Lazy evaluation simulation
    """
    
    def __init__(self, data: Any = None, partitions: int = 4):
        """
        Initialize RDD simulator.
        
        Args:
            data: Initial data (list, DataFrame, etc.)
            partitions: Number of partitions
        """
        self.data = data
        self.partitions = partitions
        self.transformations = []  # Track lazy transformations
        self._cached = False
        print(f"[Spark RDD] Created with {partitions} partitions")
    
    def map(self, func: Callable) -> 'SparkRDDSimulator':
        """
        Transformation: Apply function to each element.
        
        Args:
            func: Function to apply
            
        Returns:
            New RDD
        """
        new_rdd = SparkRDDSimulator(self.data, self.partitions)
        new_rdd.transformations = self.transformations + [('map', func)]
        print(f"[Spark RDD] Transformation added: map")
        return new_rdd
    
    def filter(self, func: Callable) -> 'SparkRDDSimulator':
        """
        Transformation: Filter elements by predicate.
        
        Args:
            func: Predicate function
            
        Returns:
            New RDD
        """
        new_rdd = SparkRDDSimulator(self.data, self.partitions)
        new_rdd.transformations = self.transformations + [('filter', func)]
        print(f"[Spark RDD] Transformation added: filter")
        return new_rdd
    
    def flatMap(self, func: Callable) -> 'SparkRDDSimulator':
        """
        Transformation: Map and flatten results.
        
        Args:
            func: Function that returns iterable
            
        Returns:
            New RDD
        """
        new_rdd = SparkRDDSimulator(self.data, self.partitions)
        new_rdd.transformations = self.transformations + [('flatMap', func)]
        print(f"[Spark RDD] Transformation added: flatMap")
        return new_rdd
    
    def reduceByKey(self, func: Callable) -> 'SparkRDDSimulator':
        """
        Transformation: Reduce values by key.
        
        Args:
            func: Reduction function
            
        Returns:
            New RDD
        """
        new_rdd = SparkRDDSimulator(self.data, self.partitions)
        new_rdd.transformations = self.transformations + [('reduceByKey', func)]
        print(f"[Spark RDD] Transformation added: reduceByKey")
        return new_rdd
    
    def groupByKey(self) -> 'SparkRDDSimulator':
        """
        Transformation: Group values by key.
        
        Returns:
            New RDD
        """
        new_rdd = SparkRDDSimulator(self.data, self.partitions)
        new_rdd.transformations = self.transformations + [('groupByKey', None)]
        print(f"[Spark RDD] Transformation added: groupByKey")
        return new_rdd
    
    def _execute_transformations(self):
        """Execute all queued transformations (lazy evaluation)."""
        print(f"\n[Spark RDD] Executing {len(self.transformations)} transformations...")
        
        result = self.data
        
        for transform_type, func in self.transformations:
            if transform_type == 'map':
                result = [func(item) for item in result]
            elif transform_type == 'filter':
                result = [item for item in result if func(item)]
            elif transform_type == 'flatMap':
                result = [subitem for item in result for subitem in func(item)]
            elif transform_type == 'reduceByKey':
                # Group by key then reduce
                grouped = {}
                for key, value in result:
                    if key not in grouped:
                        grouped[key] = []
                    grouped[key].append(value)
                result = [(key, self._reduce_list(values, func)) for key, values in grouped.items()]
            elif transform_type == 'groupByKey':
                grouped = {}
                for key, value in result:
                    if key not in grouped:
                        grouped[key] = []
                    grouped[key].append(value)
                result = list(grouped.items())
        
        return result
    
    def _reduce_list(self, values: List, func: Callable):
        """Reduce a list using binary function."""
        if not values:
            return None
        result = values[0]
        for val in values[1:]:
            result = func(result, val)
        return result
    
    def collect(self) -> List:
        """
        Action: Return all elements as a list.
        
        Returns:
            List of all elements
        """
        print(f"[Spark RDD] Action: collect()")
        result = self._execute_transformations()
        print(f"[Spark RDD] Collected {len(result)} elements")
        return result
    
    def count(self) -> int:
        """
        Action: Count number of elements.
        
        Returns:
            Number of elements
        """
        print(f"[Spark RDD] Action: count()")
        result = self._execute_transformations()
        return len(result)
    
    def take(self, n: int) -> List:
        """
        Action: Return first n elements.
        
        Args:
            n: Number of elements
            
        Returns:
            List of first n elements
        """
        print(f"[Spark RDD] Action: take({n})")
        result = self._execute_transformations()
        return result[:n]
    
    def cache(self) -> 'SparkRDDSimulator':
        """Mark RDD for caching."""
        self._cached = True
        print(f"[Spark RDD] Marked for caching")
        return self


def run_hadoop_batch_job(input_file: str, output_dir: str = "powerbi_exports") -> Dict:
    """
    Simulate Hadoop batch processing job.
    
    Args:
        input_file: Path to input CSV
        output_dir: Output directory
        
    Returns:
        Job statistics
    """
    print("\n" + "="*70)
    print("HADOOP BATCH PROCESSING JOB")
    print("="*70)
    
    # Initialize HDFS
    hdfs = HDFSSimulator()
    
    # Read and partition data
    print("\n[Hadoop] Reading input data...")
    df = pd.read_csv(input_file)
    print(f"[Hadoop] Loaded {len(df)} records")
    
    # Write to HDFS
    metadata = hdfs.write_file(df, "plant_health_partitioned", partitions=4)
    
    # Read back from HDFS
    print("\n[Hadoop] Reading from HDFS...")
    df_hdfs = hdfs.read_file("plant_health_partitioned")
    
    # Export cleaned data
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "cleaned_data.csv")
    df_hdfs.to_csv(output_file, index=False)
    print(f"[Hadoop] Exported to: {output_file}")
    
    stats = {
        'records_processed': len(df_hdfs),
        'partitions': metadata['partitions'],
        'output_file': output_file,
        'timestamp': datetime.now().isoformat()
    }
    
    print("="*70)
    print("HADOOP JOB COMPLETED")
    print("="*70 + "\n")
    
    return stats


def run_spark_analytics(input_file: str, output_dir: str = "powerbi_exports") -> Dict:
    """
    Run Spark RDD analytics pipeline.
    
    Args:
        input_file: Path to input CSV
        output_dir: Output directory
        
    Returns:
        Analytics results
    """
    print("\n" + "="*70)
    print("SPARK ANALYTICS PIPELINE")
    print("="*70)
    
    # Load data
    df = pd.read_csv(input_file)
    records = df.to_dict('records')
    
    # Create RDD
    print(f"\n[Spark] Creating RDD from {len(records)} records...")
    rdd = SparkRDDSimulator(records, partitions=4)
    
    # Example 1: Map-Reduce word count on health status
    print("\n--- Analytics 1: Health Status Distribution ---")
    status_rdd = (rdd
                  .map(lambda x: (x['Plant_Health_Status'], 1))
                  .reduceByKey(lambda a, b: a + b))
    status_counts = dict(status_rdd.collect())
    print("Results:", status_counts)
    
    # Example 2: Filter and aggregate
    print("\n--- Analytics 2: High Stress Plants ---")
    high_stress_rdd = (rdd
                      .filter(lambda x: x['Plant_Health_Status'] == 'High Stress')
                      .map(lambda x: ('High_Stress_Moisture', x['Soil_Moisture'])))
    high_stress_data = high_stress_rdd.collect()
    print(f"Found {len(high_stress_data)} high stress records")
    
    # Example 3: Complex transformation
    print("\n--- Analytics 3: Sensor Averages by Status ---")
    sensor_fields = ['Soil_Moisture', 'Ambient_Temperature', 'Humidity']
    
    aggregations = {}
    for field in sensor_fields:
        field_rdd = (rdd
                    .map(lambda x: (x['Plant_Health_Status'], (x[field], 1)))
                    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
                    .map(lambda x: (x[0], x[1][0] / x[1][1])))  # Calculate average
        
        aggregations[field] = dict(field_rdd.collect())
    
    # Create summary DataFrame
    summary_data = []
    for status in status_counts.keys():
        row = {'Health_Status': status, 'Count': status_counts[status]}
        for field in sensor_fields:
            row[f'Avg_{field}'] = aggregations[field].get(status, 0)
        summary_data.append(row)
    
    summary_df = pd.DataFrame(summary_data)
    
    # Export results
    os.makedirs(output_dir, exist_ok=True)
    summary_file = os.path.join(output_dir, "analytics_summary.csv")
    summary_df.to_csv(summary_file, index=False)
    print(f"\n[Spark] Exported summary to: {summary_file}")
    
    print("="*70)
    print("SPARK ANALYTICS COMPLETED")
    print("="*70 + "\n")
    
    return {
        'status_distribution': status_counts,
        'aggregations': aggregations,
        'summary_file': summary_file
    }


if __name__ == "__main__":
    # Test pipelines
    input_file = "plant_health_data.csv"
    
    if os.path.exists(input_file):
        # Run Hadoop job
        hadoop_stats = run_hadoop_batch_job(input_file)
        print(f"\nHadoop Stats: {hadoop_stats}")
        
        # Run Spark analytics
        spark_results = run_spark_analytics(input_file)
        print(f"\nSpark Results: {spark_results['status_distribution']}")
    else:
        print(f"Input file not found: {input_file}")
