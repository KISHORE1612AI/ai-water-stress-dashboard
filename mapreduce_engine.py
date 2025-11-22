"""
MapReduce Engine - Custom Python Implementation
=================================================
Implements Map, Shuffle, and Reduce phases for distributed data processing simulation.
Demonstrates core MapReduce concepts for Big Data Analytics.
"""

from collections import defaultdict
from typing import Callable, List, Tuple, Any, Dict
import multiprocessing as mp
from functools import reduce
import pandas as pd


class MapReduceEngine:
    """
    Custom MapReduce framework implementing the three core phases:
    1. Map Phase: Transform input data into key-value pairs
    2. Shuffle Phase: Group values by key
    3. Reduce Phase: Aggregate grouped values
    """
    
    def __init__(self, num_workers: int = 4):
        """
        Initialize MapReduce engine.
        
        Args:
            num_workers: Number of parallel workers for processing
        """
        self.num_workers = min(num_workers, mp.cpu_count())
        self.map_results = []
        self.shuffle_results = defaultdict(list)
        self.reduce_results = {}
    
    def map_phase(self, data: List[Any], mapper_func: Callable) -> List[Tuple]:
        """
        Map Phase: Apply mapper function to each data element.
        
        Args:
            data: Input data list
            mapper_func: Function that transforms each element to (key, value) pairs
            
        Returns:
            List of (key, value) tuples
        """
        print(f"[MAP PHASE] Processing {len(data)} records with {self.num_workers} workers...")
        
        # Parallel mapping using multiprocessing
        chunk_size = max(1, len(data) // self.num_workers)
        with mp.Pool(self.num_workers) as pool:
            results = pool.map(mapper_func, data, chunksize=chunk_size)
        
        # Flatten results (each mapper may emit multiple key-value pairs)
        self.map_results = [item for sublist in results if sublist for item in (sublist if isinstance(sublist, list) else [sublist])]
        
        print(f"[MAP PHASE] Generated {len(self.map_results)} key-value pairs")
        return self.map_results
    
    def shuffle_phase(self, map_output: List[Tuple] = None) -> Dict[Any, List]:
        """
        Shuffle Phase: Group all values by their keys.
        
        Args:
            map_output: List of (key, value) tuples from map phase
            
        Returns:
            Dictionary with keys mapped to lists of values
        """
        if map_output is None:
            map_output = self.map_results
        
        print(f"[SHUFFLE PHASE] Grouping {len(map_output)} pairs by key...")
        
        self.shuffle_results = defaultdict(list)
        for key, value in map_output:
            self.shuffle_results[key].append(value)
        
        print(f"[SHUFFLE PHASE] Created {len(self.shuffle_results)} unique groups")
        return dict(self.shuffle_results)
    
    def reduce_phase(self, reducer_func: Callable, shuffle_output: Dict = None) -> Dict:
        """
        Reduce Phase: Aggregate values for each key.
        
        Args:
            reducer_func: Function that aggregates list of values into single result
            shuffle_output: Grouped data from shuffle phase
            
        Returns:
            Dictionary with keys mapped to aggregated values
        """
        if shuffle_output is None:
            shuffle_output = self.shuffle_results
        
        print(f"[REDUCE PHASE] Reducing {len(shuffle_output)} groups...")
        
        self.reduce_results = {}
        for key, values in shuffle_output.items():
            self.reduce_results[key] = reducer_func(key, values)
        
        print(f"[REDUCE PHASE] Completed. Results: {len(self.reduce_results)} aggregations")
        return self.reduce_results
    
    def run(self, data: List[Any], mapper_func: Callable, reducer_func: Callable) -> Dict:
        """
        Execute complete MapReduce workflow.
        
        Args:
            data: Input data
            mapper_func: Map function
            reducer_func: Reduce function
            
        Returns:
            Final aggregated results
        """
        print("\n" + "="*70)
        print("MAPREDUCE EXECUTION STARTED")
        print("="*70)
        
        # Execute three phases
        self.map_phase(data, mapper_func)
        self.shuffle_phase()
        results = self.reduce_phase(reducer_func)
        
        print("="*70)
        print("MAPREDUCE EXECUTION COMPLETED")
        print("="*70 + "\n")
        
        return results


# Example mapper and reducer functions for plant health analysis

def plant_health_mapper(row: Dict) -> List[Tuple]:
    """
    Example mapper: Extract health status and sensor readings.
    
    Args:
        row: Dictionary representing a data row
        
    Returns:
        List of (key, value) tuples
    """
    if isinstance(row, dict) and 'Plant_Health_Status' in row:
        status = row['Plant_Health_Status']
        # Emit multiple metrics per record
        return [
            (f"health_{status}", 1),  # Count by health status
            (f"moisture_{status}", float(row.get('Soil_Moisture', 0))),
            (f"temp_{status}", float(row.get('Ambient_Temperature', 0))),
        ]
    return []


def aggregation_reducer(key: str, values: List[float]) -> Dict:
    """
    Example reducer: Calculate statistics for grouped values.
    
    Args:
        key: Group key
        values: List of values to aggregate
        
    Returns:
        Dictionary with aggregated statistics
    """
    return {
        'count': len(values),
        'sum': sum(values),
        'mean': sum(values) / len(values) if values else 0,
        'min': min(values) if values else 0,
        'max': max(values) if values else 0,
    }


def word_count_example(text_data: List[str]) -> Dict[str, int]:
    """
    Classic word count MapReduce example.
    
    Args:
        text_data: List of text strings
        
    Returns:
        Word frequency dictionary
    """
    def mapper(text):
        words = text.lower().split()
        return [(word, 1) for word in words]
    
    def reducer(key, values):
        return sum(values)
    
    engine = MapReduceEngine()
    return engine.run(text_data, mapper, reducer)


if __name__ == "__main__":
    # Test MapReduce engine with sample data
    print("Testing MapReduce Engine...\n")
    
    # Test 1: Word count
    texts = ["hello world", "hello mapreduce", "big data analytics"]
    results = word_count_example(texts)
    print("Word Count Results:", results)
    
    # Test 2: Plant health aggregation
    sample_data = [
        {'Plant_Health_Status': 'Healthy', 'Soil_Moisture': 35.5, 'Ambient_Temperature': 25.0},
        {'Plant_Health_Status': 'Healthy', 'Soil_Moisture': 38.2, 'Ambient_Temperature': 24.5},
        {'Plant_Health_Status': 'High Stress', 'Soil_Moisture': 15.3, 'Ambient_Temperature': 30.2},
    ]
    
    engine = MapReduceEngine()
    results = engine.run(sample_data, plant_health_mapper, aggregation_reducer)
    print("\nPlant Health MapReduce Results:")
    for key, value in results.items():
        print(f"  {key}: {value}")
