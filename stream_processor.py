"""
Stream Analytics Processor
===========================
Simulates real-time data streaming and processing for plant health monitoring.
Implements sliding windows, sampling, and real-time aggregations.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Callable, Optional
from collections import deque
import time
import random
import json
import os


class SlidingWindow:
    """
    Implements sliding window for stream processing.
    Maintains a time-based or count-based window of recent events.
    """
    
    def __init__(self, window_size: int = 100, window_type: str = 'count'):
        """
        Initialize sliding window.
        
        Args:
            window_size: Size of window (number of events or seconds)
            window_type: 'count' for count-based, 'time' for time-based
        """
        self.window_size = window_size
        self.window_type = window_type
        self.window = deque(maxlen=window_size if window_type == 'count' else None)
        self.timestamps = deque()
        
        print(f"[Sliding Window] Initialized: {window_type}-based, size={window_size}")
    
    def add(self, event: Dict) -> None:
        """
        Add event to window.
        
        Args:
            event: Event dictionary
        """
        if 'timestamp' not in event:
            event['timestamp'] = datetime.now().isoformat()
        
        if self.window_type == 'count':
            self.window.append(event)
        else:  # time-based
            current_time = datetime.fromisoformat(event['timestamp'])
            self.window.append(event)
            self.timestamps.append(current_time)
            
            # Remove old events outside window
            cutoff_time = current_time - timedelta(seconds=self.window_size)
            while self.timestamps and self.timestamps[0] < cutoff_time:
                self.timestamps.popleft()
                self.window.popleft()
    
    def get_window(self) -> List[Dict]:
        """Get current window contents."""
        return list(self.window)
    
    def get_statistics(self) -> Dict:
        """Calculate statistics for current window."""
        if not self.window:
            return {}
        
        # Convert to DataFrame for easy aggregation
        df = pd.DataFrame(list(self.window))
        
        stats = {
            'count': len(df),
            'window_type': self.window_type,
            'window_size': self.window_size
        }
        
        # Calculate numeric column statistics
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            stats[f'{col}_mean'] = df[col].mean()
            stats[f'{col}_min'] = df[col].min()
            stats[f'{col}_max'] = df[col].max()
            stats[f'{col}_std'] = df[col].std()
        
        return stats
    
    def clear(self) -> None:
        """Clear window contents."""
        self.window.clear()
        self.timestamps.clear()


class StreamProcessor:
    """
    Real-time stream processing engine.
    Handles data ingestion, windowing, filtering, and aggregation.
    """
    
    def __init__(self):
        """Initialize stream processor."""
        self.windows = {}
        self.filters = []
        self.aggregators = []
        self.event_count = 0
        
        print("[Stream Processor] Initialized")
    
    def create_window(self, name: str, window_size: int = 100, window_type: str = 'count') -> None:
        """
        Create a new sliding window.
        
        Args:
            name: Window name
            window_size: Window size
            window_type: Window type ('count' or 'time')
        """
        self.windows[name] = SlidingWindow(window_size, window_type)
        print(f"[Stream Processor] Created window: {name}")
    
    def add_filter(self, filter_func: Callable) -> None:
        """
        Add filter function to stream.
        
        Args:
            filter_func: Function that returns True to keep event
        """
        self.filters.append(filter_func)
    
    def add_aggregator(self, agg_func: Callable) -> None:
        """
        Add aggregation function.
        
        Args:
            agg_func: Aggregation function
        """
        self.aggregators.append(agg_func)
    
    def process_event(self, event: Dict) -> Dict:
        """
        Process a single event through the pipeline.
        
        Args:
            event: Event dictionary
            
        Returns:
            Processed event (or None if filtered out)
        """
        self.event_count += 1
        
        # Apply filters
        for filter_func in self.filters:
            if not filter_func(event):
                return None
        
        # Add to all windows
        for window in self.windows.values():
            window.add(event)
        
        # Apply aggregators
        for agg_func in self.aggregators:
            agg_func(event)
        
        return event
    
    def process_batch(self, events: List[Dict]) -> List[Dict]:
        """
        Process batch of events.
        
        Args:
            events: List of events
            
        Returns:
            List of processed events
        """
        print(f"\n[Stream Processor] Processing batch of {len(events)} events...")
        
        processed = []
        for event in events:
            result = self.process_event(event)
            if result:
                processed.append(result)
        
        print(f"[Stream Processor] Processed {len(processed)} events (filtered {len(events) - len(processed)})")
        return processed
    
    def get_window_stats(self, window_name: str) -> Dict:
        """Get statistics for a specific window."""
        if window_name in self.windows:
            return self.windows[window_name].get_statistics()
        return {}
    
    def get_all_stats(self) -> Dict:
        """Get statistics for all windows."""
        stats = {'total_events_processed': self.event_count}
        for name, window in self.windows.items():
            stats[name] = window.get_statistics()
        return stats


class SensorDataSimulator:
    """
    Simulates real-time sensor data stream for plant health monitoring.
    """
    
    def __init__(self, base_data: pd.DataFrame = None):
        """
        Initialize sensor simulator.
        
        Args:
            base_data: Historical data to base simulations on
        """
        self.base_data = base_data
        self.sensor_ranges = self._calculate_ranges()
        
        print("[Sensor Simulator] Initialized")
    
    def _calculate_ranges(self) -> Dict:
        """Calculate realistic ranges from base data."""
        if self.base_data is None:
            # Default ranges
            return {
                'Soil_Moisture': (10, 45),
                'Ambient_Temperature': (15, 35),
                'Soil_Temperature': (15, 30),
                'Humidity': (30, 80),
                'Light_Intensity': (200, 1000),
                'Soil_pH': (5.5, 7.5),
                'Nitrogen_Level': (10, 50),
                'Phosphorus_Level': (10, 50),
                'Potassium_Level': (10, 50),
                'Chlorophyll_Content': (20, 50),
                'Electrochemical_Signal': (0, 2)
            }
        
        ranges = {}
        numeric_cols = self.base_data.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            ranges[col] = (self.base_data[col].min(), self.base_data[col].max())
        
        return ranges
    
    def generate_event(self, plant_id: int = 1, add_noise: bool = True) -> Dict:
        """
        Generate a single sensor event.
        
        Args:
            plant_id: Plant ID
            add_noise: Add random noise to values
            
        Returns:
            Sensor event dictionary
        """
        event = {
            'timestamp': datetime.now().isoformat(),
            'Plant_ID': plant_id
        }
        
        for sensor, (min_val, max_val) in self.sensor_ranges.items():
            base_value = random.uniform(min_val, max_val)
            if add_noise:
                noise = random.gauss(0, (max_val - min_val) * 0.05)
                value = max(min_val, min(max_val, base_value + noise))
            else:
                value = base_value
            
            event[sensor] = round(value, 2)
        
        # Determine health status based on sensor values
        if event['Soil_Moisture'] < 20 or event['Ambient_Temperature'] > 30:
            event['Plant_Health_Status'] = 'High Stress'
        elif event['Soil_Moisture'] < 30:
            event['Plant_Health_Status'] = 'Moderate Stress'
        else:
            event['Plant_Health_Status'] = 'Healthy'
        
        return event
    
    def generate_stream(self, num_events: int = 100, delay: float = 0.0) -> List[Dict]:
        """
        Generate stream of events.
        
        Args:
            num_events: Number of events to generate
            delay: Delay between events (seconds)
            
        Returns:
            List of events
        """
        print(f"[Sensor Simulator] Generating {num_events} events...")
        
        events = []
        for i in range(num_events):
            event = self.generate_event()
            events.append(event)
            
            if delay > 0:
                time.sleep(delay)
        
        print(f"[Sensor Simulator] Generated {len(events)} events")
        return events


def run_stream_analytics_demo(input_file: str = "plant_health_data.csv", 
                              output_dir: str = "powerbi_exports") -> Dict:
    """
    Demonstrate stream analytics capabilities.
    
    Args:
        input_file: Path to historical data
        output_dir: Output directory
        
    Returns:
        Analytics results
    """
    print("\n" + "="*70)
    print("STREAM ANALYTICS DEMONSTRATION")
    print("="*70)
    
    # Load historical data
    df = pd.read_csv(input_file)
    
    # Initialize components
    simulator = SensorDataSimulator(df)
    processor = StreamProcessor()
    
    # Create windows
    processor.create_window('recent_100', window_size=100, window_type='count')
    processor.create_window('last_5min', window_size=300, window_type='time')
    
    # Add filter: only process high stress events
    processor.add_filter(lambda event: event.get('Soil_Moisture', 100) < 25)
    
    # Generate and process stream
    events = simulator.generate_stream(num_events=500)
    processed_events = processor.process_batch(events)
    
    # Get statistics
    stats = processor.get_all_stats()
    
    # Export time series data
    os.makedirs(output_dir, exist_ok=True)
    
    # Convert events to DataFrame
    events_df = pd.DataFrame(processed_events)
    time_series_file = os.path.join(output_dir, "time_series.csv")
    events_df.to_csv(time_series_file, index=False)
    
    print(f"\n[Stream Analytics] Exported time series to: {time_series_file}")
    print(f"[Stream Analytics] Window statistics: {stats}")
    
    print("="*70)
    print("STREAM ANALYTICS COMPLETED")
    print("="*70 + "\n")
    
    return {
        'total_events': len(events),
        'processed_events': len(processed_events),
        'filter_rate': len(processed_events) / len(events) if events else 0,
        'stats': stats,
        'output_file': time_series_file
    }


if __name__ == "__main__":
    # Test stream processing
    if os.path.exists("plant_health_data.csv"):
        results = run_stream_analytics_demo()
        print(f"\nStream Analytics Results:")
        print(json.dumps(results, indent=2, default=str))
    else:
        print("Data file not found")
