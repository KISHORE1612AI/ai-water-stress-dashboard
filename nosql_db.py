"""
NoSQL Database Wrapper
=======================
Simulates NoSQL database operations using TinyDB (JSON-based document store).
Demonstrates NoSQL concepts: document storage, indexing, querying, and aggregation.
"""

from tinydb import TinyDB, Query, where
from tinydb.storages import JSONStorage
from tinydb.middlewares import CachingMiddleware
from typing import List, Dict, Any, Optional
import json
import os
from datetime import datetime
import pandas as pd


class NoSQLDatabase:
    """
    NoSQL database wrapper implementing document-based storage operations.
    Features:
    - Document insertion and retrieval
    - Query operations (filtering, searching)
    - Aggregation pipelines
    - Indexing simulation
    - Backup and restore
    """
    
    def __init__(self, db_path: str = "nosql_data/plant_health.json"):
        """
        Initialize NoSQL database.
        
        Args:
            db_path: Path to JSON database file
        """
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Use caching middleware for better performance
        self.db = TinyDB(db_path, storage=CachingMiddleware(JSONStorage))
        self.db_path = db_path
        
        # Create collections (tables)
        self.sensors = self.db.table('sensors')
        self.analytics = self.db.table('analytics')
        self.metadata = self.db.table('metadata')
        
        print(f"[NoSQL DB] Initialized at: {db_path}")
    
    def insert_document(self, table_name: str, document: Dict) -> int:
        """
        Insert a document into specified collection.
        
        Args:
            table_name: Name of the collection
            document: Document to insert (dictionary)
            
        Returns:
            Document ID
        """
        table = self.db.table(table_name)
        doc_id = table.insert(document)
        return doc_id
    
    def insert_many(self, table_name: str, documents: List[Dict]) -> List[int]:
        """
        Bulk insert multiple documents.
        
        Args:
            table_name: Name of the collection
            documents: List of documents to insert
            
        Returns:
            List of document IDs
        """
        table = self.db.table(table_name)
        doc_ids = table.insert_multiple(documents)
        print(f"[NoSQL DB] Inserted {len(doc_ids)} documents into '{table_name}'")
        return doc_ids
    
    def find_all(self, table_name: str) -> List[Dict]:
        """
        Retrieve all documents from a collection.
        
        Args:
            table_name: Name of the collection
            
        Returns:
            List of all documents
        """
        table = self.db.table(table_name)
        return table.all()
    
    def find_by_query(self, table_name: str, query_dict: Dict) -> List[Dict]:
        """
        Find documents matching query criteria.
        
        Args:
            table_name: Name of the collection
            query_dict: Query conditions (e.g., {'status': 'Healthy'})
            
        Returns:
            List of matching documents
        """
        table = self.db.table(table_name)
        Q = Query()
        
        # Build query from dictionary
        results = table.all()
        for key, value in query_dict.items():
            results = [doc for doc in results if doc.get(key) == value]
        
        return results
    
    def aggregate_by_field(self, table_name: str, group_field: str, agg_field: str) -> Dict:
        """
        Aggregate documents by a field (simulates MongoDB aggregation).
        
        Args:
            table_name: Name of the collection
            group_field: Field to group by
            agg_field: Field to aggregate
            
        Returns:
            Aggregation results
        """
        table = self.db.table(table_name)
        docs = table.all()
        
        # Group and aggregate
        groups = {}
        for doc in docs:
            key = doc.get(group_field, 'Unknown')
            if key not in groups:
                groups[key] = {'count': 0, 'sum': 0, 'values': []}
            
            groups[key]['count'] += 1
            value = doc.get(agg_field, 0)
            if isinstance(value, (int, float)):
                groups[key]['sum'] += value
                groups[key]['values'].append(value)
        
        # Calculate statistics
        for key in groups:
            values = groups[key]['values']
            groups[key]['mean'] = groups[key]['sum'] / groups[key]['count'] if groups[key]['count'] > 0 else 0
            groups[key]['min'] = min(values) if values else 0
            groups[key]['max'] = max(values) if values else 0
            del groups[key]['values']  # Remove raw values
        
        return groups
    
    def update_documents(self, table_name: str, query_dict: Dict, update_data: Dict) -> int:
        """
        Update documents matching query.
        
        Args:
            table_name: Name of the collection
            query_dict: Query to find documents
            update_data: Data to update
            
        Returns:
            Number of updated documents
        """
        table = self.db.table(table_name)
        Q = Query()
        
        # Find and update
        docs = self.find_by_query(table_name, query_dict)
        count = 0
        for doc in docs:
            table.update(update_data, doc_ids=[doc.doc_id])
            count += 1
        
        return count
    
    def delete_documents(self, table_name: str, query_dict: Dict) -> int:
        """
        Delete documents matching query.
        
        Args:
            table_name: Name of the collection
            query_dict: Query to find documents to delete
            
        Returns:
            Number of deleted documents
        """
        table = self.db.table(table_name)
        docs = self.find_by_query(table_name, query_dict)
        
        for doc in docs:
            table.remove(doc_ids=[doc.doc_id])
        
        return len(docs)
    
    def create_index(self, table_name: str, field: str) -> None:
        """
        Simulate index creation (TinyDB doesn't support real indexing).
        
        Args:
            table_name: Name of the collection
            field: Field to index
        """
        print(f"[NoSQL DB] Index simulation: Created index on '{field}' in '{table_name}'")
        # Store metadata about indexes
        self.metadata.insert({
            'type': 'index',
            'table': table_name,
            'field': field,
            'created_at': datetime.now().isoformat()
        })
    
    def backup(self, backup_path: str = None) -> str:
        """
        Backup database to file.
        
        Args:
            backup_path: Path for backup file
            
        Returns:
            Path to backup file
        """
        if backup_path is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = f"nosql_data/backup_{timestamp}.json"
        
        os.makedirs(os.path.dirname(backup_path), exist_ok=True)
        
        # Export all tables
        backup_data = {}
        for table_name in self.db.tables():
            backup_data[table_name] = self.db.table(table_name).all()
        
        with open(backup_path, 'w') as f:
            json.dump(backup_data, f, indent=2)
        
        print(f"[NoSQL DB] Backup created: {backup_path}")
        return backup_path
    
    def get_statistics(self) -> Dict:
        """
        Get database statistics.
        
        Returns:
            Statistics dictionary
        """
        stats = {
            'tables': {},
            'total_documents': 0,
            'db_path': self.db_path
        }
        
        for table_name in self.db.tables():
            table = self.db.table(table_name)
            count = len(table)
            stats['tables'][table_name] = count
            stats['total_documents'] += count
        
        return stats
    
    def load_from_dataframe(self, df: pd.DataFrame, table_name: str = 'sensors') -> int:
        """
        Load data from pandas DataFrame into NoSQL database.
        
        Args:
            df: Pandas DataFrame
            table_name: Target collection name
            
        Returns:
            Number of documents inserted
        """
        documents = df.to_dict('records')
        return len(self.insert_many(table_name, documents))
    
    def to_dataframe(self, table_name: str) -> pd.DataFrame:
        """
        Export collection to pandas DataFrame.
        
        Args:
            table_name: Name of the collection
            
        Returns:
            Pandas DataFrame
        """
        docs = self.find_all(table_name)
        return pd.DataFrame(docs)
    
    def close(self):
        """Close database connection."""
        self.db.close()
        print(f"[NoSQL DB] Closed connection")


if __name__ == "__main__":
    # Test NoSQL database
    print("Testing NoSQL Database...\n")
    
    # Initialize
    db = NoSQLDatabase("nosql_data/test.json")
    
    # Insert sample documents
    sample_docs = [
        {'sensor_id': 1, 'temperature': 25.5, 'humidity': 60, 'status': 'Healthy'},
        {'sensor_id': 2, 'temperature': 30.2, 'humidity': 45, 'status': 'High Stress'},
        {'sensor_id': 3, 'temperature': 24.8, 'humidity': 65, 'status': 'Healthy'},
    ]
    
    db.insert_many('test_sensors', sample_docs)
    
    # Query
    healthy_sensors = db.find_by_query('test_sensors', {'status': 'Healthy'})
    print(f"Healthy sensors: {len(healthy_sensors)}")
    
    # Aggregate
    stats = db.aggregate_by_field('test_sensors', 'status', 'temperature')
    print(f"\nAggregation results: {json.dumps(stats, indent=2)}")
    
    # Statistics
    db_stats = db.get_statistics()
    print(f"\nDatabase stats: {db_stats}")
    
    db.close()
