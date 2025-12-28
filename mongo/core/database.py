#!/usr/bin/env python3
"""
    MongoDB Database Module
"""

from pymongo import MongoClient
from typing import Dict, List, Any, Optional
import time

class MongoDBConnection:
    """Соединение с MongoDB"""
    
    def __init__(self, uri: str = "mongodb://admin:admin123@localhost:27017/?authSource=admin", 
                 database: str = "ecommerce"):
        self.uri = uri
        self.database_name = database
        self.client: Optional[MongoClient] = None
        self.db = None
    
    def connect(self) -> None:
        """Установить соединение"""
        if self.client is None:
            self.client = MongoClient(self.uri)
            self.db = self.client[self.database_name]
    
    def close(self) -> None:
        """Закрыть соединение"""
        if self.client:
            self.client.close()
            self.client = None
            self.db = None
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def get_collection(self, name: str):
        """Получить коллекцию"""
        if not self.db:
            raise RuntimeError("Database not connected")
        return self.db[name]

class QueryResult:
    """Результат выполнения запроса"""
    
    def __init__(self, documents: List[Dict[str, Any]], execution_time_ms: float, 
                 count: Optional[int] = None, query_info: Optional[str] = None):
        self.documents = documents
        self.execution_time_ms = execution_time_ms
        self.count = count or len(documents)
        self.query_info = query_info
    
    @property
    def execution_time_sec(self) -> float:
        return self.execution_time_ms / 1000.0
    
    def __len__(self) -> int:
        return self.count

class MongoDBBaseOperations:
    """Базовые операции MongoDB"""
    
    def __init__(self, connection: MongoDBConnection):
        self.connection = connection
    
    def find(self, collection: str, query: Dict[str, Any], 
             options: Optional[Dict[str, Any]] = None) -> QueryResult:
        """Выполнить find запрос"""
        start_time = time.time()
        coll = self.connection.get_collection(collection)
        
        if options and 'limit' in options:
            cursor = coll.find(query).limit(options['limit'])
        else:
            cursor = coll.find(query)
        
        documents = list(cursor)
        execution_time = time.time() - start_time
        
        return QueryResult(documents, execution_time * 1000, query_info=str(query))
    
    def aggregate(self, collection: str, pipeline: List[Dict[str, Any]]) -> QueryResult:
        """Выполнить агрегацию"""
        start_time = time.time()
        coll = self.connection.get_collection(collection)
        
        documents = list(coll.aggregate(pipeline))
        execution_time = time.time() - start_time
        
        return QueryResult(documents, execution_time * 1000, 
                          query_info=f"Aggregation with {len(pipeline)} stages")
    
    def insert_many(self, collection: str, documents: List[Dict[str, Any]], 
                    batch_size: int = 1000) -> QueryResult:
        """Массовая вставка документов"""
        start_time = time.time()
        coll = self.connection.get_collection(collection)
        
        # Очистка коллекции перед загрузкой
        coll.delete_many({})
        
        # Пакетная вставка
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i + batch_size]
            coll.insert_many(batch)
        
        execution_time = time.time() - start_time
        
        return QueryResult([], execution_time * 1000, 
                          count=len(documents), 
                          query_info=f"Inserted {len(documents)} docs")
    
    def create_indexes(self, collection: str, indexes: List[Dict[str, Any]]) -> QueryResult:
        """Навешивание индексов"""
        start_time = time.time()
        coll = self.connection.get_collection(collection)
        
        index_names = []
        for index_spec in indexes:
            if isinstance(index_spec, dict) and 'keys' in index_spec:
                result = coll.create_index(index_spec['keys'])
                index_names.append(result)
            else:
                result = coll.create_index(index_spec)
                index_names.append(result)
        
        execution_time = time.time() - start_time
        
        return QueryResult([], execution_time * 1000,
                          count=len(index_names),
                          query_info=f"Created indexes: {index_names}")
    
    def get_collection_stats(self, collection: str) -> Dict[str, Any]:
        """Получить статистику по коллекции MongoDB"""
        coll = self.connection.get_collection(collection)
        stats = coll.database.command("collstats", collection)
        return stats
    
    def explain_query(self, collection: str, query: Dict[str, Any]) -> Dict[str, Any]:
        """Объяснение плана выполнения запроса MongoDB"""
        coll = self.connection.get_collection(collection)
        return coll.find(query).limit(1).explain()
