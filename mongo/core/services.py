#!/usr/bin/env python3
"""
    MongoDB Services Module
"""

from typing import Dict, Any, List, Tuple
import pandas as pd

from ..core.database import MongoDBBaseOperations, QueryResult
from ..core.models import (
    CategoryModel, ProductModel, QueryTemplates, 
    StatisticsHelper, IndexSpecification
)

class DataLoaderService:
    """Сервис загрузки данных MongoDB"""
    
    def __init__(self, db_ops: MongoDBBaseOperations):
        self.db_ops = db_ops
    
    def load_categories(self, parquet_path: str) -> Tuple[QueryResult, Dict[str, Any]]:
        """Загрузка категорий с materialized path"""
        df = pd.read_parquet(parquet_path)
        
        # Извлечение уникальных категорий из DataFrame
        categories_df = df[['Partner_Name', 'Category_ID', 'Category_FullPathName']].drop_duplicates()
        
        # Обработка иерархии категорий
        categories_df['path_array'] = categories_df['Category_FullPathName'].str.split('\\')
        categories_df['level'] = categories_df['path_array'].apply(len)
        categories_df['name'] = categories_df['path_array'].apply(lambda x: x[-1])
        
        # Parent path
        def get_parent_path(path_array):
            return '\\'.join(path_array[:-1]) if len(path_array) > 1 else None
        
        categories_df['parent_path'] = categories_df['path_array'].apply(get_parent_path)
        
        # Подсчет товаров в категориях
        product_counts = df.groupby(['Partner_Name', 'Category_ID']).size().reset_index(name='total_products')
        categories_df = categories_df.merge(product_counts, on=['Partner_Name', 'Category_ID'])
        
        # Создание документов MongoDB
        documents = []
        for _, row in categories_df.iterrows():
            doc = {
                "_id": f"{row['Partner_Name']}_{row['Category_ID']}",
                "partner": row['Partner_Name'],
                "category_id": row['Category_ID'],
                "name": row['name'],
                "path": row['Category_FullPathName'].replace('\\', '/'),
                "path_array": row['path_array'],
                "level": row['level'],
                "parent_path": row['parent_path'].replace('\\', '/') if row['parent_path'] else None,
                "metadata": {
                    "total_products": int(row['total_products']),
                    "last_updated": pd.Timestamp.utcnow().isoformat()
                }
            }
            documents.append(doc)
        
        result = self.db_ops.insert_many("categories", documents)
        
        stats = {
            'total_categories': len(documents),
            'max_depth': categories_df['level'].max(),
            'avg_depth': categories_df['level'].mean()
        }
        
        return result, stats
    
    def load_products(self, parquet_path: str) -> Tuple[QueryResult, Dict[str, Any]]:
        """Загрузка товаров с embedded documents"""
        df = pd.read_parquet(parquet_path)
        
        # Создание документов с хлебными крошками
        documents = []
        for _, row in df.iterrows():
            path_array = row['Category_FullPathName'].str.split('\\').tolist()
            
            breadcrumbs = [
                {"level": i+1, "name": name}
                for i, name in enumerate(path_array, 1)
            ]
            
            doc = {
                "_id": f"{row['Partner_Name']}_{row['Offer_ID']}",
                "partner": row['Partner_Name'],
                "offer_id": row['Offer_ID'],
                "name": row['Offer_Name'],
                "type": row['Offer_Type'],
                "category": {
                    "id": row['Category_ID'],
                    "name": path_array[-1],
                    "full_path": row['Category_FullPathName'].replace('\\', '/'),
                    "breadcrumbs": breadcrumbs
                }
            }
            documents.append(doc)
        
        result = self.db_ops.insert_many("products", documents)
        
        stats = {
            'total_products': len(documents),
            'unique_types': df['Offer_Type'].nunique(),
            'partners': df['Partner_Name'].nunique()
        }
        
        return result, stats

class IndexingService:
    """Сервис управления индексами MongoDB"""
    
    def __init__(self, db_ops: MongoDBBaseOperations):
        self.db_ops = db_ops
    
    def create_all_indexes(self) -> Dict[str, QueryResult]:
        """Создание всех индексов для обеих коллекций MongoDB"""
        
        # Индексы для categories
        categories_indexes = [
            ("path", "text"),  # текстовый индекс
            ("path_array", 1),  # восходящий
            ("partner", 1, "level", 1),  # составной
            ("metadata.total_products", -1)  # нисходящий
        ]
        
        # Индексы для products  
        products_indexes = [
            ("partner", 1, "category.id", 1),
            ("category.breadcrumbs.name", 1),
            ("type", 1, "partner", 1),
            ("offer_id", 1)
        ]
        
        results = {}
        
        # Индексы для categories
        cats_result = self.db_ops.create_indexes("categories", categories_indexes)
        results['categories'] = cats_result
        
        # Индексы для products
        prods_result = self.db_ops.create_indexes("products", products_indexes)
        results['products'] = prods_result
        
        return results

class CategoryQueryService:
    """Сервис запросов к категориям MongoDB"""
    
    def __init__(self, db_ops: MongoDBBaseOperations):
        self.db_ops = db_ops
    
    def find_root_categories(self, partner: str = "_ozon") -> QueryResult:
        """Найти корневые категории партнера MongoDB"""
        query = {"partner": partner, "level": 1}
        return self.db_ops.find("categories", query)
    
    def find_subcategories(self, parent_name: str) -> QueryResult:
        """Найти подкатегории (используя path_array) MongoDB"""
        query = {"path_array": parent_name}
        return self.db_ops.find("categories", query)
    
    def get_top_categories(self, limit: int = 10) -> QueryResult:
        """Топ категорий по количеству товаров"""
        pipeline = [
            {"$sort": {"metadata.total_products": -1}},
            {"$limit": limit},
            {"$project": {"name": 1, "metadata.total_products": 1, "level": 1, "partner": 1}}
        ]
        return self.db_ops.aggregate("categories", pipeline)

class ProductQueryService:
    """Сервис запросов к товарам (SRP)"""
    
    def __init__(self, db_ops: MongoDBBaseOperations):
        self.db_ops = db_ops
    
    def find_products_by_type_and_category(self, product_type: str, 
                                          breadcrumb_name: str) -> QueryResult:
        """Поиск товаров по типу и хлебными крошками"""
        query = {
            "type": product_type,
            "category.breadcrumbs.name": breadcrumb_name
        }
        return self.db_ops.find("products", query)
    
    def get_products_by_level(self, level: int) -> QueryResult:
        """Товары определенного уровня иерархии MongoDB"""
        query = {
            f"category.breadcrumbs.{level-1}": {"$exists": True},
            f"category.breadcrumbs.{level}": {"$exists": False}
        }
        return self.db_ops.find("products", query)
    
    def aggregate_by_first_level_categories(self) -> QueryResult:
        """Агрегация по категориям 1-го уровня MongoDB"""
        pipeline = [
            {"$match": {"category.breadcrumbs.0": {"$exists": True}}},
            {"$project": {"first_level": {"$arrayElemAt": ["$category.breadcrumbs.name", 0]}}},
            {"$group": {"_id": "$first_level", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10},
            {"$project": {"category_name": "$_id", "product_count": "$count", "_id": 0}}
        ]
        return self.db_ops.aggregate("products", pipeline)

class AnalyticsService:
    """Сервис аналитики MongoDB"""
    
    def __init__(self, db_ops: MongoDBBaseOperations):
        self.db_ops = db_ops
    
    def get_hierarchy_stats(self) -> QueryResult:
        """Статистика по уровням иерархии MongoDB"""
        pipeline = [
            {"$unwind": "$path_array"},
            {"$group": {"_id": {"level": "$level", "name": "$path_array"}, "products": {"$sum": "$metadata.total_products"}}},
            {"$sort": {"level": 1, "products": -1}},
            {"$limit": 30}
        ]
        return self.db_ops.aggregate("categories", pipeline)
    
    def find_leaf_categories(self, limit: int = 10) -> QueryResult:
        """Поиск категорий-листьев (без подкатегорий) MongoDB"""
        pipeline = [
            {"$lookup": {"from": "categories", "localField": "path", "foreignField": "parent_path", "as": "children"}},
            {"$match": {"children.0": {"$exists": False}}},
            {"$project": {"name": 1, "level": 1, "products": "$metadata.total_products"}},
            {"$sort": {"products": -1}},
            {"$limit": limit}
        ]
        return self.db_ops.aggregate("categories", pipeline)
    
    def get_partner_stats(self) -> QueryResult:
        """Статистика по партнерам и уровням MongoDB"""
        pipeline = [
            {"$group": {"_id": {"partner": "$partner", "level": "$level"}, "categories": {"$sum": 1}, "products": {"$sum": "$metadata.total_products"}}},
            {"$sort": {"_id.partner": 1, "_id.level": 1}}
        ]
        return self.db_ops.aggregate("categories", pipeline)
