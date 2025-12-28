#!/usr/bin/env python3
"""
Отображение индексов - утилита для анализа производительности
"""

import sys
from pathlib import Path

# Добавляем core в Python path
current_dir = Path(__file__).parent.parent
sys.path.append(str(current_dir / "core"))

from core.database import MongoDBConnection
from core.database import MongoDBBaseOperations
from core.models import StatisticsHelper

def main():
    """Анализ индексов MongoDB"""
    
    with MongoDBConnection() as db_conn:
        db_ops = MongoDBBaseOperations(db_conn)
        
        print("=" * 60)
        print(" АНАЛИЗ ИНДЕКСОВ MONGODB")
        print("=" * 60)
        
        # Индексы для categories
        print("\n Индексы коллекции categories:")
        categories_indexes = db_ops.get_collection("categories").list_indexes()
        
        for idx in categories_indexes:
            keys = ', '.join([f"{k[0]}: {k[1]}" for k in idx['key']])
            print(f"   • {idx['name']}: {keys}")
        
        print("\n Индексы коллекции products:")
        products_indexes = db_ops.get_collection("products").list_indexes()
        
        for idx in products_indexes:
            keys = ', '.join([f"{k[0]}: {k[1]}" for k in idx['key']])
            print(f"   • {idx['name']}: {keys}")
        
        # Статистика размеров
        print("\n Статистика размеров:")
        
        for collection in ["categories", "products"]:
            stats = db_ops.get_collection_stats(collection)
            
            size_mb = stats["size"] / 1024**2
            indexes_mb = stats["totalIndexSize"] / 1024**2
            pct = (indexes_mb / size_mb) * 100
            
            print(f"\n {collection.capitalize()}:")
            print(f"   • Данные: {size_mb:.2f} MB")
            print(f"   • Индексы: {indexes_mb:.2f} MB ({pct:.1f}%)")
            print(f"   • Документов: {StatisticsHelper.format_number(stats['count'])}")
        
        total_categories_stats = db_ops.get_collection_stats("categories")
        total_products_stats = db_ops.get_collection_stats("products")
        
        total_data_mb = (total_categories_stats["size"] + total_products_stats["size"]) / 1024**2
        total_indexes_mb = (total_categories_stats["totalIndexSize"] + total_products_stats["totalIndexSize"]) / 1024**2
        total_pct = (total_indexes_mb / total_data_mb) * 100
        
        print(f"\n Общая статистика:")
        print(f"   • Данных: {total_data_mb:.2f} MB")
        print(f"   • Индексов: {total_indexes_mb:.2f} MB ({total_pct:.1f}%)")
        
        # Рекомендации
        print("\n Рекомендации:")
        if 5 <= total_pct <= 25:
            print("    Процент индексов в оптимальном диапазоне (5-25%)")
        else:
            print("    Процент индексов вне оптимального диапазона")

if __name__ == "__main__":
    main()
