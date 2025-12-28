#!/usr/bin/env python3
"""
Загрузка данных вMongoDB - Рефактор версия
SOLID: Single Responsibility - каждый模块 один responsibility
KISS: Простота и понятность кода
OOP: Объектно-ориентированный подход
DRY: Избегание дублирования кода
"""

import sys
import os
from pathlib import Path

# Добавляем core в Python path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir / "core"))

from core.database import MongoDBConnection
from core.services import DataLoaderService, IndexingService
from core.models import StatisticsHelper

def print_section(title: str) -> None:
    """Печать раздела с оформлением"""
    print(f"\n{'='*60}")
    print(f" {title}")
    print(f"{'='*60}")

def print_result(description: str, result, additional_stats: dict = None) -> None:
   """Форматированный вывод результата"""
    print(f"\n {description}")
    print(f"   • Время: {StatisticsHelper.format_time(result.execution_time_ms)}")
    print(f"   • Количество: {StatisticsHelper.format_number(result.count)}")
    
    if additional_stats:
        for key, value in additional_stats.items():
            print(f"   • {key}: {value}")

def main():
    """Основная функция загрузки данных"""
    print_section("ЗАГРУЗКА ДАННЫХ MONGODB")
    
    # Путь к данным
    parquet_path = "C:/VSCode projects/Databases/clickhouse-mongo-subd/SnapShotForMongoDB/ozon_inference_2025_10_17_offers_2025_10_17.pq"
    
    # Подключение к базе данных
    with MongoDBConnection() as db_conn:
        from core.database import MongoDBBaseOperations
        db_ops = MongoDBBaseOperations(db_conn)
        
        # Инициализация сервисов
        data_loader = DataLoaderService(db_ops)
        index_service = IndexingService(db_ops)
        
        # Загрузка категорий
        print_section("ЗАГРУЗКА КОЛЛЕКЦИИ CATEGORIES")
        print(f" Источник: {os.path.basename(parquet_path)}")
        
        print(" Создание документов категорий...")
        categories_result, categories_stats = data_loader.load_categories(parquet_path)
        
        print_result("Коллекция categories загружена", categories_result, categories_stats)
        
        # Загрузка товаров
        print_section("ЗАГРУЗКА КОЛЛЕКЦИИ PRODUCTS")
        
        print(" Создание документов товаров...")
        products_result, products_stats = data_loader.load_products(parquet_path)
        
        print_result("Коллекция products загружена", products_result, products_stats)
        
        # Создание индексов
        print_section("СОЗДАНИЕ ИНДЕКСОВ")
        
        print(" Создание индексов для оптимизации...")
        index_results = index_service.create_all_indexes()
        
        for collection, result in index_results.items():
            print_result(f"Индексы коллекции {collection}", result)
        
        # Итоговая статистика
        print_section("ИТОГОВАЯ СТАТИСТИКА")
        
        # Получаем статистику коллекций
        categories_stats = db_ops.get_collection_stats("categories")
        products_stats = db_ops.get_collection_stats("products")
        
        total_categories_size = categories_stats["size"] / 1024**2
        total_categories_indexes = categories_stats["totalIndexSize"] / 1024**2
        categories_pct = (total_categories_indexes / total_categories_size) * 100
        
        total_products_size = products_stats["size"] / 1024**2
        total_products_indexes = products_stats["totalIndexSize"] / 1024**2
        products_pct = (total_products_indexes / total_products_size) * 100
        
        print(f" Categories:")
        print(f"   • Документов: {StatisticsHelper.format_number(categories_stats['count'])}")
        print(f"   • Данные: {total_categories_size:.2f} MB")
        print(f"   • Индексы: {total_categories_indexes:.2f} MB ({categories_pct:.1f}%)")
        
        print(f" Products:")
        print(f"   • Документов: {StatisticsHelper.format_number(products_stats['count'])}")
        print(f"   • Данные: {total_products_size:.2f} MB")
        print(f"   • Индексы: {total_products_indexes:.2f} MB ({products_pct:.1f}%)")
        
        total_size_mb = total_categories_size + total_categories_indexes + total_products_size + total_products_indexes
        print(f"\n ИТОГО:")
        print(f"   • Общий размер базы: {total_size_mb:.2f} MB")
        print(f"   • Общий процент индексов: {((total_categories_indexes + total_products_indexes) / (total_categories_size + total_products_size) * 100):.1f}%")
        
        if ((total_categories_indexes + total_products_indexes) / (total_categories_size + total_products_size) * 100) < 25:
            print("    Процент индексов в оптимальном диапазоне (5-25%)")
        else:
            print("    Процент индексов выше рекомендуемого диапазона")

if __name__ == "__main__":
    try:
        main()
        print(f"\n Загрузка данных успешно завершена!")
    except Exception as e:
        print(f"\n Ошибка: {e}")
        sys.exit(1)
