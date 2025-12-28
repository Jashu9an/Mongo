#!/usr/bin/env python3
"""
    MongoDB Analytics Aggregations Script
"""

import sys
from pathlib import Path

# Добавляем core в Python path
current_dir = Path(__file__).parent.parent
sys.path.append(str(current_dir / "core"))

from core.database import MongoDBConnection
from core.database import MongoDBBaseOperations
from core.services import AnalyticsService
from core.models import StatisticsHelper

def print_section(title: str) -> None:
    """Печать заголовка раздела MongoDB"""
    print(f"\n{'='*60}")
    print(f" {title}")
    print(f"{'='*60}")

def print_aggregation_result(pipeline_name: str, result, stages_count: int = 3) -> None:
    """Форматированный вывод результата агрегации MongoDB"""
    print(f"\n {pipeline_name}")
    print(f"    Pipeline: {stages_count} stages")
    print(f"    Результаты:")
    print(f"      • Документов: {StatisticsHelper.format_number(len(result.documents))}")
    print(f"      • Время: {StatisticsHelper.format_time(result.execution_time_ms)}")

def format_top_categories_table(documents, limit: int = 10) -> str:
    """Форматирование таблицы топ категорий MongoDB"""
    if not documents:
        return "Нет данных"
    
    table = f"{'#':>3} | {'ID':>8} | {'Товаров':^8} | {'Ур.':>3} | {'Категория'}\n"
    table += f"{'-'*3} | {'-'*8} | {'-'*8} | {'-'*3} | {'-'*40}\n"
    
    for i, cat in enumerate(documents[:limit], 1):
        category_id = str(cat.get('category_id', 'N/A'))[:8]
        products = cat.get('products', cat.get('total_products', 0))
        
        # Форматирование количества товаров
        if isinstance(products, int):
            products_str = f"{products:,}" if products < 1000 else str(products)
        else:
            products_str = str(products)
        
        output = f"{i:>3} | {category_id:>8} | {products_str:>8} | {cat.get('level', '?'):>3} | {cat.get('name', 'N/A')}"
        
        # Если имя категории длинное, обрезаем с многоточием
        if len(output) > 70:
            name_short = (f"{cat.get('name', 'N/A')[:35]}...")
            output = f"{i:>3} | {category_id:>8} | {products_str:>8} | {cat.get('level', '?'):>3} | {name_short}"
        
        table += output + "\n"
    
    return table.rstrip()

def format_categories_by_level(documents) -> str:
    """Форматирование категорий по уровням MongoDB"""
    if not documents:
        return "Нет данных"
    
    table = f"{"Название категории":^45} | {"Товаров":>10}\n"
    table += f"{'-'*45} | {'-'*10}\n"
    
    for doc in documents[:15]:  # Показываем первые 15
        name = doc.get('_id', {}).get('name', 'N/A')
        products = doc.get('products', 0)
        
        name_short = name[:42] + "..." if len(name) > 42 else name
        table += f"{name_short:<45} | {products:>10,}\n"
    
    return table

def execute_aggregations_part_3_1():
    """Выполнение агрегаций Части 3.1: Агрегационный фреймворк"""
    
    with MongoDBConnection() as db_conn:
        db_ops = MongoDBBaseOperations(db_conn)
        analytics = AnalyticsService(db_ops)
        
        # АГРЕГАЦИЯ 1: Топ-10 категорий по товарам
        print_section("АГРЕГАЦИЯ 1: Топ-10 категорий по количеству товаров")
        
        pipeline1 = [
            {"$group": {
                "_id": "$category_id",
                "category_name": {"$first": "$name"},
                "path": {"$first": "$path"},
                "total_products": {"$sum": "$metadata.total_products"},
                "level": {"$first": "$level"},
                "partner": {"$first": "$partner"}
            }},
            {"$sort": {"total_products": -1}},
            {"$limit": 10},
            {"$project": {
                "_id": "$_id",
                "name": "$category_name",
                "path": "$path", 
                "products": "$total_products",
                "level": "$level",
                "partner": "$partner"
            }}
        ]
        
        # Выполняем напрямую через db_ops для демонстрации pipeline
        result1 = db_ops.aggregate("categories", pipeline1)
        
        print_aggregation_result("Топ-10 категорий по количеству товаров", result1, 4)
        
        # Вывод таблицы
        if result1.documents:
            print(f"\n Топ-10 категорий:")
            print(format_top_categories_table(result1.documents))
        
        # АГРЕГАЦИЯ 2: Иерархическая статистика
        print_section("АГРЕГАЦИЯ 2: Иерархическая статистика по уровням")
        
        result2 = analytics.get_hierarchy_stats()
        
        print_aggregation_result("Распределение товаров по уровням и категориям (unwind + group)", result2, 4)
        
        if result2.documents:
            print(f"\n Распределение по уровням (топ по товарам):")
            
            # Группируем по уровням для удобного вывода
            by_level = {}
            for doc in result2.documents:
                level = doc.get('level', 0)
                if level not in by_level:
                    by_level[level] = []
                by_level[level].append(doc)
            
            for level in sorted(by_level.keys()):
                print(f"\n УРОВЕНЬ {level}:")
                print(format_categories_by_level(by_level[level]))
        
        # АГРЕГАЦИЯ 3: Категории-'листья'
        print_section("АГРЕГАЦИЯ 3: Категории-'листья' (конечные узлы)")
        
        result3 = analytics.find_leaf_categories(10)
        
        print_aggregation_result("Поиск категорий без подкатегорий (lookup + match)", result3, 5)
        
        if result3.documents:
            print(f"\n Топ-10 категорий-'листьев':")
            print(f"{'#':>3} | {'Ур.':>3} | {'Товаров':^10} | {'Категория'}")
            print(f"{'-'*3} | {'-'*3} | {'-'*10} | {'-'*40}")
            
            for i, leaf in enumerate(result3.documents, 1):
                name = leaf.get('name', 'N/A')
                name_short = (name[:37] + "...") if len(name) > 37 else name
                products = leaf.get('products', 0)
                
                print(f"{i:>3} | {leaf.get('level', '?'):>3} | {products:>10,} | {name_short}")
        
            # ДОПОЛНИТЕЛЬНАЯ АНАЛИТИКА: Партнеры и уровни MongoDB
        print_section("ДОПОЛНИТЕЛЬНАЯ АНАЛИТИКА: Распределение по партнерам и уровням")
        
        result4 = analytics.get_partner_stats()
        
        print_aggregation_result("Статистика по партнерам и уровням", result4, 2)
        
        if result4.documents:
            print(f"\n Распределение категорий и товаров:")
            print(f"{'Партнер':>10} | {'Ур.':>3} | {'Категорий':^10} | {'Товаров':^12}")
            print(f"{'-'*10} | {'-'*3} | {'-'*10} | {'-'*12}")
            
            for stat in result4.documents:
                partner = stat.get('partner', 'N/A')
                level = stat.get('level', 0)
                categories = stat.get('categories_count', 0)
                products = stat.get('total_products', 0)
                
                products_str = f"{products:,}" if isinstance(products, int) else str(products)
                print(f"{partner:>10} | {level:>3} | {categories:>10,} | {products_str:>12}")

def main():
    """Основная функция"""
    
    print_section("ЧАСТЬ 3.1: АГРЕГАЦИОННЫЙ ФРЕЙМВОРК ПО КАТЕГОРИЯМ")
    
    try:
        execute_aggregations_part_3_1()
        
        print_section("ИТОГИ ЧАСТИ 3.1")
        print("    Агрегационный фреймворк эффективно анализирует иерархические данные")
        print("    $unwind + $group обеспечивает мощную аналитику по уровням")
        print("    $lookup позволяет находить категориальные отношения")
        print("    Сложные аналитические запросы выполняются за секунды")
        print("    MongoDB идеально подходит для иерархических big data")
        
    except Exception as e:
        print(f"\n Ошибка выполнения: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
