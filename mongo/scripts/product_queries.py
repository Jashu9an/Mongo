#!/usr/bin/env python3
"""
    MongoDB Product Queries Script
"""

import sys
from pathlib import Path

# Добавляем core в Python path
current_dir = Path(__file__).parent.parent
sys.path.append(str(current_dir / "core"))

from core.database import MongoDBConnection
from core.database import MongoDBBaseOperations
from core.services import ProductQueryService
from core.models import StatisticsHelper

def print_section(title: str) -> None:
    """Печать заголовка раздела MongoDB"""
    print(f"\n{'='*60}")
    print(f" {title}")
    print(f"{'='*60}")

def print_query_result(query_desc: str, result, extra_info: dict = None) -> None:
    """Форматированный вывод результата MongoDB"""
    print(f"\n {query_desc}")
    print(f"    Результаты:")
    print(f"      • Документов: {StatisticsHelper.format_number(len(result.documents))}")
    print(f"      • Время: {StatisticsHelper.format_time(result.execution_time_ms)}")
    
    if extra_info:
        for key, value in extra_info.items():
            print(f"      • {key}: {value}")

def format_breadcrumbs(breadcrumbs: list) -> str:
    """Форматирование навигационной цепочки MongoDB"""
    if not breadcrumbs:
        return "N/A"
    return " → ".join([f"У{b['level']}: {b['name']}" for b in breadcrumbs])

def execute_queries_part_2_2():
    """Выполнение запросов Части 2.2: Товары и embedded documents MongoDB"""
    
    with MongoDBConnection() as db_conn:
        db_ops = MongoDBBaseOperations(db_conn)
        product_service = ProductQueryService(db_ops)
        
        # ЗАПРОС 1: Степлеры + Пневмоинструменты MongoDB
        print_section("ЗАПРОС 1: Товары 'Степлер строительный' в категории 'Пневмоинструменты'")
        
        result1 = product_service.find_products_by_type_and_category("Степлер строительный", "Пневмоинструменты")
        
        print_query_result(
            "Товары: type='Степлер строительный' + breadcrumbs.name='Пневмоинструменты'", 
            result1,
            {"Индекс": "category.breadcrumbs.name_1"}
        )
        
        # Примеры с хлебными крошками MongoDB
        if result1.documents:
            print(f"    Примеры:")
            for i, doc in enumerate(result1.documents[:3], 1):
                breadcrumbs = doc.get('breadcrumbs', [])
                print(f"      {i}. {doc.get('name', 'N/A')[:60]}...")
                print(f"         Навигация: {format_breadcrumbs(breadcrumbs)}")
        
        # ЗАПРОС 2: Товары 4-го уровня MongoDB
        print_section("ЗАПРОС 2: Товары на 4-м уровне иерархии")
        
        result2 = product_service.get_products_by_level(4)
        
        print_query_result("Товары с 4-мя breadcrumbs (уровень иерархии)", result2)
        
        # Примеры 4-го уровня MongoDB
        if result2.documents:
            print(f"    Примеры товаров 4-го уровня:")
            for i, doc in enumerate(result2.documents[:3], 1):
                name = doc.get('category', {}).get('name', 'N/A')
                breadcrumbs = doc.get('category', {}).get('breadcrumbs', [])
                print(f"      {i}. Категория: {name}")
                print(f"         Навигация: {format_breadcrumbs(breadcrumbs)}")
        
        # ЗАПРОС 3: Агрегация по категориям 1-го уровня MongoDB
        print_section("ЗАПРОС 3: Агрегация товаров по категориям 1-го уровня")
        
        result3 = product_service.aggregate_by_first_level_categories()
        
        print_query_result(
            "Агрегация по первым элементам breadcrumbs", 
            result3,
            {"Pipeline stages": "5 (match → project → group → sort → project)"}
        )
        
        # Вывод топ категорий MongoDB
        if result3.documents:
            print(f"    Распределение товаров по категориям 1-го уровня:")
            print(f"{'#':>3} | {'Категория':^40} | {'Товаров':>12}")
            print(f"{'-'*3} | {'-'*40} | {'-'*12}")
            
            for i, stat in enumerate(result3.documents, 1):
                category_name = stat.get('category_name', 'N/A')[:38] + "..." if len(stat.get('category_name', 'N/A')) > 38 else stat.get('category_name', 'N/A')
                product_count = stat.get('product_count', 0)
                print(f"{i:>3} | {category_name:>40} | {product_count:>12,}")
            
            # Статистика MongoDB
            max_products = max(stat.get('product_count', 0) for stat in result3.documents)
            min_products = min(stat.get('product_count', 0) for stat in result3.documents)
            avg_products = sum(stat.get('product_count', 0) for stat in result3.documents) / len(result3.documents)
            
            print(f"\n    Статистика:")
            print(f"      • Максимум: {max_products:,} товаров")
            print(f"      • Минимум: {min_products:,} товаров")
            print(f"      • В среднем: {avg_products:,.0f} товаров")
            print(f"      • Категорий: {len(result3.documents)}")
        
        # Анализ эффективности хлебных крошек MongoDB
        print_section("АНАЛИЗ ЭФФЕКТИВНОСТИ BREADCRUMBS")
        
        # Сравнение подходов MongoDB
        import time
        
        # Поиск через хлебные крошки
        start = time.time()
        breadcrumb_results = list(db_ops.find("products", {"category.breadcrumbs.name": "Пневмоинструменты"}))
        time_breadcrumbs = time.time() - start
        
        # Эмуляция JOIN (поиск по category.id) MongoDB (как в SQL)
        start = time.time()
        category_ids = [doc['category']['id'] for doc in breadcrumb_results[:100]]
        join_results = list(db_ops.find("products", {"category.id": {"$in": category_ids}}))
        time_join = time.time() - start
        
        print(f"\n Сравнение подходов:")
        print(f"   • Breadcrumbs: {time_breadcrumbs*1000:.2f} мсек, {len(breadcrumb_results):,} результатов")
        print(f"   • Эмуляция JOIN: {time_join*1000:.2f} мсек, {len(join_results):,} результатов (100 категорий)")
        
        if time_breadcrumbs < time_join:
            speedup = time_join / time_breadcrumbs
            print(f"    Breadcrumbs быстрее в {speedup:.1f} раз")
            
            if speedup > 10:
                print(f"    Значительное преимущество денормализации")

def main():
    """Основная функция"""
    
    print_section("ЧАСТЬ 2.2: РАБОТА С ТОВАРАМИ И ВЛОЖЕННЫМИ ДОКУМЕНТАМИ")
    
    try:
        execute_queries_part_2_2()
        print("    Embedded Documents pattern MongoDB обеспечивает мгновенный доступ к категории")
        print("    Хлебные крошки позволяют строить навигационные цепочки без JOIN MongoDB")
        print("    Денормализация критически важна для e-commerce производительности")
        print("    Агрегации с группировкой эффективны для аналитики по иерархии")
        
    except Exception as e:
        print(f"\n Ошибка выполнения: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
