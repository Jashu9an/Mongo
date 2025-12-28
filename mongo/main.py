#!/usr/bin/env python3
"""
    MongoDB Main Script
"""

import sys
import time
from pathlib import Path

# Добавляем core в Python path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir / "core"))

def print_banner(title: str) -> None:
    """Печать баннера MongoDB"""
    lines = [
        "=" * 80,
        f" Лаба с монгодб",
        f" {title}",
        "=" * 80
    ]
    for line in lines:
        print(line)

def print_menu() -> None:
    """Опции монги"""
    options = [
        "1. Анализ данных и проектирование схемы MongoDB",
        "2. Загрузка данных в MongoDB", 
        "3. Создание индексов MongoDB",
        "4. Базовые запросы - Навигация по категориям (Часть 2.1)",
        "5. Товары и embedded documents (Часть 2.2)",
        "6. Агрегационный фреймворк (Часть 3.1)",
        "7. Показать статистику индексов MongoDB",
        "8. Запустить всю последовательность MongoDB"
    ]
    
    for option in options:
        print(f"   {option}")
    
    print("\n" + "="*80)

def load_data_part_1() -> None:
    """Загрузка данных MongoDB"""
    print_banner("Часть 1: ЗАГРУЗКА ДАННЫХ")
    
    try:
        from scripts.load_data import main as load_main
        result = load_main()
        return result
    except Exception as e:
        print(f" Ошибка загрузки данных: {e}")
        return 1

def run_queries_part_2_1() -> None:
    """Навигация по категориям MongoDB"""
    print_banner("Часть 2.1: НАВИГАЦИЯ ПО ИЕРАРХИИ КАТЕГОРИЙ")
    
    try:
        from scripts.category_navigation import main as queries_2_1_main
        return queries_2_1_main()
    except Exception as e:
        print(f" Ошибка выполнения запросов 2.1: {e}")
        return 1

def run_queries_part_2_2() -> None:
    """Товары и embedded documents MongoDB"""
    print_banner("Часть 2.2: РАБОТА С ТОВАРАМИ И ВЛОЖЕННЫМИ ДОКУМЕНТАМИ")
    
    try:
        from scripts.product_queries import main as queries_2_2_main
        return queries_2_2_main()
    except Exception as e:
        print(f" Ошибка выполнения запросов 2.2: {e}")
        return 1

def run_queries_part_3() -> None:
    """Агрегационный фреймворк MongoDB"""
    print_banner("Часть 3: АГРЕГАЦИОННЫЙ ФРЕЙМВОРК")
    
    try:
        from scripts.analytics_aggregations import main as queries_3_main
        return queries_3_main()
    except Exception as e:
        print(f" Ошибка выполнения агрегаций: {e}")
        return 1

def show_indexes_info() -> None:
    """Статистика индексов MongoDB"""
    print_banner("СТАТИСТИКА ИНДЕКСОВ")
    
    try:
        from utils.show_indexes import main as indexes_main
        return indexes_main()
    except Exception as e:
        print(f" Ошибка получения статистики индексов: {e}")
        return 1

def run_full_sequence() -> None:
    """Пейплайн джоб MongoDB"""
    
    start_time = time.time()
    
    parts = [
        ("Загрузка данных", load_data_part_1),
        ("Запросы 2.1", run_queries_part_2_1),
        ("Запросы 2.2", run_queries_part_2_2),
        ("Агрегации", run_queries_part_3)
    ]
    
    results = []
    
    for part_name, part_func in parts:
        print(f"\n Выполнение: {part_name}")
        print("-" * 60)
        
        try:
            result = part_func()
            results.append((part_name, result))
            
            if result == 0:
                print(f" {part_name} - успешно")
            else:
                print(f" {part_name} - ошибка (код: {result})")
                
        except Exception as e:
            print(f" {part_name} - исключение: {e}")
            results.append((part_name, 1))
        
        print(f"Время: {time.strftime('%H:%M:%S')}")
    
    # Итоги MongoDB
    total_time = time.time() - start_time
    successful = sum(1 for _, result in results if result == 0)
    
    print_banner("ИТОГИ ПЕЙПЛАЙНА")
    print(f"Общее время: {total_time/60:.1f} минут")
    print(f"Выполнено успешно: {successful}/{len(results)} частей")
    
    for part_name, result in results:
        status = " Успешно" if result == 0 else " Ошибка"
        print(f"   {part_name}: {status}")
    
    if successful == len(results):
        print("\n Пейплайн джоб MongoDB успешно завершен!")
        return 0
    else:
        print("\n Рекомендуется проверить части с ошибками")
        return 1

def analyze_data_part_1() -> None:
    """Анализ исходных данных MongoDB"""
    print_banner("АНАЛИЗ ИСХОДНЫХ ДАННЫХ")
    
    try:
        # Запускаем анализ данных MongoDB   
        from shutil import copy
        src = current_dir / "analyze_data.py" 
        dst = current_dir / "scripts" / "analyze_data.py"
        
        if src.exists():
            if not (current_dir / "scripts").exists():
                (current_dir / "scripts").mkdir(exist_ok=True)
            copy(src, dst)
            print(" Скрипт анализа скопирован в scripts/")
            print(" Результаты анализа сохранятся в корне mongodb_lab/")
        else:
            print(" Скрипт анализа не найден")
            return 1
            
        # Запускаем анализ данных MongoDB
        return 0
    except Exception as e:
        print(f" Ошибка анализа данных: {e}")
        return 1

def main() -> None:
    """Главная функция MongoDB"""
    print_banner("Лабораторная работа №3: Работа с MongoDB")
    
    while True:
        print_menu()
        
        try:
            choice = input("\n Выберите действие (1-8): ").strip()
            
            if choice == "1":
                analyze_data_part_1()
            elif choice == "2":
                load_data_part_1()
            elif choice == "3":
                show_indexes_info()
            elif choice == "4":
                run_queries_part_2_1()
            elif choice == "5":
                run_queries_part_2_2()
            elif choice == "6":
                run_queries_part_3()
            elif choice == "7":
                show_indexes_info()
            elif choice == "8":
                return run_full_sequence()
            else:
                print(" Неверный выбор. Введите число от 1 до 8.")
                
        except KeyboardInterrupt:
            print("\n\n Работа прервана пользователем")
            break
        except Exception as e:
            print(f" Произошла ошибка: {e}")
            continue

if __name__ == "__main__":
    main()
