#!/usr/bin/env python3
"""
Нагрузочное тестирование ClickHouse e-commerce аналитики
Сравнение сырых данных vs материализованных представлений
"""

import clickhouse_connect
import time
import statistics
import concurrent.futures
import json
from datetime import datetime

class ClickHousePerformanceTest:
    def __init__(self, host='localhost', port=8123, user='default', password=''):
        self.client = clickhouse_connect.get_client(host=host, port=port, user=user, password=password)
        self.results = {}
    
    def execute_query(self, query, description):
        """Выполнение запроса с замером времени"""
        start_time = time.time()
        result = self.client.query(query).result_rows
        end_time = time.time()
        
        execution_time = end_time - start_time
        row_count = len(result)
        
        return {
            'time': execution_time,
            'rows': row_count,
            'description': description
        }
    
    def benchmark_query(self, query, description, iterations=10):
        """Бенчмарк запроса с несколькими итерациями"""
        times = []
        
        for i in range(iterations):
            result = self.execute_query(query, f"{description} - итерация {i+1}")
            times.append(result['time'])
        
        avg_time = statistics.mean(times)
        min_time = min(times)
        max_time = max(times)
        std_dev = statistics.stdev(times) if len(times) > 1 else 0
        
        return {
            'avg_time': avg_time,
            'min_time': min_time,
            'max_time': max_time,
            'std_dev': std_dev,
            'iterations': iterations,
            'description': description
        }
    
    def test_basic_queries(self):
        """Тест базовых аналитических запросов"""
        print("🔍 Тестирование базовых запросов...")
        
        queries = [
            {
                'name': 'COUNT всех товаров',
                'raw': "SELECT COUNT(*) FROM ecommerce.ecom_offers",
                'mv': None  # Этот запрос имеет смысл только для сырых данных
            },
            {
                'name': 'COUNT по категориям (TOP 10)',
                'raw': "SELECT category_id, COUNT(*) as cnt FROM ecommerce.ecom_offers GROUP BY category_id ORDER BY cnt DESC LIMIT 10",
                'mv': "SELECT category_id, SUM(products_count) as cnt FROM ecommerce.catalog_by_category_mv GROUP BY category_id ORDER BY cnt DESC LIMIT 10"
            },
            {
                'name': 'Статистика по топ категории',
                'raw': "SELECT category_id, COUNT(*), AVG(price), MIN(price), MAX(price) FROM ecommerce.ecom_offers WHERE category_id = 7508",
                'mv': "SELECT category_id, SUM(total_price)/SUM(products_count) as avg_price, min_price, max_price FROM ecommerce.catalog_by_category_mv WHERE category_id = 7508 GROUP BY category_id"
            },
            {
                'name': 'Топ брендов по категории',
                'raw': "SELECT vendor, COUNT(*) FROM ecommerce.ecom_offers WHERE category_id = 7508 AND vendor != '' AND vendor != 'Unknown' GROUP BY vendor ORDER BY COUNT(*) DESC LIMIT 5",
                'mv': "SELECT vendor, SUM(products_count) FROM ecommerce.catalog_by_brand_mv WHERE category_id = 7508 GROUP BY vendor ORDER BY SUM(products_count) DESC LIMIT 5"
            }
        ]
        
        for query_info in queries:
            print(f"\n--- {query_info['name']} ---")
            
            # Тест сырых данных
            if query_info['raw']:
                raw_result = self.benchmark_query(query_info['raw'], f"Сырые данные: {query_info['name']}")
                self.results[f"{query_info['name']}_raw"] = raw_result
                print(f"Сырые:   {raw_result['avg_time']:.4f}s (min: {raw_result['min_time']:.4f}s, max: {raw_result['max_time']:.4f}s)")
            
            # Тест материализованных представлений
            if query_info['mv']:
                mv_result = self.benchmark_query(query_info['mv'], f"МВ: {query_info['name']}")
                self.results[f"{query_info['name']}_mv"] = mv_result
                print(f"МВ:      {mv_result['avg_time']:.4f}s (min: {mv_result['min_time']:.4f}s, max: {mv_result['max_time']:.4f}s)")
                
                # Сравнение производительности
                if raw_result and mv_result:
                    speedup = raw_result['avg_time'] / mv_result['avg_time']
                    print(f"Ускорение: {speedup:.2f}x")
    
    def test_concurrent_load(self, concurrent_users=10, queries_per_user=5):
        """Тест нагрузочной способности с параллельными запросами"""
        print(f"\n⚡ Нагрузочное тестирование: {concurrent_users} пользователей, {queries_per_user} запросов каждый")
        
        queries = [
            "SELECT COUNT(*) FROM ecommerce.ecom_offers",
            "SELECT category_id, COUNT(*) FROM ecommerce.ecom_offers GROUP BY category_id LIMIT 5",
            "SELECT vendor, COUNT(*) FROM ecommerce.ecom_offers WHERE vendor != '' GROUP BY vendor LIMIT 10"
        ]
        
        def user_simulation(user_id):
            """Симуляция работы пользователя"""
            user_times = []
            for i in range(queries_per_user):
                query = queries[i % len(queries)]
                start_time = time.time()
                try:
                    rows = self.client.query(query).result_rows
                    execution_time = time.time() - start_time
                    user_times.append(execution_time)
                except Exception as e:
                    print(f"Пользователь {user_id}, запрос {i}: ошибка {e}")
                    user_times.append(10.0) # таймаут как ошибка
            return user_times
        
        # Запуск параллельных пользователей
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrent_users) as executor:
            futures = [executor.submit(user_simulation, user_id) for user_id in range(concurrent_users)]
            all_times = []
            
            for future in concurrent.futures.as_completed(futures):
                user_times = future.result()
                all_times.extend(user_times)
        
        if all_times:
            avg_response_time = statistics.mean(all_times)
            max_response_time = max(all_times)
            qps = (concurrent_users * queries_per_user) / sum(all_times)
            
            print(f"Среднее время ответа: {avg_response_time:.4f}s")
            print(f"Максимальное время: {max_response_time:.4f}s")
            print(f"QPS (запросов/сек): {qps:.2f}")
            
            return {
                'avg_response_time': avg_response_time,
                'max_response_time': max_response_time,
                'qps': qps,
                'total_queries': len(all_times)
            }
        
        return None
    
    def save_results(self, filename='performance_results.json'):
        """Сохранение результатов тестирования"""
        timestamp = datetime.now().isoformat()
        results_with_timestamp = {
            'timestamp': timestamp,
            'environment': {
                'dataset_size': '3.99M records',
                'materialized_views': 4,
                'clickhouse_version': self.client.query('SELECT version()').first_row[0]
            },
            'benchmarks': self.results
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(results_with_timestamp, f, indent=2, ensure_ascii=False)
        
        print(f"\n📊 Результаты сохранены в {filename}")
    
    def generate_report(self):
        """Генерация текстового отчета"""
        report = []
        report.append("=" * 60)
        report.append("CLICKHOUSE PERFORMANCE TEST REPORT")
        report.append("=" * 60)
        report.append(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Данные: 3.99M записей ecommerce")
        report.append(f"Материализованные представления: 4")
        report.append("")
        
        # Анализ ускорения
        speedups = []
        for key, result in self.results.items():
            if key.endswith('_raw') and f"{key[:-4]}_mv" in self.results:
                raw_time = result['avg_time']
                mv_time = self.results[f"{key[:-4]}_mv"]['avg_time']
                speedup = raw_time / mv_time
                speedups.append(speedup)
                report.append(f"📈 Ускорение {key[:-4]}: {speedup:.2f}x")
        
        if speedups:
            avg_speedup = statistics.mean(speedups)
            report.append("")
            report.append(f"🎯 Среднее ускорение: {avg_speedup:.2f}x")
            report.append(f"📊 Максимум ускорения: {max(speedups):.2f}x")
            report.append(f"📉 Минимум ускорения: {min(speedups):.2f}x")
        
        report.append("")
        report.append("ДЕТАЛЬНЫЕ РЕЗУЛЬТАТЫ:")
        for key, result in self.results.items():
            query_type = "Сырые данные" if key.endswith('_raw') else "МВ"
            report.append(f"{query_type}: {result['description']}")
            report.append(f"  Среднее время: {result['avg_time']:.4f}s")
            report.append(f"  Минимум: {result['min_time']:.4f}s")
            report.append(f"  Максимум: {result['max_time']:.4f}s")
            report.append("")
        
        return "\n".join(report)

def main():
    print("🚀 Запуск нагрузочного тестирования ClickHouse")
    print("=" * 50)
    
    test = ClickHousePerformanceTest()
    
    try:
        # Тестирование базовых запросов
        test.test_basic_queries()
        
        # Нагрузочное тестирование
        concurrent_results = test.test_concurrent_load(concurrent_users=5, queries_per_user=3)
        
        # Сохранение результатов
        test.save_results('C:/VSCode projects/Databases/clickhouse-mongo-subd/performance_results.json')
        
        # Генерация отчета
        report = test.generate_report()
        print("\n" + report)
        
        # Сохранение отчета в файл
        with open('C:/VSCode projects/Databases/clickhouse-mongo-subd/performance_report.txt', 'w', encoding='utf-8') as f:
            f.write(report)
        
        print(f"\n✅ Тестирование завершено!")
        print(f"📄 Отчет сохранен в performance_report.txt")
        print(f"📊 Детальные результаты в performance_results.json")
        
    except Exception as e:
        print(f"❌ Ошибка при тестировании: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()
