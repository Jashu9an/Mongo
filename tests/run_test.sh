#!/bin/bash

echo "🔧 Установка зависимостей для нагрузочного тестирования..."

# Установка clickhouse-connect
python3 -m pip install clickhouse-connect > /dev/null 2>&1 || echo "❌ Ошибка установки clickhouse-connect"

echo "✅ Зависимости установлены"
echo ""
echo "🚀 Запуск нагрузочного тестирования..."
echo "Данные: 3.99M записей в ClickHouse"
echo ""

python3 performance_tests/load_test.py
