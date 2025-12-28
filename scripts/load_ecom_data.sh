#!/bin/bash

# Скрипт для порционной загрузки данных
# Разделим CSV файл на части по 100K строк

echo "Начинаем загрузку данных в ClickHouse..."

# Подсчет общего количества строк (без заголовка)
TOTAL_LINES=$(tail -n +2 "C:/VSCode projects/Databases/clickhouse-mongo-subd/ecom_offer/10ozon 2.csv" | wc -l)
echo "Всего строк для загрузки: $TOTAL_LINES"

# Размер порции
BATCH_SIZE=100000
LOADED_LINES=0

# Пропускаем заголовок и загружаем порциями
tail -n +2 "C:/VSCode projects/Databases/clickhouse-mongo-subd/ecom_offer/10ozon 2.csv" | split -l $BATCH_SIZE -d - /tmp/ecom_batch_

for batch_file in /tmp/ecom_batch_*; do
    if [ -f "$batch_file" ]; then
        echo "Загрузка файла: $batch_file"
        
        # Убираем первую колонку с индексом и загружаем в ClickHouse
        cut -d',' -f2- "$batch_file" | docker exec -i clickhouse-server clickhouse-client --query "
            INSERT INTO ecommerce.ecom_offers(offer_id, price, seller_id, category_id, vendor) FORMAT CSV" 2>/dev/null
        
        if [ $? -eq 0 ]; then
            BATCH_LINES=$(wc -l < "$batch_file")
            LOADED_LINES=$((LOADED_LINES + BATCH_LINES))
            PROGRESS=$((LOADED_LINES * 100 / TOTAL_LINES))
            echo "Прогресс: $LOADED_LINES/$TOTAL_LINES ($PROGRESS%)"
        else
            echo "Ошибка при загрузке $batch_file"
        fi
        
        rm "$batch_file"
    fi
done

echo "Загрузка завершена! Всего загружено: $LOADED_LINES строк"

# Проверка итогового количества
CLICKHOUSE_COUNT=$(docker exec clickhouse-server clickhouse-client --query "SELECT COUNT(*) FROM ecommerce.ecom_offers" 2>/dev/null | tr -d '\n')
echo "Проверка в ClickHouse: $CLICKHOUSE_COUNT строк"
