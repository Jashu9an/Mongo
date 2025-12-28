#!/bin/bash

echo "=== ЗАГРУЗКА ПОЛНОГО НАБОРА ДАННЫХ ==="
echo "Всего строк для загрузки: 36,876,834 (без заголовка)"

# Размер порции - 500K строк для производительности
BATCH_SIZE=500000
TOTAL_LINES=36876834
START_TIME=$(date +%s)

# Прогресс-трекер
loaded_lines=0
batch_num=1

# Функция для отображения прогресса
show_progress() {
    local current=$1
    local total=$2
    local percent=$((current * 100 / total))
    local elapsed=$(($(date +%s) - START_TIME))
    local eta=0
    if [ $current -gt 0 ]; then
        eta=$((elapsed * total / current - elapsed))
    fi
    
    printf "\rПрогресс: %d/%d (%d%%) | Элапс: %ds | ETA: %ds | Порция #%d" \
           $current $total $percent $elapsed $eta $batch_num
}

# Порционная загрузка
echo "Начинаем загрузку..."

for ((start=2; start<=TOTAL_LINES; start+=BATCH_SIZE)); do
    # Вычисляем конец текущей порции
    end=$((start + BATCH_SIZE - 1))
    if [ $end -gt $TOTAL_LINES ]; then
        end=$TOTAL_LINES
    fi
    
    echo -e "\n--- Загрузка порции #$batch_num: строки $start-$end ---"
    
    # Извлекаем порцию данных, пропуская первую колонку и заголовок
    sed -n "${start},${end}p" "C:/VSCode projects/Databases/clickhouse-mongo-subd/ecom_offer/10ozon 2.csv" | \
    cut -d',' -f2- | \
    docker exec -i clickhouse-server clickhouse-client \
        --query "INSERT INTO ecommerce.ecom_offers(offer_id, price, seller_id, category_id, vendor) FORMAT CSV" \
        --progress 2>/dev/null
    
    # Проверка успешности загрузки
    if [ $? -eq 0 ]; then
        batch_size=$((end - start + 1))
        loaded_lines=$((loaded_lines + batch_size))
        show_progress $loaded_lines $TOTAL_LINES
    else
        echo "❌ Ошибка при загрузке порции $batch_num"
        exit 1
    fi
    
    batch_num=$((batch_num + 1))
    
    # Небольшая пауза между порциями для разгрузки системы
    sleep 2
done

echo -e "\n=== ЗАГРУЗКА ЗАВЕРШЕНА ==="
echo "Общее время: $(($(date +%s) - START_TIME)) секунд"

# Проверка результата
echo "Проверяем количество записей в ClickHouse..."
ch_count=$(docker exec clickhouse-server clickhouse-client --query "SELECT COUNT(*) FROM ecommerce.ecom_offers" 2>/dev/null | tr -d '\n')
echo "Загружено записей в ClickHouse: $ch_count"

if [ "$ch_count" = "$TOTAL_LINES" ]; then
    echo "✅ ЗАГРУЗКА УСПЕШНА! Все строки загружены."
else
    echo "⚠️  Расхождение: ожидалось $TOTAL_LINES, загружено $ch_count"
fi
