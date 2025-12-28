
# Лабораторная работа 3: mongodb

## Запуск инфраструктуры

### 1. Запуск контейнеров
```bash
docker-compose up -d
```

### 2. Проверка сервисов

#### ClickHouse
- Веб интерфейс: http://localhost:8123 (через nginx прокси)
- Native интерфейс: localhost:9363 (доступен в Docker сети)
- MySQL совместимый: localhost:9000 (доступен в Docker сети)
- Примечание: HTTP интерфейс доступен через nginx прокси для решения проблем с сетевыми настройками macOS

#### Prometheus  
- Веб интерфейс: http://localhost:9090

#### Grafana
- Веб интерфейс: http://localhost:3000
- Логин: admin, пароль: admin123

### 3. Проверка подключения к ClickHouse
```bash
# Через веб интерфейс (рекомендуется)
curl "http://localhost:8123/?query=SELECT%20version()&user=default&password="

# Через docker exec
docker exec -it clickhouse-server clickhouse-client -u default

# Проверить доступность
curl "http://localhost:8123/ping"
```

### 4. Примеры запросов
```bash
# Простой запрос
curl "http://localhost:8123/?query=SELECT%201%20as%20test"

# Выполнить запрос с результатом в формате TabSeparated
curl "http://localhost:8123/?query=SELECT%20version()&default_format=TabSeparated"

# Создать таблицу
curl "http://localhost:8123/?query=CREATE%20TABLE%20test%20(id%20UInt8,%20name%20String)%20ENGINE%20Memory"
```

## Структура проекта

```
.
├── docker-compose.yml          # Основной файл Docker Compose
├── ecom_offer/                 # Данные каталога товаров
│   └── 10ozon 2.csv
├── raw_event/                  # Данные событий пользователей
│   └── part-00000-*.parquet
├── clickhouse/                 # Конфигурация ClickHouse
│   ├── config/
│   │   └── metrics.xml
│   └── users/
├── prometheus/                 # Конфигурация Prometheus
│   └── prometheus.yml
├── grafana/                    # Конфигурация Grafana
│   ├── provisioning/
│   │   ├── datasources/
│   │   │   └── prometheus.yml
│   │   └── dashboards/
│   │       └── dashboards.yml
│   └── dashboards/
└── scripts/                    # SQL скрипты и скрипты тестирования
```
=======
# Mongo
MongoDB lab
>>>>>>> 8fe2bcb5e1502b47c64400c04b62015adc7c9776
