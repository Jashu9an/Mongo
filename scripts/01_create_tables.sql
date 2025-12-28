-- Создание базы данных если не существует
CREATE DATABASE IF NOT EXISTS ecommerce;

-- Использование базы данных
USE ecommerce;

-- Создание таблицы каталога товаров
CREATE TABLE IF NOT EXISTS ecom_offers (
    offer_id UInt64,
    price Decimal64(2),
    seller_id UInt64,
    category_id UInt64,
    vendor String,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (category_id, seller_id, offer_id)
SETTINGS index_granularity = 8192;

-- Создание таблицы сырых событий пользователей
CREATE TABLE IF NOT EXISTS raw_events (
    Hour UInt8,
    DeviceTypeName String,
    ApplicationName String,
    OSName String,
    ProvinceName String,
    ContentUnitID UInt64,
    created_at DateTime DEFAULT now(),
    event_date Date DEFAULT toDate(created_at)
) ENGINE = MergeTree()
PARTITION BY event_date
ORDER BY (event_date, Hour, DeviceTypeName, ApplicationName, ContentUnitID)
SETTINGS index_granularity = 8192;

-- Создание таблицы для связи событий и предложений
-- ContentUnitID из raw_events соответствует offer_id из ecom_offers
CREATE TABLE IF NOT EXISTS offers_events_mapping (
    offer_id UInt64,
    content_unit_id UInt64,
    mapping_created_at DateTime DEFAULT now()
) ENGINE = Join(ANY, LEFT, offer_id)
ORDER BY offer_id;
