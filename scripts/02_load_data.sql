-- Загрузка данных из CSV файла в таблицу ecom_offers
-- Пропускаем первую колонку с индексом и загружаем основные данные

INSERT INTO ecommerce.ecom_offers 
(offer_id, price, seller_id, category_id, vendor, created_at, updated_at)
SELECT 
    toUInt64OrNull(offer_id) as offer_id,
    toDecimal64OrNull(price, 2) as price,
    toUInt64OrNull(seller_id) as seller_id,
    toUInt64OrNull(category_id) as category_id,
    if(empty(vendor), 'Unknown', vendor) as vendor,
    now() as created_at,
    now() as updated_at
FROM file('data/ecom_offer/10ozon 2.csv', 'CSV', 'offer_id String, price String, seller_id String, category_id String, vendor String')
WHERE offer_id != '' AND price != '';

-- Загрузка данных из Parquet файла в таблицу raw_events
INSERT INTO ecommerce.raw_events 
(Hour, DeviceTypeName, ApplicationName, OSName, ProvinceName, ContentUnitID, created_at, event_date)
SELECT 
    toUInt8OrNull(Hour) as Hour,
    if(empty(DeviceTypeName), 'Unknown', DeviceTypeName) as DeviceTypeName,
    if(empty(ApplicationName), 'Unknown', ApplicationName) as ApplicationName,
    if(empty(OSName), 'Unknown', OSName) as OSName,
    if(empty(ProvinceName), 'Unknown', ProvinceName) as ProvinceName,
    toUInt64OrNull(ContentUnitID) as ContentUnitID,
    now() as created_at,
    toDate(now()) as event_date
FROM file('data/raw_event/part-00000-fe025c1f-5ca7-4f31-8143-5b648fcc9879-c000.snappy.parquet');
