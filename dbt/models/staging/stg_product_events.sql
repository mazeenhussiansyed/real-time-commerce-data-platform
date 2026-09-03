{{ config(materialized='view') }}

select
    event_id,
    operation,
    is_deleted,

    nullif(record_json ->> 'product_id', '')::bigint as product_id,
    record_json ->> 'sku' as sku,
    record_json ->> 'product_name' as product_name,
    record_json ->> 'category' as category,
    nullif(record_json ->> 'unit_price', '')::numeric(14, 2) as unit_price,
    nullif(record_json ->> 'active', '')::boolean as active,
    nullif(record_json ->> 'created_at', '')::timestamptz as created_at,
    nullif(record_json ->> 'updated_at', '')::timestamptz as updated_at,

    event_timestamp,
    source_timestamp_ms,
    connector_timestamp_ms,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    ingestion_run_id,
    ingested_at,
    warehouse_loaded_at

from {{ ref('stg_bronze_events') }}

where source_table = 'products'
