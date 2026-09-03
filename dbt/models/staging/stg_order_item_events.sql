{{ config(materialized='view') }}

select
    event_id,
    operation,
    is_deleted,

    nullif(record_json ->> 'order_item_id', '')::bigint as order_item_id,
    nullif(record_json ->> 'order_id', '')::bigint as order_id,
    nullif(record_json ->> 'product_id', '')::bigint as product_id,
    nullif(record_json ->> 'quantity', '')::integer as quantity,
    nullif(record_json ->> 'unit_price', '')::numeric(14, 2) as unit_price,
    nullif(record_json ->> 'line_total', '')::numeric(14, 2) as line_total,

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

where source_table = 'order_items'
