{{ config(materialized='view') }}

select
    event_id,
    operation,
    is_deleted,

    nullif(record_json ->> 'order_id', '')::bigint as order_id,
    nullif(record_json ->> 'customer_id', '')::bigint as customer_id,
    record_json ->> 'order_status' as order_status,
    nullif(record_json ->> 'order_total', '')::numeric(14, 2) as order_total,
    record_json ->> 'currency' as currency,
    nullif(record_json ->> 'ordered_at', '')::timestamptz as ordered_at,
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

where source_table = 'orders'
