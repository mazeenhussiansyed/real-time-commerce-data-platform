{{ config(materialized='view') }}

select
    event_id,
    operation,
    is_deleted,

    nullif(record_json ->> 'payment_id', '')::bigint as payment_id,
    nullif(record_json ->> 'order_id', '')::bigint as order_id,
    record_json ->> 'payment_status' as payment_status,
    record_json ->> 'payment_method' as payment_method,
    nullif(record_json ->> 'amount', '')::numeric(14, 2) as amount,
    nullif(record_json ->> 'paid_at', '')::timestamptz as paid_at,
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

where source_table = 'payments'
