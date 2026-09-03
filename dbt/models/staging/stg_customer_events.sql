{{ config(materialized='view') }}

select
    event_id,
    operation,
    is_deleted,

    nullif(record_json ->> 'customer_id', '')::bigint as customer_id,
    record_json ->> 'first_name' as first_name,
    record_json ->> 'last_name' as last_name,
    record_json ->> 'email' as email,
    record_json ->> 'city' as city,
    record_json ->> 'state_code' as state_code,
    record_json ->> 'customer_status' as customer_status,
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

where source_table = 'customers'
