{{ config(materialized='view') }}

select
    event_id,
    ingestion_run_id,
    ingested_at,
    event_timestamp,
    operation,
    operation = 'd' as is_deleted,
    source_timestamp_ms,
    connector_timestamp_ms,
    source_lsn,
    source_transaction_id,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    nullif(key_json, '')::jsonb as key_json,
    nullif(before_json, '')::jsonb as before_json,
    nullif(after_json, '')::jsonb as after_json,
    coalesce(
        nullif(after_json, '')::jsonb,
        nullif(before_json, '')::jsonb
    ) as record_json,
    payload_json,
    value_json,
    schema_name,
    source_table,
    event_date,
    warehouse_loaded_at
from {{ source('raw', 'bronze_events') }}
