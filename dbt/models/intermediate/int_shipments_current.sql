{{ config(materialized='view') }}

with ranked_events as (

    select
        *,
        row_number() over (
            partition by shipment_id
            order by
                source_timestamp_ms desc nulls last,
                kafka_timestamp desc nulls last,
                kafka_offset desc,
                event_id desc
        ) as event_rank

    from {{ ref('stg_shipment_events') }}

)

select
    shipment_id,
    order_id,
    shipment_status,
    carrier,
    tracking_code,
    shipped_at,
    delivered_at,
    created_at,
    updated_at,

    event_id as current_event_id,
    operation as latest_operation,
    event_timestamp as source_event_at,
    kafka_partition,
    kafka_offset,
    warehouse_loaded_at

from ranked_events

where event_rank = 1
  and not is_deleted
