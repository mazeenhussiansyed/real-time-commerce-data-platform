{{ config(materialized='table') }}

select
    md5(shipments.shipment_id::text) as shipment_key,
    md5(shipments.order_id::text) as order_key,

    shipments.shipment_id,
    shipments.order_id,
    shipments.shipment_status,
    shipments.carrier,
    shipments.tracking_code,
    shipments.shipped_at,
    shipments.delivered_at,
    shipments.created_at,
    shipments.updated_at,

    shipments.shipment_status = 'packed' as is_packed,
    shipments.shipment_status = 'shipped' as is_shipped,
    shipments.shipment_status = 'delivered' as is_delivered,

    case
        when shipments.shipped_at is not null
        then round(
            (
                extract(
                    epoch from shipments.shipped_at - shipments.created_at
                ) / 3600.0
            )::numeric,
            2
        )
    end as hours_to_ship,

    case
        when shipments.delivered_at is not null
        then round(
            (
                extract(
                    epoch from shipments.delivered_at - shipments.created_at
                ) / 3600.0
            )::numeric,
            2
        )
    end as hours_to_deliver,

    shipments.current_event_id,
    shipments.latest_operation,
    shipments.source_event_at,
    shipments.warehouse_loaded_at

from {{ ref('int_shipments_current') }} as shipments
