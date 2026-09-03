{{ config(materialized='table') }}

select
    md5(items.order_item_id::text) as order_item_key,
    md5(items.order_id::text) as order_key,
    products.product_key,

    items.order_item_id,
    items.order_id,
    items.product_id,
    items.quantity,
    items.unit_price,
    items.line_total,

    items.current_event_id,
    items.latest_operation,
    items.source_event_at,
    items.warehouse_loaded_at

from {{ ref('int_order_items_current') }} as items

left join {{ ref('dim_products') }} as products
    on items.product_id = products.product_id
