{{ config(materialized='table') }}

select
    md5(product_id::text) as product_key,

    product_id,
    sku,
    product_name,
    category,
    unit_price,
    active as is_active,
    created_at,
    updated_at,

    current_event_id,
    latest_operation,
    source_event_at,
    warehouse_loaded_at

from {{ ref('int_products_current') }}
