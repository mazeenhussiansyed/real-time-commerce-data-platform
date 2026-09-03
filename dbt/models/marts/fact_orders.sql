{{ config(materialized='table') }}

select
    md5(orders.order_id::text) as order_key,
    customers.customer_key,

    orders.order_id,
    orders.customer_id,
    orders.order_status,
    orders.order_total,
    orders.currency,
    orders.ordered_at,
    orders.ordered_at::date as order_date,
    date_trunc('month', orders.ordered_at)::date as order_month,

    orders.order_status = 'cancelled' as is_cancelled,
    orders.order_status = 'delivered' as is_delivered,

    orders.current_event_id,
    orders.latest_operation,
    orders.source_event_at,
    orders.warehouse_loaded_at

from {{ ref('int_orders_current') }} as orders

left join {{ ref('dim_customers') }} as customers
    on orders.customer_id = customers.customer_id
    and orders.ordered_at >= customers.valid_from
    and (
        orders.ordered_at < customers.valid_to
        or customers.valid_to is null
    )
    and not customers.is_deleted
