with item_totals as (

    select
        order_key,
        round(sum(line_total), 2) as calculated_order_total

    from {{ ref('fact_order_items') }}

    group by order_key

)

select
    orders.order_id,
    orders.order_total,
    coalesce(items.calculated_order_total, 0)
        as calculated_order_total

from {{ ref('fact_orders') }} as orders

left join item_totals as items
    on orders.order_key = items.order_key

where abs(
    orders.order_total
    - coalesce(items.calculated_order_total, 0)
) > 0.01
