{{ config(materialized='table') }}

with customer_metrics as (

    select
        customer_id,

        count(*) as lifetime_order_count,

        count(*) filter (
            where not is_cancelled
        ) as non_cancelled_order_count,

        count(*) filter (
            where is_cancelled
        ) as cancelled_order_count,

        count(*) filter (
            where is_delivered
        ) as delivered_order_count,

        round(sum(order_total), 2) as lifetime_order_value,
        round(avg(order_total), 2) as average_order_value,

        min(ordered_at) as first_order_at,
        max(ordered_at) as most_recent_order_at

    from {{ ref('fact_orders') }}

    group by customer_id

)

select
    customers.customer_key,
    customers.customer_id,
    customers.full_name,
    customers.email,
    customers.city,
    customers.state_code,
    customers.customer_status,

    coalesce(metrics.lifetime_order_count, 0)
        as lifetime_order_count,

    coalesce(metrics.non_cancelled_order_count, 0)
        as non_cancelled_order_count,

    coalesce(metrics.cancelled_order_count, 0)
        as cancelled_order_count,

    coalesce(metrics.delivered_order_count, 0)
        as delivered_order_count,

    coalesce(metrics.lifetime_order_value, 0)
        as lifetime_order_value,

    coalesce(metrics.average_order_value, 0)
        as average_order_value,

    metrics.first_order_at,
    metrics.most_recent_order_at,

    case
        when coalesce(metrics.lifetime_order_value, 0) >= 7500
            then 'high_value'
        when coalesce(metrics.lifetime_order_value, 0) >= 3500
            then 'medium_value'
        else 'standard_value'
    end as customer_value_segment

from {{ ref('dim_customers') }} as customers

left join customer_metrics as metrics
    on customers.customer_id = metrics.customer_id

where customers.is_current
  and not customers.is_deleted
