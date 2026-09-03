{{ config(materialized='table') }}

with product_metrics as (

    select
        product_id,

        count(distinct order_id) as order_count,
        count(*) as order_line_count,
        sum(quantity) as units_sold,
        round(sum(line_total), 2) as gross_product_revenue,
        round(avg(unit_price), 2) as average_selling_price

    from {{ ref('fact_order_items') }}

    group by product_id

)

select
    products.product_key,
    products.product_id,
    products.sku,
    products.product_name,
    products.category,
    products.unit_price as current_unit_price,
    products.is_active,

    coalesce(metrics.order_count, 0) as order_count,
    coalesce(metrics.order_line_count, 0) as order_line_count,
    coalesce(metrics.units_sold, 0) as units_sold,

    coalesce(metrics.gross_product_revenue, 0)
        as gross_product_revenue,

    coalesce(metrics.average_selling_price, 0)
        as average_selling_price,

    case
        when coalesce(metrics.gross_product_revenue, 0) >= 30000
            then 'top_performer'
        when coalesce(metrics.gross_product_revenue, 0) >= 15000
            then 'core_performer'
        else 'lower_performer'
    end as product_performance_segment

from {{ ref('dim_products') }} as products

left join product_metrics as metrics
    on products.product_id = metrics.product_id
