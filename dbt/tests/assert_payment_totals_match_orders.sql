with payment_totals as (

    select
        order_key,
        count(*) as payment_count,
        round(sum(amount), 2) as total_payment_amount

    from {{ ref('fact_payments') }}

    group by order_key

)

select
    orders.order_id,
    orders.order_total,
    coalesce(payments.payment_count, 0) as payment_count,
    coalesce(payments.total_payment_amount, 0)
        as total_payment_amount

from {{ ref('fact_orders') }} as orders

left join payment_totals as payments
    on orders.order_key = payments.order_key

where coalesce(payments.payment_count, 0) <> 1
   or abs(
       orders.order_total
       - coalesce(payments.total_payment_amount, 0)
   ) > 0.01
