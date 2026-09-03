{{ config(materialized='table') }}

with order_metrics as (

    select
        order_date,

        count(*) as order_count,

        count(*) filter (
            where not is_cancelled
        ) as non_cancelled_order_count,

        count(*) filter (
            where is_cancelled
        ) as cancelled_order_count,

        count(*) filter (
            where is_delivered
        ) as delivered_order_count,

        round(sum(order_total), 2) as gross_order_value,

        round(
            sum(order_total) filter (
                where not is_cancelled
            ),
            2
        ) as non_cancelled_order_value,

        round(avg(order_total), 2) as average_order_value

    from {{ ref('fact_orders') }}

    group by order_date

),

payment_metrics as (

    select
        orders.order_date,

        count(*) as payment_count,

        count(*) filter (
            where payments.is_captured
        ) as captured_payment_count,

        count(*) filter (
            where payments.is_refunded
        ) as refunded_payment_count,

        round(sum(payments.amount), 2) as total_payment_value,

        round(
            sum(payments.amount) filter (
                where payments.is_captured
            ),
            2
        ) as captured_payment_value,

        round(
            sum(payments.amount) filter (
                where payments.is_refunded
            ),
            2
        ) as refunded_payment_value

    from {{ ref('fact_payments') }} as payments

    inner join {{ ref('fact_orders') }} as orders
        on payments.order_key = orders.order_key

    group by orders.order_date

),

shipment_metrics as (

    select
        orders.order_date,

        count(*) as shipment_count,

        count(*) filter (
            where shipments.is_packed
        ) as packed_shipment_count,

        count(*) filter (
            where shipments.is_shipped
        ) as shipped_shipment_count,

        count(*) filter (
            where shipments.is_delivered
        ) as delivered_shipment_count,

        round(avg(shipments.hours_to_ship), 2) as average_hours_to_ship,

        round(avg(shipments.hours_to_deliver), 2)
            as average_hours_to_deliver

    from {{ ref('fact_shipments') }} as shipments

    inner join {{ ref('fact_orders') }} as orders
        on shipments.order_key = orders.order_key

    group by orders.order_date

)

select
    orders.order_date,
    orders.order_count,
    orders.non_cancelled_order_count,
    orders.cancelled_order_count,
    orders.delivered_order_count,
    orders.gross_order_value,
    orders.non_cancelled_order_value,
    orders.average_order_value,

    coalesce(payments.payment_count, 0) as payment_count,
    coalesce(payments.captured_payment_count, 0)
        as captured_payment_count,
    coalesce(payments.refunded_payment_count, 0)
        as refunded_payment_count,
    coalesce(payments.total_payment_value, 0)
        as total_payment_value,
    coalesce(payments.captured_payment_value, 0)
        as captured_payment_value,
    coalesce(payments.refunded_payment_value, 0)
        as refunded_payment_value,

    coalesce(shipments.shipment_count, 0) as shipment_count,
    coalesce(shipments.packed_shipment_count, 0)
        as packed_shipment_count,
    coalesce(shipments.shipped_shipment_count, 0)
        as shipped_shipment_count,
    coalesce(shipments.delivered_shipment_count, 0)
        as delivered_shipment_count,
    shipments.average_hours_to_ship,
    shipments.average_hours_to_deliver

from order_metrics as orders

left join payment_metrics as payments
    on orders.order_date = payments.order_date

left join shipment_metrics as shipments
    on orders.order_date = shipments.order_date
