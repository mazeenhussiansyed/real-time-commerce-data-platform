with fact_totals as (

    select
        (select count(*) from {{ ref('fact_orders') }})
            as order_count,

        (select count(*) from {{ ref('fact_payments') }})
            as payment_count,

        (select count(*) from {{ ref('fact_shipments') }})
            as shipment_count,

        (
            select round(sum(order_total), 2)
            from {{ ref('fact_orders') }}
        ) as order_value,

        (
            select round(sum(amount), 2)
            from {{ ref('fact_payments') }}
        ) as payment_value

),

mart_totals as (

    select
        sum(order_count) as order_count,
        sum(payment_count) as payment_count,
        sum(shipment_count) as shipment_count,
        round(sum(gross_order_value), 2) as order_value,
        round(sum(total_payment_value), 2) as payment_value

    from {{ ref('mart_daily_commerce') }}

)

select
    facts.order_count as fact_order_count,
    marts.order_count as mart_order_count,
    facts.payment_count as fact_payment_count,
    marts.payment_count as mart_payment_count,
    facts.shipment_count as fact_shipment_count,
    marts.shipment_count as mart_shipment_count,
    facts.order_value as fact_order_value,
    marts.order_value as mart_order_value,
    facts.payment_value as fact_payment_value,
    marts.payment_value as mart_payment_value

from fact_totals as facts

cross join mart_totals as marts

where facts.order_count <> marts.order_count
   or facts.payment_count <> marts.payment_count
   or facts.shipment_count <> marts.shipment_count
   or abs(facts.order_value - marts.order_value) > 0.01
   or abs(facts.payment_value - marts.payment_value) > 0.01
