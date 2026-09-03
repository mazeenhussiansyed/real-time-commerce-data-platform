{{ config(materialized='table') }}

select
    md5(payments.payment_id::text) as payment_key,
    md5(payments.order_id::text) as order_key,

    payments.payment_id,
    payments.order_id,
    payments.payment_status,
    payments.payment_method,
    payments.amount,
    payments.paid_at,
    payments.created_at,
    payments.updated_at,

    payments.payment_status = 'authorized' as is_authorized,
    payments.payment_status = 'captured' as is_captured,
    payments.payment_status = 'refunded' as is_refunded,
    payments.payment_status = 'pending' as is_pending,

    payments.current_event_id,
    payments.latest_operation,
    payments.source_event_at,
    payments.warehouse_loaded_at

from {{ ref('int_payments_current') }} as payments
