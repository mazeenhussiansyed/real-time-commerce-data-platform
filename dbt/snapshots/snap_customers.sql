{% snapshot snap_customers %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
        hard_deletes='new_record'
    )
}}

select
    customer_id,
    first_name,
    last_name,
    email,
    city,
    state_code,
    customer_status,
    created_at,
    updated_at,
    current_event_id,
    latest_operation,
    source_event_at,
    warehouse_loaded_at

from {{ ref('int_customers_current') }}

{% endsnapshot %}
