{{ config(materialized='table') }}

select
    md5(
        customer_id::text
        || '|'
        || dbt_valid_from::text
    ) as customer_key,

    customer_id,
    first_name,
    last_name,
    first_name || ' ' || last_name as full_name,
    email,
    city,
    state_code,
    customer_status,
    created_at,
    updated_at,

    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    coalesce(dbt_is_deleted::boolean, false) as is_deleted,

    (
        dbt_valid_to is null
        and not coalesce(dbt_is_deleted::boolean, false)
    ) as is_current,

    current_event_id,
    latest_operation,
    source_event_at,
    warehouse_loaded_at

from {{ ref('snap_customers') }}
