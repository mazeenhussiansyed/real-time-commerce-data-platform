with ordered_versions as (

    select
        customer_id,
        dbt_valid_from,
        dbt_valid_to,

        lead(dbt_valid_from) over (
            partition by customer_id
            order by dbt_valid_from
        ) as next_valid_from

    from {{ ref('snap_customers') }}

)

select
    customer_id,
    dbt_valid_from,
    dbt_valid_to,
    next_valid_from

from ordered_versions

where next_valid_from is not null
  and (
      dbt_valid_to is null
      or dbt_valid_to > next_valid_from
  )
