select
    customer_id,
    count(*) filter (
        where dbt_valid_to is null
    ) as latest_version_count

from {{ ref('snap_customers') }}

group by customer_id

having count(*) filter (
    where dbt_valid_to is null
) <> 1
