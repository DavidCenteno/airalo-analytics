with source as (
    select
        user_id,
        created_at,
        platform,
        acquisition_channel,
        ip_country
    from {{ source('raw', 'users') }}
)

select
    safe_cast(user_id as int64) as user_id,
    safe_cast(created_at as timestamp) as created_at,
    platform as platform,
    acquisition_channel as acquisition_channel,
    ip_country as ip_country
from source
