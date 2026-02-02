select
    user_id,
    created_at,
    platform,
    acquisition_channel,
    ip_country
from {{ ref('stg_users') }}
