with source as (
    select * from {{ source('raw', 'exchange_rates') }}
)

select
    upper(currency) as currency,
    safe_cast(usd_rate as numeric) as usd_rate
from source
