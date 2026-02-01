with source as (
    select
        currency,
        usd_rate
    from {{ source('raw', 'exchange_rates') }}
)

select
    upper(currency) as currency,
    safe_cast(usd_rate as numeric) as usd_rate
from source
