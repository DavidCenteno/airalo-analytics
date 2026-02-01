with source as (
    select
        order_id,
        user_id,
        created_at,
        updated_at,
        amount,
        currency,
        esim_package,
        payment_method,
        card_country,
        destination_country,
        status
    from {{ source('raw', 'orders') }}
)

select
    order_id,
    safe_cast(user_id as int64) as user_id,
    safe_cast(created_at as timestamp) as created_at,
    safe_cast(updated_at as timestamp) as updated_at,
    safe_cast(amount as numeric) as amount,
    upper(currency) as currency,
    esim_package,
    payment_method,
    card_country,
    destination_country,
    status
from source
where nullif(user_id, 'None') is not null -- removed user_id None in one case