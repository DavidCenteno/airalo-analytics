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
from {{ ref('stg_orders') }}
qualify row_number() over (
    partition by order_id
    order by updated_at desc
) = 1
