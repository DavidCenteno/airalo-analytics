with base as (
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
    from {{ ref('int_orders_latest_state') }}
),

orders_windows as (
    select
        *,

        -- first completed purchase per user
        min(
            case when status = 'completed' then created_at end
        ) over (
            partition by user_id
        ) as first_completed_purchase_at,

        -- most recent completed purchase per user
        max(
            case when status = 'completed' then created_at end
        ) over (
            partition by user_id
        ) as last_completed_purchase_at,

        -- order number (sequence) for completed orders only
        case
            when status = 'completed' then
                row_number() over (
                    partition by user_id
                    order by created_at
                )
        end as completed_order_number,

        -- previous completed order timestamp
        lag(
            case when status = 'completed' then created_at end
        ) over (
            partition by user_id
            order by created_at
        ) as previous_completed_order_at

    from base
)

select
    o.order_id,
    o.user_id,
    o.created_at,
    o.updated_at,
    o.amount as amount_local_currency,
    o.currency,
    round(safe_divide(o.amount, er.usd_rate)) as amount_usd,
    o.esim_package,
    o.payment_method,
    o.card_country,
    o.destination_country,
    o.status,

    -- new vs returning
    o.completed_order_number = 1 as is_new_customer,

    -- user data 
    u.platform,
    u.acquisition_channel,
    u.ip_country as ip_country,

    -- days between first and last completed purchase
    date_diff(
        o.last_completed_purchase_at,
        o.first_completed_purchase_at,
        day
    ) as days_between_first_and_last_completed,

    -- time since previous completed order
    date_diff(
        o.created_at,
        o.previous_completed_order_at,
        day
    ) as days_since_previous_completed_order,

    -- order index (completed only)
    o.completed_order_number

from orders_windows o

left join {{ ref('dim_users') }} u 
    on o.user_id = u.user_id

left join {{ ref('stg_exchange_rates') }} er
  on o.currency = er.currency