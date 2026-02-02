select
    u.user_id,
    u.created_at,
    u.platform,
    u.acquisition_channel,
    u.ip_country,

    f.completed_orders,
    f.first_purchase_at,
    f.last_purchase_at,
    f.ltv_usd,
    f.aov_usd,
    
    date_diff(f.last_purchase_at, f.first_purchase_at, day) as days_between_first_and_last_purchase,

    date_diff(current_date(), date(f.last_purchase_at), day) as days_since_last_purchase

from {{ ref('dim_users') }} u
left join {{ ref('fct_users') }} f
    on u.user_id = f.user_id
