select
    user_id,

    countif(status = 'completed') as completed_orders,

    min(case when status = 'completed' then created_at end)
        as first_purchase_at,

    max(case when status = 'completed' then created_at end)
        as last_purchase_at,

    sum(case when status = 'completed' then amount_usd end)
        as ltv_usd,

    avg(case when status = 'completed' then amount_usd end)
        as aov_usd

from {{ ref('fct_orders') }}
group by 1