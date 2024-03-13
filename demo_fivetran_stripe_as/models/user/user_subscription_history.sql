with subscription_history as (
    select
    base.fivetran_start,
    base.customer,
    base.current_period_end as subscription_current_period_end,
    base.current_period_start as subscription_current_period_start,
    base.status as subscription_status,
    base.start_date as subscription_started_at,
    base.cancel_at as subscription_cancelled_at,
    base.ended_at as subscription_ended_at,
    item.quantity as subscription_quantity,
    plan.amount as subscription_mrr,
    plan.currency as subscription_currency,
    plan.interval_type as subscription_interval

    from {{ ref('subscription_history')}} base
    left join {{ref('subscription_item')}} item on base.id = item.subscription_id
    left join {{ref('plan')}} plan on item.plan_id = plan.id
)

select * from subscription_history