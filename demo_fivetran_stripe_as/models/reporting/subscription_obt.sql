with base as (
    select 
    FORMAT_TIMESTAMP('%Y-%m', ts) as d_month_year,
    COUNT_IF(activity = 'subscription created') as m_new_subscriptions,
    SUM_IF(json_field.subscription_amount, activity = 'subscription created') as m_new_mrr,
    COUNT_IF(activity = 'subscription renewed') as retained_subscriptions,
    SUM_IF(json_field.subscription_amount, activity = 'subscription created') as m_retained_mrr,
    json_field.subscription_interval as d_subscription_interval
)

select *
from base