with subscription_cancelled_base as (
  SELECT distinct
  id,
  cancel_at,
  customer


  FROM {{ref('subscription_history')}}
  where cancel_at is not null
),

subscription_cancelled_prep as (
  select base.*,
  item.quantity,
  plan.amount,
  plan.currency,
  plan.interval_type
  from subscription_cancelled_base base
  left join {{ref('subscription_item')}} item on base.id = item.subscription_id
  left join {{ref('plan')}} plan on item.plan_id = plan.id
),

subscription_cancelled as (
  select
  id as activity_id,
  cancel_at as ts,
  "subscription cancelled" as activity,
  customer,
  JSON_OBJECT("subscription_amount",amount,"subscription_currency",currency,"subscription_interval",interval_type) as json_field
  from subscription_cancelled_prep
)

select * from subscription_cancelled