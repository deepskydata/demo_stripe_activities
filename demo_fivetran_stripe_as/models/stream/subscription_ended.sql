with subscription_ended_base as (
  SELECT distinct
  id,
  ended_at,
  customer


  FROM {{ref('subscription_history')}}
  where ended_at is not null
),

subscription_ended_prep as (
  select base.*,
  item.quantity,
  plan.amount,
  plan.currency,
  plan.interval_type
  from subscription_ended_base base
  left join {{ref('subscription_item')}} item on base.id = item.subscription_id
  left join {{ref('plan')}} plan on item.plan_id = plan.id
),

subscription_ended as (
  select
  id as activity_id,
  ended_at as ts,
  "subscription ended" as activity,
  customer,
  JSON_OBJECT("subscription_amount",amount,"subscription_currency",currency,"subscription_interval",interval_type) as json_field
  from subscription_ended_prep
)

select * from subscription_ended