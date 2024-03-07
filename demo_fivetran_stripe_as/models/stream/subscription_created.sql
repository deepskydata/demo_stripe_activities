with subscription_created_base as (
  SELECT distinct
  id,
  start_date,
  customer

  from {{ref('subscription_history')}}
  
),

subscription_created_prep as (
  select base.*,
  item.quantity,
  plan.amount,
  plan.currency,
  plan.interval_type
  from subscription_created_base base
  left join {{ref('subscription_item')}} item on base.id = item.subscription_id
  left join {{ref('plan')}} plan on item.plan_id = plan.id
),

subscription_created as (
  select
  id as activity_id,
  start_date as ts,
  "subscription created" as activity,
  customer,
  JSON_OBJECT("subscription_amount",amount,"subscription_currency",currency,"subscription_interval",interval_type) as json_field
  from subscription_created_prep
)

select * from subscription_created