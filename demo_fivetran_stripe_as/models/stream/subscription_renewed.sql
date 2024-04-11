with subscription_renewed as (
  select
  STRING(TO_JSON(id)) as activity_id,
  period_start as ts,
  "subscription renewed" as activity,
  customer,
  JSON_OBJECT("subscription_amount",total,"subscription_currency",currency) as json_field,
  row_number() OVER (PARTITION BY customer order by period_start) as rank
  FROM {{ref('invoice')}}
)

select
activity_id,
ts,
activity,
customer,
json_field
from subscription_renewed
where rank > 1 and ts < CURRENT_TIMESTAMP()