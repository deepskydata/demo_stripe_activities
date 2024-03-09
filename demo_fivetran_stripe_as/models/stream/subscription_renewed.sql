with subscription_renewed as (
  select
  cast(id as string) as activity_id,
  period_start as ts,
  "subscription renewed" as activity,
  customer,
  JSON_OBJECT("subscription_amount",total,"subscription_currency",currency) as json_field
  FROM {{ref('invoice')}}
  where id not in (
    select
    min(id) OVER(PARTITION BY subscription order by period_start)
    from {{ref('invoice')}}
  )
  
)

select * from subscription_renewed