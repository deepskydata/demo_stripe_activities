select * from {{ ref('subscription_created')}}

union all

select * from {{ ref('subscription_renewed')}}

union all 

select * from {{ ref('subscription_cancelled')}}

union all

select * from {{ ref('subscription_ended')}}