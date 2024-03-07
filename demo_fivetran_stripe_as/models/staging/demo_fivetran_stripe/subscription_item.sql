with raw_source as (

    select *
    from {{ source('demo_fivetran_stripe', 'subscription_item') }}

),

final as (

    select
        cast(id as bytes) as id,
        cast(subscription_id as string) as subscription_id,
        cast(plan_id as bytes) as plan_id,
        cast(_fivetran_start as timestamp) as fivetran_start,
        cast(created as timestamp) as created,
        cast(quantity as int64) as quantity

    from raw_source

)

select * from final
