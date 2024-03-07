with raw_source as (

    select *
    from {{ source('demo_fivetran_stripe', 'invoice') }}

),

final as (

    select
        cast(id as bytes) as id,
        cast(_fivetran_start as timestamp) as fivetran_start,
        cast(auto_advance as boolean) as auto_advance,
        cast(collection_method as string) as collection_method,
        cast(currency as string) as currency,
        cast(customer as string) as customer,
        cast(period_end as timestamp) as period_end,
        cast(period_start as timestamp) as period_start,
        cast(status as string) as status,
        cast(subscription as string) as subscription,
        cast(total as int64) as total,
        cast(created as timestamp) as created,
        cast(paid as boolean) as paid

    from raw_source

)

select * from final
