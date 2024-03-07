with raw_source as (

    select *
    from {{ source('demo_fivetran_stripe', 'plan') }}

),

final as (

    select
        cast(id as bytes) as id,
        cast(amount as int64) as amount,
        cast(active as boolean) as active,
        cast(billing_scheme as string) as billing_scheme,
        cast(currency as string) as currency,
        cast(`interval` as string) as interval_type,
        cast(interval_count as int64) as interval_count

    from raw_source

)

select * from final
