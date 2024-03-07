with raw_source as (

    select *
    from {{ source('demo_fivetran_stripe', 'subscription_history') }}

),

final as (

    select
        cast(id as string) as id,
        cast(_fivetran_start as timestamp) as fivetran_start,
        cast(default_payment_method_id as string) as default_payment_method_id,
        cast(invoice_id as bytes) as invoice_id,
        cast(application_fee_percent as int64) as application_fee_percent,
        cast(billing_cycle_anchor as timestamp) as billing_cycle_anchor,
        cast(cancel_at as timestamp) as cancel_at,
        cast(cancel_at_period_end as boolean) as cancel_at_period_end,
        cast(created as timestamp) as created,
        cast(collection_method as string) as collection_method,
        cast(current_period_end as timestamp) as current_period_end,
        cast(current_period_start as timestamp) as current_period_start,
        cast(customer as string) as customer,
        cast(days_until_due as int64) as days_until_due,
        cast(ended_at as timestamp) as ended_at,
        cast(livemode as boolean) as livemode,
        cast(quantity as int64) as quantity,
        cast(start_date as timestamp) as start_date,
        cast(status as string) as status

    from raw_source

)

select * from final
