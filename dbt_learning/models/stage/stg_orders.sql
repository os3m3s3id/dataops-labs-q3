with source as (

    select * from {{ ref('raw_orders') }}

),

cleaned as (

    select
        order_id::integer as order_id,
        trim(customer_id)::text as customer_id,
        order_date::date as order_date,
        lower(trim(status))::text as order_status,
        store_id,
        coalesce(shipping_fee, 0)::numeric(12,2) as shipping_fee,
        trim(currency) as currency
    from source

),

deduped as (

    select
        *,
        row_number() over (
            partition by order_id
            order by
                case when order_status = 'completed' then 1 else 2 end,
                order_date
        ) as rn
    from cleaned

)

select
    order_id,
    customer_id,
    order_date,
    order_status,
    store_id,
    shipping_fee,
    currency
from deduped
where rn = 1