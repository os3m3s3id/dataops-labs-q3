with stg_customers as (

    select * from {{ ref('stg_customers') }}

),

final as (

    select
        customer_id,
        first_name || ' ' || last_name as full_name,
        email,
        phone,
        country,
        city,
        signup_date
    from stg_customers

)

select * from final