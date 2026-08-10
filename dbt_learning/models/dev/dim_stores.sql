with stg_stores as (

    select * from {{ ref('stg_stores') }}

),

final as (

    select
        store_id,
        store_name,
        city,
        country,
        region,
        opened_date
    from stg_stores

)

select * from final