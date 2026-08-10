with stg_products as (

    select * from {{ ref('stg_products') }}

),

final as (

    select
        product_id,
        product_name,
        category,
        subcategory,
        currency,
        launch_date,
        is_active,
        cost_price,
        list_price,
        list_price - cost_price as unit_margin
    from stg_products

)

select * from final