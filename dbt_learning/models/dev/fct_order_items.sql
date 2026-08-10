---This changes the behavior of the table instead of dropping and build again it just insert new rows and merge (If not exist) or update (if exist)
{{ config(
    materialized='incremental',
    unique_key='order_item_id'
) }}


with order_items as (

    select * from {{ ref('stg_order_items') }}
    -- By doing this we will tell dbt to not take take the entier tables and join them in every run, we till it to skip the rows which are already built.ABORT
    {% if is_incremental() %}
        where order_item_id > (select coalesce(max(order_item_id), 0) from {{ this }})
    {% endif %}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

products as (

    select * from {{ ref('stg_products') }}

),

joined as (

    select
        order_items.order_item_id,
        order_items.order_id,
        order_items.product_id,
        orders.customer_id,
        orders.store_id,
        orders.order_date,
        orders.order_status,
        order_items.quantity,
        order_items.unit_price,
        order_items.discount_pct,
        products.cost_price
    from order_items
    left join orders on order_items.order_id = orders.order_id
    left join products on order_items.product_id = products.product_id

),

final as (

    select
        order_item_id,
        order_id,
        product_id,
        customer_id,
        store_id,
        order_date,
        order_status,
        quantity,
        unit_price,
        discount_pct,
        cost_price,
        quantity * unit_price as gross_amount,
        quantity * unit_price * discount_pct / 100 as discount_amount,
        quantity * unit_price * (1 - discount_pct / 100) as net_amount,
        quantity * cost_price as total_cost,
        (quantity * unit_price * (1 - discount_pct / 100)) - (quantity * cost_price) as margin
    from joined

)

select * from final