with orders as (

    select * from {{ ref('stg_orders') }}

),

order_items as (

    select
        order_id,
        sum(net_amount) as order_total,
        count(*) as item_count
    from {{ ref('fct_order_items') }}
    group by order_id

),

final as (

    select
        orders.order_id,
        orders.customer_id,
        orders.store_id,
        orders.order_date,
        orders.order_status,
        orders.shipping_fee,
        orders.currency,
        coalesce(order_items.order_total, 0) as order_total,
        coalesce(order_items.item_count, 0) as item_count

    from orders
    left join order_items
        on orders.order_id = order_items.order_id

)

select * from final