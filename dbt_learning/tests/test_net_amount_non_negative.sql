{{ config(severity='warn') }}

select *
from {{ ref('fct_order_items') }}
where net_amount < 0