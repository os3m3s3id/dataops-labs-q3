{% snapshot snap_products %}
{{
    config(
        target_schema='RAW',
        unique_key='product_id',
        strategy='check',
        check_cols=['list_price', 'is_active']
    )
}}
select * from {{ ref('raw_products') }}
{% endsnapshot %}