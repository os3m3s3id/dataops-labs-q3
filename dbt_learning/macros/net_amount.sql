{% macro net_amount(quantity, unit_price, discount_pct) %}
    ({{ quantity }} * {{ unit_price }} * (1 - {{ discount_pct }} / 100.0))
{% endmacro %}