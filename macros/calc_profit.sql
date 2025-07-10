{% macro generate_profit_table(sales) %}
select
product_id,
SUM(quantity_sold * unit_sell_price) as total_revenue,
SUM(quantity_sold * unit_purchase_cost) as total_cost,
SUM(quantity_sold * unit_sell_price) - SUM(quantity_sold * unit_purchase_cost) as total_profit
from {{source('airbnb', sales)}}
group by product_id
{% endmacro %}