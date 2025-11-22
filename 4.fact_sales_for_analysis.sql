-- =============================================
-- Fact Tables Creation
-- =============================================

USE DATABASE db_ecommerce;
USE SCHEMA analytics;

-- created fact_sales
create or replace table analytics.fact_sales as 
select 
row_number() over(order by o.id, oi.id) as sales_key,
o.id as order_id, 
oi.id as order_items_id,
dc.customer_key as customer_key,
dc.customer_name as customer_name,
dp.product_key as product_key,
dp.product_name as product_name,
o.order_date,
o.order_status,
oi.product_id,
oi.quantity,
oi.unit_price,
oi.discounts,
case 
    when oi.discounts > 0 then 'Discounted'
    else 'Full Price'
end as discounts_flag,
oi.quantity * oi.unit_price as gross_amount,
(oi.quantity * oi.unit_price) - oi.discounts as net_amount,
oi.quantity * dp.cost_price as cost_amount,
((oi.quantity * oi.unit_price) - oi.discounts) - (oi.quantity * dp.cost_price) as net_profit_amount,
case 
    when oi.quantity >= 10 then 'Bulk Order'
    when oi.quantity >= 5 then 'Mid Size'
    when oi.quantity >= 2 then 'Small Order'
    else 'Single Order'
end as order_size
from 
raw_data.orders o join raw_data.order_items oi on 
o.id = oi.order_id join dim_customers dc on 
o.customer_id = dc.customer_id join dim_products dp on 
oi.product_id = dp.product_id;
