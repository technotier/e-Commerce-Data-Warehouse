-- =============================================
-- Dimension Tables Creation
-- =============================================

USE DATABASE db_ecommerce;
USE SCHEMA analytics;

-- create analytics.dim_customers
create or replace table analytics.dim_customers as 
select
row_number() over(order by id) as customer_key,
id as customer_id,
initcap(trim(concat(first_name, ' ', last_name))) as customer_name,
lower(trim(email)) as email,
trim(phone) as phone,
upper(trim(city)) as city,
upper(trim(country)) as country,
case 
    when lower(trim(gender)) = 'male' then 'M'
    when lower(trim(gender)) = 'female' then 'F'
end as gender,
signup_date,
year(signup_date) as signup_year,
month(signup_date) as signup_month,
quarter(signup_date) as signup_quarter,
datediff('day', signup_date, current_date()) as days_as_customer,
case 
    when datediff('day', signup_date, current_date()) > 730 then 'Super Loyal'
    when datediff('day', signup_date, current_date()) > 365 then 'Loyal'
    when datediff('day', signup_date, current_date()) > 180 then 'Champion'
    when datediff('day', signup_date, current_date()) > 90 then 'Regular'
    when datediff('day', signup_date, current_date()) > 45 then 'New'
    else 'Very New'
end as customer_segment,
dob,
datediff('year', dob, current_date()) as age,
case 
    when datediff('year', dob, current_date()) between 13 and 19 then 'Teenage'
    when datediff('year', dob, current_date()) between 20 and 30 then 'Young Adult'
    when datediff('year', dob, current_date()) between 31 and 40 then 'Adult'
    when datediff('year', dob, current_date()) between 41 and 59 then 'Mid Age'
    when datediff('year', dob, current_date()) > 59 then 'Senior Citizen'
    else 'Unknown'
end as age_group
from 
raw_data.customers;

-- create analytics.dim_products
create or replace table analytics.dim_products as 
select 
row_number() over(order by p.id) as product_key,
p.id as product_id,
p.category_id,
initcap(trim(p.product_name)) as product_name,
initcap(trim(c.category_name)) as category_name,
p.sale_price as sale_price,
p.cost_price as cost_price,
p.sale_price - p.cost_price as profit_margin,
round((p.sale_price - p.cost_price) * 100.0 / nullif(p.sale_price, 0), 2) as profit_percent,
p.stock_quantity as stock_quantity,
case 
    when p.stock_quantity > 100 then 'High Stock'
    when p.stock_quantity > 50 then 'Medium'
    when p.stock_quantity > 25 then 'Low Stock'
    else 'Out of Stock'
end as stock_status,
case 
    when p.sale_price > 1000 then 'Luxury'
    when p.sale_price > 750 then 'Premium'
    when p.sale_price > 500 then 'High Price'
    when p.sale_price > 250 then 'In Budget'
    when p.sale_price > 100 then 'Economy'
    else 'Low Price'
end as price_segment,
case 
    when (p.sale_price - p.cost_price) * 100.0 / nullif(p.sale_price, 0) > 50 then 'High Margin'
    when (p.sale_price - p.cost_price) * 100.0 / nullif(p.sale_price, 0) > 25 then 'Medium Margin'
    else 'Low Margin'
end as margin_category,
current_timestamp() as dim_products_updated
from 
raw_data.products p join raw_data.category c on 
p.category_id = c.id;


