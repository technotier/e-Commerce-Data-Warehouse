-- =============================================
-- ETL Automation with Streams & Tasks
-- =============================================

-- Create Streams for Change Data Capture
create or replace stream raw_data.customers_stream on table raw_data.customers;
create or replace stream raw_data.products_stream on table raw_data.products;
create or replace stream raw_data.category_stream on table raw_data.category;
create or replace stream raw_data.orders_stream on table raw_data.orders;
create or replace stream raw_data.order_items_stream on table raw_data.order_items;

-- =============================================
-- Task for Dim Customers Automation
-- =============================================
create or replace task analytics.customers_task
warehouse = wh_etl
schedule = '1 minute'
as 
merge into analytics.dim_customers as target 
using 
(
select
id as customer_id,
ROW_NUMBER() OVER(ORDER BY id) + (SELECT COALESCE(MAX(customer_key), 0) FROM analytics.dim_customers) AS customer_key,
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
end as age_group,
metadata$action as stream_action,
metadata$isupdate as is_update
from
raw_data.customers_stream
) as source on target.customer_id = source.customer_id 
when matched and source.stream_action = 'DELETE' then delete 
when matched and source.stream_action = 'INSERT' and source.is_update = TRUE then 
update set
customer_name = source.customer_name,
email = source.email,
phone = source.phone,
city = source.city,
country = source.country,
gender = source.gender,
signup_date = source.signup_date,
signup_year = source.signup_year,
signup_month = source.signup_month,
signup_quarter = source.signup_quarter,
days_as_customer = source.days_as_customer,
customer_segment = source.customer_segment,
dob = source.dob,
age = source.age,
age_group = source.age_group
when not matched and source.stream_action = 'INSERT' then 
insert(
customer_key, customer_id, customer_name, email, phone, city, country, gender, signup_date, signup_year, signup_month, signup_quarter, days_as_customer, customer_segment, dob, age, age_group
)
values(
source.customer_key, source.customer_id, source.customer_name, source.email, source.phone, source.city, source.country, source.gender, source.signup_date, source.signup_year, source.signup_month,
source.signup_quarter, source.days_as_customer, source.customer_segment, source.dob, source.age, source.age_group
);

-- =============================================
-- Task for Dim Products Automation
-- =============================================
create or replace task analytics.products_task 
warehouse = wh_etl
schedule = '1 minute'
as 
merge into analytics.dim_products as target 
using 
(
select 
ROW_NUMBER() OVER(ORDER BY p.id) + (SELECT COALESCE(MAX(product_key), 0) FROM analytics.dim_products) AS product_key,
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
p.metadata$action as stream_action,
p.metadata$isupdate as is_update
from raw_data.products_stream p left join 
raw_data.category c on p.category_id = c.id
) as source on target.product_id = source.product_id
when matched and source.stream_action = 'DELETE' then delete 
when matched and source.stream_action = 'INSERT' and source.is_update =TRUE then 
update set
category_id = source.category_id,
product_name = source.product_name,
category_name = source.category_name,
sale_price = source.sale_price,
cost_price = source.cost_price,
profit_margin = source.profit_margin,
profit_percent = source.profit_percent,
stock_quantity = source.stock_quantity,
stock_status = source.stock_status,
price_segment = source.price_segment,
margin_category = source.margin_category
when not matched and source.stream_action = 'INSERT' then 
insert
(
product_key, product_id, category_id, product_name, category_name, sale_price, cost_price, profit_margin,
profit_percent, stock_quantity, stock_status, price_segment, margin_category
)
values 
(
source.product_key, source.product_id, source.category_id, source.product_name, source.category_name, source.sale_price, source.cost_price, source.profit_margin,
source.profit_percent, source.stock_quantity, source.stock_status, source.price_segment, source.margin_category
);

-- =============================================
-- Task for Fact Sales Automation
-- =============================================
create or replace task analytics.fact_sales_task
warehouse = wh_etl
schedule = '1 MINUTE'
as
merge into analytics.fact_sales as target 
using 
(
select 
ROW_NUMBER() OVER(ORDER BY o.id, oi.id) + (SELECT COALESCE(MAX(sales_key), 0) FROM analytics.fact_sales)
AS sales_key,
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
end as order_size,
o.metadata$action as stream_action,
o.metadata$isupdate as is_update
from 
raw_data.orders_stream o join raw_data.order_items_stream oi on 
o.id = oi.order_id join analytics.dim_customers dc on 
o.customer_id = dc.customer_id join analytics.dim_products dp on 
oi.product_id = dp.product_id
where o.metadata$action = 'INSERT'
) as source on target.order_id = source.order_id and target.order_items_id = source.order_items_id
when matched and source.stream_action = 'DELETE' then delete 
when matched and source.stream_action = 'INSERT' then 
update set
customer_key = source.customer_key,
customer_name = source.customer_name,
product_key = source.product_key,
product_name = source.product_name,
order_date = source.order_date,
order_status = source.order_status,
product_id = source.product_id,
quantity = source.quantity,
unit_price = source.unit_price,
discounts = source.discounts,
discounts_flag = source.discounts_flag,
gross_amount = source.gross_amount,
net_amount = source.net_amount,
cost_amount = source.cost_amount,
net_profit_amount = source.net_profit_amount,
order_size = source.order_size
when not matched and source.stream_action = 'INSERT' then 
insert(
sales_key, order_id, order_items_id, customer_key, customer_name, product_key, product_name, order_date,
order_status, product_id, quantity, unit_price, discounts, discounts_flag, gross_amount, net_amount,
cost_amount, net_profit_amount, order_size
)
values 
(
source.sales_key, source.order_id, source.order_items_id, source.customer_key, source.customer_name, source.product_key, source.product_name, source.order_date,
source.order_status, source.product_id, source.quantity, source.unit_price, source.discounts, source.discounts_flag, source.gross_amount, source.net_amount,
source.cost_amount, source.net_profit_amount, source.order_size
);

-- =============================================
-- Enable All Tasks
-- =============================================
alter task analytics.customers_task;
alter task analytics.products_task;
alter task analytics.fact_sales_task;










