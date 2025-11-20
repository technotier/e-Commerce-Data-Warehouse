-- Next 30 days Sales Forecast
create or replace view reports.sales_forecast_view as
with 
daily_total_cte as
(
select 
order_date,
sum(net_amount) as daily_total
from 
analytics.fact_sales
group by 1
),
avg_sales_cte as
(
select 
order_date,
daily_total,
round(avg(daily_total) over(order by order_date rows 7 preceding), 2) as weekly_avg,
round(avg(daily_total) over(order by order_date rows 30 preceding), 2) as monthly_avg,
case 
    when dayofweek(order_date) in (1, 7) then 'Weekend'
    else 'Weekday'
end as day_type
from 
daily_total_cte
order by order_date
)
select 
order_date, daily_total, weekly_avg, monthly_avg, day_type,
-- 5% increase
monthly_avg * 1.05 as optimistic_forecast,
-- 5% decline
monthly_avg * 0.95 as conservative_forecast
from 
avg_sales_cte;

select * from reports.sales_forecast_view
limit 5;

-- Churned Customers Detections
create or replace view reports.churned_customers_view as 
select 
f.customer_name,
c.customer_segment,
c.age_group,
sum(f.net_amount) as total_spent,
count(distinct f.order_id) as total_orders,
round(sum(f.net_amount) / count(distinct f.order_id), 2) as aov,
datediff('day', max(f.order_date), current_date()) as days_since_last_order,
case 
    when datediff('day', max(f.order_date), current_date()) > 90 then 'Yes'
    else 'No'
end as is_churned
from 
analytics.fact_sales f join analytics.dim_customers c on 
f.customer_key = c.customer_id
group by 1, 2, 3;

select * from reports.churned_customers_view limit 5;

-- Product Demand Analysis
create or replace view reports.product_demand_view as 
select 
f.product_name,
p.price_segment,
p.margin_category,
sum(f.net_amount) as total_sales,
sum(f.quantity) as unit_sold,
round(corr(f.quantity, dayofmonth(order_date)), 2) as daily_seasonality,
round(corr(f.quantity, month(order_date)), 2) as monthly_seasonalit
from 
analytics.fact_sales f join analytics.dim_products p 
on f.product_id = p.product_id
group by 1, 2, 3;

select * from reports.product_demand_view limit 5;

-- Discount Impact Analysis
create or replace view reports.discount_flag_view as
select
product_name,
discounts_flag,
sum(net_amount) as total_sales,
sum(quantity) as unit_sold
from
analytics.fact_sales
group by 1, 2;

select * from reports.discount_flag_view limit 5;

-- MoM Growth Rate Calculation
create or replace view reports.mom_growth_view as
with 
monthly_sales_cte as
(
select 
date_trunc('month', order_date) as start_month,
sum(net_amount) as total_amount
from 
analytics.fact_sales
group by 1
),
prev_month_cte as 
(
select 
start_month,
total_amount,
lag(total_amount) over(order by start_month) as prev_total,
total_amount - lag(total_amount) over(order by start_month) as sales_change
from 
monthly_sales_cte
),
mom_growth_cte as
(
select
start_month,
total_amount,
prev_total,
sales_change,
round(sales_change * 100.0 / nullif(prev_total, 0), 2) as mom_growth
from 
prev_month_cte
)
select 
to_char(start_month, 'Mon-YYYY') as month,
total_amount, prev_total,
sales_change,
mom_growth,
case 
    when mom_growth > 0 then 'Growth'
    when mom_growth < 0 then 'Decline'
    else 'No Change'
end as growth_trend
from 
mom_growth_cte;

select * from reports.mom_growth_view limit 5;