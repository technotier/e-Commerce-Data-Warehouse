-- =============================================
-- Raw Tables Creation
-- =============================================

use database db_ecommerce;
use schema raw_data;

-- customers table 
create or replace table customers
(
    id int autoincrement primary key,
    first_name varchar(100) not null,
    last_name varchar(100),
    email varchar(100) unique,
    phone string unique,
    city varchar(50),
    country varchar(50),
    gender string,
    signup_date date,
    created_at timestamp_ntz default current_timestamp(),
    updated_at timestamp_ntz default current_timestamp()
);

-- category table
create or replace table category
(
    id int primary key,
    category_name varchar(100),
    created_at timestamp_ntz default current_timestamp(),
    updated_at timestamp_ntz default current_timestamp()
);

-- products table
create or replace table products
(
    id int autoincrement primary key,
    category_id int references category(id),
    product_name varchar(100),
    sale_price number(10, 2),
    cost_price number(10, 2),
    stock_quantity int,
    created_at timestamp_ntz default current_timestamp(),
    updated_at timestamp_ntz default current_timestamp()
);

-- orders table
create or replace table orders
(
    id int autoincrement primary key,
    customer_id int references customers(id),
    order_date date,
    order_status varchar(100),
    created_at timestamp_ntz default current_timestamp(),
    updated_at timestamp_ntz default current_timestamp()
);

-- order_items table
create or replace table order_items
(
    id int autoincrement primary key,
    order_id int references orders(id),
    product_id int references products(id),
    quantity int,
    unit_price number(10, 2),
    discounts number(10, 2),
    created_at timestamp_ntz default current_timestamp(),
    updated_at timestamp_ntz default current_timestamp()
);

-- payments table
create or replace table payments
(
    id int autoincrement primary key,
    order_id int references orders(id),
    payment_method varchar(100),
    payment_status varchar(100),
    payment_date date,
    created_at timestamp_ntz default current_timestamp(),
    updated_at timestamp_ntz default current_timestamp()
);

-- reviews table
create or replace table reviews
(
    id int autoincrement primary key,
    customer_id int references customers(id),
    product_id int references products(id),
    rating int,
    review_text varchar(500),
    review_date date,
    created_at timestamp_ntz default current_timestamp(),
    updated_at timestamp_ntz default current_timestamp()
);
