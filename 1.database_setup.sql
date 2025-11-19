-- =============================================
-- E-Commerce Data Warehouse - Database Setup
-- =============================================

-- Create Warehouse
CREATE OR REPLACE WAREHOUSE wh_ecommerce
WAREHOUSE_SIZE = 'X-SMALL'
AUTO_SUSPEND = 300
AUTO_RESUME = TRUE
INITIALLY_SUSPENDED = TRUE
COMMENT = 'Main warehouse for e-commerce data processing';

-- Create Database and Schemas
CREATE OR REPLACE DATABASE db_ecommerce;
USE DATABASE db_ecommerce;

CREATE OR REPLACE SCHEMA raw_data
COMMENT = 'Raw data from source systems';

CREATE OR REPLACE SCHEMA analytics
COMMENT = 'Cleaned and transformed data for analytics';

-- ETL Warehouse
CREATE OR REPLACE WAREHOUSE wh_etl
WAREHOUSE_SIZE = 'X-SMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE
INITIALLY_SUSPENDED = TRUE
COMMENT = 'ETL processing warehouse';