-- Create schema for Snowflake

-- Create database and schema if not exists
CREATE DATABASE IF NOT EXISTS FASHION_RETAIL_DB;
USE DATABASE FASHION_RETAIL_DB;
CREATE SCHEMA IF NOT EXISTS RETAIL_SCHEMA;
USE SCHEMA RETAIL_SCHEMA;

-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS FASHION_RETAIL_WH
WITH WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 300
AUTO_RESUME = TRUE;

-- Create stage
CREATE STAGE IF NOT EXISTS fashion_retail_stage
URL = 's3://fashion-retail-data-lake'
CREDENTIALS = (AWS_KEY_ID = 'your_aws_key' AWS_SECRET_KEY = 'your_aws_secret');

-- File formats
CREATE FILE FORMAT IF NOT EXISTS csv_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
NULL_IF = ('NULL', 'null')
EMPTY_AS_NULL = TRUE;

CREATE FILE FORMAT IF NOT EXISTS json_format
TYPE = 'JSON'
NULL_IF = ('NULL', 'null');

-- Staging tables
CREATE TABLE IF NOT EXISTS stg_products (
    product_id INTEGER,
    product_name VARCHAR,
    category VARCHAR,
    subcategory VARCHAR,
    brand VARCHAR,
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    supplier_id INTEGER,
    region VARCHAR,
    created_date DATE,
    updated_date DATE
);

CREATE TABLE IF NOT EXISTS stg_transactions (
    sale_id VARCHAR,
    product_id INTEGER,
    sale_date DATE,
    region VARCHAR,
    store_id INTEGER,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    customer_id INTEGER,
    payment_method VARCHAR
);

-- Dimension table
CREATE TABLE IF NOT EXISTS products_dim (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR,
    category VARCHAR,
    subcategory VARCHAR,
    brand VARCHAR,
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    supplier_id INTEGER,
    region VARCHAR,
    created_date DATE,
    updated_date DATE
);

-- Fact table
CREATE TABLE IF NOT EXISTS sales_fact (
    sale_id VARCHAR PRIMARY KEY,
    product_id INTEGER,
    sale_date DATE,
    region VARCHAR,
    store_id INTEGER,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    customer_id INTEGER,
    payment_method VARCHAR,
    FOREIGN KEY (product_id) REFERENCES products_dim(product_id)
);

-- Recommendations table
CREATE TABLE IF NOT EXISTS product_recommendations (
    region VARCHAR,
    product_id INTEGER,
    total_quantity INTEGER,
    rank INTEGER,
    PRIMARY KEY (region, product_id)
);