-- Create schema for DuckDB

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