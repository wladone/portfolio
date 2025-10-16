-- Transform products: Load from staging to dimension with cleaning and validation

-- For DuckDB/Snowflake compatible
-- Use REPLACE to handle existing data
INSERT OR REPLACE INTO products_dim
SELECT
    product_id,
    TRIM(UPPER(product_name)) as product_name,
    TRIM(UPPER(category)) as category,
    TRIM(UPPER(subcategory)) as subcategory,
    TRIM(UPPER(brand)) as brand,
    ROUND(price, 2) as price,
    ROUND(cost, 2) as cost,
    supplier_id,
    TRIM(UPPER(region)) as region,
    created_date,
    COALESCE(updated_date, created_date) as updated_date
FROM stg_products
WHERE
    product_id IS NOT NULL
    AND product_name IS NOT NULL
    AND category IS NOT NULL
    AND price > 0
    AND cost > 0;