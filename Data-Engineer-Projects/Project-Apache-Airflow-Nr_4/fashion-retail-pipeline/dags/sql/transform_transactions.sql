-- Transform transactions: Load from staging to fact with cleaning and validation

-- For DuckDB/Snowflake compatible
INSERT OR IGNORE INTO sales_fact
SELECT
    st.sale_id,
    st.product_id,
    st.sale_date,
    TRIM(UPPER(st.region)) as region,
    st.store_id,
    st.quantity,
    ROUND(st.unit_price, 2) as unit_price,
    ROUND(st.total_amount, 2) as total_amount,
    st.customer_id,
    TRIM(UPPER(st.payment_method)) as payment_method
FROM stg_transactions st
INNER JOIN products_dim pd ON st.product_id = pd.product_id
WHERE
    st.sale_id IS NOT NULL
    AND st.product_id IS NOT NULL
    AND st.sale_date IS NOT NULL
    AND st.quantity > 0
    AND st.unit_price > 0
    AND st.total_amount > 0;