-- Revenue trend for the trailing 24 hours
SELECT
  window_start,
  window_end,
  warehouse_code,
  warehouse_country,
  revenue_euro,
  total_quantity,
  margin_euro,
  distinct_orders
FROM gold_sales_performance
WHERE window_start >= current_timestamp() - INTERVAL 24 HOURS
ORDER BY window_start ASC, warehouse_code;
