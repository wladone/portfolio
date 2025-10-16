-- Top selling products by quantity in the trailing 24 hours
SELECT
  product_id,
  product_name,
  category,
  SUM(quantity) AS units_sold,
  SUM(sale_amount_euro) AS revenue_euro,
  SUM(margin_euro) AS margin_euro
FROM silver_sales_enriched
WHERE event_time >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY product_id, product_name, category
ORDER BY units_sold DESC
LIMIT 25;
