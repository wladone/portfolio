-- Warehouse/category inventory heatmap
WITH exploded AS (
  SELECT
    product_id,
    product_name,
    category,
    wb.warehouse_code,
    wb.quantity_on_hand
  FROM gold_inventory_levels
  LATERAL VIEW explode(warehouse_breakdown) AS wb
), aggregated AS (
  SELECT
    warehouse_code,
    category,
    SUM(quantity_on_hand) AS total_quantity
  FROM exploded
  GROUP BY warehouse_code, category
)
SELECT
  warehouse_code,
  category,
  total_quantity,
  total_quantity / SUM(total_quantity) OVER (PARTITION BY warehouse_code) AS share_within_warehouse
FROM aggregated
ORDER BY warehouse_code, total_quantity DESC;
