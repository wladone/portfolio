-- BigQuery DDL with partitioning and clustering for streaming aggregates.
-- Replace PROJECT_ID and, if needed, DATASET_NAME before executing.

CREATE TABLE IF NOT EXISTS `PROJECT_ID.ecommerce.product_views_summary`
(
  product_id STRING NOT NULL,
  window_start TIMESTAMP NOT NULL,
  window_end TIMESTAMP NOT NULL,
  view_count INT64 NOT NULL
)
PARTITION BY DATE(window_start)
CLUSTER BY product_id;

CREATE TABLE IF NOT EXISTS `PROJECT_ID.ecommerce.sales_summary`
(
  product_id STRING NOT NULL,
  store_id STRING NOT NULL,
  window_start TIMESTAMP NOT NULL,
  window_end TIMESTAMP NOT NULL,
  sales_count INT64 NOT NULL
)
PARTITION BY DATE(window_start)
CLUSTER BY product_id;

CREATE TABLE IF NOT EXISTS `PROJECT_ID.ecommerce.inventory_summary`
(
  product_id STRING NOT NULL,
  warehouse_id STRING NOT NULL,
  window_start TIMESTAMP NOT NULL,
  window_end TIMESTAMP NOT NULL,
  stock_count INT64 NOT NULL
)
PARTITION BY DATE(window_start)
CLUSTER BY product_id;
