-- Databricks Delta Live Tables pipeline for the Global Electronics Lakehouse
-- This SQL pipeline defines Bronze/Silver/Gold layers with expectations, deduplication, and anomaly detection.

CREATE OR REFRESH LIVE TABLE bronze_inventory
COMMENT "Daily inventory snapshots ingested via Auto Loader (CSV)."
AS
SELECT
  CAST(inventory_date AS DATE) AS inventory_date,
  UPPER(TRIM(warehouse_code)) AS warehouse_code,
  TRIM(product_id) AS product_id,
  TRIM(product_name) AS product_name,
  TRIM(category) AS category,
  CAST(quantity_on_hand AS INT) AS quantity_on_hand,
  CAST(unit_cost_euro AS DECIMAL(10,2)) AS unit_cost_euro,
  input_file_name() AS input_file,
  current_timestamp() AS ingestion_ts
FROM cloud_files(
  "{{config('pipelines.electronics.source_inventory')}}",
  "csv",
  map(
    "header", "true",
    "delimiter", ",",
    "cloudFiles.format", "csv",
    "cloudFiles.schemaHints", "inventory_date DATE, warehouse_code STRING, product_id STRING, product_name STRING, category STRING, quantity_on_hand DOUBLE, unit_cost_euro DOUBLE",
    "cloudFiles.inferColumnTypes", "false",
    "cloudFiles.useIncrementalListing", "true",
    "cloudFiles.schemaEvolutionMode", "addNewColumns"
  )
);

CREATE OR REFRESH LIVE TABLE bronze_suppliers
COMMENT "Supplier master data ingested from JSON lines via Auto Loader."
AS
SELECT
  TRIM(supplier_id) AS supplier_id,
  TRIM(supplier_name) AS supplier_name,
  CAST(rating AS DOUBLE) AS rating,
  CAST(lead_time_days AS INT) AS lead_time_days,
  CAST(preferred AS BOOLEAN) AS preferred,
  LOWER(TRIM(contact_email)) AS contact_email,
  category_focus,
  input_file_name() AS input_file,
  current_timestamp() AS ingestion_ts
FROM cloud_files(
  "{{config('pipelines.electronics.source_suppliers')}}",
  "json",
  map(
    "multiLine", "false",
    "cloudFiles.format", "json",
    "cloudFiles.schemaHints", "supplier_id STRING, supplier_name STRING, rating DOUBLE, lead_time_days INT, preferred BOOLEAN, contact_email STRING, category_focus ARRAY<STRING>",
    "cloudFiles.inferColumnTypes", "false",
    "cloudFiles.useIncrementalListing", "true",
    "cloudFiles.schemaEvolutionMode", "addNewColumns"
  )
);

CREATE OR REFRESH STREAMING LIVE TABLE bronze_sales_stream
COMMENT "Raw sales payloads streamed from Kafka topic global_electronics_sales."
AS
SELECT
  CAST(value AS STRING) AS raw_value,
  CAST(key AS STRING) AS message_key,
  headers,
  timestamp AS kafka_timestamp,
  current_timestamp() AS ingestion_ts
FROM STREAM(
  read_kafka(
    bootstrap_servers => '{{config("pipelines.electronics.kafka_bootstrap")}}',
    subscribe => 'global_electronics_sales',
    starting_offsets => 'latest',
    fail_on_data_loss => 'false',
    include_headers => 'true'
  )
);

CREATE OR REFRESH STREAMING LIVE TABLE bronze_sales_parsed
COMMENT "Parsed sales events with normalized schema and derived metrics."
AS
WITH parsed AS (
  SELECT
    from_json(
      raw_value,
      'struct<event_id:string,event_time:string,order_id:string,product_id:string,warehouse_code:string,channel:string,payment_type:string,customer_id:string,quantity:int,unit_price_euro:double,discount_rate:double>'
    ) AS payload,
    message_key,
    kafka_timestamp,
    ingestion_ts
  FROM LIVE.bronze_sales_stream
)
SELECT
  payload.event_id,
  payload.order_id,
  payload.product_id,
  UPPER(TRIM(payload.warehouse_code)) AS warehouse_code,
  TRIM(payload.channel) AS channel,
  TRIM(payload.payment_type) AS payment_type,
  TRIM(payload.customer_id) AS customer_id,
  CAST(payload.quantity AS INT) AS quantity,
  CAST(payload.unit_price_euro AS DECIMAL(10,2)) AS unit_price_euro,
  COALESCE(CAST(payload.discount_rate AS DOUBLE), 0.0) AS discount_rate,
  CAST(payload.quantity * payload.unit_price_euro * (1 - COALESCE(payload.discount_rate, 0.0)) AS DECIMAL(12,2)) AS sale_amount_euro,
  to_timestamp(payload.event_time) AS event_time,
  DATE(to_timestamp(payload.event_time)) AS event_date,
  message_key,
  kafka_timestamp,
  ingestion_ts
FROM parsed
WHERE payload.order_id IS NOT NULL AND payload.product_id IS NOT NULL;

CREATE OR REFRESH LIVE TABLE silver_inventory
COMMENT "Cleaned inventory snapshots with enforced data quality constraints."
CONSTRAINT non_negative_inventory EXPECT (quantity_on_hand >= 0) ON VIOLATION FAIL UPDATE
AS
SELECT
  inventory_date,
  warehouse_code,
  product_id,
  product_name,
  category,
  CAST(quantity_on_hand AS INT) AS quantity_on_hand,
  CAST(unit_cost_euro AS DECIMAL(10,2)) AS unit_cost_euro,
  ingestion_ts
FROM LIVE.bronze_inventory;

CREATE OR REFRESH LIVE TABLE silver_suppliers
COMMENT "Validated supplier dimension with rating expectation enforcement."
CONSTRAINT rating_between_zero_and_five EXPECT (rating BETWEEN 0 AND 5) ON VIOLATION DROP ROW
AS
SELECT
  supplier_id,
  supplier_name,
  rating,
  lead_time_days,
  preferred,
  contact_email,
  category_focus,
  ingestion_ts
FROM LIVE.bronze_suppliers;

CREATE OR REFRESH LIVE TABLE silver_product_reference
COMMENT "Product reference table derived from the latest inventory snapshots."
AS
SELECT
  product_id,
  max_by(product_name, inventory_date) AS product_name,
  max_by(category, inventory_date) AS category,
  AVG(unit_cost_euro) AS avg_unit_cost_euro
FROM LIVE.silver_inventory
GROUP BY product_id;

CREATE OR REFRESH STREAMING LIVE TABLE silver_sales_cleaned
COMMENT "Deduplicated sales events with quality expectations and watermarking."
CONSTRAINT quantity_positive EXPECT (quantity > 0) ON VIOLATION DROP ROW
CONSTRAINT amount_positive EXPECT (sale_amount_euro > 0) ON VIOLATION DROP ROW
AS
WITH dedup_source AS (
  SELECT
    event_id,
    event_time,
    event_date,
    order_id,
    product_id,
    warehouse_code,
    channel,
    payment_type,
    customer_id,
    quantity,
    unit_price_euro,
    discount_rate,
    sale_amount_euro,
    kafka_timestamp,
    ingestion_ts,
    ROW_NUMBER() OVER (
      PARTITION BY order_id, product_id, warehouse_code
      ORDER BY event_time DESC, ingestion_ts DESC
    ) AS rn
  FROM STREAM(LIVE.bronze_sales_parsed)
  APPLY WATERMARK event_time INTERVAL 2 HOURS
)
SELECT
  event_id,
  event_time,
  event_date,
  order_id,
  product_id,
  warehouse_code,
  channel,
  payment_type,
  customer_id,
  quantity,
  unit_price_euro,
  discount_rate,
  sale_amount_euro,
  kafka_timestamp,
  ingestion_ts
FROM dedup_source
WHERE rn = 1;

CREATE OR REFRESH STREAMING LIVE TABLE silver_sales_enriched
COMMENT "Sales facts enriched with product attributes and margin calculation."
CONSTRAINT product_known EXPECT (product_name IS NOT NULL) ON VIOLATION DROP ROW
AS
SELECT
  s.event_id,
  s.event_time,
  s.event_date,
  s.order_id,
  s.product_id,
  pr.product_name,
  pr.category,
  s.warehouse_code,
  CASE s.warehouse_code
    WHEN 'FRA' THEN 'France'
    WHEN 'BUC' THEN 'Romania'
    WHEN 'MAD' THEN 'Spain'
    ELSE 'Unknown'
  END AS warehouse_country,
  s.channel,
  s.payment_type,
  s.customer_id,
  s.quantity,
  s.unit_price_euro,
  s.discount_rate,
  s.sale_amount_euro,
  pr.avg_unit_cost_euro,
  CAST(s.sale_amount_euro - s.quantity * pr.avg_unit_cost_euro AS DECIMAL(12,2)) AS margin_euro,
  s.kafka_timestamp,
  s.ingestion_ts
FROM STREAM(LIVE.silver_sales_cleaned) s
LEFT JOIN LIVE.silver_product_reference pr
  ON s.product_id = pr.product_id;

CREATE OR REFRESH STREAMING LIVE TABLE silver_sales_invalid_products
COMMENT "Sales events referencing unknown products for observability and replay."
AS
SELECT
  s.*
FROM STREAM(LIVE.silver_sales_cleaned) s
LEFT JOIN LIVE.silver_product_reference pr
  ON s.product_id = pr.product_id
WHERE pr.product_id IS NULL;

CREATE OR REFRESH LIVE TABLE gold_inventory_levels
COMMENT "Latest inventory position per product aggregated across all warehouses."
AS
WITH latest_per_warehouse AS (
  SELECT
    warehouse_code,
    product_id,
    product_name,
    category,
    unit_cost_euro,
    inventory_date,
    quantity_on_hand,
    ROW_NUMBER() OVER (
      PARTITION BY warehouse_code, product_id
      ORDER BY inventory_date DESC
    ) AS rn
  FROM LIVE.silver_inventory
)
SELECT
  product_id,
  max_by(product_name, inventory_date) AS product_name,
  max_by(category, inventory_date) AS category,
  SUM(quantity_on_hand) AS total_quantity_on_hand,
  SUM(quantity_on_hand * unit_cost_euro) AS stock_value_euro,
  MAX(inventory_date) AS as_of_date,
  COLLECT_LIST(STRUCT(warehouse_code, quantity_on_hand)) AS warehouse_breakdown
FROM latest_per_warehouse
WHERE rn = 1
GROUP BY product_id;

CREATE OR REFRESH STREAMING LIVE TABLE gold_sales_performance
COMMENT "Hourly revenue and quantity performance per warehouse with watermarking."
AS
SELECT
  s.warehouse_code,
  s.warehouse_country,
  window_start AS window_start,
  window_end AS window_end,
  SUM(s.quantity) AS total_quantity,
  SUM(s.sale_amount_euro) AS revenue_euro,
  SUM(s.margin_euro) AS margin_euro,
  COUNT(DISTINCT s.order_id) AS distinct_orders
FROM (
  SELECT
    warehouse_code,
    warehouse_country,
    order_id,
    quantity,
    sale_amount_euro,
    margin_euro,
    window.start AS window_start,
    window.end AS window_end
  FROM (
    SELECT
      warehouse_code,
      warehouse_country,
      order_id,
      quantity,
      sale_amount_euro,
      margin_euro,
      window(event_time, '1 hour', '5 minutes') AS window
    FROM STREAM(LIVE.silver_sales_enriched)
    APPLY WATERMARK event_time INTERVAL 26 HOURS
  )
) s
GROUP BY s.warehouse_code, s.warehouse_country, window_start, window_end;

CREATE OR REFRESH STREAMING LIVE TABLE gold_order_anomalies
COMMENT "Order-level anomaly detection using IQR over the trailing 24 hours."
AS
WITH order_totals AS (
  SELECT
    order_id,
    warehouse_code,
    warehouse_country,
    MAX(event_time) AS order_timestamp,
    SUM(sale_amount_euro) AS order_total_euro,
    SUM(quantity) AS total_quantity
  FROM STREAM(LIVE.silver_sales_enriched)
  APPLY WATERMARK event_time INTERVAL 26 HOURS
  GROUP BY order_id, warehouse_code, warehouse_country
),
windowed AS (
  SELECT
    order_id,
    warehouse_code,
    warehouse_country,
    order_timestamp,
    order_total_euro,
    total_quantity,
    window(order_timestamp, '24 hours', '1 hour') AS stats_window
  FROM order_totals
),
window_stats AS (
  SELECT
    warehouse_code,
    warehouse_country,
    stats_window,
    percentile_approx(order_total_euro, 0.25, 100) AS q1,
    percentile_approx(order_total_euro, 0.75, 100) AS q3
  FROM windowed
  GROUP BY warehouse_code, warehouse_country, stats_window
)
SELECT
  w.order_id,
  w.warehouse_code,
  w.warehouse_country,
  w.order_timestamp AS event_time,
  w.order_total_euro,
  w.total_quantity,
  w.stats_window.start AS window_start,
  w.stats_window.end AS window_end,
  ws.q1,
  ws.q3,
  (ws.q3 - ws.q1) AS iqr,
  CASE
    WHEN ws.q3 = ws.q1 THEN FALSE
    WHEN w.order_total_euro < ws.q1 - 1.5 * (ws.q3 - ws.q1) THEN TRUE
    WHEN w.order_total_euro > ws.q3 + 1.5 * (ws.q3 - ws.q1) THEN TRUE
    ELSE FALSE
  END AS is_anomalous
FROM windowed w
JOIN window_stats ws
  ON w.warehouse_code = ws.warehouse_code
 AND w.warehouse_country = ws.warehouse_country
 AND w.stats_window = ws.stats_window;
