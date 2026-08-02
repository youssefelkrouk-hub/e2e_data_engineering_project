-- ============================================================================
-- main.sql
--
-- Query collection for the ecommerce_data PostgreSQL warehouse.
-- Run against the 'warehouse-db' service (localhost:5433 from the host,
-- or warehouse-db:5432 from inside the Docker network).
--
-- Organized by layer: raw_layer -> refined_layer -> report_layer
-- ============================================================================


-- ----------------------------------------------------------------------------
-- 0. Sanity checks: schemas and tables
-- ----------------------------------------------------------------------------

-- List all schemas in the database
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name IN ('raw_layer', 'refined_layer', 'report_layer');

-- List all tables across the three warehouse schemas
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema IN ('raw_layer', 'refined_layer', 'report_layer')
ORDER BY table_schema, table_name;

-- Row counts per table (quick health check)
SELECT 'raw_layer.sales' AS table_name, COUNT(*) AS row_count FROM raw_layer.sales
UNION ALL
SELECT 'refined_layer.sales', COUNT(*) FROM refined_layer.sales
UNION ALL
SELECT 'report_layer.sales_by_country', COUNT(*) FROM report_layer.sales_by_country
UNION ALL
SELECT 'report_layer.sales_by_product', COUNT(*) FROM report_layer.sales_by_product
UNION ALL
SELECT 'report_layer.sales_by_payment_method', COUNT(*) FROM report_layer.sales_by_payment_method;


-- ----------------------------------------------------------------------------
-- 1. raw_layer.sales — raw ingested data
-- ----------------------------------------------------------------------------

-- Most recently ingested rows
SELECT *
FROM raw_layer.sales
ORDER BY ingestion_timestamp DESC
LIMIT 20;

-- Rows ingested today
SELECT *
FROM raw_layer.sales
WHERE ingestion_timestamp >= CURRENT_DATE
ORDER BY ingestion_timestamp DESC;

-- Number of rows ingested per day (ingestion history)
SELECT
    CAST(ingestion_timestamp AS DATE) AS ingestion_day,
    COUNT(*) AS rows_ingested
FROM raw_layer.sales
GROUP BY ingestion_day
ORDER BY ingestion_day DESC;


-- ----------------------------------------------------------------------------
-- 2. refined_layer.sales — cleaned & validated data
-- ----------------------------------------------------------------------------

-- Sample of refined data
SELECT *
FROM refined_layer.sales
ORDER BY ingestion_timestamp DESC
LIMIT 20;

-- Valid vs invalid transaction counts
SELECT
    is_valid_transaction,
    COUNT(*) AS transaction_count
FROM refined_layer.sales
GROUP BY is_valid_transaction;

-- Total revenue from valid transactions only
SELECT
    SUM(total_amount) AS total_revenue,
    COUNT(*) AS total_valid_orders,
    AVG(total_amount) AS avg_order_value
FROM refined_layer.sales
WHERE is_valid_transaction = TRUE;

-- Invalid transactions, to inspect why they were flagged
SELECT order_id, quantity, unit_price, order_status, email, is_valid_transaction
FROM refined_layer.sales
WHERE is_valid_transaction = FALSE
ORDER BY ingestion_timestamp DESC
LIMIT 20;


-- ----------------------------------------------------------------------------
-- 3. report_layer — aggregated reporting tables
-- ----------------------------------------------------------------------------

-- Today's report snapshot: revenue by country
SELECT *
FROM report_layer.sales_by_country
WHERE report_date = CURRENT_DATE
ORDER BY total_revenue DESC;

-- Today's report snapshot: revenue by product
SELECT *
FROM report_layer.sales_by_product
WHERE report_date = CURRENT_DATE
ORDER BY total_revenue DESC;

-- Today's report snapshot: revenue by payment method
SELECT *
FROM report_layer.sales_by_payment_method
WHERE report_date = CURRENT_DATE
ORDER BY total_revenue DESC;

-- Top 5 countries by revenue (today's snapshot)
SELECT country_code, total_revenue, total_orders, avg_order_value
FROM report_layer.sales_by_country
WHERE report_date = CURRENT_DATE
ORDER BY total_revenue DESC
LIMIT 5;

-- Top 5 best-selling products by number of orders (today's snapshot)
SELECT product_id, product_name, total_orders, total_revenue
FROM report_layer.sales_by_product
WHERE report_date = CURRENT_DATE
ORDER BY total_orders DESC
LIMIT 5;

-- Most popular payment method by order count (today's snapshot)
SELECT payment_method, total_orders, total_revenue, avg_order_value
FROM report_layer.sales_by_payment_method
WHERE report_date = CURRENT_DATE
ORDER BY total_orders DESC;


-- ----------------------------------------------------------------------------
-- 4. Cross-layer checks (data quality / pipeline validation)
-- ----------------------------------------------------------------------------

-- Rows in raw_layer that have NOT yet made it into refined_layer
-- (should be 0 right after a successful DAG run)
SELECT COUNT(*) AS unprocessed_rows
FROM raw_layer.sales r
WHERE r.order_id NOT IN (SELECT order_id FROM refined_layer.sales);

-- Compare row counts: raw vs refined
SELECT
    (SELECT COUNT(*) FROM raw_layer.sales) AS raw_row_count,
    (SELECT COUNT(*) FROM refined_layer.sales) AS refined_row_count,
    (SELECT COUNT(DISTINCT order_id) FROM raw_layer.sales) AS raw_distinct_orders;