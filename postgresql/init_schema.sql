-- ============================================================================
-- init_schema.sql
--
-- Initializes the ecommerce_data warehouse schema (raw_layer / refined_layer
-- / report_layer) — medallion architecture.
--
-- This DDL is compatible with BOTH backends used in this project:
--   - DuckDB    (run via the DuckDB CLI or duckdb.connect(...).execute(open(...).read()))
--   - PostgreSQL (run via psql, pgAdmin, or any Postgres client)
--
-- Usage:
--   DuckDB:
--     duckdb /opt/airflow/db/ecommerce_data.duckdb -c ".read init_schema.sql"
--
--   PostgreSQL (from the host, warehouse-db mapped to localhost:5433):
--     psql -h localhost -p 5433 -U dwh_user -d ecommerce_data -f init_schema.sql
--
--   PostgreSQL (from inside the Airflow container):
--     docker exec -it airflow-airflow-worker-1 \
--       psql -h warehouse-db -U dwh_user -d ecommerce_data -f /opt/airflow/db/init_schema.sql
-- ============================================================================


-- ----------------------------------------------------------------------------
-- 1. Schemas
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS raw_layer;
CREATE SCHEMA IF NOT EXISTS refined_layer;
CREATE SCHEMA IF NOT EXISTS report_layer;


-- ----------------------------------------------------------------------------
-- 2. Raw layer — raw data exactly as received from the Mockaroo API
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_layer.sales (
    order_id             INTEGER,
    order_date           DATE,
    customer_id          INTEGER,
    customer_name        VARCHAR,
    product_id           INTEGER,
    product_name         VARCHAR,
    quantity             INTEGER,
    unit_price           DECIMAL(10, 2),
    payment_method       VARCHAR,
    order_status         VARCHAR,
    email                VARCHAR,
    gender               VARCHAR,
    country_code         VARCHAR,
    ingestion_timestamp  TIMESTAMP DEFAULT current_timestamp
);


-- ----------------------------------------------------------------------------
-- 3. Refined layer — cleaned, validated, deduplicated data
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS refined_layer.sales (
    order_id              INTEGER PRIMARY KEY,
    order_date            DATE,
    customer_id           INTEGER,
    customer_name         VARCHAR,
    product_id            INTEGER,
    product_name          VARCHAR,
    quantity              INTEGER,
    unit_price            DECIMAL(10, 2),
    total_amount          DECIMAL(12, 2),
    payment_method        VARCHAR,
    order_status          VARCHAR,
    email                 VARCHAR,
    gender                VARCHAR,
    country_code          VARCHAR,
    is_valid_transaction  BOOLEAN,
    ingestion_timestamp   TIMESTAMP DEFAULT current_timestamp
);


-- ----------------------------------------------------------------------------
-- 4. Report layer — pre-aggregated reporting tables
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS report_layer.sales_by_country (
    country_code     VARCHAR,
    total_revenue    DECIMAL(14, 2),
    total_orders     INTEGER,
    avg_order_value  DECIMAL(12, 2),
    report_date      DATE DEFAULT current_date
);

CREATE TABLE IF NOT EXISTS report_layer.sales_by_product (
    product_id       INTEGER,
    product_name     VARCHAR,
    total_revenue    DECIMAL(14, 2),
    total_orders     INTEGER,
    avg_order_value  DECIMAL(12, 2),
    report_date      DATE DEFAULT current_date
);

CREATE TABLE IF NOT EXISTS report_layer.sales_by_payment_method (
    payment_method   VARCHAR,
    total_revenue    DECIMAL(14, 2),
    total_orders     INTEGER,
    avg_order_value  DECIMAL(12, 2),
    report_date      DATE DEFAULT current_date
);


-- ----------------------------------------------------------------------------
-- 5. Sanity check — list everything just created
-- ----------------------------------------------------------------------------
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema IN ('raw_layer', 'refined_layer', 'report_layer')
ORDER BY table_schema, table_name;