"""
init_db_schema.py

Initializes the ecommerce_data warehouse schema (raw_layer /
refined_layer / report_layer) on BOTH storage backends:
  - DuckDB (local file, /opt/airflow/db/ecommerce_data.duckdb)
  - PostgreSQL (warehouse-db container)

Run this once before the Airflow DAG starts writing data, and again
any time the schema needs to be (re)created on either backend.
"""

import logging
import os

import duckdb
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("db_setup.log", encoding="utf-8"),
    ],
)

# ----------------------------------------------------------------------------
# Connection configs
# ----------------------------------------------------------------------------
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/opt/airflow/db/ecommerce_data.duckdb")

PG_CONFIG = {
    "host": os.environ.get("WAREHOUSE_DB_HOST", "warehouse-db"),
    "port": int(os.environ.get("WAREHOUSE_DB_PORT", 5432)),
    "dbname": os.environ.get("WAREHOUSE_DB_NAME", "ecommerce_data"),
    "user": os.environ.get("WAREHOUSE_DB_USER", "dwh_user"),
    "password": os.environ.get("WAREHOUSE_DB_PASSWORD", "dwh_password"),
}

# ----------------------------------------------------------------------------
# Shared DDL (same table structure on both backends)
# ----------------------------------------------------------------------------
DDL_STATEMENTS = [
    "CREATE SCHEMA IF NOT EXISTS raw_layer;",
    "CREATE SCHEMA IF NOT EXISTS refined_layer;",
    "CREATE SCHEMA IF NOT EXISTS report_layer;",

    """
    CREATE TABLE IF NOT EXISTS raw_layer.sales (
        order_id INTEGER,
        order_date DATE,
        customer_id INTEGER,
        customer_name VARCHAR,
        product_id INTEGER,
        product_name VARCHAR,
        quantity INTEGER,
        unit_price DECIMAL(10, 2),
        payment_method VARCHAR,
        order_status VARCHAR,
        email VARCHAR,
        gender VARCHAR,
        country_code VARCHAR,
        ingestion_timestamp TIMESTAMP DEFAULT current_timestamp
    );
    """,

    """
    CREATE TABLE IF NOT EXISTS refined_layer.sales (
        order_id INTEGER PRIMARY KEY,
        order_date DATE,
        customer_id INTEGER,
        customer_name VARCHAR,
        product_id INTEGER,
        product_name VARCHAR,
        quantity INTEGER,
        unit_price DECIMAL(10, 2),
        total_amount DECIMAL(12, 2),
        payment_method VARCHAR,
        order_status VARCHAR,
        email VARCHAR,
        gender VARCHAR,
        country_code VARCHAR,
        is_valid_transaction BOOLEAN,
        ingestion_timestamp TIMESTAMP DEFAULT current_timestamp
    );
    """,

    """
    CREATE TABLE IF NOT EXISTS report_layer.sales_by_country (
        country_code VARCHAR,
        total_revenue DECIMAL(14, 2),
        total_orders INTEGER,
        avg_order_value DECIMAL(12, 2),
        report_date DATE DEFAULT current_date
    );
    """,

    """
    CREATE TABLE IF NOT EXISTS report_layer.sales_by_product (
        product_id INTEGER,
        product_name VARCHAR,
        total_revenue DECIMAL(14, 2),
        total_orders INTEGER,
        avg_order_value DECIMAL(12, 2),
        report_date DATE DEFAULT current_date
    );
    """,

    """
    CREATE TABLE IF NOT EXISTS report_layer.sales_by_payment_method (
        payment_method VARCHAR,
        total_revenue DECIMAL(14, 2),
        total_orders INTEGER,
        avg_order_value DECIMAL(12, 2),
        report_date DATE DEFAULT current_date
    );
    """,
]


def init_duckdb_schema():
    """Create schemas/tables on the local DuckDB file."""
    conn = None
    try:
        conn = duckdb.connect(DUCKDB_PATH)
        logging.info("[DuckDB] Connected at %s.", DUCKDB_PATH)
        for stmt in DDL_STATEMENTS:
            conn.execute(stmt)
        logging.info("[DuckDB] Schemas and tables created successfully.")
    except Exception as e:
        logging.error("[DuckDB] An error occurred: %s", e)
        raise
    finally:
        if conn:
            conn.close()
            logging.info("[DuckDB] Connection closed.")


def init_postgres_schema():
    """Create schemas/tables on the PostgreSQL warehouse."""
    conn = None
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()
        logging.info("[PostgreSQL] Connected at %s:%s/%s.",
                     PG_CONFIG["host"], PG_CONFIG["port"], PG_CONFIG["dbname"])
        for stmt in DDL_STATEMENTS:
            cur.execute(stmt)
        cur.close()
        logging.info("[PostgreSQL] Schemas and tables created successfully.")
    except Exception as e:
        logging.error("[PostgreSQL] An error occurred: %s", e)
        raise
    finally:
        if conn:
            conn.close()
            logging.info("[PostgreSQL] Connection closed.")


def init_db_schema():
    """Initialize the warehouse schema on both DuckDB and PostgreSQL."""
    init_duckdb_schema()
    init_postgres_schema()
    logging.info("Schema initialization complete on BOTH backends (DuckDB + PostgreSQL).")


if __name__ == "__main__":
    init_db_schema()