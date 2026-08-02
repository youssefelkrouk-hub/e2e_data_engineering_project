"""
sales.py

Airflow DAG (classic syntax with DAG(...) as dag:) that extracts sales data
from the Mockaroo API (CSV format) and loads it through a medallion
architecture, writing to BOTH storage backends at every layer:

    - DuckDB   (local file: /opt/airflow/db/ecommerce_data.duckdb)
    - PostgreSQL (warehouse-db container)

    raw_layer.sales -> refined_layer.sales -> report_layer.*

Pipeline:
    fetch_sales_data >> store_sales_data_to_db >> transform_to_refined >> generate_reports
"""

import logging
import os
from datetime import datetime, timedelta
from io import StringIO

import duckdb
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------
API_URL = "https://my.api.mockaroo.com/sales_data"
API_KEY = os.environ.get("MOCKAROO_API_KEY", "c5e8c340")

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/opt/airflow/db/ecommerce_data.duckdb")

PG_HOST = os.environ.get("WAREHOUSE_DB_HOST", "warehouse-db")
PG_PORT = os.environ.get("WAREHOUSE_DB_PORT", "5432")
PG_DB = os.environ.get("WAREHOUSE_DB_NAME", "ecommerce_data")
PG_USER = os.environ.get("WAREHOUSE_DB_USER", "dwh_user")
PG_PASSWORD = os.environ.get("WAREHOUSE_DB_PASSWORD", "dwh_password")
PG_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

default_args = {
    "owner": "youssef",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

RAW_COLUMNS = [
    'order_id', 'order_date', 'quantity', 'payment_method', 'order_status',
    'customer_id', 'customer_name', 'email', 'gender', 'country_code',
    'product_id', 'product_name', 'unit_price'
]

TRANSFORM_SQL = """
    INSERT INTO refined_layer.sales (
        order_id, order_date, customer_id, customer_name,
        product_id, product_name, quantity, unit_price,
        total_amount, payment_method, order_status,
        email, gender, country_code, is_valid_transaction,
        ingestion_timestamp
    )
    SELECT
        order_id, order_date, customer_id, customer_name,
        product_id, product_name, quantity, unit_price,
        quantity * unit_price AS total_amount,
        payment_method, order_status, email, gender, country_code,
        (
            quantity > 0
            AND unit_price > 0
            AND order_status NOT IN ('cancelled', 'refunded')
            AND email IS NOT NULL
            AND email != ''
        ) AS is_valid_transaction,
        current_timestamp AS ingestion_timestamp
    FROM raw_layer.sales
    WHERE order_id NOT IN (SELECT order_id FROM refined_layer.sales)
"""

REPORT_QUERIES = {
    "sales_by_country": ("country_code", """
        SELECT country_code, SUM(total_amount) AS total_revenue,
               COUNT(*) AS total_orders, AVG(total_amount) AS avg_order_value
        FROM refined_layer.sales WHERE is_valid_transaction = TRUE
        GROUP BY country_code
    """),
    "sales_by_product": ("product_id, product_name", """
        SELECT product_id, product_name, SUM(total_amount) AS total_revenue,
               COUNT(*) AS total_orders, AVG(total_amount) AS avg_order_value
        FROM refined_layer.sales WHERE is_valid_transaction = TRUE
        GROUP BY product_id, product_name
    """),
    "sales_by_payment_method": ("payment_method", """
        SELECT payment_method, SUM(total_amount) AS total_revenue,
               COUNT(*) AS total_orders, AVG(total_amount) AS avg_order_value
        FROM refined_layer.sales WHERE is_valid_transaction = TRUE
        GROUP BY payment_method
    """),
}


# ----------------------------------------------------------------------------
# Task callables — defined OUTSIDE the with DAG() block
# ----------------------------------------------------------------------------
def fetch_sales_data(**kwargs):
    """Fetch sales data from the Mockaroo API and push it to XCom."""
    headers = {"X-API-Key": API_KEY}
    try:
        response = requests.get(API_URL, headers=headers, timeout=30)
        if response.status_code != 200:
            raise ValueError(f"Error fetching data, status code: {response.status_code}")

        data = pd.read_csv(StringIO(response.text))
        data_dict = data.to_dict(orient="records")
        logging.info("Successfully fetched sales data from the API (%d rows).", len(data_dict))

        kwargs['ti'].xcom_push(key='sales_data', value=data_dict)
        return data_dict

    except Exception as e:
        logging.error(f"Failed to fetch data from API: {e}")
        raise


def store_sales_data_to_db(**kwargs):
    """Insert raw sales data into raw_layer.sales on BOTH DuckDB and PostgreSQL."""
    data_dict = kwargs['ti'].xcom_pull(task_ids='fetch_sales_data', key='sales_data')
    df = pd.DataFrame(data_dict)[RAW_COLUMNS]

    # --- DuckDB ---
    duck_conn = None
    try:
        duck_conn = duckdb.connect(DUCKDB_PATH)
        for record in df.to_dict(orient="records"):
            duck_conn.execute("""
                INSERT INTO raw_layer.sales
                (order_id, order_date, quantity, payment_method, order_status,
                 customer_id, customer_name, email, gender, country_code,
                 product_id, product_name, unit_price)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, tuple(record[col] for col in RAW_COLUMNS))
        logging.info("[DuckDB] Inserted %d rows into raw_layer.sales.", len(df))
    except Exception as e:
        logging.error("[DuckDB] Failed to insert data: %s", e)
        raise
    finally:
        if duck_conn:
            duck_conn.close()
            logging.info("[DuckDB] Connection closed.")

    # --- PostgreSQL ---
    pg_engine = None
    try:
        pg_engine = create_engine(PG_URL)
        df.to_sql(name='sales', schema='raw_layer', con=pg_engine, if_exists='append', index=False)
        logging.info("[PostgreSQL] Inserted %d rows into raw_layer.sales.", len(df))
    except Exception as e:
        logging.error("[PostgreSQL] Failed to insert data: %s", e)
        raise
    finally:
        if pg_engine:
            pg_engine.dispose()
            logging.info("[PostgreSQL] Connection closed.")


def transform_to_refined(**kwargs):
    """Transform raw -> refined on BOTH DuckDB and PostgreSQL (idempotent)."""
    # --- DuckDB ---
    duck_conn = None
    try:
        duck_conn = duckdb.connect(DUCKDB_PATH)
        duck_conn.execute(TRANSFORM_SQL)
        count = duck_conn.execute("SELECT COUNT(*) FROM refined_layer.sales").fetchone()[0]
        logging.info("[DuckDB] refined_layer.sales updated. Total rows: %d", count)
    except Exception as e:
        logging.error("[DuckDB] Failed to transform data: %s", e)
        raise
    finally:
        if duck_conn:
            duck_conn.close()
            logging.info("[DuckDB] Connection closed.")

    # --- PostgreSQL ---
    pg_engine = None
    try:
        pg_engine = create_engine(PG_URL)
        with pg_engine.begin() as conn:
            conn.execute(text(TRANSFORM_SQL + " ON CONFLICT (order_id) DO NOTHING"))
            count = conn.execute(text("SELECT COUNT(*) FROM refined_layer.sales")).scalar()
        logging.info("[PostgreSQL] refined_layer.sales updated. Total rows: %d", count)
    except Exception as e:
        logging.error("[PostgreSQL] Failed to transform data: %s", e)
        raise
    finally:
        if pg_engine:
            pg_engine.dispose()
            logging.info("[PostgreSQL] Connection closed.")


def generate_reports(**kwargs):
    """Refresh report_layer.* on BOTH DuckDB and PostgreSQL (idempotent per day)."""
    # --- DuckDB ---
    duck_conn = None
    try:
        duck_conn = duckdb.connect(DUCKDB_PATH)
        for report_name, (group_cols, select_sql) in REPORT_QUERIES.items():
            duck_conn.execute(f"DELETE FROM report_layer.{report_name} WHERE report_date = current_date")
            cols = group_cols.replace(" ", "").split(",")
            insert_cols = ", ".join(cols + ["total_revenue", "total_orders", "avg_order_value"])
            duck_conn.execute(f"INSERT INTO report_layer.{report_name} ({insert_cols}) {select_sql}")
        logging.info("[DuckDB] Reports refreshed successfully.")
    except Exception as e:
        logging.error("[DuckDB] Failed to generate reports: %s", e)
        raise
    finally:
        if duck_conn:
            duck_conn.close()
            logging.info("[DuckDB] Connection closed.")

    # --- PostgreSQL ---
    pg_engine = None
    try:
        pg_engine = create_engine(PG_URL)
        with pg_engine.begin() as conn:
            for report_name, (group_cols, select_sql) in REPORT_QUERIES.items():
                conn.execute(text(f"DELETE FROM report_layer.{report_name} WHERE report_date = current_date"))
                cols = group_cols.replace(" ", "").split(",")
                insert_cols = ", ".join(cols + ["total_revenue", "total_orders", "avg_order_value"])
                conn.execute(text(f"INSERT INTO report_layer.{report_name} ({insert_cols}) {select_sql}"))
        logging.info("[PostgreSQL] Reports refreshed successfully.")
    except Exception as e:
        logging.error("[PostgreSQL] Failed to generate reports: %s", e)
        raise
    finally:
        if pg_engine:
            pg_engine.dispose()
            logging.info("[PostgreSQL] Connection closed.")


# ----------------------------------------------------------------------------
# Define the DAG
# ----------------------------------------------------------------------------
with DAG(
    'load_sales_data_to_db',
    default_args=default_args,
    description='ETL DAG: Mockaroo API -> raw_layer -> refined_layer -> report_layer (dual: DuckDB + PostgreSQL)',
    schedule='@daily',
    catchup=False,
    start_date=datetime(2026, 7, 1),
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_sales_data',
        python_callable=fetch_sales_data,
    )

    store_task = PythonOperator(
        task_id='store_sales_data_to_db',
        python_callable=store_sales_data_to_db,
    )

    transform_task = PythonOperator(
        task_id='transform_to_refined',
        python_callable=transform_to_refined,
    )

    report_task = PythonOperator(
        task_id='generate_reports',
        python_callable=generate_reports,
    )

    fetch_task >> store_task >> transform_task >> report_task