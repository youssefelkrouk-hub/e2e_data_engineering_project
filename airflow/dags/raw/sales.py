"""
sales.py

Airflow DAG (classic syntax with DAG(...) as dag:) that extracts sales data
from the Mockaroo API (CSV format) and loads it through a medallion
architecture in DuckDB:

    raw_layer.sales -> refined_layer.sales -> report_layer.*

Pipeline:
    fetch_sales_data >> store_sales_data_to_db >> transform_to_refined >> generate_reports
"""

import logging
from datetime import datetime, timedelta
from io import StringIO

import duckdb
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------
API_URL = "https://my.api.mockaroo.com/sales_data"
API_KEY = "c5e8c340"
DUCKDB_PATH = "/opt/airflow/db/ecommerce_data.duckdb"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

default_args = {
    "owner": "youssef",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
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
    """Store the fetched sales data into DuckDB raw_layer.sales table."""
    data_dict = kwargs['ti'].xcom_pull(task_ids='fetch_sales_data', key='sales_data')

    conn = duckdb.connect(DUCKDB_PATH)
    try:
        for record in data_dict:
            conn.execute("""
                INSERT INTO raw_layer.sales
                (order_id, order_date, quantity, payment_method, order_status,
                 customer_id, customer_name, email, gender, country_code,
                 product_id, product_name, unit_price)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                record['order_id'], record['order_date'], record['quantity'],
                record['payment_method'], record['order_status'],
                record['customer_id'], record['customer_name'], record['email'],
                record['gender'], record['country_code'], record['product_id'],
                record['product_name'], record['unit_price']
            ))

        logging.info(
            "Data successfully inserted into DuckDB raw_layer.sales table (%d rows).",
            len(data_dict),
        )

    except Exception as e:
        logging.error(f"Failed to insert data into DuckDB: {e}")
        raise
    finally:
        conn.close()
        logging.info("Closed DuckDB connection (store_sales_data_to_db).")


def transform_to_refined(**kwargs):
    """Clean, validate, and enrich raw sales data into the refined layer (idempotent)."""
    conn = duckdb.connect(DUCKDB_PATH)
    try:
        conn.execute("""
            INSERT INTO refined_layer.sales (
                order_id, order_date, customer_id, customer_name,
                product_id, product_name, quantity, unit_price,
                total_amount, payment_method, order_status,
                email, gender, country_code, is_valid_transaction,
                ingestion_timestamp
            )
            SELECT
                order_id,
                order_date,
                customer_id,
                customer_name,
                product_id,
                product_name,
                quantity,
                unit_price,
                quantity * unit_price AS total_amount,
                payment_method,
                order_status,
                email,
                gender,
                country_code,
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
        """)

        count = conn.execute("SELECT COUNT(*) FROM refined_layer.sales").fetchone()[0]
        logging.info("Refined layer updated. Total rows now: %d", count)

    except Exception as e:
        logging.error(f"Failed to transform data into refined layer: {e}")
        raise
    finally:
        conn.close()
        logging.info("Closed DuckDB connection (transform_to_refined).")


def generate_reports(**kwargs):
    """Generate/refresh aggregated reports from the refined layer (idempotent per day)."""
    conn = duckdb.connect(DUCKDB_PATH)
    try:
        # Delete today's report rows first to avoid duplicates if the DAG
        # runs more than once on the same day.
        conn.execute("DELETE FROM report_layer.sales_by_country WHERE report_date = current_date")
        conn.execute("DELETE FROM report_layer.sales_by_product WHERE report_date = current_date")
        conn.execute("DELETE FROM report_layer.sales_by_payment_method WHERE report_date = current_date")

        # Report by country
        conn.execute("""
            INSERT INTO report_layer.sales_by_country
            (country_code, total_revenue, total_orders, avg_order_value)
            SELECT
                country_code,
                SUM(total_amount) AS total_revenue,
                COUNT(*) AS total_orders,
                AVG(total_amount) AS avg_order_value
            FROM refined_layer.sales
            WHERE is_valid_transaction = TRUE
            GROUP BY country_code
        """)

        # Report by product
        conn.execute("""
            INSERT INTO report_layer.sales_by_product
            (product_id, product_name, total_revenue, total_orders, avg_order_value)
            SELECT
                product_id,
                product_name,
                SUM(total_amount) AS total_revenue,
                COUNT(*) AS total_orders,
                AVG(total_amount) AS avg_order_value
            FROM refined_layer.sales
            WHERE is_valid_transaction = TRUE
            GROUP BY product_id, product_name
        """)

        # Report by payment method
        conn.execute("""
            INSERT INTO report_layer.sales_by_payment_method
            (payment_method, total_revenue, total_orders, avg_order_value)
            SELECT
                payment_method,
                SUM(total_amount) AS total_revenue,
                COUNT(*) AS total_orders,
                AVG(total_amount) AS avg_order_value
            FROM refined_layer.sales
            WHERE is_valid_transaction = TRUE
            GROUP BY payment_method
        """)

        logging.info("Reports generated/refreshed successfully for today.")

    except Exception as e:
        logging.error(f"Failed to generate reports: {e}")
        raise
    finally:
        conn.close()
        logging.info("Closed DuckDB connection (generate_reports).")


# ----------------------------------------------------------------------------
# Define the DAG
# ----------------------------------------------------------------------------
with DAG(
    'load_sales_data_to_db',
    default_args=default_args,
    description='ETL DAG: Mockaroo API -> raw_layer -> refined_layer -> report_layer (DuckDB)',
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