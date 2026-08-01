























































"""
sales.py

DAG Airflow (syntaxe classique with DAG(...) as dag:) qui extrait les données
de vente depuis l'API Mockaroo (format CSV) et les charge dans la table
raw_layer.sales de DuckDB.

Pipeline :
    fetch_sales_data >> store_sales_data_to_db
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
# Fonctions Python (callables des tâches) — définies EN DEHORS du with DAG()
# ----------------------------------------------------------------------------
def fetch_sales_data(**kwargs):
    """Fetch sales data from the Mockaroo API and return it as a DataFrame."""
    headers = {"X-API-Key": API_KEY}
    try:
        response = requests.get(API_URL, headers=headers, timeout=30)
        if response.status_code != 200:
            raise ValueError(f"Error fetching data, status code: {response.status_code}")

        data = pd.read_csv(StringIO(response.text))
        # Convert DataFrame to a list of dictionaries
        data_dict = data.to_dict(orient="records")
        logging.info("Successfuly fetched sales data from the API.")

        # Push data_dict to XCom pour que la tâche suivante puisse la récupérer
        kwargs['ti'].xcom_push(key='sales_data', value=data_dict)
        return data_dict

    except Exception as e:
        logging.error(f"Failed to fetch data from API: {e}")
        raise


def store_sales_data_to_db(**kwargs):
    """Store the fetched sales data into DuckDB raw_layer.sales table."""
    # Récupère les données poussées par fetch_sales_data via XCom
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
        logging.info("Closed DuckDB connection.")


# ----------------------------------------------------------------------------
# Define the DAG
# ----------------------------------------------------------------------------
with DAG(
    'load_sales_data_to_db',
    default_args=default_args,
    description='A simple DAG to load sales data from an API to DuckDB',
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

    fetch_task >> store_task