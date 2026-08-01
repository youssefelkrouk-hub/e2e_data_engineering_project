import requests
import os
import logging
import pandas as pd 
from io import StringIO
import duckdb



logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("db_setup.log", encoding="utf-8"),
    ],
)


def fetch_sales_data(**kwargs):
    """Fetch sales data from the Mickaroo API and return it as a DataFrame"""
    api_url="https://my.api.mockaroo.com/sales_data"
    headers={"X-API-KEY":"c5e8c340"}
    try:
        response=requests.get(api_url,headers=headers)
        if response.status_code!=200:
            raise ValueError(f"Error fetching data,status code :{response.status_code} ")

        else:
            data=pd.read_csv(StringIO(response.text)) 
            #convert DataFrame to a list of dictionnaries
            data_dict=data.to_dict(orient="records")
            logging.info("Successfuly fetched sales data from API")
            kwargs['ti'].xcom_push(key='sales_data',value=data_dict)
            return data_dict

    except Exception as e: 
        logging.error(f"Failed to fetch data from API: {e}")
        raise


def store_sales_data_to_duckdb(data_dict):
    """Store the fetched sales data into DuckDB raw_layer.sales table."""
    # Convert list of dictionaries to a DataFrame
    data = pd.DataFrame(data_dict)

    duckdb_path = r'C:\Users\Youssef ElKrouk\Documents\DATAENG\init\db\ecommerce_data.duckdb'
    conn = duckdb.connect(duckdb_path)
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
        logging.info("Data successfully inserted into DuckDB raw_layer.sales table.")

    except Exception as e:
        logging.error(f"Failed to insert data into DuckDB: {e}")
        raise
    finally:
        conn.close()
        logging.info("Closed DuckDB connection.")






data_dict=fetch_sales_data()
store_sales_data_to_duckdb(data_dict)