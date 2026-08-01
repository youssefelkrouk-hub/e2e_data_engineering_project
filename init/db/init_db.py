import duckdb
import logging
import time
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("db_setup.log", encoding="utf-8"),
    ],
)


def init_db_schema():
    try:
        conn = duckdb.connect("ecommerce_data.duckdb")
        logging.info("Connected to DuckDB database.")


        # Create Raw Schema
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw_layer;")
        logging.info("Created raw_layer schema.")

        # Create Refined Schema
        conn.execute("CREATE SCHEMA IF NOT EXISTS refined_layer;")
        logging.info("Created refined_layer schema.")

        # Create the Report Schema
        conn.execute("CREATE SCHEMA IF NOT EXISTS report_layer;")
        logging.info("Created report_layer schema.")


        # Create Raw Schema Table
        conn.execute("""
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
        """)
        logging.info("Created raw_layer.sales table.")

        conn.execute("""
        ALTER TABLE raw_layer.sales
        ADD COLUMN IF NOT EXISTS ingestion_timestamp TIMESTAMP DEFAULT current_timestamp;
        """)

        logging.info("Added ingestion_timestamp column to raw_layer.sales.")

        # Create Refined Schema Table
        conn.execute("""
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
                is_valid_transaction BOOLEAN
            );
        """)
        logging.info("Created refined_layer.sales table.")

        conn.execute("""
        ALTER TABLE refined_layer.sales
        ADD COLUMN IF NOT EXISTS ingestion_timestamp TIMESTAMP DEFAULT current_timestamp;
     
        """)
        logging.info("Added ingestion_timestamp column to refined_layer.sales.")

        # Create Report Schema Tables
        conn.execute("""
            CREATE TABLE IF NOT EXISTS report_layer.sales_by_country (
                country_code VARCHAR,
                total_revenue DECIMAL(14, 2),
                total_orders INTEGER,
                avg_order_value DECIMAL(12, 2),
                report_date DATE DEFAULT current_date
            );
        """)
        logging.info("Created report_layer.sales_by_country table.")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS report_layer.sales_by_product (
                product_id INTEGER,
                product_name VARCHAR,
                total_revenue DECIMAL(14, 2),
                total_orders INTEGER,
                avg_order_value DECIMAL(12, 2),
                report_date DATE DEFAULT current_date
            );
        """)
        logging.info("Created report_layer.sales_by_product table.")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS report_layer.sales_by_payment_method (
                payment_method VARCHAR,
                total_revenue DECIMAL(14, 2),
                total_orders INTEGER,
                avg_order_value DECIMAL(12, 2),
                report_date DATE DEFAULT current_date
            );
        """)
        logging.info("Created report_layer.sales_by_payment_method table.")

        # Close connection
        conn.close() 
        logging.info("Closed the database connection.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise


if __name__ == "__main__":
    init_db_schema()