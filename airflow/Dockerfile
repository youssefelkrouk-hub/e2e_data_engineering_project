ARG AIRFLOW_VERSION=3.3.0
FROM apache/airflow:${AIRFLOW_VERSION}

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt