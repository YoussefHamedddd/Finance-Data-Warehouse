FROM apache/airflow:2.7.2

USER root
RUN apt-get update && apt-get install -y \
    g++ \
    gcc \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    duckdb==0.9.2 \
    ijson \
    psycopg2-binary \
    dbt-core \
    dbt-postgres
