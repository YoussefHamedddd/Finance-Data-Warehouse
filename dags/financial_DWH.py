from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import json

# Configuration - paths inside Docker container
base_path = "/opt/airflow/dags/data/"

pg_conn_str = "host=172.28.176.1 port=5432 dbname=financial_Transactions user=postgres password=12345"
TARGET_SCHEMA = "bronze"

def convert_to_jsonl(input_path):
    """Convert JSON to JSONL format for efficient processing"""
    import ijson
    output_path = input_path.replace('.json', '.jsonl')
    
    if os.path.exists(output_path):
        print(f"JSONL file already exists: {output_path}")
        return output_path
    
    print(f"Converting {input_path} to JSONL...")
    with open(input_path, 'rb') as f, open(output_path, 'w') as out:
        for id, value in ijson.kvitems(f, 'target'):
            out.write(json.dumps({"id": id, "value": value}) + '\n')
    
    print(f"Conversion complete: {output_path}")
    return output_path

def duck_ingest_logic():
    """Ingest data from files into PostgreSQL using DuckDB"""
    import duckdb
    
    print("Starting DuckDB ingestion...")
    con = duckdb.connect()
    
    try:
        # Install and load PostgreSQL extension
        print("Installing PostgreSQL extension...")
        con.execute("INSTALL postgres; LOAD postgres;")
        
        # Connect to PostgreSQL
        print(f"Connecting to PostgreSQL: {pg_conn_str}")
        con.execute(f"ATTACH '{pg_conn_str}' AS pg_db (TYPE POSTGRES);")
        
        # Create schema
        print(f"Creating schema: {TARGET_SCHEMA}")
        # Try to create schema, ignore if exists
        try:
            con.execute(f"CREATE SCHEMA pg_db.{TARGET_SCHEMA};")
            print(f"Schema {TARGET_SCHEMA} created")
        except Exception as e:
            if "already exists" in str(e):
                print(f"Schema {TARGET_SCHEMA} already exists, continuing...")
            else:
                raise
        
        # Define files to process
        files_to_process = {
            "users": "users_data.csv",
            "cards": "cards_data.csv",
            "mcc_codes": "mcc_codes.json",
            "fraud_labels": "train_fraud_labels.json",
            "transactions": "transactions_data.csv"
        }
        
        # Process each file
        for table_name, filename in files_to_process.items():
            file_path = os.path.join(base_path, filename)
            
            if not os.path.exists(file_path):
                print(f"File not found: {file_path}")
                continue
            
            print(f"Processing {table_name} from {filename}...")
            
            try:
                if table_name == "fraud_labels":
                    # Convert large JSON to JSONL for efficient processing
                    jsonl_path = convert_to_jsonl(file_path)
                    con.execute(f"""
                        CREATE OR REPLACE TABLE pg_db.{TARGET_SCHEMA}.{table_name} 
                        AS SELECT * FROM read_json_auto('{jsonl_path}');
                    """)
                    
                elif table_name == "mcc_codes":
                    # Load MCC codes as key-value pairs
                    with open(file_path, 'r', encoding='utf-8') as f:
                        mcc_data = json.load(f)
                    
                    # Escape single quotes in values
                    values_list = ','.join(
                        f"('{k}', '{str(v).replace(chr(39), chr(39)+chr(39))}')" 
                        for k, v in mcc_data.items()
                    )
                    
                    con.execute(f"""
                        CREATE OR REPLACE TABLE pg_db.{TARGET_SCHEMA}.{table_name} 
                        AS SELECT * FROM (VALUES {values_list}) AS t(id, value);
                    """)
                    
                elif filename.endswith('.csv'):
                    # Load CSV files
                    con.execute(f"""
                        CREATE OR REPLACE TABLE pg_db.{TARGET_SCHEMA}.{table_name} 
                        AS SELECT * FROM read_csv_auto('{file_path}', 
                                                       quote='\"', 
                                                       ignore_errors=True);
                    """)
                
                # Verify table was created
                result = con.execute(f"""
                    SELECT COUNT(*) as count 
                    FROM pg_db.{TARGET_SCHEMA}.{table_name}
                """).fetchone()
                
                print(f"Successfully loaded {table_name}: {result[0]} rows")
                
            except Exception as e:
                print(f" Error processing {table_name}: {str(e)}")
                raise
        
        print(" All files processed successfully!")
        
    except Exception as e:
        print(f" Fatal error in duck_ingest_logic: {str(e)}")
        raise
    finally:
        con.close()
        print("DuckDB connection closed")

# DAG default arguments
default_args = {
    'owner': 'youssef',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'financial_full_pipeline',
    default_args=default_args,
    description='Pipeline: DuckDB Ingestion -> DBT Transformations',
    schedule_interval='@daily',
    catchup=False,
    tags=['data-warehouse', 'financial', 'etl']
) as dag:

    # Task 1: DuckDB Bronze Ingestion
    task_bronze = PythonOperator(
        task_id='duckdb_ingest_bronze',
        python_callable=duck_ingest_logic
    )

    # Task 2: DBT Silver Layer Transformation
    task_dbt_silver = BashOperator(
        task_id='dbt_run_silver',
        bash_command='cd /opt/airflow/my_analysis && dbt run --profiles-dir .'
    )

    # Task 3: DBT Gold Dimensions
    task_dbt_gold_dims = BashOperator(
        task_id='dbt_run_gold_dimensions',
        bash_command='cd /opt/airflow/my_analysis && dbt run --select path:models/gold/dimensions --profiles-dir .',
    )

    # Task 4: DBT Gold Fact Table
    task_dbt_gold_fact = BashOperator(
        task_id='dbt_run_gold_fact',
        bash_command='cd /opt/airflow/my_analysis && dbt run --select path:models/gold/facts --profiles-dir .',
    )

    # Define task dependencies
    task_bronze >> task_dbt_silver >> task_dbt_gold_dims >> task_dbt_gold_fact