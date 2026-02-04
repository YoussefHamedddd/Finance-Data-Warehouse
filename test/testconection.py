import duckdb
import os
import ijson
import json
import time  

# Configuration
base_path = "/mnt/c/Users/dell/Desktop/DWH/finacel trasncation data/"
pg_conn_str = "host=172.28.176.1 port=5432 dbname=financial_Transactions user=postgres password=12345"
TARGET_SCHEMA = "bronze" 

def convert_to_jsonl(input_path):
    output_path = input_path.replace('.json', '.jsonl')
    if os.path.exists(output_path):
        return output_path
    
    print(f"Converting {os.path.basename(input_path)} to JSONL (Streaming mode)...")
    with open(input_path, 'rb') as f, open(output_path, 'w') as out:
        for id, value in ijson.kvitems(f, 'target'):
            out.write(json.dumps({"id": id, "value": value}) + '\n')
    return output_path

def duck_ingest_final():
    start_time_total = time.time()  
    con = duckdb.connect()
    
    try:
        print(f"Connecting to Postgres... targeting schema: {TARGET_SCHEMA}")
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute(f"ATTACH '{pg_conn_str}' AS pg_db (TYPE POSTGRES);")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS pg_db.{TARGET_SCHEMA};")
        
        files_to_process = {
            "users": "users_data.csv",
            "cards": "cards_data.csv",
            "mcc_codes": "mcc_codes.json",
            "fraud_labels": "train_fraud_labels.json",
            "transactions": "transactions_data.csv"
        }
        
        for table_name, filename in files_to_process.items():
            file_path = os.path.join(base_path, filename)
            if not os.path.exists(file_path): 
                print(f"File not found: {filename}")
                continue
            
            start_time_table = time.time()  
            print(f"Ingesting {table_name}...")
            
            # 1. Handling fraud_labels
            if table_name == "fraud_labels":
                jsonl_path = convert_to_jsonl(file_path)
                con.execute(f"""
                    CREATE OR REPLACE TABLE pg_db.{TARGET_SCHEMA}.{table_name} AS 
                    SELECT * FROM read_json_auto('{jsonl_path}');
                """)
            
            # 2. Handling mcc_codes
            elif table_name == "mcc_codes":
                with open(file_path, 'r', encoding='utf-8') as f:
                    mcc_data = json.load(f)
                values_list = ','.join(
                    f"('{k}', '{str(v).replace(chr(39), chr(39)+chr(39))}')" 
                    for k, v in mcc_data.items()
                )
                con.execute(f"""
                    CREATE OR REPLACE TABLE pg_db.{TARGET_SCHEMA}.{table_name} AS 
                    SELECT * FROM (VALUES {values_list}) AS t(id, value);
                """)
            
            # 3. CSV Files
            elif filename.endswith('.csv'):
                con.execute(f"""
                    CREATE OR REPLACE TABLE pg_db.{TARGET_SCHEMA}.{table_name} AS 
                    SELECT * FROM read_csv_auto('{file_path}', quote='"', ignore_errors=True);
                """)
            
            # 4. Other JSON Files
            else:
                con.execute(f"""
                    CREATE OR REPLACE TABLE pg_db.{TARGET_SCHEMA}.{table_name} AS 
                    SELECT * FROM read_json_auto('{file_path}');
                """)
            
           
            end_time_table = time.time()
            duration_table = end_time_table - start_time_table
          
            row_count = con.execute(f"SELECT count(*) FROM pg_db.{TARGET_SCHEMA}.{table_name}").fetchone()[0]
            
            print(f"{table_name}: {row_count:,} rows ingested in {duration_table:.2f} seconds.")

    except Exception as e:
        print(f"Error occurred: {e}")
    
    finally:
        con.close()
        end_time_total = time.time()
        total_duration = end_time_total - start_time_total
        print("-" * 50)
        print(f"All processes finished in {total_duration/60:.2f} minutes.")
        print("-" * 50)

if __name__ == "__main__":
    duck_ingest_final()