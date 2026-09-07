import os
import time
import schedule
import subprocess
from datetime import datetime, timezone
from google.cloud import bigquery

if 'GOOGLE_JSON_KEY' in os.environ:
    creds_path = '/tmp/google_creds.json'
    if os.name == 'nt':
        creds_path = 'google_creds.json'
        
    with open(creds_path, 'w') as f:
        f.write(os.environ['GOOGLE_JSON_KEY'])
    
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = creds_path
    print("Successfully loaded Google Cloud credentials.")

def log_to_bigquery(pipeline_name, status, duration_seconds):
    try:
        client = bigquery.Client()
        table_id = "pns-data-warehouse.pns_core.deploy_logs"
        
        schema = [
            bigquery.SchemaField("execution_time", "TIMESTAMP"),
            bigquery.SchemaField("pipeline_name", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("duration_seconds", "FLOAT"),
        ]
        table = bigquery.Table(table_id, schema=schema)
        try:
            client.get_table(table_id)
        except Exception:
            client.create_table(table)
            
        rows_to_insert = [
            {
                "execution_time": datetime.now(timezone.utc).isoformat(),
                "pipeline_name": pipeline_name,
                "status": status,
                "duration_seconds": duration_seconds
            }
        ]
        
        errors = client.insert_rows_json(table_id, rows_to_insert)
        if errors:
            print(f"Error logging {pipeline_name} to BigQuery: {errors}")
    except Exception as e:
        print(f"Failed to log {pipeline_name} due to: {e}")

def run_pipeline(name, script):
    print(f"Running {name}...")
    start_time = time.time()
    result = subprocess.run(["python", script])
    end_time = time.time()
    
    duration = end_time - start_time
    status = "SUCCESS" if result.returncode == 0 else "ERROR"
    
    log_to_bigquery(name, status, duration)
    return status

def run_all():
    print("--- Starting Full Pipeline Sync ---")
    run_pipeline("Shopify Pipeline", "shopify_pipeline.py")
    run_pipeline("Gorgias Pipeline", "gorgias_pipeline.py")
    run_pipeline("GMC Pipeline", "gmc_pipeline.py")
    run_pipeline("SQL Modeling", "sql_modeling.py")
    print("--- Sync Complete ---")

if __name__ == "__main__":
    run_all()
    schedule.every(10).minutes.do(run_all)
    print("Scheduler started. Pipelines will run every 10 minutes.")

    while True:
        schedule.run_pending()
        time.sleep(60)
