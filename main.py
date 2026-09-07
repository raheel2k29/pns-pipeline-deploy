import os
import time
import schedule
import subprocess

if 'GOOGLE_JSON_KEY' in os.environ:
    creds_path = '/tmp/google_creds.json'
    if os.name == 'nt':
        creds_path = 'google_creds.json'
        
    with open(creds_path, 'w') as f:
        f.write(os.environ['GOOGLE_JSON_KEY'])
    
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = creds_path
    print("Successfully loaded Google Cloud credentials.")

def run_shopify():
    print("Running Shopify Pipeline...")
    subprocess.run(["python", "shopify_pipeline.py"])

def run_gorgias():
    print("Running Gorgias Pipeline...")
    subprocess.run(["python", "gorgias_pipeline.py"])

def run_gmc():
    print("Running GMC Pipeline...")
    subprocess.run(["python", "gmc_pipeline.py"])

def run_sql():
    print("Running SQL Models...")
    subprocess.run(["python", "sql_modeling.py"])

def run_all():

    print("--- Starting Full Pipeline Sync ---")
    run_shopify()
    run_gorgias()
    run_gmc()
    run_sql()
    print("--- Sync Complete ---")

run_all()
schedule.every(10).minutes.do(run_all)
print("Scheduler started. Pipelines will run every 10 minutes.")

while True:
    schedule.run_pending()
    time.sleep(60)




