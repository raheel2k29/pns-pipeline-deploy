import os
import pandas as pd
import requests
import google.auth
from google.auth.transport.requests import Request
from google.cloud import bigquery
from datetime import datetime

MERCHANT_ID = os.environ.get('MERCHANT_ID', '5829285869')
BQ_PROJECT_ID = os.environ.get('BQ_PROJECT_ID')
BQ_DATASET_ID = os.environ.get('BQ_DATASET_ID', 'pns_core')

def get_gmc_issues():
    print(f"Fetching Google Merchant Center diagnostics for {MERCHANT_ID} using Merchant API v1...")
    try:
        credentials, project = google.auth.default(scopes=['https://www.googleapis.com/auth/content'])
        credentials.refresh(Request())
        token = credentials.token
        
        # New Google Merchant API v1 endpoints
        url = f"https://merchantapi.googleapis.com/products/v1beta/accounts/{MERCHANT_ID}/products"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        all_issues = []
        next_page_token = None
        
        while True:
            params = {}
            if next_page_token:
                params['pageToken'] = next_page_token
                
            response = requests.get(url, headers=headers, params=params)
            if response.status_code != 200:
                print(f"Error from Merchant API v1: {response.text}")
                break
                
            data = response.json()
            products = data.get('products', [])
            
            for prod in products:
                product_id = prod.get('name', '')
                title = prod.get('title', '')
                
                status = prod.get('productStatus', {})
                issues = status.get('itemLevelIssues', [])
                
                for issue in issues:
                    all_issues.append({
                        "product_id": product_id,
                        "title": title,
                        "issue_code": issue.get('code', ''),
                        "description": issue.get('description', ''),
                        "resolution": issue.get('resolution', ''),
                        "destination": issue.get('destination', ''),
                        "servability": issue.get('servability', ''),
                        "updated_at": datetime.now().isoformat()
                    })
                    
            next_page_token = data.get('nextPageToken')
            if not next_page_token:
                break
                
        print(f"Successfully extracted {len(all_issues)} GMC product issues.")
        return all_issues
    except Exception as e:
        print(f"Error fetching GMC data: {str(e)}")
        return []

def load_to_bigquery(df, table_name):
    if df.empty: return
    client = bigquery.Client(project=BQ_PROJECT_ID)
    dataset_ref = client.dataset(BQ_DATASET_ID)
    try: client.get_dataset(dataset_ref)
    except Exception: client.create_dataset(bigquery.Dataset(dataset_ref))
    
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    )
    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result() 
        print(f"Successfully loaded data into {table_name}!")
    except Exception as e:
        print(f"Failed to load to BigQuery: {str(e)}")

if __name__ == "__main__":
    raw_issues = get_gmc_issues()
    if raw_issues:
        df_issues = pd.DataFrame(raw_issues)
        load_to_bigquery(df_issues, "gmc_diagnostics")
    else:
        print("No GMC issues found or error occurred.")
