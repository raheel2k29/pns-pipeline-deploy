import os
import requests
import pandas as pd
from google.cloud import bigquery
from requests.auth import HTTPBasicAuth

# --- Configuration ---
GORGIAS_DOMAIN = os.environ.get('GORGIAS_DOMAIN', 'paws-n-shades-llc.gorgias.com')
GORGIAS_EMAIL = os.environ.get('GORGIAS_EMAIL', 'raheel2k29@gmail.com')
GORGIAS_API_KEY = os.environ.get('GORGIAS_API_KEY')
BQ_PROJECT_ID = os.environ.get('BQ_PROJECT_ID')
BQ_DATASET_ID = os.environ.get('BQ_DATASET_ID', 'pns_core')

def get_gorgias_tickets():
    print(f"Fetching tickets from {GORGIAS_DOMAIN}...")
    url = f"https://{GORGIAS_DOMAIN}/api/tickets?limit=100&order_by=created_datetime:desc"
    response = requests.get(url, auth=HTTPBasicAuth(GORGIAS_EMAIL, GORGIAS_API_KEY))
    if response.status_code != 200:
        print(f"Error fetching Gorgias data: {response.text}")
        return []
    tickets = response.json().get('data', [])
    print(f"Successfully extracted {len(tickets)} tickets.")
    return tickets

def transform_tickets(tickets_data):
    processed = []
    for ticket in tickets_data:
        processed.append({
            "ticket_id": str(ticket['id']),
            "subject": ticket.get('subject', ''),
            "status": ticket.get('status', 'unknown'),
            "created_at": ticket.get('created_datetime'),
            "updated_at": ticket.get('updated_datetime'),
            "customer_id": str(ticket.get('customer', {}).get('id', '')),
            "channel": ticket.get('channel', 'email'),
            "csat_score": float(ticket.get('satisfaction_survey', {}).get('rating', 0) or 0),
            "first_response_time_minutes": float(ticket.get('first_response_time', 0) or 0)
        })
    return pd.DataFrame(processed)

def load_to_bigquery(df, table_name):
    if df.empty:
        return
    client = bigquery.Client(project=BQ_PROJECT_ID)
    
    # Create dataset if it doesn't exist
    dataset_ref = client.dataset(BQ_DATASET_ID)
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        client.create_dataset(bigquery.Dataset(dataset_ref))

    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    )
    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result() 
        print(f"Successfully loaded data into {table_id}!")
    except Exception as e:
        print(f"Failed to load to BigQuery: {str(e)}")

if __name__ == "__main__":
    raw_tickets = get_gorgias_tickets()
    if raw_tickets:
        df_tickets = transform_tickets(raw_tickets)
        load_to_bigquery(df_tickets, "gorgias_tickets")
