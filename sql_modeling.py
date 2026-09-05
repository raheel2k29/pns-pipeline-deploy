import os
from google.cloud import bigquery

BQ_PROJECT_ID = os.environ.get('BQ_PROJECT_ID')
BQ_DATASET_CORE = os.environ.get('BQ_DATASET_ID', 'pns_core')
BQ_DATASET_ANALYTICS = 'pns_analytics'

def run_sql_models():
    print("Running BigQuery SQL Modeling...")
    try:
        client = bigquery.Client(project=BQ_PROJECT_ID)
        dataset_ref = client.dataset(BQ_DATASET_ANALYTICS)
        try: client.get_dataset(dataset_ref)
        except Exception: client.create_dataset(bigquery.Dataset(dataset_ref))
            
        sql_supplier = f'''
        CREATE OR REPLACE VIEW {BQ_PROJECT_ID}.{BQ_DATASET_ANALYTICS}.supplier_performance AS
        SELECT 
            COALESCE(l.name, f.location_id) as supplier_name,
            COUNT(DISTINCT o.order_id) as total_orders,
            COUNT(DISTINCT f.fulfillment_id) as total_fulfillments,
            COUNT(DISTINCT r.refund_id) as total_refunds,
            SAFE_DIVIDE(COUNT(DISTINCT r.refund_id), COUNT(DISTINCT o.order_id)) as refund_rate,
            AVG(TIMESTAMP_DIFF(TIMESTAMP(f.created_at), TIMESTAMP(o.created_at), HOUR)) as avg_fulfillment_speed_hours
        FROM {BQ_PROJECT_ID}.{BQ_DATASET_CORE}.shopify_orders o
        LEFT JOIN {BQ_PROJECT_ID}.{BQ_DATASET_CORE}.shopify_fulfillments f ON o.order_id = f.order_id
        LEFT JOIN {BQ_PROJECT_ID}.{BQ_DATASET_CORE}.shopify_refunds r ON o.order_id = r.order_id
        LEFT JOIN {BQ_PROJECT_ID}.{BQ_DATASET_CORE}.shopify_locations l ON f.location_id = l.location_id
        GROUP BY supplier_name
        '''
        client.query(sql_supplier).result()
        print("Successfully created/updated analytics.supplier_performance SQL View!")

        sql_cx = f'''
        CREATE OR REPLACE VIEW {BQ_PROJECT_ID}.{BQ_DATASET_ANALYTICS}.cx_metrics AS
        SELECT 
            DATE(TIMESTAMP(created_at)) as report_date,
            channel,
            COUNT(DISTINCT ticket_id) as total_tickets,
            COUNT(DISTINCT CASE WHEN status = 'closed' THEN ticket_id ELSE NULL END) as resolved_tickets,
            AVG(CASE WHEN status = 'closed' THEN TIMESTAMP_DIFF(TIMESTAMP(updated_at), TIMESTAMP(created_at), HOUR) ELSE NULL END) as avg_resolution_time_hours
        FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY ticket_id ORDER BY updated_at DESC, created_at DESC) as rn
            FROM {BQ_PROJECT_ID}.{BQ_DATASET_CORE}.gorgias_tickets
        )
        WHERE rn = 1
        GROUP BY report_date, channel
        '''
        client.query(sql_cx).result()
        print("Successfully created/updated analytics.cx_metrics SQL View!")
        
    except Exception as e:
        print(f"Failed to run SQL model: {e}")

if __name__ == "__main__":
    run_sql_models()
