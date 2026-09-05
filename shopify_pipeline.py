import os
import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timezone

SHOPIFY_STORE_DOMAIN = os.environ.get('SHOPIFY_STORE_DOMAIN', 'the-source-usa.myshopify.com')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN')
BQ_PROJECT_ID = os.environ.get('BQ_PROJECT_ID')
BQ_DATASET_ID = os.environ.get('BQ_DATASET_ID', 'pns_core')

def shopify_request(endpoint):
    url = f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/2024-01/{endpoint}"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching {endpoint}: {response.text}")
        return {}

def get_last_updated_timestamp(table_name):
    client = bigquery.Client(project=BQ_PROJECT_ID)
    query = f"SELECT MAX(updated_at) as last_updated FROM {BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_name}"
    try:
        result = list(client.query(query).result())
        if result and result[0].last_updated:
            return result[0].last_updated.isoformat()
    except Exception:
        pass
    return None

def process_orders_and_related(orders_data):
    orders, fulfillments, refunds, line_items = [], [], [], []
    for order in orders_data:
        orders.append({
            "order_id": str(order['id']),
            "order_number": str(order['order_number']),
            "created_at": order['created_at'],
            "updated_at": order['updated_at'],
            "total_price": float(order['total_price']),
            "financial_status": order.get('financial_status', ''),
            "fulfillment_status": order.get('fulfillment_status') or 'unfulfilled'
        })
        
        for item in order.get('line_items', []):
            variant_id = item.get('variant_id')
            cost = None
            if variant_id:
                # Fetch Variant to get inventory_item_id
                var_data = shopify_request(f"variants/{variant_id}.json").get('variant', {})
                inv_id = var_data.get('inventory_item_id')
                if inv_id:
                    # Fetch InventoryItem to get cost
                    inv_data = shopify_request(f"inventory_items/{inv_id}.json").get('inventory_item', {})
                    cost = inv_data.get('cost')
            
            line_items.append({
                "line_item_id": str(item['id']),
                "order_id": str(order['id']),
                "product_id": str(item.get('product_id', '')),
                "variant_id": str(variant_id or ''),
                "title": item.get('title', ''),
                "vendor": item.get('vendor', ''),
                "quantity": int(item.get('quantity', 0)),
                "price": float(item.get('price', 0.0)),
                "cost": float(cost) if cost else None,
                "updated_at": order['updated_at']
            })
            
        for f in order.get('fulfillments', []):
            fulfillments.append({
                "fulfillment_id": str(f['id']),
                "order_id": str(order['id']),
                "status": f.get('status', ''),
                "created_at": f.get('created_at'),
                "updated_at": f.get('updated_at'),
                "location_id": str(f.get('location_id', '')),
                "tracking_company": f.get('tracking_company', '')
            })
            
        for r in order.get('refunds', []):
            refunds.append({
                "refund_id": str(r['id']),
                "order_id": str(order['id']),
                "created_at": r.get('created_at'),
                "processed_at": r.get('processed_at')
            })
            
    return pd.DataFrame(orders), pd.DataFrame(fulfillments), pd.DataFrame(refunds), pd.DataFrame(line_items)

def load_to_bigquery(df, table_name):
    if df is None or df.empty: return
    client = bigquery.Client(project=BQ_PROJECT_ID)
    dataset_ref = client.dataset(BQ_DATASET_ID)
    try: client.get_dataset(dataset_ref)
    except Exception: client.create_dataset(bigquery.Dataset(dataset_ref))
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_name}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result() 
        print(f"Successfully loaded {len(df)} rows into {table_name}!")
    except Exception as e:
        print(f"Failed to load {table_name}: {str(e)}")

if __name__ == "__main__":
    print(f"Fetching Shopify data from {SHOPIFY_STORE_DOMAIN}...")
    
    last_updated = get_last_updated_timestamp("shopify_orders")
    endpoint = "orders.json?status=any&limit=250"
    if last_updated:
        endpoint += f"&updated_at_min={last_updated}"
        
    orders_resp = shopify_request(endpoint)
    orders_list = orders_resp.get('orders', [])
    
    if orders_list:
        df_orders, df_full, df_ref, df_lines = process_orders_and_related(orders_list)
        load_to_bigquery(df_orders, "shopify_orders")
        load_to_bigquery(df_full, "shopify_fulfillments")
        load_to_bigquery(df_ref, "shopify_refunds")
        load_to_bigquery(df_lines, "shopify_order_line_items")
    else:
        print("No new orders found.")
    
    # Products
    prod_endpoint = "products.json?limit=250"
    last_prod_updated = get_last_updated_timestamp("shopify_products")
    if last_prod_updated:
        prod_endpoint += f"&updated_at_min={last_prod_updated}"
        
    prod_resp = shopify_request(prod_endpoint)
    products_list = prod_resp.get('products', [])
    
    if products_list:
        prods = []
        for p in products_list:
            barcodes = [v.get('barcode', '') for v in p.get('variants', []) if v.get('barcode')]
            prods.append({
                "product_id": str(p['id']), 
                "title": p.get('title',''), 
                "vendor": p.get('vendor',''), 
                "product_type": p.get('product_type',''), 
                "status": p.get('status',''),
                "updated_at": p.get('updated_at', ''),
                "has_barcode": len(barcodes) > 0
            })
        df_prod = pd.DataFrame(prods)
        load_to_bigquery(df_prod, "shopify_products")
    
    loc_resp = shopify_request("locations.json")
    locations = loc_resp.get('locations', [])
    df_loc = pd.DataFrame([{"location_id": str(l['id']), "name": l.get('name','')} for l in locations])
    load_to_bigquery(df_loc, "shopify_locations")
    
    loc_ids = ",".join([str(l['id']) for l in locations])
    if loc_ids:
        inv_resp = shopify_request(f"inventory_levels.json?location_ids={loc_ids}&limit=250")
        df_inv = pd.DataFrame([{"inventory_item_id": str(i['inventory_item_id']), "location_id": str(i['location_id']), "available": i.get('available', 0), "updated_at": i.get('updated_at', '')} for i in inv_resp.get('inventory_levels', [])])
        load_to_bigquery(df_inv, "shopify_inventory")
