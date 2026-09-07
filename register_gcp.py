import os
import requests
import google.auth
from google.auth.transport.requests import Request

MERCHANT_ID = os.environ.get('MERCHANT_ID', '5829285869')
EMAIL = "raheel2k29@gmail.com"

print(f"Registering GCP project for {MERCHANT_ID}...")
credentials, project = google.auth.default(scopes=['https://www.googleapis.com/auth/content'])
credentials.refresh(Request())
token = credentials.token

url = f"https://merchantapi.googleapis.com/accounts/v1/accounts/{MERCHANT_ID}:registerGcp"
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}
payload = {
    "developerEmailAddress": EMAIL
}

response = requests.post(url, headers=headers, json=payload)
print(f"Status: {response.status_code}")
print(f"Response: {response.text}")
