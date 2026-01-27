import requests
import pandas as pd
from get_token import get_token
import logging
import os
from dotenv import load_dotenv

load_dotenv()


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

start_url = os.getenv("PRJ_START_URL")
query = """
    query purchaseorders($pageStart:Int, $pageSize: Int) {
        purchaseorders(pageStart: $pageStart, pageSize: $pageSize) {
            purchaseorders
        }
    """
headers = {
    "content-type": "application/json"
}

def get_vpo(token):
    url = f"{start_url}/api/graphql?token={token}"
    page_start = 0
    page_size = 1000
    payload = {
        "query": query,
        "variables": {
            "pageStart": page_start,  
            "pageSize": page_size 
        }
    }
    try:
        res = requests.post(url, headers=headers, json=payload)
        print(res.json())
    except Exception as e:
        logging.warning(f"API Error {e}")

#====== TEST
if __name__ == "__main__":
    token = get_token()
    if token:
        get_vpo(token)