import pandas as pd 
import requests
from get_token import get_token
import os
from dotenv import load_dotenv
import logging


logging.basicConfig(level=logging.INFO, format= '%(asctime)s - %(levelname)s - %(message)s')
start_url = os.getenv("PRJ_START_URL")
def fetch_po(token):
    url = f"{start_url}/api/graphql?token={token}"
    headers = {"content-type": "application/json"}

    page_start = 0
    page_size = 1000
    all_records = []

    query = """
    query customerPOs($pageSize: Int, $pageStart: Int, $query: CustomerPOQuery) {
        customerPOs(pageSize: $pageSize, pageStart: $pageStart, query: $query) {
            records {
                poId
                createdTime
                client
                clientPONumber
                clientPlainText
                totalAmount
            }
            totalRecords
        }
    }
    """

    while True:
        payload = {
            "query": query,
            "variables": {
                "pageSize": page_size,
                "pageStart": page_start,
                "query": {
                    "lastModifiedTime": {
                        "greaterThanOrEqual": "2024-01-01T00:00:00Z"
                    }
                }
            }
        }

        res = requests.post(url, headers=headers, json=payload)
        res.raise_for_status()

        data = res.json()["data"]["customerPOs"]
        records = data.get("records", [])
        total_records = data.get("totalRecords", 0)

        logging.info(f"Fetched {len(records)} records (pageStart={page_start})")

        if not records:
            break

        all_records.extend(records)
        page_start += page_size

        if page_start >= total_records:
            break

    logging.info(f"Total fetched records: {len(all_records)}")
    return all_records

def parse_data(records):
    """parse data recived from """  
    parsed_records = []
    for rec in records:
        data = {
            "client_po_num": rec.get("clientPONumber"),
            "customer": rec["client"].get("name"), 
            "client_plain_text": rec.get("clientPlainText"),
            "total_amount": float(rec.get("totalAmount"))
        }
        parsed_records.append(data)
    df = pd.DataFrame(parsed_records)
    return df

def load_to_azure(df):
    from azure import load_client_po, connect_to_db
    logging.info("Loading data to Azure SQL Database...")
    conn = connect_to_db()
    load_client_po(df, conn)



if __name__ == "__main__":
    token = get_token()
    if token:
        records = fetch_po(token)
        if records:
            df = parse_data(records)
            load_to_azure(df)
