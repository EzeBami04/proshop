import requests
import pandas as pd
import time
import random
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

from get_token import get_token
from azure import  load_invoice, connect_to_db
import logging
import os

#============= Config ==========================

logging.basicConfig(level=logging.INFO, format= '%(asctime)s - %(levelname)s - %(message)s')
start_url = os.getenv("PRJ_START_URL")

token = get_token()
url = f"{start_url}/api/graphql?token={token}"
headers = {
    "content-type": "application/json"
}




def create_session():
    session = requests.Session()

    retries = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["POST"]
    )

    adapter = HTTPAdapter(
        max_retries=retries,
        pool_connections=10,
        pool_maxsize=10
    )

    session.mount("https://", adapter)
    return session

def clean_part_number(val):
    if not val:
        return None
    return str(val).split(",", 1)[0].strip()


def get_invoices(token):
    logging.info("Getting invoices")
    
    try:
        session = requests.session()
        url = f"{start_url}/api/graphql?token={token}"
        query = """
            query invoices($filter: InvoiceFilter, $pageSize: Int, $pageStart: Int) {
            invoices(filter: $filter, pageSize: $pageSize, pageStart: $pageStart) {
                records {
                invoiceId
                year
                dateIssued
                }
                pageSize
                pageStart
                totalRecords
            }
            }
            """
        page_size = 500
        page_start = 0
        all_ids = []

        while True:
            variables = {
                "filter": {"lastModifiedTime": {
                        "greaterThanOrEqual": "2024-01-01T00:00:00Z"
                        }
                    },
                "pageSize": page_size,
                "pageStart": page_start
            }
            payload = {"query": query, "variables": variables}

            res = session.post(url, headers=headers, json=payload, timeout=(10, 30))
            if res.status_code == 200:
                data = res.json()
                
            elif res.status_code == 401:
                logging.warning("Token expired, refreshing...")
                token = get_token()
                return get_invoices(token)
                    
            invoices = data['data']['invoices']['records']
            total = int(data['data']['invoices']['totalRecords'])
            
            inv_id = [rec['invoiceId'] for rec in invoices]
            # print(inv_id)
            all_ids.extend(inv_id)
            logging.info(f"total invocecount: {len(all_ids)}")
            page_start += page_size 
            if page_start>= total:
                break

        logging.info(f"Fetched all invoices based on query parameters from the API: {len(invoices)}")
        return all_ids
    except Exception as e:
        logging.error(f"Bad request check parameters: {e}")


def fetch_single_invoice(inv_id, token):
    query = """
    query invoice($invoiceId: String!) {
        invoice(invoiceId: $invoiceId) {
            invoiceId
            invoiceDate
            invoiceDueDate
            clientPONum
            clientPartNumber
            soldToId
            shipToAddress
            shipToCity
            invoicedDollars
            status
        }
        }
    """

    variables = {"invoiceId": inv_id}
    payload = {"query": query, "variables": variables}

    try:
        session = create_session()
        url = f"{start_url}/api/graphql?token={token}"
        res = session.post(url, headers=headers, json=payload, timeout=(3, 30))
        print(res.status_code)
        res.raise_for_status()
        if res.status_code == 200:
            data = res.json()
            
        
        elif res.status_code == 401:
            return fetch_single_invoice(inv_id, token)
        html = data['data']['invoice'].get('clientPartNumber') or ""
        soup = BeautifulSoup(html, 'html.parser')
        part_num = soup.get_text()
        
        records = {
            "inv_date": data['data']['invoice'].get('invoiceDate'),
            "inv_id": inv_id,
            "client_po_num": data['data']['invoice'].get('clientPONum') or "",
            "client_part_num": clean_part_number(part_num) or "",
            "client_id": data['data']['invoice'].get('soldToId'or  ""),
            "ship_to_address": data['data']['invoice'].get('shipToAddress'),
            "ship_to_city": data['data']['invoice'].get('shipToCity'),
            "amount": float(data['data']['invoice'].get('invoicedDollars')),
            "status": data['data']['invoice'].get('status')
        }
        logging.info(f"fetched  invoices {inv_id}")
        df = pd.DataFrame([records])
        
        time.sleep(random.uniform(2, 5))
        return df

    except requests.exceptions.HTTPError as e:
        
        logging.error(f"Invoice {inv_id} failed: {e} retrying request")
           

            

def get_invoice_details(token):
    logging.info("Getting invoice details")

    invoice_ids = get_invoices(token)
    if not invoice_ids:
        return

    conn = connect_to_db()
    if not conn:
        return
    
    buffer = []
    BATCH_SIZE = 300

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(fetch_single_invoice, inv_id, token)
            for inv_id in invoice_ids
        ]

        for future in as_completed(futures):
            result = future.result()

            if result is not None and not result.empty:
                buffer.append(result)
                logging.info(f"Buffered invoices: {len(buffer)}")

            if len(buffer) >= BATCH_SIZE:
                df = pd.concat(buffer, ignore_index=True)
                load_invoice(df, conn)
                buffer.clear()
    # 
    if buffer:
        df = pd.concat(buffer, ignore_index=True)
        load_invoice(df, conn)


    conn.close()

    

if __name__ == "__main__":
    token = get_token()
    if token:
        get_invoice_details(token)