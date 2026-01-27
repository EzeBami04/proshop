import requests
import pandas as pd
import logging
from get_token import get_token
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)




headers = {"content-type": "application/json"}
start_url = os.getenv("PRJ_START_URL")


# ============= Get All Bills with Pagination =============
def get_all_bills(token):
    """
    Fetch all bills using pagination
    """
    logging.info("Fetching all bills...")
    url = f"{start_url}/api/graphql?token={token}"
    
    query = """
    query bills($pageSize: Int, $pageStart: Int) {
        bills(pageSize: $pageSize, pageStart: $pageStart) {
            records {
                billId
                dateIssued
                dueDate
                status
                referenceNumber
                supplierId
                supplierPlainText
                supplierAddress
                supplierAddressee
                supplierCity
                supplierState
                supplierZipCode
                totalDollars
                paymentTerms
                paymentTermsDiscount
                paymentTermsDiscountDays
            }
            pageSize
            pageStart
            totalRecords
        }
    }
    """
    
    all_bills = []
    page_size = 500
    page_start = 0
    
    try:
        while True:
            variables = {
                "pageSize": page_size,
                "pageStart": page_start,
                "filter": {"lastModifiedTime ": 
                           {"greaterThanOrEqual": "2024-01-01T00:00:00Z"}}
            }
            
            payload = {"query": query, 
                       "variables": variables}
            response = requests.post(url, headers=headers, json=payload, timeout=(30, 100))
            
            if response.status_code == 200:
                data = response.json()
                
                if 'errors' in data:
                    logging.error(f"GraphQL errors: {data['errors']}")
                    break
                
                bills_data = data['data']['bills']
                records = bills_data['records']
                total = bills_data['totalRecords']
                
                all_bills.extend(records)
                logging.info(f"Fetched {len(all_bills)} of {total} bills")
                
                page_start += page_size
                
                if page_start >= total:
                    break
                    
            elif response.status_code == 401:
                logging.warning("Token expired, refreshing...")
                token = get_token()
                url = f"{start_url}/api/graphql?token={token}"
                continue
            else:
                logging.error(f"Error: {response.status_code} - {response.text}")
                break
        bills = []
        for rec in all_bills:
            bills.append({
                "bill_id": rec.get("billId"),
                "date_issued": rec.get("dateIssued" or ""),
                "due_date": rec.get("dueDate" or ""),
                "status": rec.get("status" or ""),
                "reference_num": rec.get("referenceNumber" or ""),
                "supplier_id": rec.get("supplierId"),
                "supplierPlainText": rec.get("supplierPlainText" or ""),
                "supplierAddress": rec.get("supplierAddress" or ""),
                "supplierCity": rec.get("supplierCity" or ""),
                "supplierZipCode": rec.get("supplierZipCode" or ""),
                "totalDollars": float(rec.get("totalDollars")),
                "paymentTerms": rec.get("paymentTerms"),
                "paymentTermsDiscount": rec.get("paymentTermsDiscount"),
                " paymentTermsDiscountDays": rec.get(" paymentTermsDiscountDays")
            })
        
        df = pd.DataFrame(bills)
        
        logging.info(f"Total bills fetched: {len(bills)}")
        return df     
        
    except Exception as e:
        logging.error(f"Error fetching bills: {e}")
        return []

def load(token):
    from azure import load_bills, connect_local
    conn = connect_local()
    df = get_all_bills(token)
    load_bills(df, conn)
    

if __name__ == "__main__":
    token = get_token()
    if token:
        load(token)
