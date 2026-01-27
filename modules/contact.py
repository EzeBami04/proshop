import requests
import pandas as pd
from get_token import get_token
import os
from dotenv import load_dotenv
import logging

from modules.azure import connect_to_db
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

start_url = os.getenv("PRJ_START_URL")
query = """
    query contacts($pageSize: Int, $pageStart: Int){
    contacts(pageSize: $pageSize, pageStart: $pageStart) {
        records {
            createdTime
            name
            companyName
            mainContact
            contactEmail
            phoneNumber
            preferredStatus
            previousContactCode
            customerSupplierCode 
            previousName
            type
            paymentTerms
            priceCode
            projectCodeOnPS
            billToAddress
            billToCity
            billToState
            billToZipcode
            websiteAddress
            status}
            pageSize
            pageStart
            totalRecords
        }
    }
"""

def get_contacts(token):
    url = f"{start_url}/api/graphql?token={token}"
    headers = {
        "content-type": "application/json"
    }
    page_start = 0
    page_size = 500
   
    variables = {
        "pageSize": page_size,
        "pageStart": page_start
    }
    try:
        while True:

            payload = {
                "query": query,
                "variables": variables
            }
            
            res = requests.post(url, headers=headers, json=payload)
            res.raise_for_status()
            if res.status_code == 200:
                data = res.json()
                records = data["data"]["contacts"]["records"]
                total_records = int(data["data"]["contacts"]["totalRecords"])

                contacts = []
                for rec in records:
                    contacts.append({
                        "created_time": rec.get("createdTime"),
                        "name": rec.get("name"),
                        "company_name": rec.get("companyName"),
                        "main_contact": rec.get("mainContact"),
                        "contact_email": rec.get("contactEmail"),
                        "phone_number": rec.get("phoneNumber"),
                        "preferred_status": rec.get("preferredStatus"),
                        "previous_contact_code": rec.get("previousContactCode"),
                        "customer_supplier_code": rec.get("customerSupplierCode"),
                        "previous_name": rec.get("previousName"),
                        "type": rec.get("type"),
                        "payment_terms": rec.get("paymentTerms"),
                        "price_code": rec.get("priceCode"),
                        "project_code_on_ps": rec.get("projectCodeOnPS"),
                        "bill_to_address": rec.get("billToAddress"),
                        "bill_to_city": rec.get("billToCity"),
                        "bill_to_state": rec.get("billToState"),
                        "bill_to_zipcode": rec.get("billToZipcode"),
                        "website_address": rec.get("websiteAddress"),
                        "status": rec.get("status")
                    })
                    df = pd.DataFrame(contacts)
            logging.info(f"Fetched {len(records)} records (pageStart={page_start})")
            page_start += page_size
            if page_start >= total_records:
                break
            return df
    except Exception as e:
        logging.error(f"Error fetching contacts: {e}")
        return pd.DataFrame()

def load_contacts(df):
    """Load contact data to Azure SQL Database"""
    from azure import load_contacts, connect_local
    logging.info("Loading contact data to Azure SQL Database...")
    conn = connect_to_db()
    load_contacts(df, conn)

if __name__ == "__main__":
    token = get_token()
    contacts = get_contacts(token)
    if not contacts.empty:
        load_contacts(contacts)
    else:
        logging.info("No contact data fetched.")