import requests

import pandas as pd
import uuid
from azure import load_part, connect_to_db
from get_token import get_token
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logging.getLevelName("parts.py")
logging.basicConfig(level=logging.INFO, format= '%(asctime)s - %(levelname)s - %(message)s')

start_url = os.getenv("PRJ_START_URL")

query_prt = """
    query parts($filter: PartFilter, $pageSize: Int, $pageStart: Int, $query: PartQuery) {
        parts(filter: $filter, pageSize: $pageSize, pageStart: $pageStart, query: $query) {
            records {
                partNumber
                partName
                status
                notes
            }
            pageSize
            pageStart
            totalRecords
        }
    }
"""




def get_all_parts_paginated(token, page_size=100):
    """Get all parts with pagination"""
    url = f"{start_url}/api/graphql?token={token}"
    headers = {
        "content-type": "application/json"
    }
    
    all_parts = []
    page_start = 0
    
    query = """
    query parts($pageSize: Int, $pageStart: Int) {
        parts(pageSize: $pageSize, pageStart: $pageStart) {
            records {
                status
                partNumber
                partName
                customerPartNumber
                qtyInWip
                clientPartRev
                dollarsInInventoryForPartCapped
                inventoryAccount
                inventoryImportValueTotal
                inventoryQtyForPart
                lastActivityDate
                leastAmountToOrder
                legacyId
                minimumOrderQty
                minimumQuantityOnHand
                minReorderPoint
                multiplierMarkup
                netInspectImportNotes
                originalSortPosition
                packagingInstructions
                pricingNotes
                salesAccount
                shippingCost
                standardizedLaborClass
                templateGroup
                universalProductCode
                leadTime
                notes
            }
            pageSize
            pageStart
            totalRecords
        }
    }
    """
    
    while True:
        payload = {
            "query": query,
            "variables": {
                "pageSize": page_size,
                "pageStart": page_start
            }
        }
        
        try:
            res = requests.post(url, headers=headers, json=payload, timeout=(10, 30))
            res.raise_for_status()
            
            data = res.json()
            
            if 'data' in data and 'parts' in data['data']:
                parts_data = data['data']['parts']
                records = parts_data['records']
                total_records = int(parts_data['totalRecords'])
                
                all_parts.extend(records)
                
                logging.info(f"Fetched {len(records)} records (Total: {len(all_parts)} of {total_records})")
                
                
                if page_start + page_size >= total_records or not records:
                    break
                
                page_start += page_size
            else:
                break
        
        except Exception as e:
            logging.error(f"Error during pagination: {e}")
            break
    
    return all_parts
#======= parse records
def parse_records(records):
    parsed = []
    for rec in records:
        # print(rec)
        parsed.append({
            "unique_id": str(uuid.uuid4().hex[:8]),
            "partNumber": str(rec.get("partNumber")),
            "partName": str(rec.get("partName")),
            "client_part_num": str(rec.get("customerPartNumber")),
            "qtyInWip": (rec.get("qtyInWip")),
            "pat_rev": str(rec.get("clientPartRev")),
            "inventory_account": str(rec.get("inventoryAccount")),
            "inventory_import_value": str(rec.get("inventoryImportValueTotal")),
            "inventory_qty": str(rec.get("inventoryQtyForPart")),
            "last_activity_date": str(rec.get("lastActivityDate")),
            "least_amount_to_order": str(rec.get("leastAmountToOrder")),
            "legacy_id": str(rec.get("legacyId")),
            "minimum_order_qty": str(rec.get("minimumOrderQty")),
            "minimum_qty_on_hand": str(rec.get("minimumQuantityOnHand")),
            "min_reorder_point": str(rec.get("minReorderPoint")),
            "multiplier_markup": str(rec.get("multiplierMarkup")),
            "net_inspect_import_notes": str(rec.get("netInspectImportNotes")),
            "original_sort_position": str(rec.get("originalSortPosition")),
            "packaging_instructions": str(rec.get("packagingInstructions")),
            "pricing_notes": str(rec.get("pricingNotes")),
            "sales_account": str(rec.get("salesAccount")),
            "shipping_cost": str(rec.get("shippingCost")),
            "standardized_labor_class": str(rec.get("standardizedLaborClass")),
            "template_group": str(rec.get("templateGroup")),
            "universal": str(rec.get("universalProductCode")),
            "status": str(rec.get("status")),
            "notes": str(rec.get("notes")),
            "leadTime": str(rec.get("leadTime"))
        })
    # Convert to DataFrame for better handling
    df = pd.DataFrame(parsed)
    return df
def load_parts(df, conn):
    if df.empty:
        logging.info("No parts to load")
        return

    logging.info(f"Loading {len(df)} parts into the database")
    load_part(df, conn)
    logging.info("Parts loaded successfully")

if __name__ == "__main__":
    token = get_token()
    con = connect_to_db()
    if token:
       records = get_all_parts_paginated(token, page_size=100)
       df = parse_records(records)
       load_parts(df, con)
       