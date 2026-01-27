import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import random
import os
import re
from get_token import get_token

from azure import load_wrk_orders, connect_to_db
import pandas as pd
from dotenv import load_dotenv
import logging 

load_dotenv()

#============== config ============================
logging.basicConfig(level=logging.INFO, format= '%(astime)s - %(levelname)s - %(message)s')
password = os.getenv("PRJ_PASSWORD")
username = os.getenv("PRJ_USERNAME")
start_url = os.getenv("PRJ_START_URL")
headers = {
    'Content-Type': 'application/json'
    }
#=============================================================

#============= Variables =========
query = """
    query workOrders($filter: WorkOrderFilter, $pageSize: Int, $pageStart: Int, $query: WorkOrderQuery) {
    workOrders(filter: $filter, pageSize: $pageSize, pageStart: $pageStart, query: $query) {
        records {
            workOrderNumber
            scheduledStartDate
            scheduledEndDate
            dueDate
            dateShipped
            status
            customerPlainText
            customerPONumberPlainText
            quantityOrdered
            qtyComplete
            qtyInWIP
            qtyShipped
            qtyNotYetShipped
            quantityQueuedCalculated
            dateShipped
            daysToShip
            hoursCurrentTarget
            hoursTotalSpent
            setupTimeHoursActualLabel
            setupTimeHoursPlannedTarget
            setupTimeHoursPlannedVarianceLabor
            runningTimeHoursActualLabor
            runningTimeHoursPlannedTargetLabor
            runningTimeHoursPlannedVarianceLabor
            standardizedLaborClass
            standardizedLaborRate
            standardizedLaborClass
            standardProfitPerDLH
            standardizedLaborRate
            partPlainText
            partRev
            pfiPrice
            assemblyClass
            btiPrice
            countAsOnTime
            totalCappedWIP
            totalUncappedWIP
            estWODollarAmount
            type
            wipCogsLabor
            wipCogsMaterials
            wipDirectOverhead
            wipIndirectOverhead
        }
        pageSize
        pageStart
        totalRecords
    }
    }
    """


#=========================== Helper =====================================


def to_float(val):
    if val is None:
        return None

    s = str(val).strip()

    if s in ("", " ", "NULL", "N/A", "-", "--"):
        return None

    s = re.sub(r"[^0-9.\-]", "", s)

    if s in ("", "-", "."):
        return None

    try:
        return float(s)
    except ValueError:
        return None


def to_int(val):
    try:
        if val in (None, "", " ", "NULL", "N/A", "-", "--"):
            return None
        val = str(val).replace(",", "").strip()
        if val == "":
            return None
        return int(float(val))
    except Exception:
        return None




def create_session():
    session = requests.Session()

    retries = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST"]
    )

    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)

    return session

#=========================================================================


def fetch_page(token, page_start, page_size, session):
    """Fetch work orders page"""
    logging.info("Fetching all work orders")
    url = f"{start_url}/api/graphql?token={token}"

    payload = {
        "query": query,
        "variables": {
            "filter": {"activeInventoryFlag": True},
            "pageSize": page_size,
            "pageStart": page_start,
            "query": {
                "lastModifiedTime": {
                    "greaterThanOrEqual": "2024-01-01T00:00:00Z"
                }
            }
        }
    }

    for attempt in range(3):
        try:
            res = session.post(url, headers=headers, json=payload, timeout=(10, 180))
    
            if res.status_code == 401:
                token = get_token()
                continue
            res.raise_for_status()
            data = res.json()["data"]["workOrders"]
            return {
                "records": data["records"],
                "total": data["totalRecords"]
            }

        except requests.exceptions.ReadTimeout:
            logging.warning(f"Timeout on page {page_start}, retry {attempt+1}/3")
            time.sleep(random.uniform(3, 7))

    logging.error(f"Failed after retries on page {page_start}")
    return {"records": [], "total": 0}

def fetch_all_work_orders():
    """Fetch all work orders"""
    logging.info("Fetching all work orders concurrently")
    token = get_token()
    session = create_session()

    page_size = 400
    page_start = 0
    all_records = []
    total_records = None

    while True:
        result = fetch_page(token, page_start, page_size, session)
        records = result["records"]

        if total_records is None:
            total_records = result["total"]
            logging.info(f"Total records to fetch: {total_records}")

        if not records:
            break

        all_records.extend(records)
        logging.info(f"Fetched {len(all_records)} of {total_records}")

        page_start += page_size
        time.sleep(random.uniform(2, 5))

        if len(all_records) >= total_records:
            break

    return all_records


def transform(records):
    all_records = []
    for item in records:
        all_records.append({
            "workOrderNumber": item.get("workOrderNumber"),
            "scheduledStartDate": item.get("scheduledStartDate"),
            "scheduledEndDate": item.get("scheduledEndDate"),
            "dueDate": item.get("dueDate"),
            "customerPlainText": item.get("customerPlainText"),
            "client_po_num": item.get("customerPONumberPlainText"),

            "quantityOrdered": to_int(item.get("quantityOrdered")),
            "qtyComplete": to_int(item.get("qtyComplete")),
            "qtyInWIP": to_int(item.get("qtyInWIP")),
            "qtyShipped": to_int(item.get("qtyShipped")),
            "qtyNotYetShipped": to_int(item.get("qtyNotYetShipped")),
            "dateShipped": item.get("dateShipped"),
            "daysToShip": to_int(item.get("daysToShip")),
            "status": item.get("status"),

            "hoursCurrentTarget": item.get("hoursCurrentTarget"),
            "hoursTotalSpent": item.get("hoursTotalSpent"),
            "setupTimeHoursActualLabel": item.get("setupTimeHoursActualLabel"),
            "setupTimeHoursPlannedTarget": item.get("setupTimeHoursPlannedTarget"),
            "setupTimeHoursPlannedVarianceLabor": item.get("setupTimeHoursPlannedVarianceLabor"),

            "runningTimeHoursActualLabor": item.get("runningTimeHoursActualLabor"),
            "runningTimeHoursPlannedTargetLabor": item.get("runningTimeHoursPlannedTargetLabor"),
            "runningTimeHoursPlannedVarianceLabor": item.get("runningTimeHoursPlannedVarianceLabor"),

            "laborWIP": to_float(item.get("laborWIP")),
            "standardizedLaborClass": item.get("standardizedLaborClass"),
            "standardizedLaborRate": item.get("standardizedLaborRate"),
            "partPlainText": item.get("partPlainText"),
            "partRev": item.get("partRev"),
            "pfiPrice": item.get("pfiPrice"),
            "assemblyClass": item.get("assemblyClass"),
            "btiPrice": item.get("btiPrice"),
            "countAsOnTime": item.get("countAsOnTime"),

            "totalCappedWIP": to_float(item.get("totalCappedWIP")),
            "totalUncappedWIP": to_float(item.get("totalUncappedWIP")),
            "estWODollarAmount": to_float(item.get("estWODollarAmount")),
            "type": item.get("type"),

            "wipCogsLabor": to_float(item.get("wipCogsLabor")),
            "wipCogsMaterials": to_float(item.get("wipCogsMaterials")),
            "wipDirectOverhead": to_float(item.get("wipDirectOverhead")),
            "wipIndirectOverhead": to_float(item.get("wipIndirectOverhead"))
        })

    return pd.DataFrame(all_records)

def run():
    try:
        conn = connect_to_db()
        records = fetch_all_work_orders()

        df = transform(records)
        print({len(df.columns)})
        load_wrk_orders(df, conn)
    except Exception as e:
        logging.error(f"Error: {e}")

if __name__ == "__main__":
    run()