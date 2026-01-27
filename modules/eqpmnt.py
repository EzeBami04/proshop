import requests
import pandas as pd
import logging
from get_token import get_token
import os
from dotenv import load_dotenv

load_dotenv()


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
start_url = os.getenv("PRJ_URL")

query = """
    query equipments($pageSize: Int, $pageStart: Int) {
    equipments(pageSize: $pageSize, pageStart: $pageStart) {
        records {
        equipmentNumber
        equipmentType
        serialNumber
        legacyId
        tool
        toolName
        createdTime
        lastModifiedTime
        description
        location
        status
        }
        totalRecords
    }
    }
    """

def fetch_equipment(token):
    url = f"{start_url}/graphql?token={token}"

    headers = {
        "Content-Type": "application/json"
    }

    page_size = 300
    page_start = 0

    equipments = []

    try:
        while True:
            payload = {
                "query": query,
                "variables": {
                    "pageSize": page_size,
                    "pageStart": page_start
                }
            }

            res = requests.post(url, headers=headers, json=payload)
            res.raise_for_status()

            data = res.json()
            
            records = data["data"]["equipments"].get("records", [])
            total_records = int(data["data"]["equipments"].get("totalRecords", 0))

            logging.info(f"Fetched {len(records)} records")

            for rec in records:
                equipment = {
                    "equipment_number": rec.get("equipmentNumber"),
                    "equipment_type": rec.get("equipmentType"),
                    "serial_number": rec.get("serialNumber"),
                    "legacy_id":  rec.get("legacyId"),
                    "tool": rec.get("tool" or ""),
                    "tool_name": rec.get("toolName" or ""),
                    "created_at": rec.get("createdTime" or ""),
                    "modified_at": rec.get("lastModifiedTime" or ""),
                    "location": rec.get("location"),
                    "status": rec.get("status")
                }
                equipments.append(equipment)

            page_start += page_size
            if page_start >= total_records:
                break

        # ================= DEDUPLICATION STEP =================
        equipments_unique = []
        seen = set()

        for eq in equipments:
            en = eq["equipment_number"]
            if en and en not in seen:
                seen.add(en)
                equipments_unique.append(eq)

        logging.info(
            f"After deduplication: {len(equipments_unique)} rows "
            f"(from {len(equipments)})"
        )

        df = pd.DataFrame(equipments_unique)
        return df

    except Exception as e:
        logging.error(f"Error fetching equipment data: {e}")
        return pd.DataFrame()

def load_data(df):
    from azure import load_equipments, connect_to_db
    conn = connect_to_db()
    load_equipments(df, conn)

if __name__ == "__main__":
    token = get_token()
    equipment = fetch_equipment(token)
    load_data(equipment)

