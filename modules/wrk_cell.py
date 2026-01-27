import requests
import pandas as pd
from get_token import get_token 

import logging
import os
from dotenv import load_dotenv

load_dotenv()


logging.basicConfig(level=logging.INFO)

start_url = os.getenv("PRJ_START_URL")
query = """
    query workCell($workCell: String){
        workCell(workCell: $workCell){
            records {
            legacyId
            commonName
            shortName
            department
            class
            installGroup
            isScheduledResource
            isBottleneckResource
            totalPartsRun
            totalPartsScrapped
            standardLeadTimeDays
            scheduleEfficiencyMultiplier
            currentOp
            tieInStatus
            tieInDate
            operatorPlainText
            createdTime
            lastModTime
            }
            totalRecords
        }
        }
        """



def get_all_workcells(token):
    """
    Fetch all workcells using pagination
    """
    all_workcells = []
    page_size = 500
    page_start = 0

    url = f"{start_url}/api/graphql?token={token}"
    headers = {
        "content-type": "application/json"
    }
    
    query = """
    query workcells($pageSize: Int, $pageStart: Int) {
        workcells(pageSize: $pageSize, pageStart: $pageStart) {
            records {
                commonName
                shortName
                description
                department
                class
                installGroup
                isScheduledResource
                isBottleneckResource
                isLathe
                currentOp
                operatorPlainText
                totalPartsRun
                totalPartsScrapped
                scheduleEfficiencyMultiplier
                standardLeadTimeDays
                lastModTime
                createdTime
                hideOnSchedule
            }
            pageSize
            pageStart
            totalRecords
        }
    }
    """
    
    while True:
        variables = {
            "pageSize": page_size,
            "pageStart": page_start
        }
        
        payload = {"query": query, "variables": variables}
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            print(data)
            workcells_data = data['data']['workcells']
            records = workcells_data['records']
            total = workcells_data['totalRecords']
            
            all_workcells.extend(records)
            print(f"Fetched {len(all_workcells)} of {total} workcells")
            
            page_start += page_size
            
            if page_start >= total:
                break
        else:
            print(f"Error: {response.status_code}")
            break
    
    return all_workcells


if __name__ == "__main__":
    token = get_token()
    get_all_workcells(token)