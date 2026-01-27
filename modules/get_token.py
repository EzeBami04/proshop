import requests
import os
from dotenv import load_dotenv
import  logging 


load_dotenv()
url = os.getenv("MSP_AUTH_URL")
username = os.getenv("MSP_USERNAME")
password = os.getenv("MSP_PASSWORD")
scope = os.getenv("MSP_SCOPE")


headers = {
    'Content-Type': 'application/json'
    }

def get_token():
    logging.info("Deriving session token")
    
    payload = {
    'username': username,
    'password': password,
    'scope': scope
    }
    try:
        res = requests.request("POST", url, headers=headers, json=payload, timeout=None)
        res.raise_for_status()
        if res.status_code == 200:
            token = res.json()['authorizationResult'].get('token')
            logging.info("succesfully derived session token")
            return token
        
    except Exception as e:
        logging.warning(f'Bad request check parameters {e}')
