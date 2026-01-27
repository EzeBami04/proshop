import requests
import base64
import os
import logging
from dotenv import load_dotenv

load_dotenv()

client_id = os.getenv("QB_ID")
client_secret = os.getenv("QB_SECRET")
auth_code = os.getenv("QB_AUTH_CODE")



logging.basicConfig(level=logging.INFO, format= '%(asctime)s - %(levelname)s - %(message)s')

def get_qbk_token():
    url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": "Basic " + base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    }
    body = {
        "grant_type": "authorization_code",
        "code": auth_code,
        "redirect_uri": "https://developer.intuit.com/v2/OAuth2Playground/RedirectUrl"
    }

    res = requests.post(url, headers=headers, data=body)
    if res.status_code == 200:
        data = res.json()
        access_token = data.get("access_token")
        print("Access Token:", access_token)
        refresh_token = data.get("refresh_token")
        print("Refresh Token:", refresh_token)
        logging.info("Successfully obtained QuickBooks token")
        return {"access_token": access_token, "refresh_token": refresh_token}
    else:
        logging.error(f"Failed to obtain QuickBooks token: {res.status_code} - {res.text}")
        return None


if __name__ == "__main__":
    get_qbk_token()