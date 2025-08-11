import requests
import json
from dotenv import load_dotenv
import os
from io import StringIO 
import pandas as pd

load_dotenv()

def authentication_paximum():
    paximum_token = os.getenv("PAXIMUM_TOKEN")
    paximum_agency = os.getenv("PAXIMUM_AGENCY")
    paximum_user = os.getenv("PAXIMUM_USER")
    paximum_password = os.getenv("PAXIMUM_PASSWORD")
    
    url = "http://service.stage.paximum.com/v2/api/authenticationservice/login"

    payload = json.dumps({
        "Agency": paximum_agency,
        "User": paximum_user,
        "Password": paximum_password
    })
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': paximum_token
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    if response.status_code == 200:
        try:
            df = pd.read_json(StringIO(response.text))
            token = df.get("body").get("token")
            return token
        except Exception as e:
            print("Error parsing token:", e)
            return None
    else:
        print(f"Failed to authenticate. Status code: {response.status_code}, Response: {response.text}")
        return None

def get_supplier_own_raw_data(hotel_id):
    token = authentication_paximum()
    if not token:
        print(f"Authentication failed for hotel ID {hotel_id}.")
        return None

    url = "http://service.stage.paximum.com/v2/api/productservice/getproductInfo"

    payload = {
        "productType": 2,
        "ownerProvider": 2,
        "product": hotel_id,
        "culture": "en-US"
    }
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    response = requests.post(url, headers=headers, json=payload)  
    if response.status_code == 200:
        try:
            return response.json()  
        except Exception as e:
            print(f"Error parsing JSON for hotel ID {hotel_id}: {e}")
            return None
    else:
        print(f"Failed to fetch data for hotel ID {hotel_id}. Status code: {response.status_code}")
        return None

def save_json(raw_path, hotel_id):
    """Fetch data and save it as an actual JSON file in the specified path."""
    if not os.path.exists(raw_path):
        os.makedirs(raw_path)
    json_file_path = os.path.join(raw_path, f"{hotel_id}.json")
    json_data = get_supplier_own_raw_data(hotel_id)

    if json_data is None:
        print(f"Warning: {hotel_id}.json - Data fetch failed. Saving default empty JSON.")
        json_data = {}

    with open(json_file_path, "w", encoding="utf-8") as file:
        json.dump(json_data, file, indent=4)

    print(f"{hotel_id}.json saved successfully at {json_file_path}")

if __name__ == "__main__":
    hotel_id = input("Enter hotel ID: ").strip()
    base_path = "D:/content_for_hotel_json/cdn_row_collection/paximum"
    save_json(base_path, hotel_id)