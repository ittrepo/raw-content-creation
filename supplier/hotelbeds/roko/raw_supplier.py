import requests
import json
from dotenv import load_dotenv
import os
import hashlib
import time

load_dotenv()

HOTELSTON_API_KEY = os.getenv("HOTELBEDS_API_KEY")
HOTELSTON_API_SECRET = os.getenv("HOTELBEDS_API_SECRET")
HOTELSTON_API_SIGNATURE = os.getenv("HOTELBEDS_API_SIGNATURE")

def get_supplier_own_raw_data(hotel_id):
    HOTELSTON_API_KEY = os.getenv("HOTELBEDS_API_KEY")
    HOTELSTON_API_SECRET = os.getenv("HOTELBEDS_API_SECRET")


    api_key = HOTELSTON_API_KEY
    shared_secret = HOTELSTON_API_SECRET

    timestamp = str(int(time.time()))
    signature_data = f"{api_key}{shared_secret}{timestamp}"
    signature = hashlib.sha256(signature_data.encode('utf-8')).hexdigest()
    

    url = f"https://api.hotelbeds.com/hotel-content-api/1.0/hotels/{hotel_id}/details?language=ENG&useSecondaryLanguage=False"

    payload = {}
    headers = {
    'Api-key': f'{HOTELSTON_API_KEY}',
    'X-Signature': f'{signature}',
    'Accept-Encoding': 'gzip',
    'Content-Type': 'application/json'
    }

    response = requests.get(url, headers=headers, data=payload)

    if response.status_code == 200:
        response_data = response.text
        # print(response_data)
        # json_data = json.dumps(response_data, indent=4)
        return response_data
    else:
        print(f"Failed to fetch data. Status codeL {response.status_code}")
        return None



def save_json(raw_path, hotel_id):
    base_path = os.path.join(raw_path, hotel_id)

    if not os.path.exists(base_path):
        os.makedirs(base_path)

    row_dir = os.path.join(base_path, "supplier")

    if not os.path.exists(row_dir):
        os.makedirs(row_dir)

    def fetch_and_save_json(fetch_function, file_name, *args):
        json_data = fetch_function(*args)

        if json_data is None or "Failed to fectch data. Status Code 400" in str(json_data):
            print(f"Warning: {file_name} - Data fetch failed. Save default empty JSON.")
            json_data = "{}"
        
        full_file_path = os.path.join(row_dir, file_name)
        with open(full_file_path, "w", encoding="utf-8") as file:
            file.write(json_data)

        print(f"{file_name} saved successfully at {full_file_path}")
    
    json_data_for_basic = get_supplier_own_raw_data(hotel_id)

    fetch_and_save_json(lambda *args: json_data_for_basic, f"row_{hotel_id}.json")


supplier = "hotelbeds"
base_path = f"D:/content_for_hotel_json/raw_hotel_info/{supplier}"
hotel_id = "1"
save_json(base_path, hotel_id)