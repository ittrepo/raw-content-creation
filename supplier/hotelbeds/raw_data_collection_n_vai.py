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
    """Fetch data and save it as a JSON file in the specified path."""

    json_file_path = os.path.join(raw_path, f"{hotel_id}.json")

    json_data = get_supplier_own_raw_data(hotel_id)

    if json_data is None or "Failed to fetch data" in str(json_data):
        print(f"Warning: {hotel_id}.json - Data fetch failed. Saving default empty JSON.")
        json_data = "{}"

    with open(json_file_path, "w", encoding="utf-8") as file:
        file.write(json_data)

    print(f"{hotel_id}.json saved successfully at {json_file_path}")

supplier = "hotelbeds"
base_path = f"D:/content_for_hotel_json/cdn_row_collection/{supplier}"
hotel_id = "1"

if not os.path.exists(base_path):
    os.makedirs(base_path)

# Call the function to save JSON
save_json(base_path, hotel_id)