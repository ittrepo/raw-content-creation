import requests
import json
from dotenv import load_dotenv
import os
import hashlib
import time
import logging

load_dotenv()

BASE_PATH = r"D:/content_for_hotel_json/cdn_row_collection/hotelbeds_new_1"
REQUEST_DELAY = 1

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_supplier_own_raw_data(hotel_id):
    HOTELSTON_API_KEY = os.getenv("HOTELBEDS_API_KEY")
    HOTELSTON_API_SECRET = os.getenv("HOTELBEDS_API_SECRET")

    api_key = HOTELSTON_API_KEY
    shared_secret = HOTELSTON_API_SECRET

    timestamp = str(int(time.time()))
    signature_data = f"{api_key}{shared_secret}{timestamp}"
    signature = hashlib.sha256(signature_data.encode('utf-8')).hexdigest()

    url = f"https://api.hotelbeds.com/hotel-content-api/1.0/hotels/{hotel_id}/details?language=ENG&useSecondaryLanguage=False"

    headers = {
        'Api-key': f'{HOTELSTON_API_KEY}',
        'X-Signature': f'{signature}',
        'Accept-Encoding': 'gzip',
        'Content-Type': 'application/json'
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.text
    else:
        logging.error(f"Failed to fetch data for hotel ID {hotel_id}. Status code: {response.status_code}")
        return None

def save_json(hotel_id):
    if not os.path.exists(BASE_PATH):
        os.makedirs(BASE_PATH)
    json_file_path = os.path.join(BASE_PATH, f"{hotel_id}.json")
    json_data = get_supplier_own_raw_data(hotel_id)

    if json_data is None or "Failed to fetch data" in str(json_data):
        logging.warning(f"{hotel_id}.json - Data fetch failed. Saving default empty JSON.")
        json_data = "{}"

    try:
        with open(json_file_path, "w", encoding="utf-8") as file:
            file.write(json_data)
        logging.info(f"{hotel_id}.json saved successfully at {json_file_path}")
        return True
    except Exception as e:
        logging.error(f"Error saving JSON for hotel {hotel_id}: {e}")
        return False

if __name__ == "__main__":
    hotel_id = input("Enter hotel ID: ").strip()
    save_json(hotel_id)