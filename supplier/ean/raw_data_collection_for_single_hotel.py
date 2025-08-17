import os
import time
import hashlib
import requests
from dotenv import load_dotenv
import json

load_dotenv()

BASE_PATH = "D:/content_for_hotel_json/cdn_row_collection/ean_1"

def get_supplier_own_raw_data(hotel_id):
    EAN_API_KEY = os.getenv("EAN_API_KEY")
    EAN_API_SECRET = os.getenv("EAN_API_SECRET")
    BASE_URL = os.getenv("EAN_BASE_URL")

    timestamp = str(int(time.time()))
    signature_data = f"{EAN_API_KEY}{EAN_API_SECRET}{timestamp}"
    signature = hashlib.sha512(signature_data.encode("utf-8")).hexdigest()

    url = f"{BASE_URL}/v3/properties/content?language=en-US&supply_source=expedia&property_id={hotel_id}"

    headers = {
        "Accept": "application/json",
        "Authorization": f"EAN APIKey={EAN_API_KEY},Signature={signature},timestamp={timestamp}",
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers, timeout=100)

    if response.status_code == 200:
        return response.json().get(hotel_id, {})
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        print(response.text)
        return None

def save_json(raw_path, hotel_id):
    """
    Save hotel data as a JSON file.
    """
    if not os.path.exists(raw_path):
        os.makedirs(raw_path)
    json_file_path = os.path.join(raw_path, f"{hotel_id}.json")
    data = get_supplier_own_raw_data(hotel_id)
    
    if data is None:
        print(f"Warning: {hotel_id}.json - Data fetch failed. Saving default empty JSON.")
        data = {}
    
    try:
        with open(json_file_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)
        print(f"{hotel_id}.json saved successfully at {json_file_path}")
        return True
    except Exception as e:
        print(f"Error saving JSON for hotel {hotel_id}: {e}")
        return False

if __name__ == "__main__":
    hotel_id = input("Enter hotel ID: ").strip()
    save_json(BASE_PATH, hotel_id)