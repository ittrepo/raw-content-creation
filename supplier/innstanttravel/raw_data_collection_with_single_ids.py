import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

INNESTENT_HOTEL_KEY = os.getenv("INNESTENT_HOTEL_KEY")
INNESTENT_HOTEL_TOKEN = os.getenv("INNESTENT_HOTEL_TOKEN")
BASE_PATH = "D:/content_for_hotel_json/cdn_row_collection/innstanttravel_new"

def get_supplier_own_raw_data(hotel_id):
    INNESTENT_HOTEL_KEY = os.getenv("INNESTENT_HOTEL_KEY")
    INNESTENT_HOTEL_TOKEN = os.getenv("INNESTENT_HOTEL_TOKEN")
    url = f"https://static-data.innstant-servers.com/hotels/{hotel_id}"
    headers = {
        'aether-application-key': f'{INNESTENT_HOTEL_KEY}',
        'aether-access-token': f'{INNESTENT_HOTEL_TOKEN}'
    }
    response = requests.get(url, headers=headers, timeout=10)
    if response.status_code == 200:
        try:
            return response.json()
        except Exception as e:
            print(f"Error parsing JSON for hotel ID {hotel_id}: {e}")
            return None
    else:
        print(f"Failed to fetch data for hotel ID {hotel_id}. Status code: {response.status_code}, Response: {response.text}")
        return None

def save_json(raw_path, hotel_id):
    json_file_path = os.path.join(raw_path, f"{hotel_id}.json")
    data = get_supplier_own_raw_data(hotel_id)
    if data is None:
        print(f"Warning: {hotel_id}.json - Data fetch failed. Saving default empty JSON.")
        data = {}
    try:
        if not os.path.exists(raw_path):
            os.makedirs(raw_path)
        with open(json_file_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=2)
        print(f"{hotel_id}.json saved successfully at {json_file_path}")
        return True
    except Exception as e:
        print(f"Error saving JSON for hotel {hotel_id}: {e}")
        return False

if __name__ == "__main__":
    hotel_id = input("Enter hotel ID: ").strip()
    save_json(BASE_PATH, hotel_id)