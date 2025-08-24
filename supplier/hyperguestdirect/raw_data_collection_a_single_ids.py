import os
import json
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

BASE_PATH = "D:/content_for_hotel_json/cdn_row_collection/hyperguestdirect_new"

def get_supplier_own_raw_data(hotel_id):
    hyperguest_token = os.getenv("HYPERGUEST_TOKEN")
    url = f"https://hg-static.hyperguest.com/{hotel_id}/property-static.json"
    headers = {'Authorization': f'Bearer {hyperguest_token}'}

    response = requests.get(url, headers=headers, timeout=10)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: Received status code {response.status_code}")
        return None

def save_json(raw_path, hotel_id):
    data = get_supplier_own_raw_data(hotel_id)
    json_path = os.path.join(raw_path, f"{hotel_id}.json")

    if data is None:
        print(f"Warning: {hotel_id}.json - Data fetch failed. Saving default empty JSON.")
        data = {}

    try:
        if not os.path.exists(raw_path):
            os.makedirs(raw_path)
        with open(json_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)
        print(f"{hotel_id}.json saved successfully at {json_path}")
        return True
    except Exception as e:
        print(f"Failed to save {hotel_id}.json: {e}")
        return False

if __name__ == "__main__":
    hotel_id = input("Enter hotel ID: ").strip()
    save_json(BASE_PATH, hotel_id)