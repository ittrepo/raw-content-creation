import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

TBO_AUTHENTICATION = os.getenv("TBO_AUTHENTICATION")
BASE_PATH = "D:/content_for_hotel_json/cdn_row_collection/tbo_new"

def get_supplier_own_raw_data(hotel_id):
    url = "https://api.tbotechnology.in/TBOHolidays_HotelAPI/Hoteldetails"
    payload = {
        "Hotelcodes": hotel_id,
        "Language": "en"
    }
    headers = {
        'Authorization': TBO_AUTHENTICATION,
        'Content-Type': 'application/json'
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        data = response.json()
        status_code = data.get('Status', {}).get('Code', -1)
        if status_code == 200:
            return data
        else:
            error_desc = data.get('Status', {}).get('Description', 'No description provided')
            print(f"API Error for hotel {hotel_id}: {error_desc} (Code: {status_code})")
            return None
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error for hotel {hotel_id}: {http_err}")
    except json.JSONDecodeError as json_err:
        print(f"Failed to decode JSON for hotel {hotel_id}: {json_err}")
    except Exception as e:
        print(f"General error fetching hotel {hotel_id}: {e}")
    return None

def save_json(raw_path, hotel_id, data):
    if not os.path.exists(raw_path):
        os.makedirs(raw_path)
    json_path = os.path.join(raw_path, f"{hotel_id}.json")
    if data is None:
        print(f"Warning: {hotel_id}.json - Data fetch failed. Saving default empty JSON.")
        data = {}
    try:
        with open(json_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)
        print(f"{hotel_id}.json saved successfully at {json_path}")
        return True
    except Exception as e:
        print(f"Failed to save {hotel_id}.json: {e}")
        return False

if __name__ == "__main__":
    hotel_id = input("Enter hotel ID: ").strip()
    data = get_supplier_own_raw_data(hotel_id)
    save_json(BASE_PATH, hotel_id, data)