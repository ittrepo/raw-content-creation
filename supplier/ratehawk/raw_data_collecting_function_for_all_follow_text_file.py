import requests
import json
from dotenv import load_dotenv
import os

load_dotenv()

RATEHAWK_AUTHORIZATION = os.getenv("RATEHAWK_AUTHORIZATION")
BASE_PATH = "D:/content_for_hotel_json/cdn_row_collection/ratehawk_new"

def get_supplier_own_raw_data(hotel_id):
    url = "https://api.worldota.net/api/b2b/v3/hotel/info/"
    payload = json.dumps({
        "id": hotel_id,
        "language": "en"
    })
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {RATEHAWK_AUTHORIZATION}',
        'Cookie': '__cf_bm=XjUw7aE6GVO5x_hWbbsFZKu48AZRjs7OLBk0Ojsdetk-1756104330-1.0.1.1-fgYOFH2RGFoT0Xq3XaD6tB9SnFhVw3WwB0CqRLqsLttnO5L7Ll0C844byqwAbovnza.gm6.WhypaWmxQBDAi_hwm._7V4bLLSsbI_q1LG70; uid=TfTb8GgHbhCN1UILA5frAg=='
    }
    try:
        response = requests.post(url, headers=headers, data=payload, timeout=15)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching data for hotel {hotel_id}: {e}")
        return None

def save_json(raw_path, hotel_id, data):
    if not os.path.exists(raw_path):
        os.makedirs(raw_path)
    json_file_path = os.path.join(raw_path, f"{hotel_id}.json")
    if data is None:
        print(f"Warning: {hotel_id}.json - Data fetch failed. Saving default empty JSON.")
        data = {}
    try:
        with open(json_file_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=2)
        print(f"{hotel_id}.json saved successfully at {json_file_path}")
        return True
    except Exception as e:
        print(f"Error saving JSON for hotel {hotel_id}: {e}")
        return False

if __name__ == "__main__":
    hotel_id = input("Enter hotel ID: ").strip()
    data = get_supplier_own_raw_data(hotel_id)
    save_json(BASE_PATH, hotel_id, data)