import os
import threading
import requests
import json
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

# Paths
HOTEL_ID_LIST     = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/grnconnect/grnconnect_hotel_id_list.txt"
TRACKING_FILE     = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/grnconnect/grnconnect_tracking_file.txt"
SUCCESS_FILE      = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/grnconnect/successful_done_hotel_id_list.txt"
NOT_FOUND_FILE    = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/grnconnect/grnconnect_not_found.txt"
BASE_PATH         = "D:/content_for_hotel_json/cdn_row_collection/grnconnect_new"

# API
API_KEY = os.getenv("GRNCONNECT_API_KEY")
HEADERS = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'Accept-Encoding': 'application/gzip',
    'api-key': API_KEY
}
CITY_HEADERS = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'API-key': API_KEY
}

# thread‑safe file I/O
lock = threading.Lock()

def get_supplier_own_raw_data(hotel_id):
    os.makedirs(BASE_PATH, exist_ok=True)
    # 1) hotel
    hotel_url = f"https://api-sandbox.grnconnect.com/api/v3/hotels?hcode={hotel_id}&version=2.0"
    r = requests.get(hotel_url, headers=HEADERS); r.raise_for_status()
    hotel = r.json()['hotels'][0]
    # 2) country
    country_code = hotel.get('country')
    r = requests.get(f"https://api-sandbox.grnconnect.com/api/v3/countries/{country_code}", headers=HEADERS)
    r.raise_for_status()
    country = r.json().get('country', {})
    # 3) city
    city_code = hotel.get('city_code')
    r = requests.get(f"https://api-sandbox.grnconnect.com/api/v3/cities/{city_code}?version=2.0", headers=CITY_HEADERS)
    r.raise_for_status()
    city = r.json().get('city', {})
    # 4) images
    r = requests.get(f"https://api-sandbox.grnconnect.com/api/v3/hotels/{hotel_id}/images?version=2.0", headers=HEADERS)
    r.raise_for_status()
    images = r.json().get('images', {}).get('regular', [])

    return {
        'hotel_code': hotel_id,
        'hotel': hotel,
        'country': country,
        'city': city,
        'images': images
    }

def write_json_and_log(hotel_id):
    """
    Worker for a single hotel_id:
    - skips if file exists
    - fetches data, writes .json
    - logs hotel_id to success or not_found
    - returns hotel_id for tracking removal
    """
    out_path = os.path.join(BASE_PATH, f"{hotel_id}.json")
    # skip existing
    if os.path.exists(out_path):
        status = 'success'
    else:
        try:
            data = get_supplier_own_raw_data(hotel_id)
            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            status = 'success'
        except Exception:
            status = 'not_found'

    # thread‑safe appends
    with lock:
        if status == 'success':
            with open(SUCCESS_FILE, 'a', encoding='utf-8') as f:
                f.write(hotel_id + "\n")
        else:
            with open(NOT_FOUND_FILE, 'a', encoding='utf-8') as f:
                f.write(hotel_id + "\n")
    return hotel_id

def initialize_tracking(hotel_ids):
    if not os.path.exists(TRACKING_FILE):
        with open(TRACKING_FILE, 'w', encoding='utf-8') as f:
            f.write("\n".join(hotel_ids) + "\n")

def load_tracking():
    if not os.path.exists(TRACKING_FILE):
        return []
    with open(TRACKING_FILE, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]

def save_tracking(remaining_ids):
    with open(TRACKING_FILE, 'w', encoding='utf-8') as f:
        f.write("\n".join(remaining_ids) + "\n")

def process_hotels(concurrency=20):
    os.makedirs(BASE_PATH, exist_ok=True)

    # load master list
    if not os.path.exists(HOTEL_ID_LIST):
        print("Hotel ID list not found.")
        return
    with open(HOTEL_ID_LIST, 'r', encoding='utf-8') as f:
        all_ids = [l.strip() for l in f if l.strip()]

    initialize_tracking(all_ids)
    to_process = load_tracking()
    if not to_process:
        print("No hotel IDs left.")
        return

    # launch threads
    with ThreadPoolExecutor(max_workers=concurrency) as exe:
        futures = {exe.submit(write_json_and_log, hid): hid for hid in to_process}
        processed = []
        for fut in as_completed(futures):
            hid = futures[fut]
            try:
                fut.result()
                print(f"✔️🐍🐍🐍 Completed {hid}")
            except Exception as e:
                print(f"❌ Error {hid}: {e}")
            processed.append(hid)

    # remove processed from tracking
    remaining = [hid for hid in to_process if hid not in processed]
    save_tracking(remaining)
    print("All done.")

if __name__ == "__main__":
    process_hotels(concurrency=10)
