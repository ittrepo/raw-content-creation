import os
import threading
import requests
import json
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError

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

# thread-safe file I/O
lock = threading.Lock()

# First call to hotel endpoint to check existence

def check_hotel_exists(hotel_id):
    url = f"https://api-sandbox.grnconnect.com/api/v3/hotels?hcode={hotel_id}&version=2.0"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    return data.get('total', 0) > 0, data


def get_supplier_own_raw_data(hotel_json, hotel_id):
    os.makedirs(BASE_PATH, exist_ok=True)
    hotel = hotel_json['hotels'][0]
    country_code = hotel.get('country')
    r = requests.get(f"https://api-sandbox.grnconnect.com/api/v3/countries/{country_code}", headers=HEADERS, timeout=15)
    r.raise_for_status()
    country = r.json().get('country', {})
    city_code = hotel.get('city_code')
    r = requests.get(f"https://api-sandbox.grnconnect.com/api/v3/cities/{city_code}?version=2.0", headers=CITY_HEADERS, timeout=15)
    r.raise_for_status()
    city = r.json().get('city', {})
    r = requests.get(f"https://api-sandbox.grnconnect.com/api/v3/hotels/{hotel_id}/images?version=2.0", headers=HEADERS, timeout=15)
    r.raise_for_status()
    images = r.json().get('images', {}).get('regular', [])

    return {'hotel_code': hotel_id, 'hotel': hotel, 'country': country, 'city': city, 'images': images}


def write_json_and_log(hotel_id):
    out_path = os.path.join(BASE_PATH, f"{hotel_id}.json")
    try:
        exists, hotel_data = check_hotel_exists(hotel_id)
    except Exception:
        exists = False
    status = 'not_found'

    if not exists:
        status = 'not_found'
    elif os.path.exists(out_path):
        status = 'success'
    else:
        try:
            data = get_supplier_own_raw_data(hotel_data, hotel_id)
            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            status = 'success'
        except Exception:
            status = 'not_found'

    with lock:
        log_file = SUCCESS_FILE if status == 'success' else NOT_FOUND_FILE
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(hotel_id + "\n")
        if os.path.exists(TRACKING_FILE):
            with open(TRACKING_FILE, 'r', encoding='utf-8') as tf:
                ids = [l.strip() for l in tf if l.strip()]
            ids = [i for i in ids if i != hotel_id]
            with open(TRACKING_FILE, 'w', encoding='utf-8') as tf:
                tf.write("\n".join(ids) + "\n")
        if status == 'success':
            print(f"✔️🐍🐍🐍 Completed {hotel_id}")
        else:
            print(f"⚠️ Not found {hotel_id}")

    return status


def initialize_tracking(hotel_ids):
    if not os.path.exists(TRACKING_FILE):
        with open(TRACKING_FILE, 'w', encoding='utf-8') as f:
            f.write("\n".join(hotel_ids) + "\n")


def load_tracking():
    if not os.path.exists(TRACKING_FILE):
        return []
    with open(TRACKING_FILE, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]


def process_hotels(concurrency=20):
    os.makedirs(BASE_PATH, exist_ok=True)
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

    with ThreadPoolExecutor(max_workers=concurrency) as exe:
        futures = {exe.submit(write_json_and_log, hid): hid for hid in to_process}
        for fut in as_completed(futures):
            hid = futures[fut]
            try:
                fut.result(timeout=15)
            except TimeoutError:
                with lock:
                    if os.path.exists(TRACKING_FILE):
                        with open(TRACKING_FILE, 'r', encoding='utf-8') as tf:
                            ids = [l.strip() for l in tf if l.strip()]
                        ids = [i for i in ids if i != hid]
                        with open(TRACKING_FILE, 'w', encoding='utf-8') as tf:
                            tf.write("\n".join(ids) + "\n")
                print(f"⏰ Timeout: {hid} skipped")
            except Exception as e:
                print(f"❌ Error {hid}: {e}")

    print("Processing pass complete.")

if __name__ == "__main__":
    process_hotels(concurrency=10)
