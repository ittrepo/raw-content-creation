import requests
import json
from dotenv import load_dotenv
import os
import hashlib
import time
# from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

load_dotenv()


HOTEL_ID_LIST = "/var/www/ScirptEngine/Python-Application/Raw-Content-Create-Function/hotelbeds/hotelbeds_hotel_id_list.txt"
TRACKING_FILE = "/var/www/ScirptEngine/Python-Application/Raw-Content-Create-Function/hotelbeds/hotelbeds_tracking_file.txt"
SUCCESS_FILE = "/var/www/ScirptEngine/Python-Application/Raw-Content-Create-Function/hotelbeds/hotelbeds_successful_done_hotel_id_list.txt"
NOT_FOUND_FILE = "/var/www/ScirptEngine/Python-Application/Raw-Content-Create-Function/hotelbeds/hotelbeds_not_found.txt"
BASE_PATH = "/var/www/Storage-Contents/Hotel-Supplier-Raw-Contents/hotelbeds"

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

    payload = {}
    headers = {
        'Api-key': f'{HOTELSTON_API_KEY}',
        'X-Signature': f'{signature}',
        'Accept-Encoding': 'gzip',
        'Content-Type': 'application/json'
    }

    response = requests.get(url, headers=headers, data=payload)

    if response.status_code == 200:
        return response.text
    else:
        logging.error(f"Failed to fetch data for hotel ID {hotel_id}. Status code: {response.status_code}")
        return None

def save_json(raw_path, hotel_id):
    json_file_path = os.path.join(raw_path, f"{hotel_id}.json")
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

def initialize_tracking_file(file_path, hotel_id_list):
    """Initializes the tracking file with all hotel IDs if it doesn't exist."""
    if not os.path.exists(file_path):
        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(map(str, hotel_id_list)) + "\n")
    else:
        logging.info(f"Tracking file already exists: {file_path}")

def read_tracking_file(file_path):
    """Reads the tracking file and returns a list of remaining hotel IDs."""
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as file:
            return [line.strip() for line in file if line.strip()]
    return []

def write_tracking_file(file_path, remaining_ids):
    """Updates the tracking file with unprocessed hotel IDs."""
    try:
        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(remaining_ids) + "\n")
    except Exception as e:
        logging.error(f"Error writing to tracking file: {e}")

def append_to_success_file(file_path, hotel_ids):
    """Append successfully processed hotel IDs to the success file."""
    try:
        with open(file_path, "a", encoding="utf-8") as file:
            file.write("\n".join(hotel_ids) + "\n")
    except Exception as e:
        logging.error(f"Error writing to success file: {e}")

def append_to_not_found_file(file_path, hotel_ids):
    """Append failed hotel IDs to the not found file."""
    try:
        with open(file_path, "a", encoding="utf-8") as file:
            file.write("\n".join(hotel_ids) + "\n")
    except Exception as e:
        logging.error(f"Error writing to not found file: {e}")

def process_hotel(hotel_id):
    try:
        logging.info(f"Processing hotel ID: {hotel_id}")
        success = save_json(BASE_PATH, hotel_id)
        return hotel_id, success
    except Exception as e:
        logging.error(f"Error processing hotel ID {hotel_id}: {e}")
        return hotel_id, False
    
def process_hotels():
    if not os.path.exists(BASE_PATH):
        os.makedirs(BASE_PATH)
        logging.info(f"Created directory: {BASE_PATH}")

    if not os.path.exists(HOTEL_ID_LIST):
        logging.error(f"Hotel ID list file not found: {HOTEL_ID_LIST}")
        return

    with open(HOTEL_ID_LIST, "r", encoding="utf-8") as file:
        all_hotel_ids = [line.strip() for line in file if line.strip()]

    initialize_tracking_file(TRACKING_FILE, all_hotel_ids)

    hotel_ids_to_process = read_tracking_file(TRACKING_FILE)

    if not hotel_ids_to_process:
        logging.info("No hotel IDs left to process.")
        return

    success_hotel_ids = []
    not_found_hotel_ids = []

    for hotel_id in hotel_ids_to_process[:]:  
        hotel_id, success = process_hotel(hotel_id)
        if success:
            success_hotel_ids.append(hotel_id)
        else:
            not_found_hotel_ids.append(hotel_id)

        hotel_ids_to_process.remove(hotel_id)
        write_tracking_file(TRACKING_FILE, hotel_ids_to_process)
        time.sleep(REQUEST_DELAY)

    append_to_success_file(SUCCESS_FILE, success_hotel_ids)
    append_to_not_found_file(NOT_FOUND_FILE, not_found_hotel_ids)

    logging.info("Processing completed.")

    
# Run the function
if __name__ == "__main__":
    process_hotels()
