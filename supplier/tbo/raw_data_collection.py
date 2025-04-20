import os
import json
import requests
import time
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables
load_dotenv()

# Constants
TBO_AUTHENTICATION = os.getenv("TBO_AUTHENTICATION")
HOTEL_ID_LIST = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/tbo/tbohotel_hotel_id_list.txt"
TRACKING_FILE = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/tbo/tbohotel_tracking_file.txt"
SUCCESS_FILE = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/tbo/successful_done_hotel_id_list.txt"
NOT_FOUND_FILE = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/tbo/tbohotel_not_found.txt"
BASE_PATH = "D:/content_for_hotel_json/cdn_row_collection/tbo"

REQUEST_DELAY = 1
MAX_WORKERS = 10  # Number of concurrent threads

def get_supplier_own_raw_data(hotel_id):
    """
    Fetch hotel data from the TBO API, checking both HTTP and internal status codes.
    """
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
        response.raise_for_status()  # Raises HTTPError for 4xx/5xx responses

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
    """
    Save hotel data only if valid. Returns True if successful.
    """
    json_path = os.path.join(raw_path, f"{hotel_id}.json")

    if data is None:
        print(f"Skipping save for {hotel_id} - no valid data.")
        return False

    try:
        with open(json_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)
        print(f"Successfully saved {hotel_id}.json")
        return True
    except Exception as e:
        print(f"Failed to save {hotel_id}.json: {e}")
        return False

def initialize_tracking_file(file_path, hotel_id_list):
    """
    Initialize the tracking file with all hotel IDs if it doesn't exist.
    """
    if not os.path.exists(file_path):
        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(map(str, hotel_id_list)) + "\n")
    else:
        print(f"Tracking file already exists: {file_path}")

def read_tracking_file(file_path):
    """
    Read the tracking file and return a list of remaining hotel IDs.
    """
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as file:
            return [line.strip() for line in file if line.strip()]
    return []

def write_tracking_file(file_path, remaining_ids):
    """
    Update the tracking file with unprocessed hotel IDs.
    """
    try:
        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(remaining_ids) + "\n")
    except Exception as e:
        print(f"Error writing to tracking file: {e}")

def append_to_success_file(file_path, hotel_id):
    """
    Append successfully processed hotel ID to the success file.
    """
    try:
        with open(file_path, "a", encoding="utf-8") as file:
            file.write(hotel_id + "\n")
    except Exception as e:
        print(f"Error writing to success file: {e}")

def append_to_not_found_file(file_path, hotel_id):
    """
    Append failed hotel ID to the not found file.
    """
    try:
        with open(file_path, "a", encoding="utf-8") as file:
            file.write(hotel_id + "\n")
    except Exception as e:
        print(f"Error writing to not found file: {e}")

def process_hotel(hotel_id):
    json_path = os.path.join(BASE_PATH, f"{hotel_id}.json")

    if os.path.exists(json_path):
        print(f"Skipping {hotel_id} - JSON exists.")
        append_to_success_file(SUCCESS_FILE, hotel_id)
        return hotel_id, True

    try:
        print(f"Processing {hotel_id}")
        data = get_supplier_own_raw_data(hotel_id)
        success = save_json(BASE_PATH, hotel_id, data)

        if success:
            append_to_success_file(SUCCESS_FILE, hotel_id)
        else:
            append_to_not_found_file(NOT_FOUND_FILE, hotel_id)

        return hotel_id, success
    except Exception as e:
        print(f"Critical error processing {hotel_id}: {e}")
        return hotel_id, False

def process_hotels():
    if not os.path.exists(BASE_PATH):
        os.makedirs(BASE_PATH)
        print(f"Created directory: {BASE_PATH}")

    if not os.path.exists(HOTEL_ID_LIST):
        print(f"Hotel ID list file not found: {HOTEL_ID_LIST}")
        return

    with open(HOTEL_ID_LIST, "r", encoding="utf-8") as file:
        all_hotel_ids = [line.strip() for line in file if line.strip()]

    initialize_tracking_file(TRACKING_FILE, all_hotel_ids)

    hotel_ids_to_process = read_tracking_file(TRACKING_FILE)

    if not hotel_ids_to_process:
        print("No hotel IDs left to process.")
        return

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_hotel = {executor.submit(process_hotel, hotel_id): hotel_id for hotel_id in hotel_ids_to_process}

        for future in as_completed(future_to_hotel):
            hotel_id, success = future.result()
            hotel_ids_to_process.remove(hotel_id)
            write_tracking_file(TRACKING_FILE, hotel_ids_to_process)
            time.sleep(REQUEST_DELAY)

    print("Processing completed.")

# Run the function
if __name__ == "__main__":
    process_hotels()
