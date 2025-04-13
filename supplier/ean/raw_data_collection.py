import os
import time
import hashlib
import requests
from dotenv import load_dotenv
import json

load_dotenv()

# Constants
HOTEL_ID_LIST = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/ean/ean_hotel_id_list.txt"
TRACKING_FILE = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/ean/ean_tracking_file.txt"
SUCCESS_FILE = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/ean/successful_done_hotel_id_list.txt"
NOT_FOUND_FILE = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/ean/ean_not_found.txt"
BASE_PATH = "D:/content_for_hotel_json/cdn_row_collection/ean"


REQUEST_DELAY = 1


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

    for hotel_id in hotel_ids_to_process.copy(): 
        json_path = os.path.join(BASE_PATH, f"{hotel_id}.json")
        
        if os.path.exists(json_path):
            print(f"Skipping {hotel_id} - JSON file already exists.")
            append_to_success_file(SUCCESS_FILE, hotel_id)
            hotel_ids_to_process.remove(hotel_id)
            write_tracking_file(TRACKING_FILE, hotel_ids_to_process)
            continue

        try:
            print(f"Processing hotel ID: {hotel_id}")
            success = save_json(BASE_PATH, hotel_id)
            
            if success:
                append_to_success_file(SUCCESS_FILE, hotel_id)
            else:
                append_to_not_found_file(NOT_FOUND_FILE, hotel_id)

            hotel_ids_to_process.remove(hotel_id)
            
        except Exception as e:
            print(f"Error processing hotel ID {hotel_id}: {e}")
            continue

        write_tracking_file(TRACKING_FILE, hotel_ids_to_process)

        time.sleep(REQUEST_DELAY)

    print("Processing completed.")

# Run the function
if __name__ == "__main__":
    process_hotels()