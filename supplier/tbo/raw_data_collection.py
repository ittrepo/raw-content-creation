import requests
import json
from dotenv import load_dotenv
import os
import hashlib
import time

load_dotenv()

TBO_AUTHENTICATION = os.getenv("TBO_AUTHENTICATION")

def get_supplier_own_raw_data(hotel_id):
    TBO_AUTHENTICATION = os.getenv("TBO_AUTHENTICATION")

    url = f"https://api.tbotechnology.in/TBOHolidays_HotelAPI/Hoteldetails"

    payload = json.dumps({
        "Hotelcodes": hotel_id,
        "Language": "en"
        })
    headers = {
    'Authorization': f'{TBO_AUTHENTICATION}',
    'Content-Type': 'application/json'
    }

    response = requests.post(url, headers=headers, data=payload)

    if response.status_code == 200:
        response_data = response.text
        # print(response_data)
        # json_data = json.dumps(response_data, indent=4)
        return response_data
    else:
        print(f"Failed to fetch data. Status codeL {response.status_code}")
        return None



def save_json(raw_path, hotel_id):
    """Fetch data and save it as a JSON file in the specified path."""

    json_file_path = os.path.join(raw_path, f"{hotel_id}.json")

    json_data = get_supplier_own_raw_data(hotel_id)

    if json_data is None or "Failed to fetch data" in str(json_data):
        print(f"Warning: {hotel_id}.json - Data fetch failed. Saving default empty JSON.")
        json_data = "{}"

    with open(json_file_path, "w", encoding="utf-8") as file:
        file.write(json_data)

    print(f"{hotel_id}.json saved successfully at {json_file_path}")







HOTEL_ID_LIST = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/tbo/tbohotel_hotel_id_list.txt"
TRACKING_FILE = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/tbo/tbohotel_tracking_file.txt"
NOT_FOUND_FILE = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/tbo/tbohotel_not_found.txt"
TOTAL_DONE_FILE = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/tbo/tbohotel_total_done.txt"

def initialize_tracking_file(file_path, hotel_id_list):
    """Initializes the tracking file with all hotel IDs if it doesn't exist."""
    if not os.path.exists(file_path):
        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(map(str, hotel_id_list)) + "\n")
    else:
        print(f"Tracking file already exists: {file_path}")

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
        print(f"Error writing to tracking file: {e}")

def append_to_not_found_file(file_path, hotel_id):
    """Appends the hotel ID to the not found file."""
    try:
        with open(file_path, "a", encoding="utf-8") as file:
            file.write(hotel_id + "\n")
    except Exception as e:
        print(f"Error writing to not found file: {e}")

def append_to_total_done_file(file_path, hotel_id):
    """Appends the hotel ID to the total done file."""
    try:
        with open(file_path, "a", encoding="utf-8") as file:
            file.write(hotel_id + "\n")
    except Exception as e:
        print(f"Error writing to total done file: {e}")

def save_json(base_path, hotel_id):
    """Mock function to simulate saving JSON data."""
    file_path = os.path.join(base_path, f"{hotel_id}.json")
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump({"hotel_id": hotel_id, "status": "done"}, file, indent=4)

def process_hotels():
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

    supplier = "tbo"
    base_path = f"D:/content_for_hotel_json/cdn_row_collection/{supplier}"

    for hotel_id in hotel_ids_to_process:
        try:
            print(f"Processing hotel ID: {hotel_id}")
            save_json(base_path, hotel_id)
            append_to_total_done_file(TOTAL_DONE_FILE, hotel_id)  
        except Exception as e:
            print(f"Error processing hotel ID {hotel_id}: {e}")
            append_to_not_found_file(NOT_FOUND_FILE, hotel_id)
            continue 

        hotel_ids_to_process.remove(hotel_id)
        write_tracking_file(TRACKING_FILE, hotel_ids_to_process)

    print("Processing completed.")

# Run the function
process_hotels()